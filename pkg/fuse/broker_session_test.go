/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fuse

import (
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// fakeBroker accepts sessions and reports each accepted connection.
func fakeBroker(t *testing.T, sock string) (net.Listener, <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	conns := make(chan net.Conn, 8)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns <- c
		}
	}()
	return ln, conns
}

func accepted(t *testing.T, conns <-chan net.Conn, what string) net.Conn {
	t.Helper()
	select {
	case c := <-conns:
		return c
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: the daemon never opened a session", what)
		return nil
	}
}

// closedByPeer waits until the broker's end sees the session end.
func closedByPeer(t *testing.T, c net.Conn, what string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 16)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			t.Fatalf("%s: the session sent %q; it must stay silent", what, buf[:n])
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatalf("%s: the session did not end: %v", what, err)
		}
	}
}

func shortSock(t *testing.T) string {
	// unix socket paths are length-limited; t.TempDir() can be long.
	d, err := os.MkdirTemp("", "bs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return filepath.Join(d, "b.sock")
}

func TestBrokerSessionOffWithoutABroker(t *testing.T) {
	t.Setenv("UVOL_BROKER_SOCKET", "")
	stop := holdBrokerSession()
	stop()
	stop() // idempotent
}

func TestBrokerSessionHeldUntilStopped(t *testing.T) {
	sock := shortSock(t)
	ln, conns := fakeBroker(t, sock)
	defer ln.Close()
	t.Setenv("UVOL_BROKER_SOCKET", sock)
	stop := holdBrokerSession()
	c := accepted(t, conns, "start")
	// Held: nothing arrives and it stays open.
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, err := c.Read(make([]byte, 8)); n != 0 || !os.IsTimeout(err) {
		t.Fatalf("an idle session read returned n=%d err=%v; want a timeout (open and silent)", n, err)
	}
	stop()
	closedByPeer(t, c, "stop")
	select {
	case extra := <-conns:
		t.Fatalf("a stopped session redialed (%v)", extra.LocalAddr())
	case <-time.After(3 * brokerSessionRedialForTest):
	}
}

var brokerSessionRedialForTest = 50 * time.Millisecond

func TestBrokerSessionSurvivesABrokerRestart(t *testing.T) {
	old := brokerSessionRedial
	brokerSessionRedial = brokerSessionRedialForTest
	defer func() { brokerSessionRedial = old }()
	sock := shortSock(t)
	ln, conns := fakeBroker(t, sock)
	t.Setenv("UVOL_BROKER_SOCKET", sock)
	stop := holdBrokerSession()
	defer stop()
	first := accepted(t, conns, "start")
	// The broker restarts: its end closes, the socket file is replaced.
	_ = first.Close()
	_ = ln.Close()
	_ = os.Remove(sock)
	time.Sleep(3 * brokerSessionRedialForTest) // dials fail while it is down
	ln2, conns2 := fakeBroker(t, sock)
	defer ln2.Close()
	accepted(t, conns2, "after the restart")
}

func TestBrokerSessionWaitsForALateBroker(t *testing.T) {
	old := brokerSessionRedial
	brokerSessionRedial = brokerSessionRedialForTest
	defer func() { brokerSessionRedial = old }()
	sock := shortSock(t)
	t.Setenv("UVOL_BROKER_SOCKET", sock)
	stop := holdBrokerSession()
	defer stop()
	time.Sleep(3 * brokerSessionRedialForTest)
	ln, conns := fakeBroker(t, sock)
	defer ln.Close()
	accepted(t, conns, "late broker")
}

// The property the broker relies on: however the daemon dies, its session
// ends. The helper holds a session and is SIGKILLed.
func TestBrokerSessionEndsWhenTheDaemonDies(t *testing.T) {
	if os.Getenv("JFS_BROKER_SESSION_HELPER") == "1" {
		holdBrokerSession()
		select {}
	}
	sock := shortSock(t)
	ln, conns := fakeBroker(t, sock)
	defer ln.Close()
	cmd := exec.Command(os.Args[0], "-test.run=TestBrokerSessionEndsWhenTheDaemonDies")
	cmd.Env = append(os.Environ(), "JFS_BROKER_SESSION_HELPER=1", "UVOL_BROKER_SOCKET="+sock)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := accepted(t, conns, "helper")
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	closedByPeer(t, c, "SIGKILL")
}
