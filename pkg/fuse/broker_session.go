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
	"net"
	"os"
	"sync"
	"time"
)

// brokerSessionRedial is how long holdBrokerSession waits between attempts
// to (re)connect to the broker.
var brokerSessionRedial = 2 * time.Second

// holdBrokerSession keeps a connection to the uvol mount broker
// ($UVOL_BROKER_SOCKET) open for as long as this daemon serves, and returns
// a function that closes it.
//
// The broker frees a channel whose daemon died: requests queued on the FUSE
// connection, unchanged for minutes, and no client session on the volume.
// A session is any open connection to its socket -- which go-fuse opens only
// on the first passthrough backing registration. A daemon that never
// registers one (every read-only workload) therefore looked dead to the
// broker whenever its queue held steady, and a sequential reader holds it at
// exactly one request for as long as it reads: the broker aborted such
// mounts, healthy and mid-read, five minutes in (reads then fail with
// ENOTCONN). Holding a session from the start makes "no session" mean what
// the broker assumes it means: the process is gone -- the kernel closes this
// connection when it exits, however it exits.
//
// It sends nothing; the broker serves an idle session without cost. If the
// broker restarts, the connection is re-established.
func holdBrokerSession() (stop func()) {
	sock := os.Getenv("UVOL_BROKER_SOCKET")
	if sock == "" {
		return func() {}
	}
	done := make(chan struct{})
	var mu sync.Mutex
	var conn net.Conn
	go func() {
		for {
			c, err := net.Dial("unix", sock)
			if err == nil {
				mu.Lock()
				select {
				case <-done:
					mu.Unlock()
					_ = c.Close()
					return
				default:
				}
				conn = c
				mu.Unlock()
				logger.Infof("holding a liveness session with the mount broker at %s", sock)
				// Returns when the broker closes the connection (restart) or
				// stop closes it.
				buf := make([]byte, 64)
				for {
					if _, err := c.Read(buf); err != nil {
						break
					}
				}
				mu.Lock()
				conn = nil
				mu.Unlock()
				_ = c.Close()
			} else {
				logger.Debugf("mount broker session: dial %s: %s", sock, err)
			}
			select {
			case <-done:
				return
			case <-time.After(brokerSessionRedial):
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			mu.Lock()
			close(done)
			if conn != nil {
				_ = conn.Close()
			}
			mu.Unlock()
		})
	}
}
