//go:build !nonfsmount
// +build !nonfsmount

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

package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/meta"
	jfsnfs "github.com/juicedata/juicefs/pkg/nfs"
)

// The build tag is nonfsmount, not nonfs: nonfs already excludes the NFS
// *object storage* backend in pkg/object, which is unrelated — that reads from
// someone else's NFS server, this one is a server.

func cmdNFSMount() *cli.Command {
	selfFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "address",
			Value: "127.0.0.1:0",
			Usage: "address for the NFS server; port 0 picks a free one. Keep this on loopback — there is no authentication",
		},
		&cli.BoolFlag{
			Name:  "serve-only",
			Usage: "start the server and print its port, but do not run the mount command",
		},
		&cli.IntFlag{
			Name:  "attr-timeout-ms",
			Value: 1000,
			Usage: "how long the client may cache attributes (actimeo). NFSv3 cannot invalidate a client's cache, so this bounds staleness",
		},
		&cli.StringFlag{
			Name:  "log",
			Usage: "path for the NFS server log",
			Value: path.Join(getDefaultLogDir(), "juicefs-nfsmount.log"), //nolint:typecheck
		},
		&cli.StringFlag{
			Name:  "access-log",
			Usage: "path for JuiceFS access log",
		},
		&cli.BoolFlag{
			Name:    "background",
			Aliases: []string{"d"},
			Usage:   "run in background",
		},
	}

	return &cli.Command{
		Name:      "nfsmount",
		Action:    nfsmount,
		Category:  "SERVICE",
		Usage:     "Mount a volume via a local NFS server, without FUSE",
		ArgsUsage: "META-URL MOUNTPOINT",
		Description: `
Serves the volume over NFSv3 on loopback and mounts it with the operating
system's own NFS client, so no FUSE driver is involved.

This exists for macOS, where mounting otherwise needs macFUSE — a kernel
extension whose load requires an interactive approval, which rules out
unattended installs. mount_nfs against 127.0.0.1 needs neither root nor a kext.

On Linux the kernel mount still requires CAP_SYS_ADMIN, so prefer "juicefs
mount" there; this is for the cases where /dev/fuse is unavailable but the
mount syscall is not.

Examples:
$ juicefs nfsmount badger:///var/jfs/meta /Users/me/data

Serve without mounting, to run the mount command yourself:
$ juicefs nfsmount --serve-only badger:///var/jfs/meta /Users/me/data`,
		Flags: expandFlags(selfFlags, clientFlags(0), shareInfoFlags()),
	}
}

func nfsmount(c *cli.Context) error {
	setup(c, 2)
	metaUrl := c.Args().Get(0)
	mountPoint := c.Args().Get(1)
	addr := c.String("address")

	_, jfs := initForSvc(c, mountPoint, "nfsmount", metaUrl, addr)

	readOnly := c.Bool("read-only")
	srv, err := jfsnfs.NewServer(addr, jfs, jfs.Meta(), meta.Background(), readOnly)
	if err != nil {
		logger.Fatalf("start NFS server on %s: %s", addr, err)
	}

	// Serving must be underway before the mount command runs: mount_nfs and
	// mount.nfs connect immediately, and a socket nobody is accepting on gives
	// them EIO rather than a retry.
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	port := srv.Port()
	logger.Infof("NFS server listening on 127.0.0.1:%d", port)

	mounted := false
	var closing atomic.Bool
	var once sync.Once
	// Unmount first, then close the server: the kernel client must stop sending
	// RPCs before the handles it is talking to go away. srv.Close is what
	// flushes the pooled handles, so skipping it loses the tail of a write.
	reportAndClose := func() {
		once.Do(func() {
			closing.Store(true)
			if mounted {
				if err := runUnmount(mountPoint); err != nil {
					logger.Warnf("unmount %s: %s", mountPoint, err)
				}
			}
			opened, reused, hits, misses := srv.Stats()
			logger.Infof("handles: %d opened, %d reused; path cache: %d hits, %d misses",
				opened, reused, hits, misses)
			_ = srv.Close()
		})
	}
	onSvcShutdown(reportAndClose)

	if c.Bool("serve-only") {
		fmt.Printf("%d\n", port)
	} else {
		if err := runMount(mountPoint, port, c.Int("attr-timeout-ms"), readOnly); err != nil {
			reportAndClose()
			logger.Fatalf("mount %s: %s", mountPoint, err)
		}
		mounted = true
		logger.Infof("mounted at %s", mountPoint)
	}

	// Block until the server stops. Signals are NOT handled here: initForSvc
	// has already installed a SIGINT/SIGTERM handler that exits via os.Exit,
	// so a second handler would race it and lose, and a deferred unmount would
	// never run at all — os.Exit skips defers. Teardown is registered with that
	// handler instead.
	if err := <-serveErr; err != nil && !closing.Load() {
		// Closing the listener makes Serve return "use of closed network
		// connection". That is the shutdown working, not a fault, and logging
		// it at ERROR would train people to ignore the line that matters.
		logger.Errorf("NFS server stopped: %s", err)
	}
	reportAndClose()

	return jfs.Meta().CloseSession()
}

// mountOpts are shared by both platforms. rsize/wsize at 1 MiB match what
// hf-mount uses and what the server is willing to serve in one reply.
func mountOpts(port, attrTimeoutMs int, readOnly bool, lockOpt string) string {
	// actimeo is in seconds and rounds up: 100ms must not become 0, which
	// would disable client attribute caching altogether and make every stat an
	// RPC.
	actimeo := (attrTimeoutMs + 999) / 1000
	opts := fmt.Sprintf("%s,vers=3,tcp,rsize=1048576,wsize=1048576,actimeo=%d,port=%d,mountport=%d",
		lockOpt, actimeo, port, port)
	if readOnly {
		opts = "ro," + opts
	}

	return opts
}

func runMount(mountPoint string, port, attrTimeoutMs int, readOnly bool) error {
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}

	switch runtime.GOOS {
	case "darwin":
		// locallocks, not nolocks. macOS reads nolocks as "advisory locking is
		// unsupported" and returns ENOTSUP from flock and fcntl, which breaks
		// Python's filelock and anything built on it. locallocks keeps lock
		// handling inside the client kernel; this server implements no NLM, so
		// client-side locking is the only option in any case.
		//
		// No resvport: asking for a reserved source port is the one thing here
		// that would require root, and the whole point is not needing it.
		cmd := exec.Command("mount_nfs", "-o", mountOpts(port, attrTimeoutMs, readOnly, "locallocks"),
			"127.0.0.1:/", mountPoint)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr

		return cmd.Run()
	case "linux":
		bin, err := exec.LookPath("mount.nfs")
		if err != nil {
			// mount.nfs lives in /sbin or /usr/sbin, which are often off PATH
			// for a non-login process.
			for _, p := range []string{"/sbin/mount.nfs", "/usr/sbin/mount.nfs"} {
				if _, statErr := os.Stat(p); statErr == nil {
					bin, err = p, nil

					break
				}
			}
		}
		if err != nil {
			return fmt.Errorf("mount.nfs not found; install nfs-common (Debian/Ubuntu) or nfs-utils (RHEL/Fedora)")
		}
		args := []string{"-o", mountOpts(port, attrTimeoutMs, readOnly, "nolock"), "127.0.0.1:/", mountPoint}
		var cmd *exec.Cmd
		if os.Geteuid() == 0 {
			cmd = exec.Command(bin, args...)
		} else {
			// The mount syscall needs CAP_SYS_ADMIN on Linux however the
			// filesystem is served, so NFS buys nothing over FUSE here.
			cmd = exec.Command("sudo", append([]string{"-n", bin}, args...)...)
		}
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("%w (an NFS mount needs CAP_SYS_ADMIN; use `juicefs mount` if you have /dev/fuse instead)", err)
		}

		return nil
	default:
		return fmt.Errorf("nfsmount is not supported on %s; use --serve-only and mount it yourself", runtime.GOOS)
	}
}

func runUnmount(mountPoint string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("umount", mountPoint)
	default:
		if os.Geteuid() == 0 {
			cmd = exec.Command("umount", mountPoint)
		} else {
			cmd = exec.Command("sudo", "-n", "umount", mountPoint)
		}
	}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	// A mount with I/O still in flight can refuse to go; one retry covers the
	// common case without turning shutdown into an indefinite wait.
	if err := cmd.Run(); err != nil {
		time.Sleep(time.Second)

		return cmd.Run()
	}

	return nil
}
