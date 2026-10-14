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

// Shared plumbing for the long-running services (gateway, webdav, nfsmount).
//
// initForSvc used to live in gateway.go, which carries a !nogateway build tag.
// All three services call it, but gateway.go and webdav.go are each replaced by
// a _noop.go under their own tag, so with both nogateway and nowebdav set — as
// the Union release build does — the function was neither defined nor needed
// and nothing noticed. Adding a third caller broke that build and only that
// build:
//
//	cmd/nfsmount.go:107:12: undefined: initForSvc
//
// Nothing here is gateway-specific, so it belongs in a file with no tag.

package cmd

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

var (
	svcCleanupMu sync.Mutex
	svcCleanups  []func()
)

// onSvcShutdown registers work to run on SIGINT/SIGTERM before the metadata
// session is closed and the process exits. Needed because the signal handler
// installed below exits via os.Exit, which runs no defers.
func onSvcShutdown(f func()) {
	svcCleanupMu.Lock()
	defer svcCleanupMu.Unlock()
	svcCleanups = append(svcCleanups, f)
}

// runSvcCleanups runs them most-recently-registered first, so teardown unwinds
// in the opposite order to setup.
func runSvcCleanups() {
	svcCleanupMu.Lock()
	fns := svcCleanups
	svcCleanups = nil
	svcCleanupMu.Unlock()
	for i := len(fns) - 1; i >= 0; i-- {
		fns[i]()
	}
}

func initForSvc(c *cli.Context, mp string, svcType, metaUrl, listenAddr string) (*vfs.Config, *fs.FileSystem) {
	removePassword(metaUrl)
	metaConf := getMetaConf(c, mp, c.Bool("read-only"))
	metaCli := meta.NewClient(metaUrl, metaConf)
	if c.Bool("background") {
		if err := makeDaemonForSvc(c, metaCli, metaUrl, listenAddr); err != nil {
			logger.Fatalf("make daemon: %s", err)
		}
	}

	format, err := metaCli.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}
	if st := metaCli.Chroot(meta.Background(), metaConf.Subdir); st != 0 {
		logger.Fatalf("Chroot to %q: %s", metaConf.Subdir, st)
	}
	registerer, registry := wrapRegister(c, svcType, format.Name)

	blob, err := NewReloadableStorage(format, metaCli, updateFormat(c))
	if err != nil {
		logger.Fatalf("object storage: %s", err)
	}
	logger.Infof("Data use %s", blob)

	chunkConf := getChunkConf(c, format)
	store := chunk.NewCachedStore(blob, *chunkConf, registerer)
	registerMetaMsg(metaCli, store, chunkConf)

	err = metaCli.NewSession(true)
	if err != nil {
		logger.Fatalf("new session: %s", err)
	}
	metaCli.OnReload(func(fmt *meta.Format) {
		updateFormat(c)(fmt)
		store.UpdateLimit(fmt.UploadLimit, fmt.DownloadLimit)
	})

	// Go will catch all the signals
	signal.Ignore(syscall.SIGPIPE)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		sig := <-signalChan
		logger.Infof("Received signal %s, exiting...", sig.String())
		// Before the session goes and the process exits: this handler calls
		// os.Exit, which runs no deferred functions, so a service with real
		// teardown has no other chance to do it. juicefs nfsmount has to
		// unmount and flush its pooled handles here or the tail of a write is
		// lost on Ctrl-C.
		runSvcCleanups()
		if err := metaCli.CloseSession(); err != nil {
			logger.Fatalf("close session failed: %s", err)
		}
		object.Shutdown(blob)
		os.Exit(0)
	}()
	vfsConf := getVfsConf(c, metaConf, format, chunkConf)
	vfsConf.AccessLog = c.String("access-log")
	vfsConf.AttrTimeout = utils.Duration(c.String("attr-cache"))
	vfsConf.EntryTimeout = utils.Duration(c.String("entry-cache"))
	vfsConf.DirEntryTimeout = utils.Duration(c.String("dir-entry-cache"))
	vfsConf.Mountpoint = mp

	initBackgroundTasks(c, vfsConf, metaConf, metaCli, blob, registerer, registry)
	jfs, err := fs.NewFileSystem(vfsConf, metaCli, store, registry)
	if err != nil {
		logger.Fatalf("Initialize failed: %s", err)
	}
	jfs.InitMetrics(registerer)

	return vfsConf, jfs
}
