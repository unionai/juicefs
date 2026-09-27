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

package vfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

// gatedStore holds back uploads the test chooses, and records the ones that
// landed.
type gatedStore struct {
	object.ObjectStorage
	// hold returns a channel to wait on before the Put proceeds, or nil.
	hold   atomic.Value // func(key string) <-chan struct{}
	mu     sync.Mutex
	landed map[string]bool
	held   atomic.Int64
}

func newGatedStore() *gatedStore {
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	g := &gatedStore{ObjectStorage: blob, landed: make(map[string]bool)}
	g.setHold(func(string) <-chan struct{} { return nil })
	return g
}

func (g *gatedStore) setHold(f func(key string) <-chan struct{}) { g.hold.Store(f) }

func (g *gatedStore) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	if ch := g.hold.Load().(func(string) <-chan struct{})(key); ch != nil {
		g.held.Add(1)
		<-ch
		g.held.Add(-1)
	}
	err := g.ObjectStorage.Put(ctx, key, in, getters...)
	if err == nil {
		g.mu.Lock()
		g.landed[key] = true
		g.mu.Unlock()
	}
	return err
}

func (g *gatedStore) hasLanded(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.landed[key]
}

var chunkKeyRe = regexp.MustCompile(`(\d+)_\d+_\d+$`)

// sliceOf parses the slice id out of a chunk object key.
func sliceOf(key string) uint64 {
	m := chunkKeyRe.FindStringSubmatch(key)
	if m == nil {
		return 0
	}
	id, _ := strconv.ParseUint(m[1], 10, 64)
	return id
}

// newWritebackVFS is a VFS whose writes stage on a local disk cache and
// upload in the background — the configuration the checkpoint drain exists
// for. metaURI picks the engine: badger has a snapshot pin, sqlite does not.
func newWritebackVFS(t *testing.T, metaURI string, blob object.ObjectStorage) *VFS {
	t.Helper()
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs"
	m := meta.NewClient(metaURI, metaConf)
	format := &meta.Format{Name: "ckpt", UUID: uuid.New().String(), Storage: "mem", BlockSize: 1024, Compression: "none", DirStats: true}
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init: %s", err)
	}
	if _, err := m.Load(true); err != nil {
		t.Fatalf("load: %s", err)
	}
	if err := m.NewSession(true); err != nil {
		t.Fatalf("session: %s", err)
	}
	t.Cleanup(func() { _ = m.CloseSession(); _ = m.Shutdown() })
	cc := chunk.Config{
		BlockSize:              1 << 20,
		CacheDir:               t.TempDir(),
		CacheMode:              0600,
		CacheSize:              1 << 30,
		FreeSpace:              0.01,
		CacheChecksum:          chunk.CsNone,
		CacheScanInterval:      time.Hour,
		MaxUpload:              32,
		MaxDownload:            32,
		MaxRetries:             1,
		PutTimeout:             time.Minute,
		GetTimeout:             time.Minute,
		BufferSize:             64 << 20,
		Writeback:              true,
		WritebackThresholdSize: 4 << 20,
	}
	cc.SelfCheck(format.UUID)
	// SelfCheck appends the UUID; the cache manager falls back to a memory
	// cache (no staging at all) when that directory does not exist.
	if err := os.MkdirAll(cc.CacheDir, 0700); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, cc, registry)
	conf := &Config{Meta: metaConf, Format: *format, Version: "Juicefs", Chunk: &cc, FuseOpts: &FuseOptions{}}
	return NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)
}

func writeFile(t *testing.T, v *VFS, name string, size int) {
	t.Helper()
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	fe, fh, e := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create %s: %s", name, e)
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i*7 + len(name))
	}
	if e = v.Write(ctx, fe.Inode, buf, 0, fh); e != 0 {
		t.Fatalf("write %s: %s", name, e)
	}
	if e = v.Flush(ctx, fe.Inode, fh, 0); e != 0 {
		t.Fatalf("flush %s: %s", name, e)
	}
	v.Release(ctx, fe.Inode, fh)
}

type ckptResult struct {
	st      syscall.Errno
	elapsed time.Duration
}

func startCheckpoint(v *VFS, dst string, timeout time.Duration, remain, pinned *uint64) <-chan ckptResult {
	res := make(chan ckptResult, 1)
	go func() {
		start := time.Now()
		st := v.checkpointAndDrain(meta.Background(), dst, start.Add(timeout), remain, pinned)
		res <- ckptResult{st, time.Since(start)}
	}()
	return res
}

func waitPinned(t *testing.T, pinned *uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for atomic.LoadUint64(pinned) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the snapshot was never pinned")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// gate is a one-shot release that a failing test still fires on cleanup:
// the staging ledger is per process, so blocks a test leaves stuck in
// staging would be counted by the next test's drain.
type gate struct {
	ch   chan struct{}
	once sync.Once
}

func newGate(t *testing.T) *gate {
	g := &gate{ch: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *gate) open() { g.once.Do(func() { close(g.ch) }) }

// Holds back every upload until the returned gate opens.
func holdEarlyUploads(t *testing.T, g *gatedStore) *gate {
	release := newGate(t)
	g.setHold(func(key string) <-chan struct{} {
		if sliceOf(key) != 0 {
			return release.ch
		}
		return nil
	})
	return release
}

func nextSlice(t *testing.T, v *VFS) uint64 {
	t.Helper()
	var id uint64
	if st := v.Meta.NewSlice(meta.Background(), &id); st != 0 {
		t.Fatalf("new slice: %s", st)
	}
	return id
}

// The drain waits for exactly the blocks the snapshot can reference: it
// returns while blocks written AFTER the pin are still stuck in staging, and
// only once every block written before it has landed.
func TestCheckpointDrainDoesNotChaseLaterWrites(t *testing.T) {
	g := newGatedStore()
	v := newWritebackVFS(t, "badger://"+filepath.Join(t.TempDir(), "meta"), g)
	firstEarly := nextSlice(t, v)
	release := holdEarlyUploads(t, g) // hold every upload until we say
	var early []string
	for i := 0; i < 6; i++ {
		writeFile(t, v, fmt.Sprintf("early-%d", i), 256<<10)
	}
	cutoff := nextSlice(t, v) // every slice written from here on is "late"

	var remain, pinned uint64
	res := startCheckpoint(v, filepath.Join(t.TempDir(), "snap.tar"), 30*time.Second, &remain, &pinned)
	waitPinned(t, &pinned)

	// Late writes: their uploads never finish during the checkpoint.
	lateGate := newGate(t)
	g.setHold(func(key string) <-chan struct{} {
		id := sliceOf(key)
		switch {
		case id >= cutoff:
			return lateGate.ch
		case id >= firstEarly:
			return release.ch
		}
		return nil
	})
	for i := 0; i < 8; i++ {
		writeFile(t, v, fmt.Sprintf("late-%d", i), 128<<10)
	}
	select {
	case r := <-res:
		t.Fatalf("checkpoint returned %s before the early blocks were uploaded", r.st)
	case <-time.After(300 * time.Millisecond):
	}
	if atomic.LoadUint64(&remain) == 0 {
		t.Fatalf("remain = 0 while early uploads are held")
	}
	release.open()
	var r ckptResult
	select {
	case r = <-res:
	case <-time.After(20 * time.Second):
		t.Fatalf("checkpoint did not return after the early blocks were released: it is chasing the late writes")
	}
	if r.st != 0 {
		t.Fatalf("checkpoint = %s", r.st)
	}
	if g.held.Load() == 0 {
		t.Fatalf("no late upload was still held when the checkpoint returned; the test proved nothing")
	}
	g.mu.Lock()
	for key := range g.landed {
		if sliceOf(key) >= firstEarly && sliceOf(key) < cutoff {
			early = append(early, key)
		}
	}
	g.mu.Unlock()
	if len(early) < 6 {
		t.Fatalf("only %d early blocks landed, want >= 6: %v", len(early), early)
	}
}

// Every block written before the checkpoint is in object storage when it
// returns, and it does not return before.
func TestCheckpointDrainWaitsForEveryEarlierBlock(t *testing.T) {
	g := newGatedStore()
	v := newWritebackVFS(t, "badger://"+filepath.Join(t.TempDir(), "meta"), g)
	release := holdEarlyUploads(t, g)
	var keys []string
	for i := 0; i < 5; i++ {
		writeFile(t, v, fmt.Sprintf("f-%d", i), 300<<10)
	}
	var remain, pinned uint64
	res := startCheckpoint(v, filepath.Join(t.TempDir(), "snap.tar"), 30*time.Second, &remain, &pinned)
	waitPinned(t, &pinned)
	time.Sleep(700 * time.Millisecond)
	select {
	case r := <-res:
		t.Fatalf("returned %s with uploads held", r.st)
	default:
	}
	held := atomic.LoadUint64(&remain)
	if held == 0 {
		t.Fatalf("remain = 0 with uploads held")
	}
	release.open()
	r := <-res
	if r.st != 0 {
		t.Fatalf("checkpoint = %s", r.st)
	}
	g.mu.Lock()
	for k := range g.landed {
		if sliceOf(k) != 0 {
			keys = append(keys, k)
		}
	}
	g.mu.Unlock()
	if uint64(len(keys)) < held {
		t.Fatalf("%d chunk objects landed, but %d blocks were pending", len(keys), held)
	}
	if n := chunk.PendingStagedThrough(chunk.StagingMark()); n != 0 {
		t.Fatalf("%d blocks still staged after a completed drain with no later writes", n)
	}
}

// With a pinning engine, out-of-band flushes are admitted again at the pin,
// not after the drain.
func TestCheckpointReleasesTheQuiesceAtThePin(t *testing.T) {
	g := newGatedStore()
	v := newWritebackVFS(t, "badger://"+filepath.Join(t.TempDir(), "meta"), g)
	release := holdEarlyUploads(t, g)
	writeFile(t, v, "f", 200<<10)
	var remain, pinned uint64
	res := startCheckpoint(v, filepath.Join(t.TempDir(), "snap.tar"), 30*time.Second, &remain, &pinned)
	waitPinned(t, &pinned)

	admitted := make(chan struct{})
	go func() {
		v.BeginExternalFlush()
		close(admitted)
		v.EndExternalFlush()
	}()
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatalf("an external flush was still blocked after the pin, during the drain")
	}
	select {
	case r := <-res:
		t.Fatalf("drain finished (%s) before the held uploads were released", r.st)
	default:
	}
	release.open()
	if r := <-res; r.st != 0 {
		t.Fatalf("checkpoint = %s", r.st)
	}
}

// An engine without a pin (sqlite) still gets a correct drain, marked when
// the snapshot call returns.
func TestCheckpointWithoutAPinStillDrains(t *testing.T) {
	g := newGatedStore()
	v := newWritebackVFS(t, "sqlite3://"+filepath.Join(t.TempDir(), "meta.db"), g)
	if _, ok := v.Meta.(meta.PinnedCheckpointer); ok {
		t.Fatalf("sqlite is expected to have no pin; pick another engine for this test")
	}
	release := holdEarlyUploads(t, g)
	writeFile(t, v, "f", 200<<10)
	var remain, pinned uint64
	res := startCheckpoint(v, filepath.Join(t.TempDir(), "snap.db"), 30*time.Second, &remain, &pinned)
	waitPinned(t, &pinned) // set once the snapshot call returns
	time.Sleep(600 * time.Millisecond)
	select {
	case r := <-res:
		t.Fatalf("returned %s with uploads held", r.st)
	default:
	}
	release.open()
	if r := <-res; r.st != 0 {
		t.Fatalf("checkpoint = %s", r.st)
	}
}

// A drain that cannot finish times out, and leaves external flushes
// admitted rather than wedged.
func TestCheckpointDrainTimesOutAndReleasesTheQuiesce(t *testing.T) {
	g := newGatedStore()
	v := newWritebackVFS(t, "badger://"+filepath.Join(t.TempDir(), "meta"), g)
	holdEarlyUploads(t, g) // never opened during the test; cleanup opens it
	writeFile(t, v, "f", 200<<10)
	var remain, pinned uint64
	r := <-startCheckpoint(v, filepath.Join(t.TempDir(), "snap.tar"), 2*time.Second, &remain, &pinned)
	if r.st != syscall.ETIMEDOUT {
		t.Fatalf("checkpoint = %s, want ETIMEDOUT", r.st)
	}
	if atomic.LoadUint64(&remain) == 0 {
		t.Fatalf("timed out with remain = 0")
	}
	done := make(chan struct{})
	go func() { v.BeginExternalFlush(); v.EndExternalFlush(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("external flushes stayed quiesced after a timed-out checkpoint")
	}
}

// Nothing staged: the checkpoint returns as soon as the snapshot is written.
func TestCheckpointWithNothingStagedReturnsPromptly(t *testing.T) {
	g := newGatedStore()
	v := newWritebackVFS(t, "badger://"+filepath.Join(t.TempDir(), "meta"), g)
	writeFile(t, v, "f", 64<<10)
	// Let the upload land before checkpointing.
	deadline := time.Now().Add(10 * time.Second)
	for chunk.PendingStagedThrough(chunk.StagingMark()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("upload never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	var remain, pinned uint64
	r := <-startCheckpoint(v, filepath.Join(t.TempDir(), "snap.tar"), 30*time.Second, &remain, &pinned)
	if r.st != 0 || atomic.LoadUint64(&pinned) != 1 {
		t.Fatalf("checkpoint = %s pinned=%d", r.st, pinned)
	}
	if r.elapsed > 5*time.Second {
		t.Fatalf("took %s with nothing to drain", r.elapsed)
	}
}

// On the wire: while the drain is still waiting, progress frames already
// report the pin, so the caller (juicefs checkpoint -> "snapshot pinned")
// can release its writers before the command returns.
func TestCheckpointVerbReportsThePinBeforeTheDrainEnds(t *testing.T) {
	g := newGatedStore()
	v := newWritebackVFS(t, "badger://"+filepath.Join(t.TempDir(), "meta"), g)
	release := holdEarlyUploads(t, g)
	writeFile(t, v, "f", 200<<10)
	go func() { time.Sleep(1500 * time.Millisecond); release.open() }()

	dst := filepath.Join(t.TempDir(), "snap.tar")
	payload := utils.NewBuffer(8 + uint32(len(dst)))
	payload.Put32(30) // drain timeout, seconds
	payload.Put32(uint32(len(dst)))
	payload.Put([]byte(dst))
	out := &bytes.Buffer{}
	v.handleInternalMsg(meta.NewContext(10, 0, []uint32{0}), meta.Checkpoint, utils.FromBuffer(payload.Bytes()), out)

	b := out.Bytes()
	if len(b) == 0 || b[len(b)-1] != 0 {
		t.Fatalf("status byte = %v, want 0 (%d bytes of reply)", b[len(b)-1:], len(b))
	}
	var pinnedWhileDraining, frames int
	for off := 0; off+17 <= len(b)-1; off += 17 {
		if b[off] != meta.CPROGRESS {
			t.Fatalf("frame at %d starts with %d, want CPROGRESS", off, b[off])
		}
		frames++
		remain := binary.BigEndian.Uint64(b[off+1 : off+9])
		pinned := binary.BigEndian.Uint64(b[off+9 : off+17])
		if pinned == 1 && remain > 0 {
			pinnedWhileDraining++
		}
	}
	if pinnedWhileDraining == 0 {
		t.Fatalf("no progress frame reported the pin while blocks were still draining (%d frames)", frames)
	}
}
