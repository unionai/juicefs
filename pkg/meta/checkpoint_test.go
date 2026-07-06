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

package meta

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// SQLite: CheckpointStore must produce a consistent, openable copy of the
// live database via VACUUM INTO, including uncheckpointed WAL content.
func TestCheckpointStoreSQLite(t *testing.T) {
	tmp := t.TempDir()
	m, err := newSQLMeta("sqlite3", filepath.Join(tmp, "live.db"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	if err = m.Init(testFormat(), true); err != nil {
		t.Fatalf("init: %s", err)
	}
	ctx := Background()
	var inode Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "f1", 0644, 022, 0, &inode, &attr); st != 0 {
		t.Fatalf("create file: %s", st)
	}

	dst := filepath.Join(tmp, "snap.db")
	if err := m.CheckpointStore(ctx, dst); err != nil {
		t.Fatalf("CheckpointStore: %s", err)
	}
	// Snapshot must be independently openable and contain the file.
	db, err := sql.Open("sqlite3", dst)
	if err != nil {
		t.Fatalf("open snapshot: %s", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("select count(*) from jfs_edge where name = x'6631'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("snapshot missing created file: n=%d err=%s", n, err)
	}
	// Overwrite semantics: a second checkpoint over the same dst succeeds.
	if err := m.CheckpointStore(ctx, dst); err != nil {
		t.Fatalf("CheckpointStore over existing dst: %s", err)
	}
}

// A failed checkpoint write must not destroy a prior good snapshot living
// at dst — regression test for the old os.Remove(dst)-then-VACUUM-INTO
// sequence, which deleted dst unconditionally up front, before even
// attempting the new write, so ANY failure of that write (disk full, crash)
// left dst missing instead of holding the last good snapshot. Force a
// deterministic, root-safe write failure with a size-capped tmpfs (a
// realistic stand-in for "disk full" mid-VACUUM-INTO) rather than
// permissions, which root bypasses.
func TestCheckpointStoreSQLiteFailedWritePreservesDst(t *testing.T) {
	tmp := t.TempDir()
	dstDir := filepath.Join(tmp, "dst")
	if err := os.Mkdir(dstDir, 0755); err != nil {
		t.Fatalf("mkdir: %s", err)
	}
	if err := exec.Command("mount", "-t", "tmpfs", "-o", "size=256k", "tmpfs", dstDir).Run(); err != nil {
		t.Skipf("cannot mount a size-capped tmpfs in this environment: %s", err)
	}
	defer exec.Command("umount", dstDir).Run()

	m, err := newSQLMeta("sqlite3", filepath.Join(tmp, "live.db"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	if err = m.Init(testFormat(), true); err != nil {
		t.Fatalf("init: %s", err)
	}
	ctx := Background()
	for i := 0; i < 20; i++ {
		var inode Ino
		var attr Attr
		if st := m.Create(ctx, RootInode, fmt.Sprintf("f%d", i), 0644, 022, 0, &inode, &attr); st != 0 {
			t.Fatalf("create file: %s", st)
		}
	}

	dst := filepath.Join(dstDir, "snap.db")
	if err := m.CheckpointStore(ctx, dst); err != nil {
		t.Skipf("first checkpoint didn't fit the capped tmpfs at all: %s", err)
	}
	want, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read first snapshot: %s", err)
	}

	// Fill the rest of the tmpfs so the next checkpoint's temp-file write
	// fails partway through with ENOSPC.
	filler := filepath.Join(dstDir, "filler")
	_ = exec.Command("dd", "if=/dev/zero", "of="+filler, "bs=1k", "count=256").Run()
	defer os.Remove(filler)

	for i := 0; i < 200; i++ {
		var inode Ino
		var attr Attr
		if st := m.Create(ctx, RootInode, fmt.Sprintf("g%d", i), 0644, 022, 0, &inode, &attr); st != 0 {
			t.Fatalf("create file: %s", st)
		}
	}
	if err := m.CheckpointStore(ctx, dst); err == nil {
		_ = os.Remove(filler)
		t.Skipf("second checkpoint unexpectedly fit the capped tmpfs; can't exercise the write-failure path here")
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("dst missing after failed checkpoint: %s", err)
	}
	if string(got) != string(want) {
		t.Fatalf("failed checkpoint destroyed the prior good snapshot at dst")
	}
	entries, err := os.ReadDir(dstDir)
	if err != nil {
		t.Fatalf("readdir: %s", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("failed checkpoint leaked a temp file: %s", e.Name())
		}
	}
}

// Badger: CheckpointStore streams a non-empty backup of the live store.
func TestCheckpointStoreBadger(t *testing.T) {
	tmp := t.TempDir()
	m, err := newKVMeta("badger", filepath.Join(tmp, "meta"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	if err = m.Init(testFormat(), true); err != nil {
		t.Fatalf("init: %s", err)
	}
	ctx := Background()
	dst := filepath.Join(tmp, "snap.bak")
	if err := m.CheckpointStore(ctx, dst); err != nil {
		t.Fatalf("CheckpointStore: %s", err)
	}
	fi, err := os.Stat(dst)
	if err != nil || fi.Size() == 0 {
		t.Fatalf("backup empty or missing: %v %v", fi, err)
	}
}

// A checkpoint must not silently piggyback on a foreign BGSAVE that was
// already running when it was requested — BGSAVE forks its point-in-time
// snapshot at start, not completion, so trusting an in-flight foreign save
// could return a snapshot missing writes made between that save's start and
// this checkpoint's request. Use Redis's rdb-key-save-delay debug knob to
// make a save slow enough to reliably observe: a checkpoint racing a
// foreign save must wait it out AND run (and wait for) its own, roughly
// doubling the time a single save takes — regression test for the old
// code, which treated a "BGSAVE already in progress" error as good enough
// and only waited for that (possibly stale) save.
func TestCheckpointStoreRedisWaitsOutForeignBgSave(t *testing.T) {
	m, err := newRedisMeta("redis", "127.0.0.1:6379/11", testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	rm := m.(*redisMeta)
	if err := rm.rdb.FlushDB(Background()).Err(); err != nil {
		t.Fatalf("flushdb: %s", err)
	}
	if err = m.Init(testFormat(), true); err != nil {
		t.Fatalf("init: %s", err)
	}
	ctx := Background()
	for i := 0; i < 200; i++ {
		if err := rm.rdb.Set(ctx, "ckpt-test-key-"+strconv.Itoa(i), "v", 0).Err(); err != nil {
			t.Fatalf("seed key: %s", err)
		}
	}
	const delayUS = "3000" // 3ms/key * 200 keys ~= 600ms per save
	if err := rm.rdb.ConfigSet(ctx, "rdb-key-save-delay", delayUS).Err(); err != nil {
		t.Skipf("rdb-key-save-delay not supported by this redis build: %s", err)
	}
	defer rm.rdb.ConfigSet(ctx, "rdb-key-save-delay", "0")

	dst := filepath.Join(t.TempDir(), "snap.rdb")

	baseStart := time.Now()
	if err := m.CheckpointStore(ctx, dst); err != nil {
		t.Fatalf("baseline CheckpointStore: %s", err)
	}
	singleSave := time.Since(baseStart)

	// Trigger a foreign BGSAVE directly, then immediately request a
	// checkpoint — it must not just ride along with the foreign save.
	if err := rm.rdb.Do(ctx, "BGSAVE").Err(); err != nil {
		t.Fatalf("trigger foreign bgsave: %s", err)
	}
	racedStart := time.Now()
	if err := m.CheckpointStore(ctx, dst); err != nil {
		t.Fatalf("raced CheckpointStore: %s", err)
	}
	racedDuration := time.Since(racedStart)

	if racedDuration < singleSave*3/2 {
		t.Fatalf("raced checkpoint (%s) did not wait out the foreign save and run its own "+
			"(single save took %s) — it likely piggybacked on the foreign save's stale result",
			racedDuration, singleSave)
	}
}

// Badger: a checkpoint restored into a fresh store must reproduce the
// filesystem and remain writable (fork-from-commit round trip).
func TestRestoreStoreBadger(t *testing.T) {
	tmp := t.TempDir()
	m, err := newKVMeta("badger", filepath.Join(tmp, "src"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	if err = m.Init(testFormat(), true); err != nil {
		t.Fatalf("init: %s", err)
	}
	ctx := Background()
	var inode Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "f1", 0644, 022, 0, &inode, &attr); st != 0 {
		t.Fatalf("create file: %s", st)
	}
	bak := filepath.Join(tmp, "snap.bak")
	if err := m.CheckpointStore(ctx, bak); err != nil {
		t.Fatalf("CheckpointStore: %s", err)
	}

	m2, err := newKVMeta("badger", filepath.Join(tmp, "dst"), testConfig())
	if err != nil {
		t.Fatalf("create dst meta: %s", err)
	}
	if err := m2.RestoreStore(ctx, bak); err != nil {
		t.Fatalf("RestoreStore: %s", err)
	}
	if _, err := m2.Load(true); err != nil {
		t.Fatalf("load restored format: %s", err)
	}
	var inode2 Ino
	if st := m2.Lookup(ctx, RootInode, "f1", &inode2, &attr, false); st != 0 {
		t.Fatalf("lookup f1 in restored store: %s", st)
	}
	if inode2 != inode {
		t.Fatalf("restored inode mismatch: %d != %d", inode2, inode)
	}
	// The restored store must accept new writes (fork continues history).
	if st := m2.Create(ctx, RootInode, "f2", 0644, 022, 0, &inode2, &attr); st != 0 {
		t.Fatalf("create in restored store: %s", st)
	}

	// Never restore into a store that already holds data.
	if err := m2.RestoreStore(ctx, bak); err == nil {
		t.Fatalf("RestoreStore into non-empty store must fail")
	}
}

// Engines without a local snapshot mechanism must refuse, not misbehave.
func TestCheckpointStoreUnsupported(t *testing.T) {
	_ = os.Remove(settingPath)
	m, err := newKVMeta("memkv", "jfs-ckpt-unsup-test", testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	if err = m.Init(testFormat(), true); err != nil {
		t.Fatalf("init: %s", err)
	}
	if err := m.CheckpointStore(Background(), "/tmp/never"); err != syscall.ENOTSUP {
		t.Fatalf("memkv CheckpointStore: got %v, want ENOTSUP", err)
	}
	if err := m.RestoreStore(Background(), "/tmp/never"); err != syscall.ENOTSUP {
		t.Fatalf("memkv RestoreStore: got %v, want ENOTSUP", err)
	}
}
