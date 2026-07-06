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
	"os"
	"path/filepath"
	"syscall"
	"testing"
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
