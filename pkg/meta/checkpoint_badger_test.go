//go:build !nobadger
// +build !nobadger

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
	"os"
	"path/filepath"
	"testing"
)

// Artifacts published before the archive format are Badger backup streams,
// and a volume whose last commit predates the change still has to mount.
func TestRestoreStoreBadgerLegacyStream(t *testing.T) {
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

	// Write the old artifact shape directly: a Badger backup stream.
	bak := filepath.Join(tmp, "legacy.bak")
	f, err := os.Create(bak)
	if err != nil {
		t.Fatalf("create legacy artifact: %s", err)
	}
	db := m.(*kvMeta).client.(*badgerClient).client
	if _, err := db.Backup(f, 0); err != nil {
		f.Close()
		t.Fatalf("badger backup: %s", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close legacy artifact: %s", err)
	}
	if archive, err := IsStoreArchive(bak); err != nil || archive {
		t.Fatalf("legacy stream must not sniff as an archive (archive=%v err=%v)", archive, err)
	}

	m2, err := newKVMeta("badger", filepath.Join(tmp, "dst"), testConfig())
	if err != nil {
		t.Fatalf("create dst meta: %s", err)
	}
	if err := m2.RestoreStore(ctx, bak); err != nil {
		t.Fatalf("RestoreStore(legacy stream): %s", err)
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
}
