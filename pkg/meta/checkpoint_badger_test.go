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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger/v4"
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

// A checkpoint must be one point in time even while the store is written
// across its whole keyspace. Every writer transaction sets the same value on
// a key at each end of the keyspace, so a copy assembled from more than one
// read timestamp shows a pair that disagrees. Badger's Stream, which this
// once used, opens a transaction per producer goroutine and does exactly
// that; this fails against it within a few rounds.
func TestCheckpointStoreBadgerIsPointInTime(t *testing.T) {
	tmp := t.TempDir()
	m, err := newKVMeta("badger", filepath.Join(tmp, "src"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	db := m.(*kvMeta).client.(*badgerClient).client
	// Filler across the keyspace, so the copy is split into several ranges.
	wb := db.NewWriteBatch()
	for i := 0; i < 1_000_000; i++ {
		if err := wb.Set([]byte(fmt.Sprintf("m/%09d", i)), make([]byte, 64)); err != nil {
			t.Fatalf("filler: %s", err)
		}
	}
	if err := wb.Flush(); err != nil {
		t.Fatalf("filler: %s", err)
	}

	const pairs = 64
	var n uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			n++
			v := []byte(strconv.FormatUint(n, 10))
			p := n % pairs
			_ = db.Update(func(txn *badger.Txn) error {
				if err := txn.Set([]byte(fmt.Sprintf("a/%02d", p)), v); err != nil {
					return err
				}
				return txn.Set([]byte(fmt.Sprintf("z/%02d", p)), v)
			})
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()

	ctx := Background()
	for round := 0; round < 5; round++ {
		arc := filepath.Join(tmp, fmt.Sprintf("snap-%d.tar", round))
		if err := m.CheckpointStore(ctx, arc); err != nil {
			t.Fatalf("CheckpointStore: %s", err)
		}
		dir := filepath.Join(tmp, fmt.Sprintf("dst-%d", round))
		if err := RestoreStoreArchive(arc, dir); err != nil {
			t.Fatalf("RestoreStoreArchive: %s", err)
		}
		opt := badger.DefaultOptions(dir)
		opt.Logger = nil
		cp, err := badger.Open(opt)
		if err != nil {
			t.Fatalf("open copy: %s", err)
		}
		err = cp.View(func(txn *badger.Txn) error {
			get := func(k string) string {
				item, err := txn.Get([]byte(k))
				if err != nil {
					return "<" + err.Error() + ">"
				}
				v, _ := item.ValueCopy(nil)
				return string(v)
			}
			for p := 0; p < pairs; p++ {
				a, z := get(fmt.Sprintf("a/%02d", p)), get(fmt.Sprintf("z/%02d", p))
				if a != z {
					return fmt.Errorf("round %d: pair %d torn: a=%s z=%s", round, p, a, z)
				}
			}
			return nil
		})
		cp.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
