//go:build !nobadger

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
	"time"
)

// Both artifact shapes for the same store, side by side. This is where the
// numbers in the checkpoint comments come from, and it is how to re-measure
// them after touching either side.
//
// Opt-in — it builds a store of N files, which takes minutes and gigabytes:
//
//	JFS_SCALE_FILES=1000000 JFS_SCALE_DIR=/tmp/scale \
//	  go test ./pkg/meta -run ScaleCheckpoint -v -timeout 60m
func TestScaleCheckpointBadger(t *testing.T) {
	nStr := os.Getenv("JFS_SCALE_FILES")
	if nStr == "" {
		t.Skip("set JFS_SCALE_FILES to run (see the comment above)")
	}
	n, _ := strconv.Atoi(nStr)
	tmp := os.Getenv("JFS_SCALE_DIR")
	if tmp == "" {
		tmp = t.TempDir()
	} else {
		os.MkdirAll(tmp, 0755)
	}
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
	var attr Attr
	dirs := make([]Ino, 100)
	for i := range dirs {
		if st := m.Mkdir(ctx, RootInode, "d"+strconv.Itoa(i), 0755, 022, 0, &dirs[i], &attr); st != 0 {
			t.Fatalf("mkdir: %s", st)
		}
	}
	start := time.Now()
	for i := 0; i < n; i++ {
		var ino Ino
		if st := m.Create(ctx, dirs[i%100], "f"+strconv.Itoa(i), 0644, 022, 0, &ino, &attr); st != 0 {
			t.Fatalf("create %d: %s", i, st)
		}
		// Give each file a slice so the index carries chunk metadata, which
		// is the bulk of a real volume's store.
		var sliceID uint64
		if st := m.NewSlice(ctx, &sliceID); st != 0 {
			t.Fatalf("newslice: %s", st)
		}
		if st := m.Write(ctx, ino, 0, 0, Slice{Id: sliceID, Size: 4096, Len: 4096}, time.Now()); st != 0 {
			t.Fatalf("write: %s", st)
		}
	}
	t.Logf("created %d files in %s", n, time.Since(start).Round(time.Millisecond))
	srcSize, srcFiles := dirSize(t, filepath.Join(tmp, "src"))
	t.Logf("live store directory: %.1f MB in %d files", float64(srcSize)/1e6, srcFiles)

	// --- new: store archive -------------------------------------------------
	arc := filepath.Join(tmp, "snap.tar")
	start = time.Now()
	if err := m.CheckpointStore(ctx, arc); err != nil {
		t.Fatalf("CheckpointStore: %s", err)
	}
	archiveWrite := time.Since(start)
	ai, _ := os.Stat(arc)

	start = time.Now()
	if err := RestoreStoreArchive(arc, filepath.Join(tmp, "dst-arc")); err != nil {
		t.Fatalf("RestoreStoreArchive: %s", err)
	}
	m2, err := newKVMeta("badger", filepath.Join(tmp, "dst-arc"), testConfig())
	if err != nil {
		t.Fatalf("open restored: %s", err)
	}
	if _, err := m2.Load(true); err != nil {
		t.Fatalf("load restored format: %s", err)
	}
	archiveRead := time.Since(start)

	// --- old: backup stream -------------------------------------------------
	stream := filepath.Join(tmp, "snap.bak")
	f, err := os.Create(stream)
	if err != nil {
		t.Fatalf("create: %s", err)
	}
	start = time.Now()
	db := m.(*kvMeta).client.(*badgerClient).client
	if _, err := db.Backup(f, 0); err != nil {
		t.Fatalf("backup: %s", err)
	}
	f.Close()
	streamWrite := time.Since(start)
	si, _ := os.Stat(stream)

	m3, err := newKVMeta("badger", filepath.Join(tmp, "dst-stream"), testConfig())
	if err != nil {
		t.Fatalf("create dst: %s", err)
	}
	start = time.Now()
	if err := m3.RestoreStore(ctx, stream); err != nil {
		t.Fatalf("RestoreStore: %s", err)
	}
	if _, err := m3.Load(true); err != nil {
		t.Fatalf("load restored format: %s", err)
	}
	streamRead := time.Since(start)

	fmt.Printf("\n=== %d files ===\n", n)
	fmt.Printf("%-22s %10s %10s %12s\n", "artifact", "produce", "restore", "size")
	fmt.Printf("%-22s %10s %10s %11.1fMB\n", "backup stream (old)", streamWrite.Round(time.Millisecond), streamRead.Round(time.Millisecond), float64(si.Size())/1e6)
	fmt.Printf("%-22s %10s %10s %11.1fMB\n", "store archive (new)", archiveWrite.Round(time.Millisecond), archiveRead.Round(time.Millisecond), float64(ai.Size())/1e6)
	fmt.Println()
}

func dirSize(t *testing.T, dir string) (int64, int) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %s", err)
	}
	var total int64
	for _, e := range ents {
		fi, err := e.Info()
		if err == nil {
			total += fi.Size()
		}
	}
	return total, len(ents)
}
