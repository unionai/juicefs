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

package chunk

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// Staged (not yet uploaded) blocks count against --cache-size: before, the
// only bound on staging was the disk's free-space ratio, which in a container
// is the node's disk, so a writer that outran its uploads filled the
// container's writable layer past its ephemeral-storage limit.

const mib = 1 << 20

func budgetStore(t *testing.T, capacity int64) *diskCache {
	t.Helper()
	freshLedger(t)
	conf := testConf()
	t.Cleanup(func() { _ = os.RemoveAll(conf.CacheDir) })
	m := new(cacheManagerMetrics)
	m.initMetrics()
	return newDiskCache(m, conf.CacheDir, capacity, conf.CacheItems, 1, &conf, nil)
}

func blockKey(id int) string { return fmt.Sprintf("%d_0_%d", id, 4*mib) }

func TestStagingStopsAtTheCacheSize(t *testing.T) {
	s := budgetStore(t, 10*mib)
	data := make([]byte, 4*mib)
	for i := 1; i <= 2; i++ {
		if _, err := s.stage(blockKey(i), data, 0); err != nil {
			t.Fatalf("stage %d: %s", i, err)
		}
	}
	if got := s.stagedBytes.Load(); got != 8*mib {
		t.Fatalf("staged bytes %d, want %d", got, 8*mib)
	}
	if _, err := s.stage(blockKey(3), data, 0); !errors.Is(err, errStageBudget) {
		t.Fatalf("third block: err %v, want errStageBudget", err)
	}
	if _, err := os.Stat(s.stagePath(blockKey(3))); !os.IsNotExist(err) {
		t.Fatalf("a refused block must not be written: %v", err)
	}
	// Removing a staged block (uploaded) returns its bytes to the budget.
	if err := s.removeStage(blockKey(1)); err != nil {
		t.Fatalf("removeStage: %s", err)
	}
	if got := s.stagedBytes.Load(); got != 4*mib {
		t.Fatalf("staged bytes after removal %d, want %d", got, 4*mib)
	}
	if _, err := s.stage(blockKey(3), data, 0); err != nil {
		t.Fatalf("stage after removal: %s", err)
	}
}

func TestStagedBytesMakeRoomAmongCachedBlocks(t *testing.T) {
	// No background scan here: it would drop these file-less cached entries.
	freshLedger(t)
	conf := testConf()
	conf.CacheEviction = Eviction2Random
	t.Cleanup(func() { _ = os.RemoveAll(conf.CacheDir) })
	s := newTestCacheStore(conf.CacheDir, &conf, nil)
	s.capacity = 100 * mib
	s.m = new(cacheManagerMetrics)
	s.m.initMetrics()
	// 90 cached (uploaded, evictable) 1 MiB blocks: 90 of the 100 MiB.
	for i := 100; i < 190; i++ {
		s.add(fmt.Sprintf("%d_0_%d", i, mib), mib, uint32(time.Now().Unix()))
	}
	// 12 MiB staged would take the disk past the budget: staging links each
	// block into the cache, which runs the cleanup, and cached blocks must give
	// way until cached + staged fit again. (The cleanup always evicts ~1% of
	// the entries; that alone would leave 101 MiB here.)
	for i := 1; i <= 3; i++ {
		if _, err := s.stage(blockKey(i), make([]byte, 4*mib), 0); err != nil {
			t.Fatalf("stage %d: %s", i, err)
		}
	}
	s.Lock()
	used := s.used
	s.Unlock()
	if used+s.stagedBytes.Load() > 100*mib {
		t.Fatalf("cached %d + staged %d exceed the 100 MiB budget", used, s.stagedBytes.Load())
	}
}

func TestStagingIsUnboundedWithoutACapacity(t *testing.T) {
	// capacity 0 (cache disabled) keeps the previous behaviour: only the
	// free-space ratio bounds staging.
	s := budgetStore(t, 0)
	for i := 1; i <= 4; i++ {
		if _, err := s.stage(blockKey(i), make([]byte, 4*mib), 0); err != nil {
			t.Fatalf("stage %d: %s", i, err)
		}
	}
}

func TestAFastWriterStaysInsideTheCacheSize(t *testing.T) {
	freshLedger(t)
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.CacheDir = t.TempDir()
	conf.BlockSize = 4 * mib
	conf.CacheSize = 10 * mib
	conf.Writeback = true
	conf.WritebackThresholdSize = conf.BlockSize + 1 // full blocks are staged too
	conf.UploadDelay = time.Hour                     // nothing staged gets uploaded during the test
	store := NewCachedStore(blob, conf, nil)
	bc := store.(*cachedStore).bcache.(*cacheManager)

	const blocks = 6 // 24 MiB through a 10 MiB cache
	w := store.NewWriter(1, 0)
	if _, err := w.WriteAt(make([]byte, blocks*4*mib), 0); err != nil {
		t.Fatalf("write: %s", err)
	}
	if err := w.Finish(blocks * 4 * mib); err != nil {
		t.Fatalf("finish: %s", err)
	}
	var staged int64
	for _, s := range bc.stores {
		staged += s.stagedBytes.Load()
	}
	if staged > int64(conf.CacheSize) {
		t.Fatalf("staged %d bytes, over the %d-byte cache size", staged, conf.CacheSize)
	}
	// What could not be staged went straight to object storage.
	objs, err := object.ListAll(ctx, blob, "", "", true, false)
	if err != nil {
		t.Fatalf("list: %s", err)
	}
	uploaded := 0
	for o := range objs {
		if o != nil && !o.IsDir() {
			uploaded++
		}
	}
	if uploaded == 0 || uploaded+int(staged/(4*mib)) < blocks {
		t.Fatalf("uploaded %d, staged %d MiB: %d blocks unaccounted", uploaded, staged/mib, blocks)
	}
	// Every block reads back, staged or uploaded.
	p := NewPage(make([]byte, blocks*4*mib))
	defer p.Release()
	if n, err := store.NewReader(1, blocks*4*mib).ReadAt(ctx, p, 0); err != nil || n != blocks*4*mib {
		t.Fatalf("read back: n=%d err=%v", n, err)
	}
}
