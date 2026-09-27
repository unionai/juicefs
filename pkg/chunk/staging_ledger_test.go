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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func freshLedger(t *testing.T) *stagingLedger {
	t.Helper()
	old := ledger
	ledger = &stagingLedger{keys: make(map[string]stagedBlock)}
	t.Cleanup(func() { ledger = old })
	return ledger
}

func TestStagingLedgerCountsOnlyBlocksBeforeTheMark(t *testing.T) {
	l := freshLedger(t)
	l.add("a", "")
	l.add("b", "")
	m := StagingMark()
	l.add("c", "")
	l.add("d", "")
	if got := PendingStagedThrough(m); got != 2 {
		t.Fatalf("pending through mark = %d, want 2 (a, b)", got)
	}
	l.remove("a")
	if got := PendingStagedThrough(m); got != 1 {
		t.Fatalf("after uploading a: %d, want 1", got)
	}
	l.remove("b")
	if got := PendingStagedThrough(m); got != 0 {
		t.Fatalf("after uploading b: %d, want 0 even with c, d still staged", got)
	}
	if got := PendingStagedThrough(StagingMark()); got != 2 {
		t.Fatalf("a later mark covers c, d: got %d, want 2", got)
	}
}

func TestStagingLedgerEmptyMarkIsZero(t *testing.T) {
	freshLedger(t)
	if m := StagingMark(); m != 0 {
		t.Fatalf("mark of an empty ledger = %d", m)
	}
	if got := PendingStagedThrough(0); got != 0 {
		t.Fatalf("pending = %d", got)
	}
}

func TestStagingLedgerRestageKeepsTheOriginalSequence(t *testing.T) {
	// A key staged before the mark and staged "again" after it (a retry) has
	// been staged since before the mark: it must still be waited for.
	l := freshLedger(t)
	l.add("a", "")
	m := StagingMark()
	l.add("a", "")
	if got := PendingStagedThrough(m); got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}
}

func TestStagingLedgerRemoveUnknownKeyIsHarmless(t *testing.T) {
	l := freshLedger(t)
	l.remove("never-staged")
	l.add("a", "")
	if got := PendingStagedThrough(StagingMark()); got != 1 {
		t.Fatalf("pending = %d", got)
	}
}

func TestStagingLedgerDropsBlocksWhoseFileVanished(t *testing.T) {
	// A path that removed a staged block without telling the ledger must not
	// turn a drain into a guaranteed timeout.
	l := freshLedger(t)
	dir := t.TempDir()
	kept, gone := filepath.Join(dir, "kept"), filepath.Join(dir, "gone")
	for _, p := range []string{kept, gone} {
		if err := os.WriteFile(p, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	l.add("kept", kept)
	l.add("gone", gone)
	m := StagingMark()
	if got := PendingStagedThrough(m); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
	_ = os.Remove(gone)
	if got := PendingStagedThrough(m); got != 1 {
		t.Fatalf("pending after the file vanished = %d, want 1", got)
	}
	l.mu.Lock()
	_, stillThere := l.keys["gone"]
	l.mu.Unlock()
	if stillThere {
		t.Fatalf("vanished entry was not dropped")
	}
}

func TestStagingLedgerConcurrentUse(t *testing.T) {
	l := freshLedger(t)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("%d_%d", w, i)
				l.add(k, "")
				if i%3 == 0 {
					_ = PendingStagedThrough(StagingMark())
				}
				l.remove(k)
			}
		}(w)
	}
	wg.Wait()
	if got := PendingStagedThrough(StagingMark()); got != 0 {
		t.Fatalf("pending after every add was removed = %d", got)
	}
}

// The ledger is fed by the disk cache itself: stage adds, removeStage
// removes (including when the file is already gone), and the staging scan
// at startup adds blocks a previous process left behind.
func TestDiskCacheFeedsTheStagingLedger(t *testing.T) {
	freshLedger(t)
	conf := testConf()
	defer os.RemoveAll(conf.CacheDir)
	m := new(cacheManagerMetrics)
	m.initMetrics()
	s := newCacheStore(m, conf.CacheDir, 1<<30, conf.CacheItems, 1, &conf, nil)

	if _, err := s.stage("1_0_4", []byte("abcd"), 0); err != nil {
		t.Fatalf("stage: %s", err)
	}
	if _, err := s.stage("2_0_4", []byte("efgh"), 0); err != nil {
		t.Fatalf("stage: %s", err)
	}
	mark := StagingMark()
	if got := PendingStagedThrough(mark); got != 2 {
		t.Fatalf("pending after two stages = %d", got)
	}
	if err := s.removeStage("1_0_4"); err != nil {
		t.Fatalf("removeStage: %s", err)
	}
	if got := PendingStagedThrough(mark); got != 1 {
		t.Fatalf("pending after removeStage = %d", got)
	}
	// Already gone from disk: still leaves the ledger.
	_ = os.Remove(s.stagePath("2_0_4"))
	if err := s.removeStage("2_0_4"); err != nil {
		t.Fatalf("removeStage of a missing file: %s", err)
	}
	ledger.mu.Lock()
	n := len(ledger.keys)
	ledger.mu.Unlock()
	if n != 0 {
		t.Fatalf("ledger keeps %d entries after both blocks left staging", n)
	}
}

func TestStagingScanAtStartupFeedsTheLedger(t *testing.T) {
	freshLedger(t)
	conf := testConf()
	defer os.RemoveAll(conf.CacheDir)
	// A block a previous process staged and never uploaded.
	m := new(cacheManagerMetrics)
	m.initMetrics()
	first := newCacheStore(m, conf.CacheDir, 1<<30, conf.CacheItems, 1, &conf, nil)
	if _, err := first.stage("chunks/0/0/5_0_4", []byte("left"), 0); err != nil {
		t.Fatalf("stage: %s", err)
	}
	freshLedger(t) // the new process starts with an empty ledger

	found := make(chan string, 1)
	m2 := new(cacheManagerMetrics)
	m2.initMetrics()
	newCacheStore(m2, conf.CacheDir, 1<<30, conf.CacheItems, 1, &conf, func(key, path string, force bool) bool {
		found <- key
		return false // leave it staged
	})
	select {
	case <-found:
	case <-time.After(10 * time.Second):
		t.Fatalf("the startup scan never found the staged block")
	}
	if got := PendingStagedThrough(StagingMark()); got != 1 {
		t.Fatalf("pending after the startup scan = %d, want 1", got)
	}
}
