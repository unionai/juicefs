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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func newDeltaTestMeta(t *testing.T) (*kvMeta, *badgerClient) {
	t.Helper()
	m, err := newKVMeta("badger", filepath.Join(t.TempDir(), "src"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	if err = m.Init(testFormat(), true); err != nil {
		t.Fatalf("init: %s", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	km := m.(*kvMeta)
	return km, km.client.(*badgerClient)
}

// state reads every key through txn: the store exactly at txn's read
// timestamp.
func state(t *testing.T, txn *badger.Txn) map[string]string {
	t.Helper()
	out := make(map[string]string)
	it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: true})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			t.Fatalf("read: %s", err)
		}
		out[string(it.Item().KeyCopy(nil))] = string(v) + "|" + strconv.Itoa(int(it.Item().UserMeta()))
	}
	return out
}

func dirState(t *testing.T, dir string) map[string]string {
	t.Helper()
	opt := badger.DefaultOptions(dir)
	opt.Logger = nil
	db, err := badger.Open(opt)
	if err != nil {
		t.Fatalf("open %s: %s", dir, err)
	}
	defer db.Close()
	txn := db.NewTransaction(false)
	defer txn.Discard()
	return state(t, txn)
}

// heldState is the store at the read timestamp of the checkpoint the client
// holds at ts (pending, or the confirmed base).
func heldState(t *testing.T, c *badgerClient, ts uint64) map[string]string {
	t.Helper()
	c.holds.mu.Lock()
	var txn *badger.Txn
	if c.holds.base != nil && c.holds.base.ReadTs() == ts {
		txn = c.holds.base
	}
	for _, p := range c.holds.pending {
		if p.ReadTs() == ts {
			txn = p
		}
	}
	c.holds.mu.Unlock()
	if txn == nil {
		t.Fatalf("no checkpoint held at %d", ts)
	}
	return state(t, txn)
}

func sameState(t *testing.T, what string, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %d keys, want %d", what, len(got), len(want))
	}
	n := 0
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			if n++; n <= 5 {
				t.Errorf("%s: key %q = %q (present %v), want %q", what, k, g, ok, v)
			}
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			if n++; n <= 5 {
				t.Errorf("%s: extra key %q", what, k)
			}
		}
	}
	if n > 0 {
		t.Fatalf("%s: %d differences", what, n)
	}
}

// churn applies a mix of namespace and data operations: creates, writes,
// renames, unlinks, rmdirs, xattrs, symlinks, truncates.
func churn(t *testing.T, m *kvMeta, round int) {
	t.Helper()
	ctx := Background()
	var attr Attr
	var dir Ino
	name := func(s string) string { return fmt.Sprintf("r%d-%s", round, s) }
	if st := m.Mkdir(ctx, RootInode, name("d"), 0755, 022, 0, &dir, &attr); st != 0 {
		t.Fatalf("mkdir: %s", st)
	}
	var inos []Ino
	for i := 0; i < 50; i++ {
		var ino Ino
		if st := m.Create(ctx, dir, "f"+strconv.Itoa(i), 0644, 022, 0, &ino, &attr); st != 0 {
			t.Fatalf("create: %s", st)
		}
		var sid uint64
		m.NewSlice(ctx, &sid)
		if st := m.Write(ctx, ino, 0, 0, Slice{Id: sid, Size: 4096, Len: 4096}, time.Now()); st != 0 {
			t.Fatalf("write: %s", st)
		}
		inos = append(inos, ino)
	}
	for i := 0; i < 10; i++ {
		if st := m.Rename(ctx, dir, "f"+strconv.Itoa(i), RootInode, name("moved"+strconv.Itoa(i)), 0, nil, nil); st != 0 {
			t.Fatalf("rename: %s", st)
		}
	}
	for i := 10; i < 25; i++ {
		if st := m.Unlink(ctx, dir, "f"+strconv.Itoa(i)); st != 0 {
			t.Fatalf("unlink: %s", st)
		}
	}
	for i := 25; i < 30; i++ {
		if st := m.SetXattr(ctx, inos[i], "user.k", []byte("v"+strconv.Itoa(round)), XattrCreateOrReplace); st != 0 {
			t.Fatalf("setxattr: %s", st)
		}
		if st := m.Truncate(ctx, inos[i], 0, 1, &attr, false); st != 0 {
			t.Fatalf("truncate: %s", st)
		}
	}
	var link Ino
	if st := m.Symlink(ctx, RootInode, name("link"), name("target"), &link, &attr); st != 0 {
		t.Fatalf("symlink: %s", st)
	}
	// Remove the previous round's directory entirely, if there is one.
	if round > 0 {
		prev := fmt.Sprintf("r%d-d", round-1)
		var pino Ino
		if st := m.Lookup(ctx, RootInode, prev, &pino, &attr, false); st == 0 {
			var entries []*Entry
			m.Readdir(ctx, pino, 0, &entries)
			for _, e := range entries {
				if n := string(e.Name); n != "." && n != ".." {
					m.Unlink(ctx, pino, n)
				}
			}
			if st := m.Rmdir(ctx, RootInode, prev); st != 0 {
				t.Fatalf("rmdir %s: %s", prev, st)
			}
		}
	}
}

func fullCheckpoint(t *testing.T, m *kvMeta, dst string) uint64 {
	t.Helper()
	ts, err := m.CheckpointStoreFullPinned(Background(), dst, nil)
	if err != nil {
		t.Fatalf("full checkpoint: %s", err)
	}
	return ts
}

func deltaCheckpoint(t *testing.T, m *kvMeta, dst string) (uint64, uint64) {
	t.Helper()
	base, ts, err := m.CheckpointStoreDeltaPinned(Background(), dst, nil)
	if err != nil {
		t.Fatalf("delta checkpoint: %s", err)
	}
	return base, ts
}

func restore(t *testing.T, base string, deltas ...string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "restored")
	if err := RestoreStoreArchive(base, dir); err != nil {
		t.Fatalf("restore base: %s", err)
	}
	if err := ApplyStoreDeltas(dir, deltas); err != nil {
		t.Fatalf("apply deltas: %s", err)
	}
	return dir
}

func TestBadgerDeltaRoundTrip(t *testing.T) {
	m, c := newDeltaTestMeta(t)
	tmp := t.TempDir()
	churn(t, m, 0)
	base := filepath.Join(tmp, "base.tar")
	ts0 := fullCheckpoint(t, m, base)
	if err := m.ConfirmCheckpoint(Background(), ts0); err != nil {
		t.Fatalf("confirm: %s", err)
	}
	churn(t, m, 1)
	d1 := filepath.Join(tmp, "d1")
	b, ts1 := deltaCheckpoint(t, m, d1)
	if b != ts0 {
		t.Fatalf("delta base %d, want the confirmed %d", b, ts0)
	}
	sameState(t, "base + d1", dirState(t, restore(t, base, d1)), heldState(t, c, ts1))
}

func TestBadgerDeltaChain(t *testing.T) {
	m, c := newDeltaTestMeta(t)
	tmp := t.TempDir()
	churn(t, m, 0)
	base := filepath.Join(tmp, "base.tar")
	ts := fullCheckpoint(t, m, base)
	if err := m.ConfirmCheckpoint(Background(), ts); err != nil {
		t.Fatal(err)
	}
	var chain []string
	for r := 1; r <= 5; r++ {
		churn(t, m, r)
		d := filepath.Join(tmp, fmt.Sprintf("d%d", r))
		b, next := deltaCheckpoint(t, m, d)
		if b != ts {
			t.Fatalf("round %d: delta base %d, want %d", r, b, ts)
		}
		chain = append(chain, d)
		sameState(t, fmt.Sprintf("chain through round %d", r), dirState(t, restore(t, base, chain...)), heldState(t, c, next))
		if err := m.ConfirmCheckpoint(Background(), next); err != nil {
			t.Fatal(err)
		}
		ts = next
	}
}

// A delta that was written but never confirmed (its publish failed) is not a
// base: the next delta covers everything since the last confirmed one.
func TestBadgerUnconfirmedDeltaMakesTheNextOneCumulative(t *testing.T) {
	m, c := newDeltaTestMeta(t)
	tmp := t.TempDir()
	churn(t, m, 0)
	base := filepath.Join(tmp, "base.tar")
	ts0 := fullCheckpoint(t, m, base)
	if err := m.ConfirmCheckpoint(Background(), ts0); err != nil {
		t.Fatal(err)
	}
	churn(t, m, 1)
	d1 := filepath.Join(tmp, "d1")
	deltaCheckpoint(t, m, d1) // never confirmed
	churn(t, m, 2)
	d2 := filepath.Join(tmp, "d2")
	b, ts2 := deltaCheckpoint(t, m, d2)
	if b != ts0 {
		t.Fatalf("second delta taken against %d, want the confirmed base %d", b, ts0)
	}
	want := heldState(t, c, ts2)
	sameState(t, "base + d2", dirState(t, restore(t, base, d2)), want)
	// Applying the unpublished d1 too is harmless: d2 supersedes it.
	sameState(t, "base + d1 + d2", dirState(t, restore(t, base, d1, d2)), want)
}

// Deletions newer than the base must survive the live store's compaction
// until the delta is taken: that is what holding the base is for. Badger
// drops a tombstone, with every version under it, in any compaction where no
// table below overlaps it and no reader can see below it (discardTs). A
// small memtable plus filler pushes everything out to tables so Flatten has
// something to compact. releaseHold simulates a client without a hold.
func testDeletionsThroughCompaction(t *testing.T, releaseHold bool) (lost int) {
	dir := filepath.Join(t.TempDir(), "src")
	opt := badger.DefaultOptions(dir)
	opt.Logger = nil
	opt.MemTableSize = 1 << 20
	opt.ValueThreshold = 1 << 10
	opt.NumVersionsToKeep = 1
	db, err := badger.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	c := &badgerClient{client: db, done: make(chan struct{})}
	c.holds.init(db)
	defer func() { c.holds.close(); _ = db.Close() }()
	tmp := t.TempDir()
	filler := func(tag string) {
		wb := db.NewWriteBatch()
		for i := 0; i < 20000; i++ {
			_ = wb.Set([]byte(fmt.Sprintf("y/%s/%06d", tag, i)), make([]byte, 256))
		}
		if err := wb.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	wb := db.NewWriteBatch()
	for i := 0; i < 20000; i++ {
		_ = wb.Set([]byte(fmt.Sprintf("x/%06d", i)), make([]byte, 128))
	}
	if err := wb.Flush(); err != nil {
		t.Fatal(err)
	}
	filler("a")
	base := filepath.Join(tmp, "base.tar")
	ts0, err := c.checkpointFullTo(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.confirmCheckpoint(ts0); err != nil {
		t.Fatal(err)
	}
	wb = db.NewWriteBatch()
	for i := 0; i < 20000; i += 2 {
		_ = wb.Delete([]byte(fmt.Sprintf("x/%06d", i)))
	}
	if err := wb.Flush(); err != nil {
		t.Fatal(err)
	}
	filler("b") // push the tombstones out of the memtable
	if releaseHold {
		c.holds.mu.Lock()
		c.holds.base.Discard() // the base's timestamp stays; nothing holds it
		c.holds.mu.Unlock()
	}
	if err := db.Flatten(2); err != nil {
		t.Fatalf("flatten: %s", err)
	}
	d := filepath.Join(tmp, "d")
	if _, _, err := c.checkpointDeltaTo(d, nil); err != nil {
		t.Fatal(err)
	}
	got := dirState(t, restore(t, base, d))
	for i := 0; i < 20000; i += 2 {
		if _, ok := got[fmt.Sprintf("x/%06d", i)]; ok {
			lost++
		}
	}
	return lost
}

func TestBadgerDeltaKeepsDeletionsThroughCompaction(t *testing.T) {
	if lost := testDeletionsThroughCompaction(t, false); lost != 0 {
		t.Fatalf("%d keys deleted after the base survive the restore", lost)
	}
}

// The control: without the hold the same compaction loses the deletions,
// which is what makes the test above mean something.
func TestBadgerDeltaCompactionLosesDeletionsWithoutTheHold(t *testing.T) {
	if lost := testDeletionsThroughCompaction(t, true); lost == 0 {
		t.Fatalf("compaction kept every tombstone even with no hold; the hold test proves nothing on this badger")
	} else {
		t.Logf("without the hold, %d deletions were lost to compaction", lost)
	}
}

func TestBadgerDeltaWithoutABase(t *testing.T) {
	m, _ := newDeltaTestMeta(t)
	tmp := t.TempDir()
	c := m.client.(*badgerClient)
	if !c.holds.expire(0) {
		t.Fatalf("the base held since open was not released")
	}
	_, _, err := m.CheckpointStoreDeltaPinned(Background(), filepath.Join(tmp, "d"), nil)
	if !errors.Is(err, ErrNoDeltaBase) {
		t.Fatalf("delta without a base: %v, want ErrNoDeltaBase", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, "d")); !os.IsNotExist(err) {
		t.Fatalf("a refused delta left a file behind")
	}
	// A confirmed full checkpoint re-establishes one.
	ts := fullCheckpoint(t, m, filepath.Join(tmp, "base.tar"))
	if err := m.ConfirmCheckpoint(Background(), ts); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.CheckpointStoreDeltaPinned(Background(), filepath.Join(tmp, "d"), nil); err != nil {
		t.Fatalf("delta after a confirmed full: %s", err)
	}
}

func TestBadgerDeltaHoldExpiresOnlyWhenOld(t *testing.T) {
	_, c := newDeltaTestMeta(t)
	if c.holds.expire(time.Hour) {
		t.Fatalf("a fresh base expired")
	}
	c.holds.mu.Lock()
	c.holds.baseAt = time.Now().Add(-2 * time.Hour)
	c.holds.mu.Unlock()
	if !c.holds.expire(time.Hour) {
		t.Fatalf("a two-hour-old base did not expire at a one-hour limit")
	}
	if c.holds.expire(time.Hour) {
		t.Fatalf("expire reported releasing an already released base")
	}
}

func TestBadgerConfirmRules(t *testing.T) {
	m, c := newDeltaTestMeta(t)
	tmp := t.TempDir()
	if err := m.ConfirmCheckpoint(Background(), 999999); err == nil {
		t.Fatalf("confirming a checkpoint that was never written succeeded")
	}
	var tss []uint64
	for i := 0; i < maxPendingCheckpoints+2; i++ {
		churn(t, m, i)
		tss = append(tss, fullCheckpoint(t, m, filepath.Join(tmp, fmt.Sprintf("c%d.tar", i))))
	}
	c.holds.mu.Lock()
	n := len(c.holds.pending)
	c.holds.mu.Unlock()
	if n != maxPendingCheckpoints {
		t.Fatalf("%d pending checkpoints held, want at most %d", n, maxPendingCheckpoints)
	}
	if err := m.ConfirmCheckpoint(Background(), tss[0]); err == nil {
		t.Fatalf("confirmed a checkpoint that was dropped from the pending set")
	}
	mid := tss[len(tss)-2]
	if err := m.ConfirmCheckpoint(Background(), mid); err != nil {
		t.Fatalf("confirm: %s", err)
	}
	c.holds.mu.Lock()
	baseTs, left := c.holds.base.ReadTs(), len(c.holds.pending)
	c.holds.mu.Unlock()
	if baseTs != mid || left != 1 {
		t.Fatalf("after confirming %d: base %d, %d pending (want the newer one kept)", mid, baseTs, left)
	}
	if err := m.ConfirmCheckpoint(Background(), mid); err == nil {
		t.Fatalf("confirming the same checkpoint twice succeeded")
	}
}

// Like a full checkpoint, a delta is one point in time under live writes.
func TestBadgerDeltaIsPointInTime(t *testing.T) {
	m, c := newDeltaTestMeta(t)
	tmp := t.TempDir()
	db := c.client
	wb := db.NewWriteBatch()
	for i := 0; i < 300000; i++ {
		_ = wb.Set([]byte(fmt.Sprintf("m/%09d", i)), make([]byte, 32))
	}
	if err := wb.Flush(); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(tmp, "base.tar")
	ts0 := fullCheckpoint(t, m, base)
	if err := m.ConfirmCheckpoint(Background(), ts0); err != nil {
		t.Fatal(err)
	}
	const pairs = 64
	var n atomic.Uint64
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			v := n.Add(1)
			p := v % pairs
			_ = db.Update(func(txn *badger.Txn) error {
				_ = txn.Set([]byte(fmt.Sprintf("a/%02d", p)), []byte(strconv.FormatUint(v, 10)))
				return txn.Set([]byte(fmt.Sprintf("z/%02d", p)), []byte(strconv.FormatUint(v, 10)))
			})
		}
	}()
	defer func() { close(stop); <-done }()
	for round := 0; round < 5; round++ {
		d := filepath.Join(tmp, fmt.Sprintf("d%d", round))
		_, ts := deltaCheckpoint(t, m, d)
		got := dirState(t, restore(t, base, d))
		for p := 0; p < pairs; p++ {
			a, z := got[fmt.Sprintf("a/%02d", p)], got[fmt.Sprintf("z/%02d", p)]
			if a != z {
				t.Fatalf("round %d: pair %d torn: a=%q z=%q", round, p, a, z)
			}
		}
		_ = ts
	}
}

func TestBadgerDeltaCorruptionIsRefusedBeforeAnythingApplies(t *testing.T) {
	m, c := newDeltaTestMeta(t)
	tmp := t.TempDir()
	churn(t, m, 0)
	base := filepath.Join(tmp, "base.tar")
	ts0 := fullCheckpoint(t, m, base)
	if err := m.ConfirmCheckpoint(Background(), ts0); err != nil {
		t.Fatal(err)
	}
	baseState := heldState(t, c, ts0)
	churn(t, m, 1)
	good := filepath.Join(tmp, "good")
	deltaCheckpoint(t, m, good)
	raw, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"flipped byte": func() []byte { b := append([]byte(nil), raw...); b[len(b)/2] ^= 0xff; return b }(),
		"truncated":    raw[:len(raw)-9],
		"bad magic":    append([]byte("NOTDELTA"), raw[8:]...),
		"empty":        {},
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			bad := filepath.Join(t.TempDir(), "bad")
			if err := os.WriteFile(bad, content, 0600); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(t.TempDir(), "restored")
			if err := RestoreStoreArchive(base, dir); err != nil {
				t.Fatal(err)
			}
			// good first, then bad: the good one must not have been applied either.
			if err := ApplyStoreDeltas(dir, []string{good, bad}); err == nil {
				t.Fatalf("applied a %s delta", name)
			}
			sameState(t, "store after a refused chain", dirState(t, dir), baseState)
		})
	}
}

func TestBadgerDeltaIsSmallForASmallChange(t *testing.T) {
	m, _ := newDeltaTestMeta(t)
	tmp := t.TempDir()
	ctx := Background()
	var attr Attr
	var ino Ino
	for i := 0; i < 20000; i++ {
		if st := m.Create(ctx, RootInode, "f"+strconv.Itoa(i), 0644, 022, 0, &ino, &attr); st != 0 {
			t.Fatal(st)
		}
	}
	base := filepath.Join(tmp, "base.tar")
	ts := fullCheckpoint(t, m, base)
	if err := m.ConfirmCheckpoint(ctx, ts); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ { // 1%
		if st := m.Create(ctx, RootInode, "g"+strconv.Itoa(i), 0644, 022, 0, &ino, &attr); st != 0 {
			t.Fatal(st)
		}
	}
	d := filepath.Join(tmp, "d")
	deltaCheckpoint(t, m, d)
	bs, _ := os.Stat(base)
	ds, _ := os.Stat(d)
	t.Logf("base archive %d bytes, 1%% delta %d bytes", bs.Size(), ds.Size())
	if ds.Size()*10 > bs.Size() {
		t.Fatalf("a 1%% change produced a delta of %d bytes against a %d-byte base", ds.Size(), bs.Size())
	}
}

func TestIsStoreDelta(t *testing.T) {
	m, _ := newDeltaTestMeta(t)
	tmp := t.TempDir()
	base := filepath.Join(tmp, "base.tar")
	if err := m.ConfirmCheckpoint(Background(), fullCheckpoint(t, m, base)); err != nil {
		t.Fatal(err)
	}
	d := filepath.Join(tmp, "d")
	deltaCheckpoint(t, m, d)
	if !IsStoreDelta(d) || IsStoreDelta(base) || IsStoreDelta(filepath.Join(tmp, "missing")) {
		t.Fatalf("IsStoreDelta misclassified a file")
	}
}

// Deltas on an engine that cannot take them are refused, not faked.
func TestDeltaCheckpointUnsupportedEngine(t *testing.T) {
	m, err := newKVMeta("memkv", "jfs-delta-unit", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.(*kvMeta).CheckpointStoreDeltaPinned(Background(), filepath.Join(t.TempDir(), "d"), nil); err != syscall.ENOTSUP {
		t.Fatalf("memkv delta: %v, want ENOTSUP", err)
	}
}
