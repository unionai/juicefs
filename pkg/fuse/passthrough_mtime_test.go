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

package fuse

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFinalMtime(t *testing.T) {
	write := time.Unix(2_000_000_000, 0)
	old := time.Unix(1_000_000, 0)
	cases := []struct {
		name       string
		lastWrite  time.Time
		explicit   *time.Time
		explicitAt time.Time
		want       time.Time
		ok         bool
	}{
		// cp -a / tar -x / rsync -t: write, then set the time, then close.
		{"explicit after the last write wins", write, &old, write.Add(time.Millisecond), old, true},
		{"explicit at the same instant as the write wins", write, &old, write, old, true},
		// set the time, then write more: the write is newer.
		{"a write after the explicit time wins", write, &old, write.Add(-time.Second), write, true},
		// plain write then close: the file changed when it was last written,
		// not when the reconcile copied it.
		{"no explicit time: the last write", write, nil, time.Time{}, write, true},
		{"no staging time: the explicit one", time.Time{}, &old, time.Now(), old, true},
		{"neither: leave it", time.Time{}, nil, time.Time{}, time.Time{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := finalMtime(c.lastWrite, c.explicit, c.explicitAt)
			if ok != c.ok || !got.Equal(c.want) {
				t.Fatalf("finalMtime = %v, %v; want %v, %v", got, ok, c.want, c.ok)
			}
		})
	}
}

// setMtime lands on the inode's writer backing, records when it arrived, and
// ignores inodes with no live passthrough writer.
func TestSetMtimeRecordsOnTheLiveWriter(t *testing.T) {
	p := &passthroughState{dir: filepath.Join(t.TempDir(), "pt"), files: make(map[uint64]*ptFile), busy: make(map[Ino]int)}
	const ino = Ino(42)
	b := fakeWriter(t, p, ino, 1, []byte("data"))
	want := time.Unix(1_000_000, 500)
	before := time.Now()
	p.setMtime(ino, want)
	if b.mtime == nil || !b.mtime.Equal(want) {
		t.Fatalf("recorded mtime %v, want %v", b.mtime, want)
	}
	if b.mtimeAt.Before(before) {
		t.Fatalf("arrival time %v is before the call (%v)", b.mtimeAt, before)
	}
	// Another inode: nothing recorded anywhere, no panic.
	p.setMtime(Ino(43), time.Unix(5, 0))
	if !b.mtime.Equal(want) {
		t.Fatalf("setMtime of another inode changed this backing's time to %v", b.mtime)
	}
	// A nil state (passthrough off) is a no-op.
	var off *passthroughState
	off.setMtime(ino, want)
}

// A read-only share is not the writer: only the writer's backing carries
// the time the reconcile will apply.
func TestSetMtimeIgnoresReadOnlyShares(t *testing.T) {
	p := &passthroughState{dir: filepath.Join(t.TempDir(), "pt"), files: make(map[uint64]*ptFile), busy: make(map[Ino]int)}
	const ino = Ino(7)
	p.files[9] = &ptFile{ino: ino, fh: 9, b: &ptBacking{}, writer: false}
	p.setMtime(ino, time.Unix(1, 0))
	if p.files[9].b.mtime != nil {
		t.Fatalf("a read-only share recorded an mtime")
	}
}
