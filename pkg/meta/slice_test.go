package meta

import (
	"math/rand"
	"testing"
	"time"
)

func TestIsHole(t *testing.T) {
	data := &slice{id: 1, pos: 0, size: 4 << 20, len: 4 << 20}
	hole := &slice{id: 0, pos: 1 << 20, len: 1 << 20}
	ss := []*slice{data, hole}
	cases := []struct {
		off, l uint32
		want   bool
	}{
		{1 << 20, 1 << 20, true},
		{1<<20 + 10, 100, true},
		{0, 1 << 20, false},
		{1<<20 - 1, 2, false},
		{2<<20 - 1, 2, false},
		{4 << 20, 1 << 20, true}, // past the last slice
		{3 << 20, 2 << 20, false},
	}
	for _, c := range cases {
		if got := isHole(ss, c.off, c.l); got != c.want {
			t.Errorf("isHole(%d, %d) = %v, want %v", c.off, c.l, got, c.want)
		}
	}
	if !isHole([]*slice{}, 0, 100) {
		t.Error("an empty chunk is all hole")
	}
	if isHole(nil, 0, 100) {
		t.Error("corrupt (nil) must not be treated as a hole")
	}
}

// The reference answer: build the chunk's visible layout and look.
func isHoleByBuild(ss []*slice, off, l uint32) bool {
	var pos uint32
	end := off + l
	for _, s := range buildSlice(ss) {
		next := pos + s.Len
		if next > off && pos < end && s.Id != 0 {
			return false
		}
		pos = next
	}
	return true
}

func TestIsHoleMatchesTheBuiltLayout(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 2000; iter++ {
		ss := []*slice{}
		for n := r.Intn(30); n > 0; n-- {
			pos := uint32(r.Intn(64))
			ln := uint32(1 + r.Intn(16))
			var id uint64
			if r.Intn(2) == 0 {
				id = uint64(1 + r.Intn(100))
			}
			ss = append(ss, &slice{id: id, pos: pos, size: ln, len: ln})
		}
		off, l := uint32(r.Intn(70)), uint32(1+r.Intn(20))
		if got, want := isHole(ss, off, l), isHoleByBuild(ss, off, l); got != want {
			t.Fatalf("isHole(%d,%d)=%v, built layout says %v; slices %v", off, l, got, want, ss)
		}
	}
}

func TestIsHoleIsCheapOnLongChunks(t *testing.T) {
	// A chunk after many mounts' trims: thousands of records.
	var ss []*slice
	for i := 0; i < 5000; i++ {
		pos := uint32(i%32) * (2 << 20)
		ss = append(ss, &slice{id: 0, pos: pos, len: 1 << 20})
	}
	start := time.Now()
	for i := 0; i < 1000; i++ {
		if !isHole(ss, uint32(i%32)*(2<<20), 1<<20) {
			t.Fatal("should be a hole")
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("1000 checks on a 5000-record chunk took %v", d)
	}
}
