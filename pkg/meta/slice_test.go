package meta

import "testing"

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
