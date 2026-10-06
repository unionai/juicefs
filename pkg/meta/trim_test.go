package meta

import (
	"testing"
	"time"
)

// Repeated full trims of a block image, as successive mounts issue them, must
// not grow the chunks: the first trim adds the holes, later ones add nothing.
func TestRepeatedTrimKeepsChunksFlat(t *testing.T) {
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Init(testFormat(), true); err != nil {
		t.Fatal(err)
	}
	if err = m.NewSession(false); err != nil {
		t.Fatal(err)
	}
	defer m.CloseSession()
	m.OnMsg(DeleteSlice, func(args ...interface{}) error { return nil })
	m.OnMsg(CompactChunk, func(args ...interface{}) error { return nil })
	ctx := Background()
	var inode Ino
	if st := m.Create(ctx, 1, "img", 0644, 022, 0, &inode, &Attr{}); st != 0 {
		t.Fatal(st)
	}
	const chunks = 32
	for c := uint32(0); c < chunks; c++ {
		for k := uint32(0); k < 16; k++ {
			var id uint64
			m.NewSlice(ctx, &id)
			m.Write(ctx, inode, c, k*(4<<20), Slice{Id: id, Size: 4 << 20, Len: 4 << 20}, time.Now())
		}
	}
	// Free space: alternate 1 MiB extents across every chunk, 32 per chunk.
	var first int
	for round := 0; round < 8; round++ {
		t0 := time.Now()
		n := 0
		for c := uint64(0); c < chunks; c++ {
			for k := uint64(0); k < 32; k++ {
				off := c*ChunkSize + k*(2<<20)
				if st := m.Fallocate(ctx, inode, fallocPunchHole|fallocKeepSize, off, 1<<20, nil); st != 0 {
					t.Fatal(st)
				}
				n++
			}
		}
		ss, _ := m.getBase().en.doRead(ctx, inode, 0)
		t.Logf("round %d: %d punches in %v, chunk 0 has %d records", round, n, time.Since(t0), len(ss))
		if round == 0 {
			first = len(ss)
		} else if len(ss) != first {
			t.Fatalf("round %d grew chunk 0 to %d records (first trim left %d)", round, len(ss), first)
		}
	}
}
