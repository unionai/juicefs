//go:build linux

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License").
 */

package fuse

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// sparseStaging makes a 64 GiB staging file holding two small data regions,
// the shape mkfs or `truncate -s` + a few writes leaves behind.
func sparseStaging(t *testing.T) (string, map[int64][]byte, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "staging")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	const size = int64(64) << 30
	regions := map[int64][]byte{
		0:       bytes.Repeat([]byte("head"), 1024),              // 4 KiB at the start
		1 << 30: bytes.Repeat([]byte("0123456789abcdef"), 1<<16), // 1 MiB at 1 GiB
	}
	for off, data := range regions {
		if _, err := f.WriteAt(data, off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	return path, regions, size
}

// The copy must visit only the data regions (holes are never read back as
// zeros and written), report the full size, and hand over exactly the bytes
// that were written, at their offsets.
func TestWalkStagingDataSkipsHoles(t *testing.T) {
	path, regions, size := sparseStaging(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var visited int64
	got := map[int64][]byte{}
	n, err := walkStagingData(f, func(off int64, data []byte) error {
		visited += int64(len(data))
		got[off] = append([]byte{}, data...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != size {
		t.Fatalf("size %d, want %d", n, size)
	}
	// Filesystems report data at block granularity, so allow slack, but a
	// dense walk of 64 GiB would be many orders of magnitude over this.
	if visited > 16<<20 {
		t.Fatalf("walked %d bytes of a file with ~1 MiB of data: holes were read", visited)
	}
	for off, want := range regions {
		var buf []byte
		for o := off; len(buf) < len(want); {
			chunk, ok := got[o]
			if !ok {
				t.Fatalf("no region delivered at %d", o)
			}
			buf = append(buf, chunk...)
			o += int64(len(chunk))
		}
		if !bytes.Equal(buf[:len(want)], want) {
			t.Fatalf("region at %d: content differs", off)
		}
	}
}

// A dense file is one data region: the walk is the same full copy as before.
func TestWalkStagingDataDense(t *testing.T) {
	path := filepath.Join(t.TempDir(), "staging")
	want := bytes.Repeat([]byte("0123456789abcdef"), 1<<19) // 8 MiB, spans the 4 MiB buffer
	writeStaging(t, path, want)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var got []byte
	var next int64
	n, err := walkStagingData(f, func(off int64, data []byte) error {
		if off != next {
			t.Fatalf("region at %d, want contiguous %d", off, next)
		}
		got = append(got, data...)
		next = off + int64(len(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(want)) || !bytes.Equal(got, want) {
		t.Fatalf("dense walk returned %d bytes (size %d), want %d identical", len(got), n, len(want))
	}
}

// The fingerprint of a sparse staging is cheap and still content-exact: a
// byte changed in the far region reads as changed, the original as unchanged.
func TestStagingUnchangedSparse(t *testing.T) {
	path, _, size := sparseStaging(t)
	m, err := fingerprintStaging(path)
	if err != nil {
		t.Fatal(err)
	}
	if m.size != uint64(size) {
		t.Fatalf("fingerprint size %d, want %d", m.size, size)
	}
	pf := &ptFile{b: &ptBacking{path: path, synced: m}}
	if !pf.stagingUnchangedLocked() {
		t.Fatalf("unchanged sparse staging must read as unchanged")
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{'X'}, 1<<30+12345); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if pf.stagingUnchangedLocked() {
		t.Fatalf("a byte changed in the far data region must read as changed")
	}
}
