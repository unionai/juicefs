/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License").
 */

package vfs

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

func runCheckpointMsg(t *testing.T, v *VFS, dst string, flags *uint8) syscall.Errno {
	t.Helper()
	n := 4 + 4 + len(dst)
	if flags != nil {
		n++
	}
	payload := make([]byte, n)
	w := utils.FromBuffer(payload)
	w.Put32(1) // drain timeout, seconds
	w.Put32(uint32(len(dst)))
	w.Put([]byte(dst))
	if flags != nil {
		w.Put8(*flags)
	}
	out := &bytes.Buffer{}
	done := make(chan struct{})
	go func() {
		v.handleInternalMsg(meta.NewContext(10, 0, []uint32{0}), meta.Checkpoint, utils.FromBuffer(w.Bytes()), out)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("checkpoint handler did not return")
	}
	_, eno := decodeControlOutput(out.Bytes())
	return eno
}

// --skip-drain returns once the snapshot is written; without it (or from an
// older client that sends no flags byte) the handler still drains. The test
// VFS has no metrics registry, so a drain reports staging unobservable (EIO):
// that is the observable difference.
func TestCheckpointSkipDrain(t *testing.T) {
	dir := t.TempDir()
	v, _ := createTestVFS(nil, "sqlite3://"+filepath.Join(dir, "meta.db"))
	v.registry = nil

	skip := uint8(1)
	dst := filepath.Join(dir, "skip.db")
	if eno := runCheckpointMsg(t, v, dst, &skip); eno != 0 {
		t.Fatalf("checkpoint with skip-drain: %s", eno)
	}
	if st, err := os.Stat(dst); err != nil || st.Size() == 0 {
		t.Fatalf("skip-drain wrote no snapshot at %s: %v", dst, err)
	}

	none := uint8(0)
	if eno := runCheckpointMsg(t, v, filepath.Join(dir, "drain.db"), &none); eno != syscall.EIO {
		t.Fatalf("checkpoint without skip-drain must still drain (EIO here), got %v", eno)
	}
	if eno := runCheckpointMsg(t, v, filepath.Join(dir, "old.db"), nil); eno != syscall.EIO {
		t.Fatalf("an old client (no flags byte) must still drain (EIO here), got %v", eno)
	}
}
