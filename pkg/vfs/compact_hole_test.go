package vfs

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

// A chunk of a block image after fstrim: data, a punched hole, data.
func holeySlices(t *testing.T, store chunk.ChunkStore) []meta.Slice {
	var slices []meta.Slice
	for i, id := range []uint64{1, 3} {
		buf := make([]byte, 1<<20)
		for j := range buf {
			buf[j] = byte(i + 1)
		}
		w := store.NewWriter(id, 0)
		if _, err := w.WriteAt(buf, 0); err != nil {
			t.Fatal(err)
		}
		if err := w.Finish(len(buf)); err != nil {
			t.Fatal(err)
		}
		slices = append(slices, meta.Slice{Id: id, Size: 1 << 20, Len: 1 << 20})
		if i == 0 {
			slices = append(slices, meta.Slice{Id: 0, Size: 62 << 20, Len: 62 << 20})
		}
	}
	return slices
}

func TestCompactHoleMemory(t *testing.T) {
	cconf := chunk.Config{BlockSize: 4 << 20, Compress: "none", MaxUpload: 2, MaxDownload: 200,
		BufferSize: 2048 << 20, CacheSize: 10 << 20, CacheDir: "memory"}
	blob, err := object.CreateStorage("file", t.TempDir()+"/", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	store := chunk.NewCachedStore(blob, cconf, nil)
	slices := holeySlices(t, store)

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	stop := make(chan struct{})
	go func() {
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peak.Load() {
				peak.Store(m.HeapInuse)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	if err := Compact(cconf, store, slices, 100, 0); err != nil {
		t.Fatal(err)
	}
	close(stop)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	objs, _ := object.ListAll(t.Context(), blob, "", "", true, false)
	var stored, n int64
	for o := range objs {
		if o != nil && len(o.Key()) > 0 && o.Key()[0:6] == "chunks" {
			stored += o.Size()
			n++
		}
	}
	t.Logf("peak heap +%d MiB, allocated %d MiB, stored %d objects / %d MiB (input data 2 MiB)",
		(int64(peak.Load())-int64(base.HeapInuse))>>20, (after.TotalAlloc-base.TotalAlloc)>>20, n, stored>>20)
}
