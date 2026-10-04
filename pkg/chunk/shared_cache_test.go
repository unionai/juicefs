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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

func sharedConf(t *testing.T, private, shared string) Config {
	conf := defaultConf
	conf.CacheDir = private
	conf.SharedCacheDir = shared
	conf.SharedCacheSize = 10 << 20
	conf.CacheFullBlock = true // what a mount defaults to (--cache-full-block)
	return conf
}

func readAll(t *testing.T, store ChunkStore, id uint64, size int) []byte {
	t.Helper()
	p := NewPage(make([]byte, size))
	n, err := store.NewReader(id, size).ReadAt(context.Background(), p, 0)
	if err != nil || n != size {
		t.Fatalf("read %d: n=%d err=%v", id, n, err)
	}
	return p.Data[:n]
}

// A block one client fetched from object storage is served to another client
// on the node from the shared read cache, without object storage.
func TestSharedReadCacheServesOtherClients(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "shared")
	mem, _ := object.CreateStorage("mem", "", "", "", "")

	data := bytes.Repeat([]byte("j"), defaultConf.BlockSize)
	w := NewCachedStore(mem, sharedConf(t, filepath.Join(root, "a"), shared), nil)
	writer := w.NewWriter(77, 0)
	if _, err := writer.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := writer.Finish(len(data)); err != nil {
		t.Fatal(err)
	}

	// B misses its own cache and the (empty) shared one, fetches, fills shared.
	b := NewCachedStore(mem, sharedConf(t, filepath.Join(root, "b"), shared), nil)
	if got := readAll(t, b, 77, len(data)); !bytes.Equal(got, data) {
		t.Fatal("B read wrong data")
	}
	key := sliceForRead(77, len(data), w.(*cachedStore)).key(0)
	// On disk, not just queued in memory (exist() also counts pending pages).
	onDisk := func(dir string) bool {
		found := false
		_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && filepath.Base(p) == filepath.Base(key) {
				found = true
			}
			return nil
		})
		return found
	}
	deadline := time.Now().Add(5 * time.Second)
	for !onDisk(shared) {
		if time.Now().After(deadline) {
			t.Fatal("B's fetch never reached the shared cache on disk")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if onDisk(filepath.Join(root, "b")) {
		t.Fatal("a fetched block went to B's private cache, not only the shared one")
	}

	// Object storage loses the block: C (a third client, cold private cache)
	// can only succeed from the shared cache.
	if err := mem.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	c := NewCachedStore(mem, sharedConf(t, filepath.Join(root, "c"), shared), nil)
	if got := readAll(t, c, 77, len(data)); !bytes.Equal(got, data) {
		t.Fatal("C did not read the block from the shared cache")
	}
}

// Writeback staging never lands in the shared directory.
func TestSharedReadCacheGetsNoStaging(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "shared")
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := sharedConf(t, filepath.Join(root, "w"), shared)
	conf.Writeback = true
	conf.UploadDelay = time.Hour                            // keep it staged
	conf.WritebackThresholdSize = 2 * defaultConf.BlockSize // stage full blocks too
	w := NewCachedStore(mem, conf, nil)
	writer := w.NewWriter(88, 0)
	data := bytes.Repeat([]byte("s"), defaultConf.BlockSize)
	if _, err := writer.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := writer.Finish(len(data)); err != nil {
		t.Fatal(err)
	}
	_ = filepath.Walk(shared, func(p string, info os.FileInfo, err error) error {
		if err == nil && info.Name() == stagingDir {
			t.Errorf("staging directory in the shared cache: %s", p)
		}
		return nil
	})
	if _, err := os.Stat(filepath.Join(root, "w", stagingDir)); err != nil {
		t.Errorf("expected the block staged privately: %v", err)
	}
}

// A block this client uploaded reaches the shared cache too, so another client
// on the node reads it locally even though this one never fetched it.
func TestSharedReadCacheGetsUploadedBlocks(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "shared")
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := bytes.Repeat([]byte("u"), defaultConf.BlockSize)
	w := NewCachedStore(mem, sharedConf(t, filepath.Join(root, "w"), shared), nil)
	writer := w.NewWriter(99, 0)
	if _, err := writer.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := writer.Finish(len(data)); err != nil {
		t.Fatal(err)
	}
	key := sliceForRead(99, len(data), w.(*cachedStore)).key(0)
	deadline := time.Now().Add(5 * time.Second)
	for {
		found := false
		_ = filepath.Walk(shared, func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && filepath.Base(p) == filepath.Base(key) {
				found = true
			}
			return nil
		})
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("an uploaded block never reached the shared cache")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := mem.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	r := NewCachedStore(mem, sharedConf(t, filepath.Join(root, "r"), shared), nil)
	if got := readAll(t, r, 99, len(data)); !bytes.Equal(got, data) {
		t.Fatal("the uploaded block was not served from the shared cache")
	}
}
