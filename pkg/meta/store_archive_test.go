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
	"archive/tar"
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func writeStoreDir(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %s", err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0644); err != nil {
			t.Fatalf("write %s: %s", name, err)
		}
	}
}

func TestStoreArchiveRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	big := make([]byte, 1<<16)
	if _, err := rand.Read(big); err != nil {
		t.Fatalf("rand: %s", err)
	}
	src := filepath.Join(tmp, "src")
	writeStoreDir(t, src, map[string][]byte{
		"000001.sst":  []byte("sst"),
		"000001.vlog": big,
		"MANIFEST":    []byte("manifest"),
		// Recreated on every open; carrying one forward is at best noise.
		"LOCK": {},
	})

	arc := filepath.Join(tmp, "store.tar")
	if err := tarDirectory(src, arc); err != nil {
		t.Fatalf("tarDirectory: %s", err)
	}
	if ok, err := IsStoreArchive(arc); err != nil || !ok {
		t.Fatalf("IsStoreArchive(%s) = %v, %v", arc, ok, err)
	}

	dst := filepath.Join(tmp, "dst")
	if err := RestoreStoreArchive(arc, dst); err != nil {
		t.Fatalf("RestoreStoreArchive: %s", err)
	}
	ents, err := os.ReadDir(dst)
	if err != nil {
		t.Fatalf("read dst: %s", err)
	}
	if len(ents) != 3 {
		t.Fatalf("expected 3 files (LOCK dropped), got %d: %v", len(ents), ents)
	}
	got, err := os.ReadFile(filepath.Join(dst, "000001.vlog"))
	if err != nil {
		t.Fatalf("read restored vlog: %s", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("restored vlog differs from the original")
	}
}

// Two archives of an unchanged directory must be byte-identical, so "did
// this checkpoint change anything?" is a checksum away.
func TestStoreArchiveIsDeterministic(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	writeStoreDir(t, src, map[string][]byte{"b.sst": []byte("b"), "a.sst": []byte("a"), "MANIFEST": []byte("m")})
	one, two := filepath.Join(tmp, "one.tar"), filepath.Join(tmp, "two.tar")
	if err := tarDirectory(src, one); err != nil {
		t.Fatalf("tar one: %s", err)
	}
	if err := tarDirectory(src, two); err != nil {
		t.Fatalf("tar two: %s", err)
	}
	a, err := os.ReadFile(one)
	if err != nil {
		t.Fatalf("read one: %s", err)
	}
	b, err := os.ReadFile(two)
	if err != nil {
		t.Fatalf("read two: %s", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("archives of an unchanged directory are not byte-identical")
	}
}

// A nested layout means the engine changed shape; dropping it silently
// would restore to a subtly different store.
func TestStoreArchiveRefusesSubdirectories(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	writeStoreDir(t, src, map[string][]byte{"MANIFEST": []byte("m")})
	if err := os.Mkdir(filepath.Join(src, "nested"), 0755); err != nil {
		t.Fatalf("mkdir nested: %s", err)
	}
	if err := tarDirectory(src, filepath.Join(tmp, "x.tar")); err == nil {
		t.Fatalf("tarDirectory must refuse a directory it does not understand")
	}
}

// A Badger backup stream is the artifact shape this format replaced, and a
// reader has to tell them apart with no header of our own to go on.
func TestIsStoreArchiveRejectsNonArchives(t *testing.T) {
	tmp := t.TempDir()
	stream := filepath.Join(tmp, "legacy.bak")
	buf := make([]byte, 8192)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand: %s", err)
	}
	if err := os.WriteFile(stream, buf, 0644); err != nil {
		t.Fatalf("write: %s", err)
	}
	if ok, err := IsStoreArchive(stream); err != nil || ok {
		t.Fatalf("IsStoreArchive(stream) = %v, %v", ok, err)
	}
	// Shorter than the magic's offset: not an archive, not an error.
	short := filepath.Join(tmp, "short.bak")
	if err := os.WriteFile(short, []byte("tiny"), 0644); err != nil {
		t.Fatalf("write: %s", err)
	}
	if ok, err := IsStoreArchive(short); err != nil || ok {
		t.Fatalf("IsStoreArchive(short) = %v, %v", ok, err)
	}
}

// The archive is ours, but it arrives from object storage.
func TestRestoreStoreArchiveRejectsHostileEntries(t *testing.T) {
	tmp := t.TempDir()
	for _, tc := range []struct {
		name string
		hdr  *tar.Header
	}{
		{"traversal", &tar.Header{Typeflag: tar.TypeReg, Name: "../escaped", Mode: 0644, Size: 3}},
		{"nested", &tar.Header{Typeflag: tar.TypeReg, Name: "sub/file", Mode: 0644, Size: 3}},
		{"absolute", &tar.Header{Typeflag: tar.TypeReg, Name: "/etc/passwd", Mode: 0644, Size: 3}},
		{"symlink", &tar.Header{Typeflag: tar.TypeSymlink, Name: "passwd", Linkname: "/etc/passwd"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arc := filepath.Join(tmp, tc.name+".tar")
			f, err := os.Create(arc)
			if err != nil {
				t.Fatalf("create: %s", err)
			}
			tw := tar.NewWriter(f)
			if err := tw.WriteHeader(tc.hdr); err != nil {
				t.Fatalf("write header: %s", err)
			}
			if tc.hdr.Size > 0 {
				if _, err := tw.Write([]byte("pwn")); err != nil {
					t.Fatalf("write body: %s", err)
				}
			}
			if err := tw.Close(); err != nil {
				t.Fatalf("close tar: %s", err)
			}
			f.Close()
			if err := RestoreStoreArchive(arc, filepath.Join(tmp, tc.name+"-dst")); err == nil {
				t.Fatalf("RestoreStoreArchive must refuse a %s entry", tc.name)
			}
		})
	}
	if _, err := os.Stat(filepath.Join(tmp, "escaped")); err == nil {
		t.Fatalf("traversal entry escaped the destination directory")
	}
}

// Restore must never merge two histories.
func TestRestoreStoreArchiveRefusesNonEmptyDestination(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	writeStoreDir(t, src, map[string][]byte{"MANIFEST": []byte("m")})
	arc := filepath.Join(tmp, "store.tar")
	if err := tarDirectory(src, arc); err != nil {
		t.Fatalf("tarDirectory: %s", err)
	}
	dst := filepath.Join(tmp, "dst")
	writeStoreDir(t, dst, map[string][]byte{"stale.sst": []byte("old")})
	if err := RestoreStoreArchive(arc, dst); err == nil {
		t.Fatalf("RestoreStoreArchive into a non-empty directory must fail")
	}
}

// Badger preallocates: a live store's active value log is a 2 GB file with
// a few KB in it. Archiving it at its apparent size would put two gigabytes
// of zeros into every artifact.
func TestStoreArchiveTrimsPreallocatedTails(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	writeStoreDir(t, src, map[string][]byte{"MANIFEST": []byte("m")})

	vlog := filepath.Join(src, "000002.vlog")
	f, err := os.Create(vlog)
	if err != nil {
		t.Fatalf("create vlog: %s", err)
	}
	if _, err := f.Write([]byte("real entries")); err != nil {
		t.Fatalf("write: %s", err)
	}
	const apparent = 2 << 30
	if err := f.Truncate(apparent); err != nil {
		t.Fatalf("truncate: %s", err)
	}
	f.Close()

	arc := filepath.Join(tmp, "store.tar")
	if err := tarDirectory(src, arc); err != nil {
		t.Fatalf("tarDirectory: %s", err)
	}
	ai, err := os.Stat(arc)
	if err != nil {
		t.Fatalf("stat archive: %s", err)
	}
	if ai.Size() > 1<<20 {
		t.Fatalf("archive is %d bytes; the preallocated tail was not trimmed", ai.Size())
	}

	dst := filepath.Join(tmp, "dst")
	if err := RestoreStoreArchive(arc, dst); err != nil {
		t.Fatalf("RestoreStoreArchive: %s", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "000002.vlog"))
	if err != nil {
		t.Fatalf("read restored vlog: %s", err)
	}
	// Every real byte survives. The trim lands on the filesystem's block
	// boundary rather than the exact last byte, so a few trailing zeros come
	// along — which is what the engine writes there anyway, and what its own
	// replay-then-truncate handles on the next open.
	if !bytes.HasPrefix(got, []byte("real entries")) {
		t.Fatalf("restored vlog does not start with the real entries: %q", got[:min(32, len(got))])
	}
	if len(got) > 1<<20 {
		t.Fatalf("restored vlog is %d bytes; the tail was not trimmed", len(got))
	}
}

// A hole in the middle is not a shape this code understands, so the file
// must come back whole rather than silently truncated at the hole.
func TestStoreArchiveKeepsFilesWithInteriorHoles(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	writeStoreDir(t, src, map[string][]byte{"MANIFEST": []byte("m")})

	p := filepath.Join(src, "000003.vlog")
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create: %s", err)
	}
	if _, err := f.Write([]byte("head")); err != nil {
		t.Fatalf("write head: %s", err)
	}
	if _, err := f.Seek(1<<20, 0); err != nil {
		t.Fatalf("seek: %s", err)
	}
	if _, err := f.Write([]byte("tail")); err != nil {
		t.Fatalf("write tail: %s", err)
	}
	f.Close()
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read source: %s", err)
	}

	arc := filepath.Join(tmp, "store.tar")
	if err := tarDirectory(src, arc); err != nil {
		t.Fatalf("tarDirectory: %s", err)
	}
	dst := filepath.Join(tmp, "dst")
	if err := RestoreStoreArchive(arc, dst); err != nil {
		t.Fatalf("RestoreStoreArchive: %s", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "000003.vlog"))
	if err != nil {
		t.Fatalf("read restored: %s", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("restored file differs: %d bytes vs %d", len(got), len(want))
	}
}
