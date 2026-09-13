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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// A *store archive* is an uncompressed tar of a metadata store DIRECTORY,
// laid out so that untarring it yields a directory an engine can open
// directly.
//
// It exists because the alternative artifact for a directory-shaped engine
// is a logical dump (BadgerDB's backup stream), and replaying such a dump
// costs as much as the original writes did. Measured on a store of 1M
// files: 5.5 s to restore the 268 MB stream, against 0.68 s to untar the
// 88 MB archive it becomes. A mount pays that cost on its critical path, so
// the archive is what the checkpoint verbs write and read.
//
// The format is deliberately boring — `tar tvf` works on it, and the files
// inside are exactly the engine's own, so a support case can open the
// restored directory with the engine's own tooling.

// tar's POSIX magic lives at offset 257 and is the cheapest way to tell an
// archive from an engine's own dump format, neither of which carries a
// header we control.
const (
	storeArchiveMagicOff = 257
	storeArchiveMagic    = "ustar"
)

// IsStoreArchive reports whether path holds a store archive (as opposed to
// an engine-native dump such as a Badger backup stream). Callers use it to
// stay compatible with artifacts published before the archive format
// existed; a short or unreadable file is simply "not an archive".
func IsStoreArchive(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	buf := make([]byte, len(storeArchiveMagic))
	if _, err := f.ReadAt(buf, storeArchiveMagicOff); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil
		}
		return false, err
	}
	return string(buf) == storeArchiveMagic, nil
}

// tarDirectory writes every regular file directly under dir into an
// uncompressed tar at dst.
//
// Store directories are flat (SSTs, value logs, MANIFEST, KEYREGISTRY), so
// this does not recurse: a subdirectory would mean the engine changed its
// layout, and silently dropping it would produce an archive that restores
// to a subtly different store. Fail loudly instead.
func tarDirectory(dir, dst string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		// LOCK is the engine's flock guard, recreated on open; carrying it
		// forward is at best noise and at worst a stale lock.
		if e.Name() == "LOCK" {
			continue
		}
		if !e.Type().IsRegular() {
			return fmt.Errorf("store directory %s contains a non-regular entry %q (%s); refusing to archive a layout this code does not understand", dir, e.Name(), e.Type())
		}
		names = append(names, e.Name())
	}
	// Deterministic order: two archives of the same directory should be
	// byte-identical, which makes "did this checkpoint change anything?"
	// answerable with a checksum.
	sort.Strings(names)

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(f)
	for _, name := range names {
		if err := appendFile(tw, dir, name); err != nil {
			f.Close()
			return err
		}
	}
	if err := tw.Close(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func appendFile(tw *tar.Writer, dir, name string) error {
	in, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	defer in.Close()
	st, err := in.Stat()
	if err != nil {
		return err
	}
	end, err := dataEnd(in, st.Size())
	if err != nil {
		return err
	}
	hdr := &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     name,
		Mode:     int64(st.Mode().Perm()),
		Size:     end,
		ModTime:  st.ModTime(),
		Format:   tar.FormatPAX,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	n, err := io.Copy(tw, io.LimitReader(in, end))
	if err != nil {
		return err
	}
	if n != end {
		// The engine is not writing to this directory while we archive it
		// (offline, or a private copy we just built), so a short read means
		// something else is — and the archive would be corrupt.
		return fmt.Errorf("%s changed size while being archived (%d != %d)", name, n, end)
	}
	return nil
}

// dataEnd returns the offset past the last real byte of f, ignoring a
// trailing hole.
//
// Badger preallocates: a live store's active value log is a 2 GB file with
// 16 KB of data in it, and its memtable WAL is a 128 MB file holding a few
// hundred bytes. Archiving those at their apparent size would put two
// gigabytes of zeros into every artifact.
//
// Trimming a *trailing* hole is what Badger itself does when it closes a
// store, and what it does again when it reopens one: the value log and the
// memtable WAL are each replayed to the first invalid entry and truncated
// there, so a file that ends early is the ordinary crash-recovery case, not
// a corrupt one. A hole in the *middle* is not something this code
// understands, so such a file is archived whole.
func dataEnd(f *os.File, size int64) (int64, error) {
	firstHole, err := f.Seek(0, unix.SEEK_HOLE)
	if err != nil {
		// No SEEK_HOLE on this filesystem: every byte is data as far as we know.
		return size, resetOffset(f)
	}
	if firstHole >= size {
		return size, resetOffset(f)
	}
	if _, err := f.Seek(firstHole, unix.SEEK_DATA); err != nil {
		if errors.Is(err, unix.ENXIO) {
			return firstHole, resetOffset(f) // trailing hole: the file really ends here
		}
		return size, resetOffset(f)
	}
	return size, resetOffset(f)
}

func resetOffset(f *os.File) error {
	_, err := f.Seek(0, io.SeekStart)
	return err
}

// RestoreStoreArchive untars a store archive into dir, which must not
// already exist or must be empty. It is the counterpart of tarDirectory and
// needs no engine: the archive already holds the engine's own files.
func RestoreStoreArchive(src, dir string) error {
	if ents, err := os.ReadDir(dir); err == nil {
		if len(ents) > 0 {
			return fmt.Errorf("destination store directory %s is not empty; restore requires a fresh, empty directory", dir)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// The archive is ours, but it arrives over the network as a
		// published artifact, so treat it as untrusted: flat regular files
		// only, no traversal, no links.
		if hdr.Typeflag != tar.TypeReg {
			return fmt.Errorf("store archive %s holds a non-regular entry %q (type %q)", src, hdr.Name, string(hdr.Typeflag))
		}
		name := hdr.Name
		if name != filepath.Base(name) || name == "." || name == ".." || strings.ContainsRune(name, os.PathSeparator) {
			return fmt.Errorf("store archive %s holds an unexpected path %q; entries must be plain filenames", src, hdr.Name)
		}
		if err := writeEntry(tr, filepath.Join(dir, name), hdr); err != nil {
			return err
		}
	}
}

func writeEntry(tr io.Reader, path string, hdr *tar.Header) error {
	mode := os.FileMode(hdr.Mode).Perm()
	if mode == 0 {
		mode = 0644
	}
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, tr); err != nil {
		out.Close()
		return err
	}
	// The restored directory is opened by the engine immediately after, and
	// a mount that survives the pod losing power must not find a half-
	// written SST, so pay for the fsync here.
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
