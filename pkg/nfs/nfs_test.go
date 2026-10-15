package nfs

import (
	"bytes"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// createTestFS mirrors pkg/fs's own test factory: memkv metadata, mem object
// store, no disk and no FUSE.
func createTestFS(t testing.TB) (*fs.FileSystem, meta.Meta) {
	t.Helper()
	m := meta.NewClient("memkv://", nil)
	format := &meta.Format{Name: "test", BlockSize: 4096, Capacity: 1 << 30, DirStats: true}
	if err := m.Init(format, true); err != nil {
		t.Fatalf("meta init: %s", err)
	}
	conf := vfs.Config{
		Meta: meta.DefaultConf(),
		Chunk: &chunk.Config{
			BlockSize:   format.BlockSize << 10,
			MaxUpload:   1,
			MaxDownload: 200,
			BufferSize:  100 << 20,
		},
		DirEntryTimeout: 100 * time.Millisecond,
		EntryTimeout:    100 * time.Millisecond,
		AttrTimeout:     100 * time.Millisecond,
		AccessLog:       filepath.Join(t.TempDir(), "juicefs.access.log"),
	}
	objStore, _ := object.CreateStorage("mem", "", "", "", "")
	store := chunk.NewCachedStore(objStore, *conf.Chunk, nil)
	jfs, err := fs.NewFileSystem(&conf, m, store, nil)
	if err != nil {
		t.Fatalf("new filesystem: %s", err)
	}
	t.Cleanup(func() { _ = jfs.Close() })

	return jfs, m
}

// serveTestNFS starts the server and returns a mounted client target.
//
// This is a real NFSv3 conversation over a real socket — XDR on the wire,
// MOUNT, LOOKUP, CREATE, WRITE, READ — so it exercises the handle encoding and
// the billy adapter exactly as a kernel client would. What it does not
// exercise is mount(2): this sandbox has neither CAP_SYS_ADMIN nor an openable
// /dev/fuse, and on macOS the kernel mount is what CI covers instead.
func serveTestNFS(t testing.TB) (*Server, *nfsc.Target) {
	t.Helper()
	jfs, m := createTestFS(t)
	srv, err := NewServer("127.0.0.1:0", jfs, m, meta.Background(), false)
	if err != nil {
		t.Fatalf("new server: %s", err)
	}
	// Serving starts before any client connects: go-nfs's listener must be
	// accepting or the mount fails with EIO.
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })

	c, err := rpc.DialTCP("tcp", (&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: srv.Port()}).String(), false)
	if err != nil {
		t.Fatalf("dial: %s", err)
	}
	t.Cleanup(c.Close)

	var mounter nfsc.Mount
	mounter.Client = c
	target, err := mounter.Mount("/", rpc.AuthNull)
	if err != nil {
		t.Fatalf("mount: %s", err)
	}
	t.Cleanup(func() { _ = mounter.Unmount() })

	return srv, target
}

func TestNFSReadWriteRoundTrip(t *testing.T) {
	srv, target := serveTestNFS(t)

	if _, err := target.Mkdir("/data", 0o755); err != nil {
		t.Fatalf("mkdir: %s", err)
	}

	// Several megabytes so the client issues many WRITE RPCs rather than one:
	// the point of the open-handle pool is what happens across RPCs.
	payload := bytes.Repeat([]byte("juicefs-over-nfs!"), 200_000)

	w, err := target.OpenFile("/data/blob", 0o644)
	if err != nil {
		t.Fatalf("openfile: %s", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("write: %s", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close after write: %s", err)
	}

	r, err := target.Open("/data/blob")
	if err != nil {
		t.Fatalf("open: %s", err)
	}
	got, err := io.ReadAll(r)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %s", err)
	}
	_ = r.Close()

	if len(got) != len(payload) {
		t.Fatalf("read back %d bytes, wrote %d", len(got), len(payload))
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("data read back over NFS does not match what was written")
	}

	// Prove the mechanism rather than trusting the pass. A green round trip
	// says nothing about whether the open-handle pool did anything: go-nfs
	// opens and closes the file on EVERY read and write RPC, so without the
	// pool `opened` would climb with the transfer. Assert that it did not.
	opened, reused, hits, misses := srv.Stats()
	t.Logf("opens=%d reused=%d pathHits=%d pathMisses=%d", opened, reused, hits, misses)
	if reused == 0 {
		t.Fatal("no handle was ever reused: the open-handle pool is inert, every RPC paid a JuiceFS open")
	}
	if opened > reused {
		t.Fatalf("opens (%d) exceed reuses (%d): the pool is not holding handles across RPCs", opened, reused)
	}
	if misses > 0 {
		t.Errorf("%d handle resolutions fell back to meta.GetPaths; the path cache should cover a live mount", misses)
	}
}

func TestNFSDirectoryOps(t *testing.T) {
	_, target := serveTestNFS(t)

	if _, err := target.Mkdir("/d", 0o755); err != nil {
		t.Fatalf("mkdir: %s", err)
	}
	for _, name := range []string{"/d/a", "/d/b", "/d/c"} {
		if _, err := target.Create(name, 0o644); err != nil {
			t.Fatalf("create %s: %s", name, err)
		}
	}

	// Does a plain lookup of a created file work over the wire at all?
	if _, _, err := target.Lookup("/d/a"); err != nil {
		t.Fatalf("wire lookup of a file we just created: %s", err)
	}
	ents, err := target.ReadDirPlus("/d")
	if err != nil {
		t.Fatalf("readdirplus: %s", err)
	}
	t.Logf("readdirplus returned %d entries", len(ents))
	seen := map[string]bool{}
	for _, e := range ents {
		t.Logf("  entry %q fileid=%d", e.Name(), e.FileId)
		seen[e.Name()] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seen[want] {
			t.Errorf("%q missing from readdir: got %v", want, seen)
		}
	}

	if err := target.Rename("/d/a", "/d/renamed"); err != nil {
		t.Fatalf("rename: %s", err)
	}
	if _, _, err := target.Lookup("/d/renamed"); err != nil {
		t.Fatalf("lookup after rename: %s", err)
	}
	if _, _, err := target.Lookup("/d/a"); err == nil {
		t.Fatal("the old name still resolves after rename")
	}

	if err := target.Remove("/d/b"); err != nil {
		t.Fatalf("remove: %s", err)
	}
	if _, _, err := target.Lookup("/d/b"); err == nil {
		t.Fatal("removed file still resolves")
	}
}

// TestHandlesAreStableAcrossRestart is the property that separates this handler
// from go-nfs's helpers.CachingHandler, which mints a random UUID per path and
// keeps the mapping only in memory — so restarting its server makes every
// handle a client holds answer ESTALE. Ours are derived from the inode, so the
// same file yields the same bytes from a cold server.
func TestHandlesAreStableAcrossRestart(t *testing.T) {
	jfs, m := createTestFS(t)

	mk := func() *Handler {
		nf := NewFilesystem(jfs, meta.Background(), false)
		t.Cleanup(nf.Close)
		h, err := NewHandler(nf, m, false)
		if err != nil {
			t.Fatalf("new handler: %s", err)
		}

		return h
	}

	first := mk()
	if eno := jfs.MkdirAll(meta.Background(), "/deep/nested", 0o755, 0o022); eno != 0 {
		t.Fatalf("mkdirall: %s", eno)
	}
	f, eno := jfs.Create(meta.Background(), "/deep/nested/file", 0o644, 0o022)
	if eno != 0 {
		t.Fatalf("create: %s", eno)
	}
	_ = f.Close(meta.Background())

	path := []string{"deep", "nested", "file"}
	h1 := first.ToHandle(first.fs, path)
	if len(h1) != handleSize {
		t.Fatalf("handle is %d bytes, want %d", len(h1), handleSize)
	}

	// A second handler shares no state with the first: same situation a client
	// is in after the server restarts.
	second := mk()
	h2 := second.ToHandle(second.fs, path)
	if !bytes.Equal(h1, h2) {
		t.Fatalf("handle changed across a restart: %x then %x", h1, h2)
	}

	// And the cold handler can resolve a handle it never minted, via the
	// meta.GetPaths fallback rather than an ESTALE.
	third := mk()
	_, p, err := third.FromHandle(h1)
	if err != nil {
		t.Fatalf("cold FromHandle: %s", err)
	}
	if got := filepath.Join(p...); got != filepath.Join(path...) {
		t.Fatalf("cold resolve gave %q, want %q", got, filepath.Join(path...))
	}
	if _, misses := third.paths.stats(); misses == 0 {
		t.Error("expected the cold resolve to miss the path cache and use GetPaths")
	}
}

func TestHandleDecodeRejectsForeignBytes(t *testing.T) {
	if _, err := decodeHandle(nil); err == nil {
		t.Error("an empty handle should not decode")
	}
	if _, err := decodeHandle(make([]byte, handleSize)); err == nil {
		t.Error("a zeroed handle has the wrong magic and should not decode")
	}
	good := encodeHandle(42)
	ino, err := decodeHandle(good)
	if err != nil {
		t.Fatalf("round trip: %s", err)
	}
	if ino != 42 {
		t.Fatalf("decoded inode %d, want 42", ino)
	}
}

// TestAdapterReadDir isolates the billy adapter from the protocol: if this
// passes while a READDIRPLUS over the wire comes back empty, the fault is in
// go-nfs's path handling or mine, not in pkg/fs.
func TestAdapterReadDir(t *testing.T) {
	jfs, m := createTestFS(t)
	nf := NewFilesystem(jfs, meta.Background(), false)
	t.Cleanup(nf.Close)
	_ = m

	if err := nf.MkdirAll("/d", 0o755); err != nil {
		t.Fatalf("mkdirall: %s", err)
	}
	for _, name := range []string{"/d/a", "/d/b"} {
		f, err := nf.Create(name)
		if err != nil {
			t.Fatalf("create %s: %s", name, err)
		}
		_ = f.Close()
	}

	// Both spellings go-nfs can hand us: "d" from fs.Join(["d"]...) and "/d".
	for _, probe := range []string{"d", "/d"} {
		fis, err := nf.ReadDir(probe)
		if err != nil {
			t.Fatalf("ReadDir(%q): %s", probe, err)
		}
		var names []string
		for _, fi := range fis {
			names = append(names, fi.Name())
		}
		t.Logf("ReadDir(%q) = %v", probe, names)
		if len(fis) != 2 {
			t.Errorf("ReadDir(%q) returned %d entries, want 2 (%v)", probe, len(fis), names)
		}
	}
}
