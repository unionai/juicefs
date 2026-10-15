package nfs

import (
	"bytes"
	"fmt"
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

// These measure the server and the JuiceFS path beneath it over a real
// loopback NFSv3 conversation. They deliberately do NOT measure a kernel mount
// — that needs CAP_SYS_ADMIN, which the sandbox this was written in does not
// have, and the end-to-end number belongs on a real client anyway (the darwin
// CI job does that). What these are for is a fast iteration loop on the parts
// we control: the handle pool, the path cache, and the per-RPC work.

// benchFS differs from createTestFS in one way that matters: the object store
// is file-backed, not "mem". With "mem" the chunks accumulate in the process
// heap for the whole run, and `go test -bench` keeps raising b.N until the
// timer budget is met — so a multi-megabyte write benchmark climbs until the
// cgroup kills it. That is how the first version of this file died: "signal:
// killed", no panic, no OOM line of its own.
func benchFS(b *testing.B) (*fs.FileSystem, meta.Meta) {
	b.Helper()
	m := meta.NewClient("memkv://", nil)
	format := &meta.Format{Name: "bench", BlockSize: 4096, Capacity: 1 << 30, DirStats: true}
	if err := m.Init(format, true); err != nil {
		b.Fatalf("meta init: %s", err)
	}
	conf := vfs.Config{
		Meta: meta.DefaultConf(),
		Chunk: &chunk.Config{
			BlockSize:   format.BlockSize << 10,
			MaxUpload:   20,
			MaxDownload: 20,
			BufferSize:  300 << 20,
			CacheDir:    filepath.Join(b.TempDir(), "cache"),
			CacheSize:   1 << 30,
		},
		DirEntryTimeout: time.Second,
		EntryTimeout:    time.Second,
		AttrTimeout:     time.Second,
		AccessLog:       filepath.Join(b.TempDir(), "access.log"),
	}
	objStore, err := object.CreateStorage("file", filepath.Join(b.TempDir(), "chunks")+"/", "", "", "")
	if err != nil {
		b.Fatalf("object store: %s", err)
	}
	store := chunk.NewCachedStore(objStore, *conf.Chunk, nil)
	jfs, err := fs.NewFileSystem(&conf, m, store, nil)
	if err != nil {
		b.Fatalf("new filesystem: %s", err)
	}
	b.Cleanup(func() { _ = jfs.Close() })

	return jfs, m
}

func benchTarget(b *testing.B) (*Server, *nfsc.Target) {
	b.Helper()
	jfs, m := benchFS(b)
	srv, err := NewServer("127.0.0.1:0", jfs, m, meta.Background(), false)
	if err != nil {
		b.Fatalf("new server: %s", err)
	}
	go func() { _ = srv.Serve() }()
	b.Cleanup(func() { _ = srv.Close() })

	c, err := rpc.DialTCP("tcp", (&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: srv.Port()}).String(), false)
	if err != nil {
		b.Fatalf("dial: %s", err)
	}
	b.Cleanup(c.Close)
	var mounter nfsc.Mount
	mounter.Client = c
	target, err := mounter.Mount("/", rpc.AuthNull)
	if err != nil {
		b.Fatalf("mount: %s", err)
	}
	b.Cleanup(func() { _ = mounter.Unmount() })

	return srv, target
}

// BenchmarkWriteThroughput is the number the handle pool exists to move. Run it
// with the pool disabled and it should collapse, because go-nfs opens and
// closes the file on every WRITE and each JuiceFS Close flushes.
func BenchmarkWriteThroughput(b *testing.B) {
	// Rewriting ONE file, not a new file per iteration. A new file each time
	// gives every iteration its own inode and therefore its own open, so the
	// pool cannot help and the measurement says nothing about it — the first
	// version of this benchmark did that and reported opens == reuses, which
	// looks like a broken pool and is really a broken benchmark. It also grew
	// without bound.
	for _, size := range []int{1 << 20, 8 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			srv, target := benchTarget(b)
			payload := bytes.Repeat([]byte("0123456789abcdef"), size/16)
			if _, err := target.Create("/w", 0o644); err != nil {
				b.Fatalf("create: %s", err)
			}
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f, err := target.OpenFile("/w", 0o644)
				if err != nil {
					b.Fatalf("openfile: %s", err)
				}
				if _, err := f.Write(payload); err != nil {
					b.Fatalf("write: %s", err)
				}
				if err := f.Close(); err != nil {
					b.Fatalf("close: %s", err)
				}
			}
			b.StopTimer()
			opened, reused, hits, misses := srv.Stats()
			b.ReportMetric(float64(opened), "opens")
			b.ReportMetric(float64(reused), "reuses")
			if opened > 2 {
				b.Errorf("%d JuiceFS opens for one file: the handle pool is not holding across RPCs", opened)
			}
			if misses > 0 {
				b.Errorf("%d path-cache misses: reverse resolution should not happen on a hot mount (hits=%d)", misses, hits)
			}
		})
	}
}

// BenchmarkReadThroughput's absolute MB/s is NOT the read speed a client will
// see, and should not be quoted as one. go-nfs-client issues 8 KiB READs
// regardless of what the server would serve, so a 1 MiB read here is ~128
// round trips — the reuses metric shows it (around 9500 reuses for ~74
// iterations). A kernel client mounted with rsize=1048576 does one. What this
// benchmark is good for is relative: per-RPC overhead, and whether the handle
// pool holds. The honest end-to-end read number comes from the darwin CI job,
// which goes through the kernel's own client.
func BenchmarkReadThroughput(b *testing.B) {
	for _, size := range []int{1 << 20, 8 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			srv, target := benchTarget(b)
			payload := bytes.Repeat([]byte("0123456789abcdef"), size/16)
			f, err := target.OpenFile("/r", 0o644)
			if err != nil {
				b.Fatalf("openfile: %s", err)
			}
			if _, err := f.Write(payload); err != nil {
				b.Fatalf("write: %s", err)
			}
			_ = f.Close()

			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := target.Open("/r")
				if err != nil {
					b.Fatalf("open: %s", err)
				}
				n, err := io.Copy(io.Discard, r)
				if err != nil && err != io.EOF {
					b.Fatalf("read: %s", err)
				}
				_ = r.Close()
				if n != int64(size) {
					b.Fatalf("read %d bytes, want %d", n, size)
				}
			}
			b.StopTimer()
			opened, reused, _, _ := srv.Stats()
			b.ReportMetric(float64(opened), "opens")
			b.ReportMetric(float64(reused), "reuses")
		})
	}
}

// BenchmarkMetadata is the half of the workload NFSv3 is expected to be worse
// at than FUSE: every LOOKUP is an RPC and there is no page-cache
// invalidation, so this is where tuning actimeo shows up.
func BenchmarkMetadata(b *testing.B) {
	b.Run("create", func(b *testing.B) {
		_, target := benchTarget(b)
		if _, err := target.Mkdir("/m", 0o755); err != nil {
			b.Fatalf("mkdir: %s", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := target.Create(fmt.Sprintf("/m/f%d", i), 0o644); err != nil {
				b.Fatalf("create: %s", err)
			}
		}
	})

	b.Run("lookup", func(b *testing.B) {
		_, target := benchTarget(b)
		if _, err := target.Mkdir("/l", 0o755); err != nil {
			b.Fatalf("mkdir: %s", err)
		}
		const n = 512
		for i := 0; i < n; i++ {
			if _, err := target.Create(fmt.Sprintf("/l/f%d", i), 0o644); err != nil {
				b.Fatalf("create: %s", err)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := target.Lookup(fmt.Sprintf("/l/f%d", i%n)); err != nil {
				b.Fatalf("lookup: %s", err)
			}
		}
	})

	b.Run("readdir512", func(b *testing.B) {
		srv, target := benchTarget(b)
		if _, err := target.Mkdir("/d", 0o755); err != nil {
			b.Fatalf("mkdir: %s", err)
		}
		for i := 0; i < 512; i++ {
			if _, err := target.Create(fmt.Sprintf("/d/f%d", i), 0o644); err != nil {
				b.Fatalf("create: %s", err)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ents, err := target.ReadDirPlus("/d")
			if err != nil {
				b.Fatalf("readdirplus: %s", err)
			}
			if len(ents) < 512 {
				b.Fatalf("readdir returned %d entries, want >=512", len(ents))
			}
		}
		b.StopTimer()
		hits, misses := srv.fs.ReaddirHintStats()
		b.ReportMetric(float64(hits), "hintHits")
		b.ReportMetric(float64(misses), "hintMiss")
	})
}

// BenchmarkHandleResolution isolates FromHandle, which every RPC pays. The
// warm case is a map hit; the cold case is meta.GetPaths walking every ancestor
// with a full readdir per level. The gap is why the path cache exists, and the
// reason a server restart here is a pause rather than an ESTALE.
func BenchmarkHandleResolution(b *testing.B) {
	jfs, m := createTestFS(b)
	nf := NewFilesystem(jfs, meta.Background(), false)
	b.Cleanup(nf.Close)
	h, err := NewHandler(nf, m, false)
	if err != nil {
		b.Fatalf("handler: %s", err)
	}
	if eno := jfs.MkdirAll(meta.Background(), "/a/b/c/d", 0o755, 0o022); eno != 0 {
		b.Fatalf("mkdirall: %s", eno)
	}
	f, eno := jfs.Create(meta.Background(), "/a/b/c/d/file", 0o644, 0o022)
	if eno != 0 {
		b.Fatalf("create: %s", eno)
	}
	_ = f.Close(meta.Background())
	fh := h.ToHandle(nf, []string{"a", "b", "c", "d", "file"})

	b.Run("warm", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, _, err := h.FromHandle(fh); err != nil {
				b.Fatalf("fromhandle: %s", err)
			}
		}
	})

	b.Run("cold", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Evict so every iteration takes the GetPaths path.
			h.paths.m.Purge()
			if _, _, err := h.FromHandle(fh); err != nil {
				b.Fatalf("fromhandle: %s", err)
			}
		}
	})
}
