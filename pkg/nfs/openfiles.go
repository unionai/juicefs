package nfs

import (
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
)

// go-nfs opens and closes the file on EVERY read and write RPC:
// nfs_onwrite.go does FromHandle -> Stat -> OpenFile(O_RDWR) -> Seek -> Write
// -> Close, and nfs_onread.go opens with a deferred Close. At wsize=1M that is
// roughly a thousand open/close pairs per gigabyte, and on JuiceFS every Close
// flushes — which throws away the chunked writeback the write path is built on.
//
// That cost is why rclone *requires* --vfs-cache-mode full before it will let
// you write to an NFS mount, and why hf-mount makes its disk-staging
// --advanced-writes mandatory on NFS rather than optional. Both are absorbing
// this, not working around some limitation of their own backends.
//
// We do not need a staging layer, because we can decouple the lifetime of the
// JuiceFS handle from the lifetime of a single RPC: keep one open handle per
// inode, hand each RPC a cheap wrapper over it, and close the real handle only
// once it has been idle. A thousand opens become one and writeback survives.
//
// The second half of the problem is that billy.File has no io.WriterAt
// (upstream billy carries a literal "TODO: Add io.WriterAt for v6"), so go-nfs
// must Seek and then Write. Sharing one file offset across concurrent WRITEs to
// different offsets in the same file would corrupt data. So the wrapper keeps
// its OWN offset and lowers Seek+Write onto Pwrite, which takes the offset
// explicitly. Concurrent writes are then independent as well as fast.

// openIdleTTL is how long a handle with no referants is kept before closing.
// Long enough to span the gap between consecutive RPCs of one transfer, short
// enough that an idle mount is not pinning dirty state indefinitely.
const openIdleTTL = 30 * time.Second

type sharedFile struct {
	f    *fs.File
	refs int
	idle time.Time
}

// openFiles keeps at most one JuiceFS handle per inode.
type openFiles struct {
	mu    sync.Mutex
	files map[meta.Ino]*sharedFile
	stop  chan struct{}

	opened uint64 // real JuiceFS opens
	reused uint64 // RPCs served off an already-open handle
}

func newOpenFiles() *openFiles {
	o := &openFiles{files: make(map[meta.Ino]*sharedFile), stop: make(chan struct{})}
	go o.reap()

	return o
}

// acquire returns a reference to the handle for ino, opening it only if nobody
// else holds one. open is called at most once per inode per lifetime.
func (o *openFiles) acquire(ino meta.Ino, open func() (*fs.File, syscall.Errno)) (*fileRef, syscall.Errno) {
	o.mu.Lock()
	if sf, ok := o.files[ino]; ok {
		sf.refs++
		o.reused++
		o.mu.Unlock()

		return &fileRef{o: o, ino: ino, sf: sf}, 0
	}
	o.mu.Unlock()

	// Opened outside the lock: an open can block on metadata I/O and must not
	// stall every other inode's RPCs.
	f, eno := open()
	if eno != 0 {
		return nil, eno
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	// Someone may have opened the same inode while we were. Keep theirs and
	// drop ours, so the one-handle-per-inode invariant holds.
	if sf, ok := o.files[ino]; ok {
		o.mu.Unlock()
		_ = f.Close(meta.Background())
		o.mu.Lock()
		sf.refs++
		o.reused++

		return &fileRef{o: o, ino: ino, sf: sf}, 0
	}
	sf := &sharedFile{f: f, refs: 1}
	o.files[ino] = sf
	o.opened++

	return &fileRef{o: o, ino: ino, sf: sf}, 0
}

// release drops one reference. The handle stays open for openIdleTTL so the
// next RPC of the same transfer reuses it.
func (o *openFiles) release(ino meta.Ino) {
	o.mu.Lock()
	defer o.mu.Unlock()
	sf, ok := o.files[ino]
	if !ok {
		return
	}
	sf.refs--
	if sf.refs <= 0 {
		sf.refs = 0
		sf.idle = time.Now()
	}
}

// forget closes an inode's handle now, referenced or not. Needed when the
// inode's name goes away (remove, rename over) so a later open does not reuse a
// handle to something that no longer exists at that path.
func (o *openFiles) forget(ino meta.Ino) {
	o.mu.Lock()
	sf, ok := o.files[ino]
	if ok {
		delete(o.files, ino)
	}
	o.mu.Unlock()
	if ok {
		_ = sf.f.Close(meta.Background())
	}
}

func (o *openFiles) reap() {
	t := time.NewTicker(openIdleTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-o.stop:
			return
		case <-t.C:
			now := time.Now()
			var dead []*sharedFile
			o.mu.Lock()
			for ino, sf := range o.files {
				if sf.refs == 0 && now.Sub(sf.idle) > openIdleTTL {
					dead = append(dead, sf)
					delete(o.files, ino)
				}
			}
			o.mu.Unlock()
			// Closed outside the lock: Close flushes, which can be slow.
			for _, sf := range dead {
				_ = sf.f.Close(meta.Background())
			}
		}
	}
}

func (o *openFiles) close() {
	close(o.stop)
	o.mu.Lock()
	files := o.files
	o.files = make(map[meta.Ino]*sharedFile)
	o.mu.Unlock()
	for _, sf := range files {
		_ = sf.f.Close(meta.Background())
	}
}

// stats reports real opens against reused handles. If a benchmark shows these
// roughly equal, the cache is not working and we are back to go-nfs's
// per-RPC open.
func (o *openFiles) stats() (opened, reused uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.opened, o.reused
}

// fileRef is one RPC's view of a shared handle: its own offset, somebody
// else's file. It satisfies billy.File.
type fileRef struct {
	o   *openFiles
	ino meta.Ino
	sf  *sharedFile

	name string
	ctx  meta.Context

	mu     sync.Mutex
	offset int64
	closed bool
	dirty  bool
}

func (r *fileRef) Name() string { return r.name }

func (r *fileRef) Read(p []byte) (int, error) {
	r.mu.Lock()
	off := r.offset
	r.mu.Unlock()
	n, err := r.sf.f.Pread(r.ctx, p, off)
	if n > 0 {
		r.mu.Lock()
		r.offset = off + int64(n)
		r.mu.Unlock()
	}

	return n, err
}

// ReadAt is what go-nfs's READ actually calls, so this is the hot read path.
func (r *fileRef) ReadAt(p []byte, off int64) (int, error) {
	return r.sf.f.Pread(r.ctx, p, off)
}

func (r *fileRef) Write(p []byte) (int, error) {
	r.mu.Lock()
	off := r.offset
	r.mu.Unlock()
	// Pwrite, not Write: the offset is explicit, so this reference's writes do
	// not race any other reference's on the same shared handle.
	n, eno := r.sf.f.Pwrite(r.ctx, p, off)
	if n > 0 {
		r.mu.Lock()
		r.offset = off + int64(n)
		r.dirty = true
		r.mu.Unlock()
	}

	return n, econv(eno)
}

func (r *fileRef) Seek(off int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch whence {
	case io.SeekStart:
		r.offset = off
	case io.SeekCurrent:
		r.offset += off
	case io.SeekEnd:
		fi, err := r.sf.f.Stat()
		if err != nil {
			return 0, err
		}
		r.offset = fi.Size() + off
	default:
		return 0, syscall.EINVAL
	}
	if r.offset < 0 {
		r.offset = 0

		return 0, syscall.EINVAL
	}

	return r.offset, nil
}

func (r *fileRef) Truncate(size int64) error {
	return econv(r.sf.f.Truncate(r.ctx, uint64(size)))
}

// Close drops this reference only. The JuiceFS handle outlives it; see the
// comment at the top of this file for why that is the whole point.
//
// It does FLUSH a reference that wrote, though, and deferring that too was a
// real bug: JuiceFS buffers writes and the inode's length is only published on
// flush, so a WRITE answered without one leaves the file apparently zero-length
// — and go-nfs's READ consults fs.Stat to decide how much there is to read, so
// every subsequent read returned EOF immediately on a file that had just been
// written. NFSv3 WRITE with how=fileSync is also required to reach stable
// storage before replying, so flushing here is the protocol's requirement as
// much as ours. Flush, not Close: durability without giving up the handle.
func (r *fileRef) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return nil
	}
	r.closed = true
	dirty := r.dirty
	r.mu.Unlock()

	var err error
	if dirty {
		err = econv(r.sf.f.Flush(r.ctx))
	}
	r.o.release(r.ino)

	return err
}

// Lock and Unlock are billy.File's flock. NFSv3 locking is a separate protocol
// (NLM) that this server does not implement, and clients are mounted with
// locallocks/nolock so the client kernel handles advisory locks itself. Nothing
// reaches here, so there is nothing to do rather than something to refuse:
// returning an error would fail RPCs that merely pass through billy.
func (r *fileRef) Lock() error   { return nil }
func (r *fileRef) Unlock() error { return nil }
