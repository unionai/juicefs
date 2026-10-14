package nfs

import (
	"os"
	"path"
	"strings"
	"syscall"

	billy "github.com/go-git/go-billy/v5"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// Filesystem adapts a JuiceFS *fs.FileSystem to billy.Filesystem, which is the
// shape go-nfs insists on.
//
// pkg/fs rather than pkg/vfs, deliberately. pkg/vfs is the inode-based layer
// the FUSE server uses and would be the better fit if we were implementing the
// NFSv3 procedures ourselves — handles are inodes, and no path would ever be
// needed. But go-nfs's Handler is path-based (ToHandle takes a []string), so a
// path layer is required either way, and pkg/fs already is one: it caches
// entries and attributes keyed by inode, and exposes FileStat.Inode() and
// Pread/Pwrite, which is everything this adapter needs.
type Filesystem struct {
	fs   *fs.FileSystem
	ctx  meta.Context
	open *openFiles

	readOnly bool
	umask    uint16
}

var _ billy.Filesystem = (*Filesystem)(nil)

// NewFilesystem wraps fs for service over NFS. ctx carries the uid/gid every
// operation runs as: NFSv3 AUTH_UNIX credentials are not honoured, because the
// server listens only on loopback and a local client's claimed uid is not a
// thing we can verify. One identity for the whole mount is the honest model.
func NewFilesystem(f *fs.FileSystem, ctx meta.Context, readOnly bool) *Filesystem {
	return &Filesystem{fs: f, ctx: ctx, open: newOpenFiles(), readOnly: readOnly, umask: 0022}
}

// Close releases the shared handles. Dirty data is flushed by the Close of each
// underlying JuiceFS handle, so this must run before the metadata engine goes
// away or the tail of a write is lost.
func (n *Filesystem) Close() { n.open.close() }

// Stats exposes both counters a benchmark needs to tell a healthy mount from
// one that has silently fallen back to go-nfs's per-RPC open or to reverse
// path resolution.
func (n *Filesystem) Stats() (opened, reused uint64) { return n.open.stats() }

func (n *Filesystem) Join(elem ...string) string { return path.Join(elem...) }

func (n *Filesystem) Root() string { return "/" }

// clean normalises a billy path to one pkg/fs accepts: rooted, no trailing
// slash except at the root.
func clean(name string) string {
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	name = path.Clean(name)

	return name
}

func (n *Filesystem) Stat(name string) (os.FileInfo, error) {
	fi, eno := n.fs.Stat(n.ctx, clean(name))
	if eno != 0 {
		return nil, econv(eno)
	}

	return fi, nil
}

func (n *Filesystem) Lstat(name string) (os.FileInfo, error) {
	fi, eno := n.fs.Lstat(n.ctx, clean(name))
	if eno != 0 {
		return nil, econv(eno)
	}

	return fi, nil
}

// inodeOf is how a path becomes a handle. It rides pkg/fs's attribute cache, so
// a repeat on a hot path is not a metadata round trip.
func (n *Filesystem) inodeOf(name string) (meta.Ino, error) {
	fi, eno := n.fs.Lstat(n.ctx, clean(name))
	if eno != 0 {
		return 0, econv(eno)
	}

	return fi.Inode(), nil
}

func (n *Filesystem) Open(name string) (billy.File, error) {
	return n.OpenFile(name, os.O_RDONLY, 0)
}

func (n *Filesystem) Create(name string) (billy.File, error) {
	return n.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (n *Filesystem) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	p := clean(name)
	if n.readOnly && flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0 {
		return nil, syscall.EROFS
	}

	var mode uint32
	if flag&os.O_WRONLY == 0 {
		mode |= vfs.MODE_MASK_R
	}
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_TRUNC) != 0 {
		mode |= vfs.MODE_MASK_W
	}

	// Create before the inode lookup, since there may not be an inode yet.
	// Create's own handle is dropped immediately: the shared one below is what
	// serves RPCs, and holding two would defeat one-handle-per-inode.
	if flag&os.O_CREATE != 0 {
		if _, eno := n.fs.Lstat(n.ctx, p); eno == syscall.ENOENT {
			f, eno := n.fs.Create(n.ctx, p, uint16(perm.Perm()), n.umask)
			if eno != 0 {
				return nil, econv(eno)
			}
			_ = f.Close(n.ctx)
		} else if eno != 0 {
			return nil, econv(eno)
		} else if flag&os.O_EXCL != 0 {
			return nil, os.ErrExist
		}
	}

	ino, err := n.inodeOf(p)
	if err != nil {
		return nil, err
	}

	ref, eno := n.open.acquire(ino, func() (*fs.File, syscall.Errno) {
		return n.fs.Open(n.ctx, p, mode)
	})
	if eno != 0 {
		return nil, econv(eno)
	}
	ref.name = p
	ref.ctx = n.ctx

	// O_TRUNC is applied after acquiring, so it also takes effect when the
	// handle was already open for someone else.
	if flag&os.O_TRUNC != 0 {
		if eno := n.fs.Truncate(n.ctx, p, 0); eno != 0 {
			_ = ref.Close()

			return nil, econv(eno)
		}
	}

	return ref, nil
}

func (n *Filesystem) ReadDir(name string) ([]os.FileInfo, error) {
	// Directory handles are not pooled: readdir is not the throughput path, and
	// a pooled directory handle would need its own invalidation on every
	// create/remove inside it.
	f, eno := n.fs.Open(n.ctx, clean(name), vfs.MODE_MASK_R)
	if eno != 0 {
		return nil, econv(eno)
	}
	defer func() { _ = f.Close(n.ctx) }()
	fis, eno := f.Readdir(n.ctx, -1)
	if eno != 0 {
		return nil, econv(eno)
	}

	// go-nfs expects "." and ".." gone; it synthesises them itself in READDIR
	// and handles them specially in LOOKUP.
	out := make([]os.FileInfo, 0, len(fis))
	for _, fi := range fis {
		if nm := fi.Name(); nm == "." || nm == ".." {
			continue
		}
		out = append(out, fi)
	}

	return out, nil
}

func (n *Filesystem) MkdirAll(name string, perm os.FileMode) error {
	if n.readOnly {
		return syscall.EROFS
	}

	return econv(n.fs.MkdirAll(n.ctx, clean(name), uint16(perm.Perm()), n.umask))
}

func (n *Filesystem) Rename(oldpath, newpath string) error {
	if n.readOnly {
		return syscall.EROFS
	}
	// A rename over an existing file unlinks it. Any pooled handle for that
	// victim inode must go, or a later open of the path could be served a
	// handle to the thing that used to be there.
	if ino, err := n.inodeOf(newpath); err == nil {
		n.open.forget(ino)
	}

	return econv(n.fs.Rename(n.ctx, clean(oldpath), clean(newpath), 0))
}

func (n *Filesystem) Remove(name string) error {
	if n.readOnly {
		return syscall.EROFS
	}
	p := clean(name)
	fi, eno := n.fs.Lstat(n.ctx, p)
	if eno != 0 {
		return econv(eno)
	}
	if fi.IsDir() {
		return econv(n.fs.Rmdir(n.ctx, p))
	}
	// Dropped before the unlink: afterwards the inode may be gone and Close
	// would be operating on a dead handle.
	n.open.forget(fi.Inode())

	return econv(n.fs.Unlink(n.ctx, p))
}

func (n *Filesystem) Symlink(target, link string) error {
	if n.readOnly {
		return syscall.EROFS
	}

	return econv(n.fs.Symlink(n.ctx, target, clean(link)))
}

func (n *Filesystem) Readlink(link string) (string, error) {
	t, eno := n.fs.Readlink(n.ctx, clean(link))
	if eno != 0 {
		return "", econv(eno)
	}

	return string(t), nil
}

// TempFile is in billy.Filesystem but no NFSv3 procedure needs it: the client
// creates its own temporaries through CREATE like any other file. Refusing is
// better than inventing a scheme nothing will exercise or test.
func (n *Filesystem) TempFile(dir, prefix string) (billy.File, error) {
	return nil, syscall.ENOTSUP
}

// Chroot likewise. The mount root is fixed when the server starts; go-nfs never
// calls this for an NFSv3 mount, and a half-working jail would be worse than
// none.
func (n *Filesystem) Chroot(p string) (billy.Filesystem, error) {
	return nil, syscall.ENOTSUP
}
