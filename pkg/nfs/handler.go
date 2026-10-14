package nfs

import (
	"context"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	billy "github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"

	"github.com/juicedata/juicefs/pkg/meta"
)

// pathCacheSize bounds the inode->path map. A miss is a slow-but-correct
// meta.GetPaths, never an error, so this is a memory/latency dial and not a
// correctness one — unlike go-nfs's own handle cache, where an eviction costs
// the client an ESTALE.
const pathCacheSize = 1 << 16

// Handler implements nfs.Handler over one JuiceFS volume.
//
// What it does differently from helpers.NewCachingHandler, which is what most
// go-nfs users (rclone included) mount with:
//
//   - A handle is derived from the inode, not minted as a random UUID. So the
//     same file always has the same handle, and restarting the server does not
//     invalidate the handles its clients are holding. For a Mac that sleeps and
//     wakes, that is the difference between a pause and an ESTALE.
//   - FromHandle is a map hit, not a scan. The caching handler's FromHandle
//     walks every key in its LRU on a miss.
//   - Eviction costs one reverse resolution instead of breaking the handle.
type Handler struct {
	fs    *Filesystem
	paths *pathCache
	meta  meta.Meta

	readOnly bool
}

var _ nfs.Handler = (*Handler)(nil)

func NewHandler(f *Filesystem, m meta.Meta, readOnly bool) (*Handler, error) {
	pc, err := newPathCache(pathCacheSize)
	if err != nil {
		return nil, err
	}

	return &Handler{fs: f, paths: pc, meta: m, readOnly: readOnly}, nil
}

// Stats reports handle-cache effectiveness alongside the open-handle counters.
func (h *Handler) Stats() (opened, reused, pathHits, pathMisses uint64) {
	o, r := h.fs.Stats()
	hits, misses := h.paths.stats()

	return o, r, hits, misses
}

// Mount hands out the single filesystem this server was started for.
//
// AuthFlavorNull only: the server listens on loopback, and an AUTH_UNIX uid
// asserted by a local client is not something it can verify, so honouring one
// would be security theatre. Every operation runs as the identity fixed when
// the server started.
func (h *Handler) Mount(_ context.Context, _ net.Conn, _ nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	return nfs.MountStatusOk, h.fs, []nfs.AuthFlavor{nfs.AuthFlavorNull}
}

// Change returns nil on a read-only mount, which is how go-nfs learns to refuse
// the mutating procedures.
func (h *Handler) Change(fs billy.Filesystem) billy.Change {
	if h.readOnly {
		return nil
	}
	if c, ok := fs.(billy.Change); ok {
		return c
	}

	return nil
}

func (h *Handler) FSStat(ctx context.Context, _ billy.Filesystem, s *nfs.FSStat) error {
	total, avail := h.fs.fs.StatFS(h.fs.ctx)
	s.TotalSize = total
	s.FreeSize = avail
	s.AvailableSize = avail
	// JuiceFS has no fixed inode budget, so there is no honest number to report
	// here. Reporting zero free files would make a client refuse to create
	// anything, so advertise the same large figure for total and free: "not a
	// constraint" is the truth, and it is what the FUSE mount effectively tells
	// the kernel too.
	const manyFiles = 1 << 40
	s.TotalFiles = manyFiles
	s.FreeFiles = manyFiles
	s.AvailableFiles = manyFiles
	// Nothing about this volume's layout is invariant: another client can be
	// writing to the same volume through a different mount.
	s.CacheHint = 0

	return nil
}

// ToHandle resolves the path to an inode and encodes that. It also records the
// inode->path mapping, which is what keeps FromHandle off the slow reverse
// lookup: every path we ever hand a client out passes through here first, with
// the path already in hand.
func (h *Handler) ToHandle(_ billy.Filesystem, path []string) []byte {
	p := "/" + strings.Join(path, "/")
	ino, err := h.fs.inodeOf(p)
	if err != nil {
		// go-nfs's signature gives us nowhere to report this. An empty handle
		// fails decode on the way back in and the client gets ESTALE, which is
		// the correct answer for a path that no longer resolves.
		return nil
	}
	h.paths.put(ino, p)

	return encodeHandle(ino)
}

// FromHandle decodes the inode and turns it back into a path, because go-nfs's
// procedures are path-based.
//
// The path is returned as a slice, not a single string, because go-nfs computes
// ".." by truncating it (nfs_onlookup.go: pPath := p[0:len(p)-1]). Returning a
// one-element opaque token would make every ".." resolve to the root.
func (h *Handler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	ino, err := decodeHandle(fh)
	if err != nil {
		return nil, nil, err
	}
	if ino == meta.RootInode {
		return h.fs, []string{}, nil
	}

	p, ok := h.paths.get(ino)
	if !ok {
		// Cold path: after a server restart, or an eviction. GetPaths walks
		// every ancestor and does a full readdir per level to find each child's
		// name, so this is O(depth x dirsize) and must never be the steady
		// state — but it is correct, and it is why a restart is a pause here
		// rather than a broken mount.
		paths := h.meta.GetPaths(h.fs.ctx, ino)
		if len(paths) == 0 {
			return nil, nil, os.ErrNotExist
		}
		p = paths[0]
		h.paths.put(ino, p)
	}

	return h.fs, splitPath(p), nil
}

func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return []string{}
	}

	return strings.Split(p, "/")
}

// InvalidateHandle is called when the client tells us a handle is gone. The
// handle bytes stay valid by construction — they are the inode — so there is
// nothing to revoke; drop the cached path and the pooled file so the next
// reference re-resolves.
func (h *Handler) InvalidateHandle(_ billy.Filesystem, fh []byte) error {
	ino, err := decodeHandle(fh)
	if err != nil {
		return err
	}
	h.fs.open.forget(ino)

	return nil
}

// readdirBatchCap bounds how many entries one READDIR/READDIRPLUS reply may
// carry. See HandleLimit for why this is spelled as a handle limit.
const readdirBatchCap = 1 << 20

// HandleLimit is named for how many handles may be outstanding, but go-nfs also
// uses it as the per-reply entry cap in READDIR and READDIRPLUS:
//
//	maxEntities := userHandle.HandleLimit() / 2
//	... if len(entities) > maxEntities { eof = false; break }
//
// So it must be a positive number even for a handler with no real limit. An
// earlier version of this returned -1, reasoning that our handles cost nothing
// to keep because they are derived rather than stored — which made maxEntities
// zero, so every READDIRPLUS broke out of the loop before emitting a single
// real entry and every directory on the mount looked empty while LOOKUP of the
// very same files worked. Return something large instead and let the DirCount
// and MaxCount byte budgets in the request do the actual bounding, which is
// what the protocol intends.
func (h *Handler) HandleLimit() int { return readdirBatchCap }

// Chmod, Lchown, Chown and Chtimes make Filesystem a billy.Change, which is
// what backs SETATTR. They live here rather than in filesystem.go to keep the
// billy.Filesystem surface and the billy.Change surface legible apart.

var _ billy.Change = (*Filesystem)(nil)

func (n *Filesystem) Chmod(name string, mode os.FileMode) error {
	if n.readOnly {
		return syscall.EROFS
	}
	f, eno := n.fs.Open(n.ctx, clean(name), 0)
	if eno != 0 {
		return econv(eno)
	}
	defer func() { _ = f.Close(n.ctx) }()

	return econv(f.Chmod(n.ctx, uint16(mode.Perm())))
}

func (n *Filesystem) Chown(name string, uid, gid int) error {
	if n.readOnly {
		return syscall.EROFS
	}
	f, eno := n.fs.Open(n.ctx, clean(name), 0)
	if eno != 0 {
		return econv(eno)
	}
	defer func() { _ = f.Close(n.ctx) }()

	return econv(f.Chown(n.ctx, uint32(uid), uint32(gid)))
}

// Lchown is Chown on the link itself. pkg/fs has no l-variant, and following
// the link here would silently change the wrong inode's ownership, so refuse
// rather than do the wrong thing quietly. NFSv3 SETATTR on a symlink is rare.
func (n *Filesystem) Lchown(name string, uid, gid int) error {
	return syscall.ENOTSUP
}

func (n *Filesystem) Chtimes(name string, atime, mtime time.Time) error {
	if n.readOnly {
		return syscall.EROFS
	}
	f, eno := n.fs.Open(n.ctx, clean(name), 0)
	if eno != 0 {
		return econv(eno)
	}
	defer func() { _ = f.Close(n.ctx) }()

	return econv(f.Utime2(n.ctx,
		atime.Unix(), int64(atime.Nanosecond()),
		mtime.Unix(), int64(mtime.Nanosecond())))
}
