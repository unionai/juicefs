// Package nfs serves a JuiceFS volume over NFSv3 on a loopback socket, so a
// client can mount it with the OS's own NFS client and no FUSE at all.
//
// This exists for macOS. Mounting there otherwise needs macFUSE, a kernel
// extension whose load requires an interactive approval — which rules out
// unattended installs and CI entirely. `mount_nfs` against 127.0.0.1 needs
// neither root nor a kext, provided the server does not ask for a reserved
// port.
//
// On Linux the kernel mount still needs CAP_SYS_ADMIN, so this is NOT a
// replacement for the uvol mount broker in unprivileged task pods.
package nfs

import (
	"encoding/binary"
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/juicedata/juicefs/pkg/meta"
)

// handleMagic tags our handles so a handle minted by some other server (or an
// older layout of this one) is rejected as stale rather than silently decoded
// into a wrong inode.
const handleMagic uint64 = 0x6a66734e46537631 // "jfsNFSv1"

// handleSize is well under go-nfs's FHSize of 64.
const handleSize = 24

// encodeHandle lays out magic | inode | reserved.
//
// The encoding is DELIBERATELY deterministic: the same inode always yields the
// same bytes. go-nfs's own helpers.CachingHandler mints a random UUID per path
// and keeps the mapping only in memory, so restarting the server invalidates
// every handle its clients hold and the mount starts answering ESTALE — on a
// laptop that merely slept. Deriving the handle from the inode means a restart
// costs us a cold path cache, not a broken mount.
//
// reserved is a generation field, zero for now. JuiceFS does not recycle
// inodes within a volume's life, so there is nothing to disambiguate yet; the
// space is here so adding one later does not change the handle length.
func encodeHandle(ino meta.Ino) []byte {
	b := make([]byte, handleSize)
	binary.BigEndian.PutUint64(b[0:8], handleMagic)
	binary.BigEndian.PutUint64(b[8:16], uint64(ino))
	binary.BigEndian.PutUint64(b[16:24], 0)

	return b
}

func decodeHandle(fh []byte) (meta.Ino, error) {
	if len(fh) != handleSize {
		return 0, fmt.Errorf("nfs handle is %d bytes, want %d", len(fh), handleSize)
	}
	if got := binary.BigEndian.Uint64(fh[0:8]); got != handleMagic {
		return 0, fmt.Errorf("nfs handle magic %#x is not ours", got)
	}

	return meta.Ino(binary.BigEndian.Uint64(fh[8:16])), nil
}

// pathCache resolves an inode back to a path.
//
// Every path we ever hand out came through ToHandle, where the full path was in
// hand — so the cache is filled on the way down and the hot path never pays a
// reverse lookup. A miss is not an error: it falls back to meta.GetPaths, which
// is correct but walks every ancestor doing a full readdir per level to find
// each child's name, so it is for a cold start, never for steady state.
//
// That asymmetry is the point. Here the map is an optimisation and losing an
// entry costs one slow resolution; in go-nfs's caching handler the equivalent
// map IS the handle's meaning, so losing an entry costs the client an ESTALE.
type pathCache struct {
	m *lru.Cache[meta.Ino, string]

	mu     sync.Mutex
	misses uint64
	hits   uint64
}

func newPathCache(size int) (*pathCache, error) {
	m, err := lru.New[meta.Ino, string](size)
	if err != nil {
		return nil, err
	}

	return &pathCache{m: m}, nil
}

func (c *pathCache) put(ino meta.Ino, path string) {
	c.m.Add(ino, path)
}

func (c *pathCache) get(ino meta.Ino) (string, bool) {
	p, ok := c.m.Get(ino)
	c.mu.Lock()
	if ok {
		c.hits++
	} else {
		c.misses++
	}
	c.mu.Unlock()

	return p, ok
}

// stats reports hits and misses, so a benchmark can tell a slow mount caused by
// reverse-lookup churn from one that is slow for some other reason.
func (c *pathCache) stats() (hits, misses uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.hits, c.misses
}
