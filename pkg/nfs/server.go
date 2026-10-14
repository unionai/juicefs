package nfs

import (
	"net"

	nfs "github.com/willscott/go-nfs"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
)

// Server is a running NFSv3 service over one JuiceFS volume.
type Server struct {
	listener net.Listener
	handler  *Handler
	fs       *Filesystem
}

// NewServer binds a loopback listener and prepares the service. Nothing is
// served until Serve is called — and the caller must start serving BEFORE
// running the client's mount command, because mount.nfs/mount_nfs connects
// immediately and fails with EIO against a socket nobody is accepting on.
//
// addr should stay on the loopback interface: there is no authentication (see
// Handler.Mount), so anything that can reach the port has the whole volume.
func NewServer(addr string, f *fs.FileSystem, m meta.Meta, ctx meta.Context, readOnly bool) (*Server, error) {
	nf := NewFilesystem(f, ctx, readOnly)
	h, err := NewHandler(nf, m, readOnly)
	if err != nil {
		return nil, err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		nf.Close()

		return nil, err
	}

	return &Server{listener: l, handler: h, fs: nf}, nil
}

// Addr is the bound address, which matters because a port of 0 is the norm
// here: the caller needs the real port to build the mount command.
func (s *Server) Addr() *net.TCPAddr { return s.listener.Addr().(*net.TCPAddr) }

// Port is what goes into both `port=` and `mountport=` in the mount options.
// go-nfs serves MOUNT and NFS on the same socket, so they are always equal —
// unlike a conventional NFS server, where mountd lives elsewhere and the client
// would consult rpcbind to find it.
func (s *Server) Port() int { return s.Addr().Port }

// Serve blocks until the listener is closed.
func (s *Server) Serve() error { return nfs.Serve(s.listener, s.handler) }

// Close stops accepting and releases the pooled handles. Their Close is what
// flushes dirty data, so this must complete before the metadata engine is torn
// down or the tail of a write is lost.
func (s *Server) Close() error {
	err := s.listener.Close()
	s.fs.Close()

	return err
}

// Stats reports the two things worth watching: whether the open-handle pool is
// actually being reused (otherwise every RPC is paying go-nfs's open/close),
// and whether handle resolution is hitting the path cache (otherwise every RPC
// is paying a full reverse path walk).
func (s *Server) Stats() (opened, reused, pathHits, pathMisses uint64) {
	return s.handler.Stats()
}
