package nfs

import (
	"os"
	"syscall"
)

// go-nfs decides an RPC's NFS status largely by sniffing the returned error
// with os.IsNotExist / os.IsExist / os.IsPermission (see statusFromWriteError
// and the per-proc handlers). A bare syscall.Errno satisfies none of those
// predicates, so returning one unmapped makes a plain ENOENT surface to the
// client as NFSStatusIO or NFSStatusServerFault — which a client retries
// instead of reporting. Translate the ones with os sentinels and pass the rest
// through; syscall.Errno is an error, and the generic mapping go-nfs applies to
// an unrecognised error is the right answer for those.
//
// pkg/fs has an equivalent econv for its WebDAV adapter, but it is unexported
// and covers only four errnos.
var errmap = map[syscall.Errno]error{
	syscall.EPERM:        os.ErrPermission,
	syscall.EACCES:       os.ErrPermission,
	syscall.ENOENT:       os.ErrNotExist,
	syscall.EEXIST:       os.ErrExist,
	syscall.EINVAL:       os.ErrInvalid,
	syscall.ENOTDIR:      syscall.ENOTDIR,
	syscall.EISDIR:       syscall.EISDIR,
	syscall.ENOTEMPTY:    syscall.ENOTEMPTY,
	syscall.EROFS:        syscall.EROFS,
	syscall.ENOSPC:       syscall.ENOSPC,
	syscall.EDQUOT:       syscall.EDQUOT,
	syscall.ENAMETOOLONG: syscall.ENAMETOOLONG,
}

// econv turns a JuiceFS errno into an error go-nfs can classify. A zero errno
// is success and must become a nil error, not a non-nil error wrapping zero —
// getting that wrong fails every successful RPC.
func econv(eno syscall.Errno) error {
	if eno == 0 {
		return nil
	}
	if e, ok := errmap[eno]; ok {
		return e
	}

	return eno
}
