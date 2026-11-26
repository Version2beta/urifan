//go:build darwin

package urihandler

import "syscall"

func selectRead(n int, set *syscall.FdSet, tv *syscall.Timeval) error {
	return syscall.Select(n, set, nil, nil, tv)
}
func selectWrite(n int, set *syscall.FdSet, tv *syscall.Timeval) error {
	return syscall.Select(n, nil, set, nil, tv)
}
