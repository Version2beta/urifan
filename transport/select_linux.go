//go:build linux

package urihandler

import "syscall"

func selectRead(n int, set *syscall.FdSet, tv *syscall.Timeval) error {
	_, err := syscall.Select(n, set, nil, nil, tv)
	return err
}
func selectWrite(n int, set *syscall.FdSet, tv *syscall.Timeval) error {
	_, err := syscall.Select(n, nil, set, nil, tv)
	return err
}
