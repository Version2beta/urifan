//go:build linux || darwin

package urihandler

import (
	"fmt"
	"syscall"
	"time"
	"unsafe"
)

func readableFD(fd int, timeout time.Duration) (bool, error) { return readyFD(fd, timeout, false) }
func writableFD(fd int, timeout time.Duration) (bool, error) { return readyFD(fd, timeout, true) }
func readyFD(fd int, timeout time.Duration, write bool) (bool, error) {
	var set syscall.FdSet
	width := int(unsafe.Sizeof(set.Bits[0]) * 8)
	if fd < 0 || fd >= len(set.Bits)*width {
		return false, fmt.Errorf("urihandler: FIFO descriptor exceeds select capacity")
	}
	index := fd / width
	bit := uint(fd % width)
	set.Bits[index] |= 1 << bit
	tv := syscall.NsecToTimeval(timeout.Nanoseconds())
	var err error
	if write {
		err = selectWrite(fd+1, &set, &tv)
	} else {
		err = selectRead(fd+1, &set, &tv)
	}
	if err == syscall.EINTR {
		return false, nil
	}
	return set.Bits[index]&(1<<bit) != 0, err
}
