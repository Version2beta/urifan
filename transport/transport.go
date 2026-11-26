package urihandler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type endpoint struct{ scheme, address string }

func parseEndpoint(raw string) (endpoint, error) {
	if strings.Contains(raw, "#") {
		return endpoint{}, ErrInvalidURI
	}
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return endpoint{}, ErrInvalidURI
	}
	e := endpoint{scheme: u.Scheme}
	switch e.scheme {
	case "file", "fifo", "unix":
		if u.Host != "" || !filepath.IsAbs(u.Path) {
			return e, ErrInvalidURI
		}
		e.address = u.Path
	case "tcp", "udp":
		if u.Path != "" || u.Host == "" {
			return e, ErrInvalidURI
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return e, fmt.Errorf("%w: %v", ErrInvalidURI, err)
		}
		e.address = u.Host
	default:
		return e, ErrInvalidURI
	}
	return e, nil
}

type transport struct {
	closer   io.Closer
	read     func([]byte) (int, error)
	writer   io.Writer
	datagram bool
	addr     net.Addr
}

func validateTransportOptions(e endpoint, o Options, output bool) error {
	if (o.FileMode != Exclusive && (e.scheme != "file" || !output)) || (o.Follow && (e.scheme != "file" || output)) || (o.CreateFIFO && e.scheme != "fifo") || (o.DatagramPayloadSize > 0 && (e.scheme != "udp" || !output)) {
		return ErrInvalidOptions
	}
	return nil
}
func openTransport(ctx context.Context, raw string, o Options, output bool) (*transport, error) {
	e, err := parseEndpoint(raw)
	if err != nil {
		return nil, err
	}
	if err = validateTransportOptions(e, o, output); err != nil {
		return nil, err
	}
	switch e.scheme {
	case "file":
		flags := os.O_RDONLY | syscall.O_NONBLOCK
		if output {
			flags = os.O_WRONLY | os.O_CREATE | syscall.O_NONBLOCK
			switch o.FileMode {
			case Exclusive:
				flags |= os.O_EXCL
			case Append:
				flags |= os.O_APPEND
			case Truncate:
				flags |= os.O_TRUNC
			}
		}
		f, err := os.OpenFile(e.address, flags, 0600)
		if err != nil {
			return nil, err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		if !st.Mode().IsRegular() {
			f.Close()
			return nil, fmt.Errorf("%w: regular file required", ErrInvalidURI)
		}
		return &transport{closer: f, read: f.Read, writer: f}, nil
	case "fifo":
		return openFIFO(ctx, e.address, o, output)
	case "tcp", "unix":
		d := net.Dialer{Timeout: o.DialTimeout}
		conn, err := d.DialContext(ctx, e.scheme, e.address)
		if err != nil {
			return nil, err
		}
		return connectionTransport(conn, o), nil
	case "udp":
		var conn *net.UDPConn
		if output {
			d := net.Dialer{Timeout: o.DialTimeout}
			c, dialErr := d.DialContext(ctx, "udp", e.address)
			if dialErr != nil {
				return nil, dialErr
			}
			conn = c.(*net.UDPConn)
		} else {
			var lc net.ListenConfig
			c, listenErr := lc.ListenPacket(ctx, "udp", e.address)
			if listenErr != nil {
				return nil, listenErr
			}
			conn = c.(*net.UDPConn)
		}
		if err = ctx.Err(); err != nil {
			conn.Close()
			return nil, err
		}
		r := &transport{closer: conn, datagram: true, addr: conn.LocalAddr()}
		r.writer = deadlineWriter{conn: conn, timeout: o.IOTimeout}
		r.read = func(b []byte) (int, error) {
			if o.IOTimeout > 0 {
				if err := conn.SetReadDeadline(time.Now().Add(o.IOTimeout)); err != nil {
					return 0, err
				}
			}
			n, _, flags, _, err := conn.ReadMsgUDP(b, nil)
			if flags&syscall.MSG_TRUNC != 0 {
				return 0, ErrDatagramTruncated
			}
			return n, err
		}
		return r, nil
	}
	return nil, ErrInvalidURI
}
func connectionTransport(c net.Conn, o Options) *transport {
	return &transport{closer: c, addr: c.LocalAddr(), writer: deadlineWriter{conn: c, timeout: o.IOTimeout}, read: func(b []byte) (int, error) {
		if o.IOTimeout > 0 {
			if err := c.SetReadDeadline(time.Now().Add(o.IOTimeout)); err != nil {
				return 0, err
			}
		}
		return c.Read(b)
	}}
}

type deadlineWriter struct {
	conn    net.Conn
	timeout time.Duration
}

func (w deadlineWriter) Write(b []byte) (int, error) {
	if w.timeout > 0 {
		if err := w.conn.SetWriteDeadline(time.Now().Add(w.timeout)); err != nil {
			return 0, err
		}
	}
	return w.conn.Write(b)
}

func openFIFO(ctx context.Context, path string, o Options, output bool) (*transport, error) {
	if o.CreateFIFO {
		if err := syscall.Mkfifo(path, 0600); err != nil && err != syscall.EEXIST {
			return nil, err
		}
	}
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if st.Mode()&os.ModeNamedPipe == 0 {
		return nil, fmt.Errorf("%w: named pipe required", ErrInvalidURI)
	}
	flags := os.O_RDONLY | syscall.O_NONBLOCK
	if output {
		flags = os.O_WRONLY | syscall.O_NONBLOCK
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, flags, 0)
		if err == syscall.ENXIO { // wrapped errors are handled below
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(o.PollInterval):
				continue
			}
		}
		if err != nil {
			if pe, ok := err.(*os.PathError); ok && pe.Err == syscall.ENXIO {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(o.PollInterval):
					continue
				}
			}
			return nil, err
		}
		info, err := f.Stat()
		if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
			f.Close()
			if err != nil {
				return nil, err
			}
			return nil, ErrInvalidURI
		}
		control, err := f.SyscallConn()
		if err != nil {
			f.Close()
			return nil, err
		}
		var fd int
		if err = control.Control(func(value uintptr) { fd = int(value) }); err != nil {
			f.Close()
			return nil, err
		}
		if output {
			return &transport{closer: f, writer: &fifoWriter{file: f, fd: fd, ctx: ctx, interval: o.PollInterval}}, nil
		}
		read := func(b []byte) (int, error) {
			for {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				ready, err := readableFD(fd, o.PollInterval)
				if err != nil {
					return 0, err
				}
				if ready {
					return f.Read(b)
				}
				// Input Close uses an internal context; detect its descriptor closure too.
				if _, err := f.Stat(); err != nil {
					return 0, err
				}
			}
		}
		return &transport{closer: f, read: read}, nil
	}
}

type fifoWriter struct {
	file     *os.File
	fd       int
	ctx      context.Context
	interval time.Duration
}

func (w *fifoWriter) Write(bytes []byte) (int, error) {
	for {
		if err := w.ctx.Err(); err != nil {
			return 0, err
		}
		n, err := w.file.Write(bytes)
		if !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EWOULDBLOCK) {
			return n, err
		}
		if n > 0 {
			return n, nil
		}
		for {
			if err := w.ctx.Err(); err != nil {
				return 0, err
			}
			if _, err := w.file.Stat(); err != nil {
				return 0, err
			}
			ready, err := writableFD(w.fd, w.interval)
			if err != nil {
				return 0, err
			}
			if ready {
				break
			}
		}
	}
}

func acquire(ctx context.Context, alloc Allocator, size int) (*Buffer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := alloc(ctx, size)
	if err != nil {
		if b != nil {
			b.Release()
		}
		return nil, err
	}
	if b == nil || cap(b.Data) < size {
		if b != nil {
			b.Release()
		}
		return nil, fmt.Errorf("%w: allocator capacity below %d", ErrInvalidBuffer, size)
	}
	b.Data = b.Data[:size]
	if err := ctx.Err(); err != nil {
		b.Release()
		return nil, err
	}
	return b, nil
}
