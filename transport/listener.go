package urihandler

import (
	"context"
	"errors"
	"net"
	"sync"
)

// Listener accepts distinct TCP/Unix connections. Each AcceptInput/AcceptOutput
// assigns the next connection one direction. Closing the listener does not close
// already-returned endpoints; their original parent context still governs them.
type Listener struct {
	parent      context.Context
	ctx         context.Context
	cancel      context.CancelFunc
	listener    net.Listener
	options     Options
	connections chan net.Conn
	done        chan struct{}
	once        sync.Once
	result      error
}

func Listen(ctx context.Context, raw string, opts Options) (*Listener, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o, err := options(opts)
	if err != nil {
		return nil, err
	}
	e, err := parseEndpoint(raw)
	if err != nil {
		return nil, err
	}
	if e.scheme != "tcp" && e.scheme != "unix" {
		return nil, ErrInvalidURI
	}
	if err = validateTransportOptions(e, o, false); err != nil {
		return nil, err
	}
	var lc net.ListenConfig
	n, err := lc.Listen(ctx, e.scheme, e.address)
	if err != nil {
		return nil, err
	}
	// Never unlink an existing caller path during setup or our socket on close.
	// Unix listener creation itself refuses an occupied path.
	if u, ok := n.(*net.UnixListener); ok {
		u.SetUnlinkOnClose(false)
	}
	internal, cancel := context.WithCancel(ctx)
	l := &Listener{parent: ctx, ctx: internal, cancel: cancel, listener: n, options: o, connections: make(chan net.Conn), done: make(chan struct{})}
	go l.run()
	return l, nil
}
func (l *Listener) Addr() net.Addr        { return l.listener.Addr() }
func (l *Listener) Close() error          { l.once.Do(func() { l.cancel(); l.listener.Close() }); return nil }
func (l *Listener) Done() <-chan struct{} { return l.done }
func (l *Listener) Wait(ctx context.Context) error {
	select {
	case <-l.done:
		return l.result
	default:
	}
	select {
	case <-l.done:
		return l.result
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (l *Listener) run() {
	stop := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-l.ctx.Done():
			l.listener.Close()
		case <-stop:
		}
	}()
	defer func() {
		close(stop)
		l.listener.Close()
		<-watchDone
		l.cancel()
		close(l.connections)
		close(l.done)
	}()
	for {
		c, err := l.listener.Accept()
		if err != nil {
			if l.ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				l.result = err
			}
			return
		}
		select {
		case l.connections <- c:
		case <-l.ctx.Done():
			c.Close()
			return
		}
	}
}
func (l *Listener) accept(ctx context.Context) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c, ok := <-l.connections:
		if !ok {
			return nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			c.Close()
			return nil, err
		}
		if l.ctx.Err() != nil {
			c.Close()
			return nil, ErrClosed
		}
		return c, nil
	}
}
func (l *Listener) AcceptInput(ctx context.Context) (*Input, error) {
	c, err := l.accept(ctx)
	if err != nil {
		return nil, err
	}
	return newInput(l.parent, connectionTransport(c, l.options), l.options), nil
}
func (l *Listener) AcceptOutput(ctx context.Context) (*Output, error) {
	c, err := l.accept(ctx)
	if err != nil {
		return nil, err
	}
	return newOutput(l.parent, connectionTransport(c, l.options), l.options), nil
}
