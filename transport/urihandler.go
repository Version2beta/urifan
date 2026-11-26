// Package urihandler opens URI-addressed pipeline endpoints. Queues transfer
// buffer custody, not delivery acknowledgements.
package urihandler

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

var (
	ErrClosed            = errors.New("urihandler: endpoint closed")
	ErrInvalidURI        = errors.New("urihandler: invalid or unsupported URI")
	ErrInvalidOptions    = errors.New("urihandler: invalid options")
	ErrInvalidBuffer     = errors.New("urihandler: invalid buffer")
	ErrDatagramTruncated = errors.New("urihandler: datagram exceeds receive buffer")
	ErrFrame             = errors.New("urihandler: invalid record framing")
	errGraceful          = errors.New("urihandler: input closed normally")
)

// Buffer is one lease over reusable storage. Do not copy a Buffer, resubmit the
// same lease, or access its data after transferring custody. Release is idempotent.
// A successful Send transfers custody; a rejected Send does not.
type Buffer struct {
	Data    []byte
	release func()
	once    sync.Once
}

// NewBuffer wraps storage without copying it. release may be nil (GC-managed
// storage); otherwise it must return promptly and must not call endpoint Wait.
func NewBuffer(data []byte, release func()) *Buffer { return &Buffer{Data: data, release: release} }
func (b *Buffer) Release() {
	if b != nil {
		b.once.Do(func() {
			if b.release != nil {
				b.release()
			}
		})
	}
}

// Allocator lends a buffer with at least size bytes of capacity. It must observe
// cancellation if it blocks. On delivery the consumer must eventually Release.
// An error with a non-nil buffer returns that buffer to its release action.
type Allocator func(context.Context, int) (*Buffer, error)

// Options are copied on open. Zero fields select defaults: 16 output queue
// slots and 64 KiB maximum buffers. Input delivery is unbuffered. QueueCapacity
// applies to output; Allocator applies to input. Callbacks remain caller-owned.
type Options struct {
	QueueCapacity       int
	MaxBufferSize       int
	Allocator           Allocator
	FileMode            FileMode
	Follow              bool
	PollInterval        time.Duration
	CreateFIFO          bool
	DialTimeout         time.Duration
	IOTimeout           time.Duration
	DatagramPayloadSize int
	FlushInterval       time.Duration
}

// FileMode is explicit authority to change an output file. Default is Exclusive.
type FileMode string

const (
	Exclusive FileMode = ""
	Append    FileMode = "append"
	Truncate  FileMode = "truncate"
)

func options(o Options) (Options, error) {
	if o.QueueCapacity == 0 {
		o.QueueCapacity = 16
	}
	if o.MaxBufferSize == 0 {
		o.MaxBufferSize = 64 * 1024
	}
	if o.QueueCapacity < 1 || o.QueueCapacity > 1024 || o.MaxBufferSize < 1 || o.MaxBufferSize > 1024*1024 || o.QueueCapacity*o.MaxBufferSize > 64*1024*1024 {
		return o, ErrInvalidOptions
	}
	if o.PollInterval == 0 {
		o.PollInterval = 10 * time.Millisecond
	}
	if (o.FileMode != Exclusive && o.FileMode != Append && o.FileMode != Truncate) || o.PollInterval < 0 || o.DialTimeout < 0 || o.IOTimeout < 0 || o.FlushInterval < 0 || o.DatagramPayloadSize < 0 || o.DatagramPayloadSize > 65507 || o.DatagramPayloadSize > o.MaxBufferSize || (o.DatagramPayloadSize > 0 && o.FlushInterval <= 0) || (o.DatagramPayloadSize == 0 && o.FlushInterval != 0) {
		return o, ErrInvalidOptions
	}
	if o.Allocator == nil {
		size := o.MaxBufferSize
		pool := &sync.Pool{New: func() any { return make([]byte, size) }}
		o.Allocator = func(ctx context.Context, n int) (*Buffer, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			bytes := pool.Get().([]byte)
			return NewBuffer(bytes, func() { pool.Put(bytes) }), nil
		}
	}
	return o, nil
}

// lifecycle has one terminal result, published by closing done. A watcher closes
// the OS handle on cancellation; finish joins that watcher before reporting Done.
// It never reclaims buffers already handed to the application.
type lifecycle struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	file      io.Closer
	done      chan struct{}
	stopWatch chan struct{}
	watchDone chan struct{}
	result    error
}

func newLifecycle(parent context.Context, f io.Closer) *lifecycle {
	ctx, cancel := context.WithCancelCause(parent)
	l := &lifecycle{ctx: ctx, cancel: cancel, file: f, done: make(chan struct{}), stopWatch: make(chan struct{}), watchDone: make(chan struct{})}
	go func() {
		defer close(l.watchDone)
		select {
		case <-ctx.Done():
			f.Close()
		case <-l.stopWatch:
		}
	}()
	return l
}
func (l *lifecycle) finish(err error) {
	close(l.stopWatch)
	if e := l.file.Close(); e != nil && !errors.Is(e, os.ErrClosed) && !errors.Is(e, net.ErrClosed) {
		err = errors.Join(err, e)
	}
	<-l.watchDone
	if cause := context.Cause(l.ctx); cause != nil && cause != errGraceful {
		err = errors.Join(err, cause)
	}
	l.result = err
	// Detach from a long-lived cancellable parent only after computing the
	// outcome, so internal cleanup does not turn normal EOF/drain into failure.
	l.cancel(nil)
	close(l.done)
}
func (l *lifecycle) wait(ctx context.Context) error {
	// Completed cleanup wins over an already-expired waiting context.
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
