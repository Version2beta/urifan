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

// Input delivers buffers on an unbuffered channel. Receiving transfers custody;
// the consumer releases each buffer or forwards it to an output. EOF closes Data.
type Input struct {
	life     *lifecycle
	data     chan *Buffer
	options  Options
	read     func([]byte) (int, error)
	datagram bool
	addr     net.Addr
}

func OpenInput(ctx context.Context, raw string, opts Options) (*Input, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o, err := options(opts)
	if err != nil {
		return nil, err
	}
	r, err := openTransport(ctx, raw, o, false)
	if err != nil {
		return nil, err
	}
	return newInput(ctx, r, o), nil
}
func newInput(ctx context.Context, r *transport, o Options) *Input {
	in := &Input{life: newLifecycle(ctx, r.closer), data: make(chan *Buffer), options: o, read: r.read, datagram: r.datagram, addr: r.addr}
	go in.run()
	return in
}
func (in *Input) Data() <-chan *Buffer { return in.data }

// Addr returns the local network address, or nil for files/FIFOs.
func (in *Input) Addr() net.Addr { return in.addr }

// Close stops further input; unlike Cancel it requests normal completion.
func (in *Input) Close() error                   { in.life.cancel(errGraceful); return nil }
func (in *Input) Cancel()                        { in.life.cancel(context.Canceled) }
func (in *Input) Done() <-chan struct{}          { return in.life.done }
func (in *Input) Wait(ctx context.Context) error { return in.life.wait(ctx) }
func (in *Input) run() {
	var outcome error
	defer func() { close(in.data); in.life.finish(outcome) }()
	for {
		if in.life.ctx.Err() != nil {
			return
		}
		b, err := in.options.Allocator(in.life.ctx, in.options.MaxBufferSize)
		if err != nil {
			if b != nil {
				b.Release()
			}
			if context.Cause(in.life.ctx) == errGraceful && errors.Is(err, context.Canceled) {
				return
			}
			outcome = err
			return
		}
		if b == nil || cap(b.Data) < in.options.MaxBufferSize {
			if b != nil {
				b.Release()
			}
			outcome = ErrInvalidBuffer
			return
		}
		if in.life.ctx.Err() != nil {
			b.Release()
			return
		}
		b.Data = b.Data[:in.options.MaxBufferSize]
		n, readErr := in.read(b.Data)
		if errors.Is(readErr, ErrDatagramTruncated) {
			b.Release()
			outcome = readErr
			return
		}
		if n > 0 || (in.datagram && readErr == nil) {
			b.Data = b.Data[:n]
			select {
			case in.data <- b:
			case <-in.life.ctx.Done():
				b.Release()
				return
			}
		} else {
			b.Release()
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				if in.options.Follow {
					select {
					case <-in.life.ctx.Done():
						return
					case <-time.After(in.options.PollInterval):
						continue
					}
				}
				return
			}
			if context.Cause(in.life.ctx) == errGraceful && (errors.Is(readErr, os.ErrClosed) || errors.Is(readErr, net.ErrClosed) || in.life.ctx.Err() != nil) {
				return
			}
			outcome = readErr
			return
		}
	}
}

// Output owns accepted buffers until release. Its queue is bounded separately
// from one in-flight write. Send is the only submission interface in this slice.
type Output struct {
	life           *lifecycle
	queue          chan *Buffer
	slots          chan struct{}
	admissionDone  chan struct{}
	mu             sync.Mutex
	closed         bool
	maxSize        int
	writer         io.Writer
	datagram       bool
	options        Options
	addr           net.Addr
	channel        chan *Buffer
	channelDone    chan struct{}
	channelStarted bool
}

func OpenOutput(ctx context.Context, raw string, opts Options) (*Output, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o, err := options(opts)
	if err != nil {
		return nil, err
	}
	r, err := openTransport(ctx, raw, o, true)
	if err != nil {
		return nil, err
	}
	return newOutput(ctx, r, o), nil
}
func newOutput(ctx context.Context, r *transport, o Options) *Output {
	out := &Output{life: newLifecycle(ctx, r.closer), queue: make(chan *Buffer, o.QueueCapacity), slots: make(chan struct{}, o.QueueCapacity), admissionDone: make(chan struct{}), maxSize: o.MaxBufferSize, writer: r.writer, datagram: r.datagram, options: o, addr: r.addr}
	if r.datagram && out.maxSize > 65507 {
		out.maxSize = 65507
	}
	if o.DatagramPayloadSize > 0 {
		out.maxSize = o.DatagramPayloadSize
	}
	out.channel = make(chan *Buffer)
	out.channelDone = make(chan struct{})
	close(out.channelDone)
	for i := 0; i < o.QueueCapacity; i++ {
		out.slots <- struct{}{}
	}
	go out.run()
	return out
}
func (out *Output) Addr() net.Addr { return out.addr }

// Acquire borrows writable storage from the output's configured byte allocator.
// The caller releases it if it is not successfully submitted.
func (out *Output) Acquire(ctx context.Context) (*Buffer, error) {
	return acquire(ctx, out.options.Allocator, out.maxSize)
}

// Send borrows b during the call. Nil means accepted: do not access b again.
// Any non-nil error leaves b with the caller. Cancellation of this operation
// alone does not cancel the output. No payload copy is performed.
func (out *Output) Send(ctx context.Context, b *Buffer) error {
	if b == nil || len(b.Data) > out.maxSize {
		return ErrInvalidBuffer
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-out.admissionDone:
		return ErrClosed
	case <-out.life.ctx.Done():
		return context.Cause(out.life.ctx)
	case <-ctx.Done():
		return ctx.Err()
	case <-out.slots:
	}
	out.mu.Lock()
	defer out.mu.Unlock()
	reject := func(err error) error { out.slots <- struct{}{}; return err }
	if err := ctx.Err(); err != nil {
		return reject(err)
	}
	if err := context.Cause(out.life.ctx); err != nil {
		return reject(err)
	}
	if out.closed {
		return reject(ErrClosed)
	}
	// A reserved slot guarantees enqueue cannot block. Serialize acceptance with
	// Close so the worker cannot finish draining before an admitted Send enqueues.
	out.queue <- b
	return nil
}
func (out *Output) stopAdmission() {
	out.mu.Lock()
	defer out.mu.Unlock()
	if !out.closed {
		out.closed = true
		close(out.admissionDone)
	}
}

// Close requests graceful completion; Wait observes completion and I/O errors.
func (out *Output) Close() error { out.stopAdmission(); return nil }

// Cancel requests abort; it cannot retract bytes already written.
func (out *Output) Cancel()                        { out.life.cancel(context.Canceled); out.stopAdmission() }
func (out *Output) Done() <-chan struct{}          { return out.life.done }
func (out *Output) Wait(ctx context.Context) error { return out.life.wait(ctx) }
func (out *Output) next() (*Buffer, bool) {
	if out.life.ctx.Err() != nil {
		return nil, false
	}
	select {
	case <-out.life.ctx.Done():
		return nil, false
	case b := <-out.queue:
		return b, true
	case <-out.admissionDone:
		select {
		case b := <-out.queue:
			return b, true
		case <-out.life.ctx.Done():
			return nil, false
		case <-out.bridgeDone():
			select {
			case b := <-out.queue:
				return b, true
			default:
				return nil, false
			}
		}
	}
}
func (out *Output) run() {
	var outcome error
	defer func() {
		if outcome != nil {
			out.life.cancel(outcome)
		}
		out.stopAdmission()
		<-out.bridgeDone()
		for {
			select {
			case b := <-out.queue:
				out.slots <- struct{}{}
				b.Release()
			default:
				out.life.finish(outcome)
				return
			}
		}
	}()
	if out.options.DatagramPayloadSize > 0 {
		outcome = out.runPacked()
		return
	}
	for {
		b, ok := out.next()
		if !ok {
			return
		}
		out.slots <- struct{}{}
		if out.life.ctx.Err() != nil {
			b.Release()
			return
		}
		outcome = out.write(b.Data)
		b.Release()
		if outcome != nil || out.life.ctx.Err() != nil {
			return
		}
	}
}
