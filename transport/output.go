package urihandler

import (
	"context"
	"io"
	"time"
)

func (out *Output) write(data []byte) error {
	if err := context.Cause(out.life.ctx); err != nil {
		return err
	}
	if out.datagram {
		n, err := out.writer.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
		return nil
	}
	for len(data) > 0 {
		if err := context.Cause(out.life.ctx); err != nil {
			return err
		}
		n, err := out.writer.Write(data)
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Packing is explicit and copies records into one pooled datagram buffer. Its
// timer starts at the first record, not at the last, bounding intentional delay.
func (out *Output) runPacked() error {
	var pack *Buffer
	var used int
	var timer *time.Timer
	var tick <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if pack != nil {
			pack.Release()
		}
	}()
	flush := func() error {
		if pack == nil {
			return nil
		}
		err := out.write(pack.Data[:used])
		pack.Release()
		pack = nil
		used = 0
		if timer != nil {
			timer.Stop()
		}
		tick = nil
		return err
	}
	add := func(b *Buffer) error {
		defer b.Release()
		if err := context.Cause(out.life.ctx); err != nil {
			return err
		}
		if len(b.Data) == 0 { // Preserve an explicit empty datagram, never merge it away.
			if err := flush(); err != nil {
				return err
			}
			return out.write(nil)
		}
		if pack != nil && used+len(b.Data) > out.options.DatagramPayloadSize {
			if err := flush(); err != nil {
				return err
			}
		}
		if pack == nil {
			var err error
			pack, err = acquire(out.life.ctx, out.options.Allocator, out.options.DatagramPayloadSize)
			if err != nil {
				return err
			}
			timer = time.NewTimer(out.options.FlushInterval)
			tick = timer.C
		}
		copy(pack.Data[used:], b.Data)
		used += len(b.Data)
		if used == out.options.DatagramPayloadSize {
			return flush()
		}
		return nil
	}
	for {
		select {
		case <-out.life.ctx.Done():
			return context.Cause(out.life.ctx)
		case b := <-out.queue:
			out.slots <- struct{}{}
			if err := add(b); err != nil {
				return err
			}
		case <-tick:
			if err := flush(); err != nil {
				return err
			}
		case <-out.admissionDone:
			// next waits for a channel bridge's final accepted submission as well.
			for {
				b, ok := out.next()
				if !ok {
					if err := context.Cause(out.life.ctx); err != nil {
						return err
					}
					return flush()
				}
				out.slots <- struct{}{}
				if err := add(b); err != nil {
					return err
				}
			}
		}
	}
}

// Channel returns an unbuffered submission channel. Sending transfers custody
// to its bridge, which may itself wait for queue capacity (one extra held item).
// Never access a submitted buffer again. Producers must select Done/ctx when
// sending, and stop before requesting graceful Close. Only a producer coordinator
// may close this channel; doing so requests graceful completion. The library
// never closes it underneath producers. Prefer Send for operation-level errors.
func (out *Output) Channel() chan<- *Buffer {
	out.mu.Lock()
	defer out.mu.Unlock()
	if !out.channelStarted && !out.closed {
		out.channelStarted = true
		out.channelDone = make(chan struct{})
		go out.bridge()
	}
	return out.channel
}
func (out *Output) bridgeDone() <-chan struct{} {
	out.mu.Lock()
	defer out.mu.Unlock()
	return out.channelDone
}
func (out *Output) bridge() {
	defer close(out.channelDone)
	for {
		select {
		case <-out.admissionDone:
			return
		case <-out.life.ctx.Done():
			return
		case b, ok := <-out.channel:
			if !ok {
				out.Close()
				return
			}
			if b == nil || len(b.Data) > out.maxSize {
				b.Release()
				out.life.cancel(ErrInvalidBuffer)
				return
			}
			// It was already accepted by the channel: graceful Close must let this
			// final item reach the queue. Abort, unlike Close, abandons it.
			select {
			case <-out.life.ctx.Done():
				b.Release()
				return
			case <-out.slots:
			}
			out.mu.Lock()
			if out.life.ctx.Err() != nil {
				out.mu.Unlock()
				out.slots <- struct{}{}
				b.Release()
				return
			}
			out.queue <- b
			out.mu.Unlock()
		}
	}
}
