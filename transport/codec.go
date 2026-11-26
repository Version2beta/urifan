package urihandler

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
)

// EncodeFunc writes into borrowed storage and returns the encoded length.
// DecodeFunc writes into a caller-owned object and must not retain borrowed bytes.
type EncodeFunc[T any] func([]byte, *T) (int, error)
type DecodeFunc[T any] func(*T, []byte) error

// Framing chooses fixed-width records or a four-byte big-endian length prefix.
// Streams require one; unframed UDP means exactly one record per datagram.
type Framing struct {
	FixedSize    int
	LengthPrefix bool
}

func (f Framing) validate(datagram bool, max int) error {
	if f.FixedSize < 0 || f.FixedSize > max || (f.FixedSize > 0 && f.LengthPrefix) || (!datagram && f.FixedSize == 0 && !f.LengthPrefix) || (f.LengthPrefix && max < 4) {
		return ErrFrame
	}
	return nil
}

type Encoder[T any] struct {
	output  *Output
	encode  EncodeFunc[T]
	framing Framing
}

func NewEncoder[T any](out *Output, encode EncodeFunc[T], framing Framing) (*Encoder[T], error) {
	if out == nil || encode == nil {
		return nil, ErrFrame
	}
	if err := framing.validate(out.datagram, out.maxSize); err != nil {
		return nil, err
	}
	if out.datagram && out.options.DatagramPayloadSize > 0 && framing.FixedSize == 0 && !framing.LengthPrefix {
		return nil, ErrFrame
	}
	return &Encoder[T]{out, encode, framing}, nil
}

// Send encodes synchronously. On return the caller can reuse value, regardless
// of success. Only the resulting byte-buffer lease enters the output queue.
func (e *Encoder[T]) Send(ctx context.Context, value *T) error {
	if value == nil {
		return ErrFrame
	}
	b, err := e.output.Acquire(ctx)
	if err != nil {
		return err
	}
	prefix := 0
	if e.framing.LengthPrefix {
		prefix = 4
	}
	n, err := e.encode(b.Data[prefix:], value)
	if err != nil {
		b.Release()
		return err
	}
	if n < 0 || n > len(b.Data)-prefix || (e.framing.FixedSize > 0 && n != e.framing.FixedSize) {
		b.Release()
		return ErrFrame
	}
	if prefix > 0 {
		binary.BigEndian.PutUint32(b.Data[:4], uint32(n))
	}
	b.Data = b.Data[:prefix+n]
	if err = e.output.Send(ctx, b); err != nil {
		b.Release()
	}
	return err
}

// Decoder is single-consumer. Close releases any partially consumed input lease;
// it does not close the source. Use the Receive context to interrupt a call.
// Receive errors from transport/framing/cancellation are terminal for this
// decoder, so partial records are never reinterpreted after a cancelled call.
type Decoder[T any] struct {
	input    *Input
	decode   DecodeFunc[T]
	framing  Framing
	held     *Buffer
	offset   int
	terminal error
}

func NewDecoder[T any](in *Input, decode DecodeFunc[T], framing Framing) (*Decoder[T], error) {
	if in == nil || decode == nil {
		return nil, ErrFrame
	}
	if err := framing.validate(in.datagram, in.options.MaxBufferSize); err != nil {
		return nil, err
	}
	return &Decoder[T]{input: in, decode: decode, framing: framing}, nil
}
func (d *Decoder[T]) releaseHeld() {
	if d.held != nil {
		d.held.Release()
		d.held = nil
		d.offset = 0
	}
}
func (d *Decoder[T]) Close() error { d.releaseHeld(); d.terminal = ErrClosed; return nil }
func (d *Decoder[T]) load(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.held != nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b, ok := <-d.input.Data():
		if !ok {
			if err := d.input.Wait(ctx); err != nil {
				return err
			}
			return io.EOF
		}
		d.held = b
		d.offset = 0
		return nil
	}
}
func (d *Decoder[T]) readExact(ctx context.Context, dst []byte) error {
	copied := 0
	for copied < len(dst) {
		if err := d.load(ctx); err != nil {
			if errors.Is(err, io.EOF) && copied > 0 {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		n := copy(dst[copied:], d.held.Data[d.offset:])
		copied += n
		d.offset += n
		if d.offset == len(d.held.Data) {
			d.releaseHeld()
		}
	}
	return nil
}
func (d *Decoder[T]) record(ctx context.Context) ([]byte, func(), error) {
	if err := d.load(ctx); err != nil {
		return nil, nil, err
	}
	if d.input.datagram {
		data := d.held.Data[d.offset:]
		n := len(data)
		prefix := 0
		if d.framing.FixedSize > 0 {
			n = d.framing.FixedSize
			if len(d.held.Data)%n != 0 {
				return nil, nil, ErrFrame
			}
		} else if d.framing.LengthPrefix {
			if len(data) < 4 {
				return nil, nil, io.ErrUnexpectedEOF
			}
			prefix = 4
			length := binary.BigEndian.Uint32(data[:4])
			if uint64(length) > uint64(len(data)-4) {
				return nil, nil, ErrFrame
			}
			n = int(length)
		}
		if n > len(data)-prefix {
			return nil, nil, ErrFrame
		}
		return data[prefix : prefix+n], func() {
			d.offset += prefix + n
			if d.offset == len(d.held.Data) {
				d.releaseHeld()
			}
		}, nil
	}
	n := d.framing.FixedSize
	if d.framing.LengthPrefix {
		var header [4]byte
		if err := d.readExact(ctx, header[:]); err != nil {
			return nil, nil, err
		}
		size := binary.BigEndian.Uint32(header[:])
		if uint64(size) > uint64(d.input.options.MaxBufferSize-4) {
			return nil, nil, ErrFrame
		}
		n = int(size)
	}
	if d.held != nil && len(d.held.Data)-d.offset >= n {
		data := d.held.Data[d.offset : d.offset+n]
		return data, func() {
			d.offset += n
			if d.offset == len(d.held.Data) {
				d.releaseHeld()
			}
		}, nil
	}
	b, err := acquire(ctx, d.input.options.Allocator, n)
	if err != nil {
		return nil, nil, err
	}
	if err = d.readExact(ctx, b.Data); err != nil {
		b.Release()
		if n > 0 && errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}
	return b.Data, b.Release, nil
}
func (d *Decoder[T]) Receive(ctx context.Context, dst *T) error {
	if dst == nil {
		return ErrFrame
	}
	if d.terminal != nil {
		return d.terminal
	}
	bytes, release, err := d.record(ctx)
	if err != nil {
		d.releaseHeld()
		d.terminal = err
		return err
	}
	defer release()
	return d.decode(dst, bytes)
}
