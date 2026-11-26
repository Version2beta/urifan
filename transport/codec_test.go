package urihandler_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

type number struct{ Value uint16 }

func encodeNumber(dst []byte, n *number) (int, error) {
	if len(dst) < 2 {
		return 0, io.ErrShortBuffer
	}
	binary.BigEndian.PutUint16(dst, n.Value)
	return 2, nil
}
func decodeNumber(n *number, src []byte) error {
	if len(src) != 2 {
		return uri.ErrFrame
	}
	n.Value = binary.BigEndian.Uint16(src)
	return nil
}

func TestTypedObjectsAreCallerOwnedAndReusable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records")
	var acquired, released atomic.Int64
	alloc := func(ctx context.Context, n int) (*uri.Buffer, error) {
		acquired.Add(1)
		return uri.NewBuffer(make([]byte, n), func() { released.Add(1) }), nil
	}
	opts := uri.Options{MaxBufferSize: 8, Allocator: alloc}
	out, err := uri.OpenOutput(context.Background(), fileURI(path), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	encoder, err := uri.NewEncoder(out, encodeNumber, uri.Framing{FixedSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	var value number
	for i := 1; i <= 20; i++ {
		value.Value = uint16(i)
		if err := encoder.Send(waitContext(t), &value); err != nil {
			t.Fatal(err)
		}
		value.Value = 0
	}
	finishOutput(t, out)
	in, err := uri.OpenInput(context.Background(), fileURI(path), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	decoder, err := uri.NewDecoder(in, decodeNumber, uri.Framing{FixedSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	for i := 1; i <= 20; i++ {
		value.Value = 0
		if err := decoder.Receive(waitContext(t), &value); err != nil {
			t.Fatal(err)
		}
		if value.Value != uint16(i) {
			t.Fatalf("record %d became %d", i, value.Value)
		}
	}
	if err := decoder.Receive(waitContext(t), &value); !errors.Is(err, io.EOF) {
		t.Fatalf("record EOF %v", err)
	}
	decoder.Close()
	if err := in.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
	if acquired.Load() != released.Load() {
		t.Fatalf("codec pool imbalance %d/%d", acquired.Load(), released.Load())
	}
}

type textRecord struct{ Text []byte }

func TestLengthPrefixedRecordsAcrossReadBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records")
	out, err := uri.OpenOutput(context.Background(), fileURI(path), uri.Options{MaxBufferSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	encode := func(dst []byte, v *textRecord) (int, error) {
		if len(v.Text) > len(dst) {
			return 0, io.ErrShortBuffer
		}
		return copy(dst, v.Text), nil
	}
	decode := func(v *textRecord, src []byte) error { v.Text = append(v.Text[:0], src...); return nil }
	encoder, err := uri.NewEncoder(out, encode, uri.Framing{LengthPrefix: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"abc", "defg", "x", ""} {
		value := textRecord{[]byte(s)}
		if err := encoder.Send(waitContext(t), &value); err != nil {
			t.Fatal(err)
		}
	}
	finishOutput(t, out)
	in, err := uri.OpenInput(context.Background(), fileURI(path), uri.Options{MaxBufferSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	decoder, err := uri.NewDecoder(in, decode, uri.Framing{LengthPrefix: true})
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	var value textRecord
	for _, s := range []string{"abc", "defg", "x", ""} {
		if err := decoder.Receive(waitContext(t), &value); err != nil {
			t.Fatal(err)
		}
		if string(value.Text) != s {
			t.Fatalf("decoded %q want %q", value.Text, s)
		}
	}
	if err := decoder.Receive(waitContext(t), &value); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}
func TestDecoderRejectsIncompleteAndOversizedRecords(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
		want error
	}{
		{"partial-header", []byte{0, 0}, io.ErrUnexpectedEOF},
		{"missing-body", []byte{0, 0, 0, 3}, io.ErrUnexpectedEOF},
		{"oversized", []byte{255, 255, 255, 255}, uri.ErrFrame},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad")
			os.WriteFile(path, test.data, 0600)
			in, err := uri.OpenInput(context.Background(), fileURI(path), uri.Options{MaxBufferSize: 8})
			if err != nil {
				t.Fatal(err)
			}
			defer in.Cancel()
			decoder, err := uri.NewDecoder(in, func(v *number, b []byte) error { t.Fatal("bad frame reached callback"); return nil }, uri.Framing{LengthPrefix: true})
			if err != nil {
				t.Fatal(err)
			}
			defer decoder.Close()
			if err := decoder.Receive(waitContext(t), new(number)); !errors.Is(err, test.want) {
				t.Fatalf("frame error %v want %v", err, test.want)
			}
		})
	}
}
func TestTypedUDPRecordsCanBePackedWithoutMPEGKnowledge(t *testing.T) {
	in, err := uri.OpenInput(context.Background(), "udp://127.0.0.1:0", uri.Options{MaxBufferSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	out, err := uri.OpenOutput(waitContext(t), "udp://"+in.Addr().String(), uri.Options{MaxBufferSize: 16, DatagramPayloadSize: 6, FlushInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	enc, err := uri.NewEncoder(out, encodeNumber, uri.Framing{FixedSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	dec, err := uri.NewDecoder(in, decodeNumber, uri.Framing{FixedSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	for i := 1; i <= 3; i++ {
		if err := enc.Send(waitContext(t), &number{uint16(i)}); err != nil {
			t.Fatal(err)
		}
	}
	finishOutput(t, out)
	for i := 1; i <= 3; i++ {
		var n number
		if err := dec.Receive(waitContext(t), &n); err != nil {
			t.Fatal(err)
		}
		if n.Value != uint16(i) {
			t.Fatal("UDP record boundaries lost")
		}
	}
}
func TestCodecErrorsReleaseOnlyLibraryAcquiredBuffers(t *testing.T) {
	var releases atomic.Int64
	out, err := uri.OpenOutput(context.Background(), fileURI(filepath.Join(t.TempDir(), "out")), uri.Options{MaxBufferSize: 8, Allocator: func(ctx context.Context, n int) (*uri.Buffer, error) {
		return uri.NewBuffer(make([]byte, n), func() { releases.Add(1) }), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	for _, n := range []int{-1, 9} {
		enc, err := uri.NewEncoder(out, func([]byte, *number) (int, error) { return n, nil }, uri.Framing{FixedSize: 2})
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.Send(waitContext(t), new(number)); !errors.Is(err, uri.ErrFrame) {
			t.Fatal(err)
		}
	}
	if releases.Load() != 2 {
		t.Fatalf("failed encodes leaked %d", releases.Load())
	}
	if _, err := uri.NewEncoder(out, encodeNumber, uri.Framing{}); !errors.Is(err, uri.ErrFrame) {
		t.Fatal("stream without framing accepted")
	}
	finishOutput(t, out)
}
