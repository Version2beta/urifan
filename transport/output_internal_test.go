package urihandler

import (
	"context"
	"errors"
	"testing"
)

type countingWriter struct{ calls int }

func (w *countingWriter) Write(b []byte) (int, error) { w.calls++; return len(b), nil }

func TestCancelledDatagramDoesNotStartWrite(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(context.Canceled)
	writer := new(countingWriter)
	out := &Output{life: &lifecycle{ctx: ctx}, datagram: true, writer: writer}
	if err := out.write([]byte("pending")); !errors.Is(err, context.Canceled) {
		t.Fatalf("write after cancellation: %v", err)
	}
	if writer.calls != 0 {
		t.Fatal("cancelled packed datagram entered transport writer")
	}
}
