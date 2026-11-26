package urihandler_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

func TestCancellationStopsFIFOAndUDPWithNoProducer(t *testing.T) {
	for _, scheme := range []string{"fifo", "udp"} {
		t.Run(scheme, func(t *testing.T) {
			address := "udp://127.0.0.1:0"
			opts := uri.Options{}
			if scheme == "fifo" {
				address = "fifo://" + filepath.Join(t.TempDir(), "pipe")
				opts.CreateFIFO = true
			}
			in, err := uri.OpenInput(context.Background(), address, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer in.Cancel()
			in.Cancel()
			if err := in.Wait(waitContext(t)); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel idle %s: %v", scheme, err)
			}
		})
	}
}

func TestCancelInterruptsBackpressuredFIFOWriteBeforeRecycling(t *testing.T) {
	address := "fifo://" + filepath.Join(t.TempDir(), "pipe")
	in, err := uri.OpenInput(context.Background(), address, uri.Options{CreateFIFO: true, MaxBufferSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	out, err := uri.OpenOutput(waitContext(t), address, uri.Options{MaxBufferSize: 1 << 20, QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	var releases atomic.Int64
	if err := out.Send(waitContext(t), uri.NewBuffer(make([]byte, 1<<20), func() { releases.Add(1) })); err != nil {
		t.Fatal(err)
	}
	// Receipt of a small prefix establishes the writer has started. No additional
	// input is consumed: the pipe cannot hold the remainder of the 1 MiB write.
	b := receive(t, in.Data())
	defer b.Release()
	if releases.Load() != 0 {
		t.Fatal("buffer recycled before pipe write completed")
	}
	out.Cancel()
	if err := out.Wait(waitContext(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("FIFO writer cancel: %v", err)
	}
	if releases.Load() != 1 {
		t.Fatalf("cancel release %d", releases.Load())
	}
	in.Cancel()
	if err := in.Wait(waitContext(t)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestParentCancellationStopsListenerAndPendingAccept(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := uri.Listen(parent, "tcp://127.0.0.1:0", uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	cancel()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if in, err := l.AcceptInput(ctx); in != nil || !errors.Is(err, uri.ErrClosed) {
		t.Fatalf("closed listener accepted: %v %v", in, err)
	}
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
