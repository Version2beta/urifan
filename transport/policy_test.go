package urihandler_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

func TestNonFileEndpointsNeverReplaceExistingFilesystemObjects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keep")
	os.WriteFile(path, []byte("preserve"), 0600)
	if in, err := uri.OpenInput(waitContext(t), "fifo://"+path, uri.Options{CreateFIFO: true}); in != nil || err == nil {
		if in != nil {
			in.Cancel()
		}
		t.Fatal("ordinary file accepted as FIFO")
	}
	if l, err := uri.Listen(waitContext(t), "unix://"+path, uri.Options{}); l != nil || err == nil {
		if l != nil {
			l.Close()
		}
		t.Fatal("ordinary file replaced by socket")
	}
	got, _ := os.ReadFile(path)
	if string(got) != "preserve" {
		t.Fatal("existing path changed")
	}
}
func TestMalformedOptionsRejectedBeforeCreatingOutput(t *testing.T) {
	for _, opts := range []uri.Options{
		{FileMode: "typo"}, {Follow: true}, {CreateFIFO: true}, {DatagramPayloadSize: 6},
		{FlushInterval: time.Second}, {DialTimeout: -1}, {IOTimeout: -1}, {PollInterval: -1},
	} {
		path := filepath.Join(t.TempDir(), "absent")
		if out, err := uri.OpenOutput(context.Background(), fileURI(path), opts); out != nil || err == nil {
			if out != nil {
				out.Cancel()
			}
			t.Fatalf("invalid options accepted: %+v", opts)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid options created file")
		}
	}
}
func TestInvalidAllocatorLeaseIsReturnedAndReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	os.WriteFile(path, []byte("x"), 0600)
	var releases atomic.Int64
	in, err := uri.OpenInput(context.Background(), fileURI(path), uri.Options{MaxBufferSize: 8, Allocator: func(context.Context, int) (*uri.Buffer, error) {
		return uri.NewBuffer(make([]byte, 1), func() { releases.Add(1) }), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	if err := in.Wait(waitContext(t)); !errors.Is(err, uri.ErrInvalidBuffer) {
		t.Fatalf("allocator error: %v", err)
	}
	if releases.Load() != 1 {
		t.Fatal("bad allocator lease leaked")
	}
}
func TestConcurrentSendAndCloseHasUnambiguousCustody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "race")
	out, err := uri.OpenOutput(context.Background(), fileURI(path), uri.Options{QueueCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	var accepted, releases atomic.Int64
	makeBuffer := func() *uri.Buffer { return uri.NewBuffer([]byte("x"), func() { releases.Add(1) }) }
	if err := out.Send(waitContext(t), makeBuffer()); err != nil {
		t.Fatal(err)
	}
	accepted.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for n := 0; n < 20; n++ {
				b := makeBuffer()
				if err := out.Send(ctx, b); err != nil {
					b.Release()
				} else {
					accepted.Add(1)
				}
			}
		}()
	}
	close(start)
	out.Close()
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("producers did not stop")
	}
	if err := out.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != accepted.Load() {
		t.Fatalf("accepted %d written %d", accepted.Load(), len(data))
	}
	if releases.Load() != 321 {
		t.Fatalf("lost/double-released custody: %d", releases.Load())
	}
}
