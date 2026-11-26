package urifan_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	fan "github.com/Version2beta/urifan"
	uri "github.com/Version2beta/urifan/transport"
)

func TestCloseDeadlineCancelsContextAwareAllocator(t *testing.T) {
	d := t.TempDir()
	in := filepath.Join(d, "in")
	os.WriteFile(in, []byte("data"), 0600)
	entered := make(chan struct{})
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: uriFile(filepath.Join(d, "out"))}}, ShutdownTimeout: 20 * time.Millisecond, Allocator: func(ctx context.Context, n int) (*uri.Buffer, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("allocator not entered")
	}
	r.Close()
	if err = r.Wait(deadline(t)); !errors.Is(err, fan.ErrShutdownTimeout) {
		t.Fatalf("close failed to cancel allocator at deadline: %v", err)
	}
	if r.Stats().RetainedBytes != 0 {
		t.Fatal("failed allocation stayed charged")
	}
}
