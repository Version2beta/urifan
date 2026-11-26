package urihandler_test

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

func fileURI(path string) string { return (&url.URL{Scheme: "file", Path: path}).String() }
func waitContext(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return c
}
func receive(t *testing.T, ch <-chan *uri.Buffer) *uri.Buffer {
	t.Helper()
	select {
	case b, ok := <-ch:
		if !ok {
			t.Fatal("input ended early")
		}
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("input stalled")
		return nil
	}
}

func TestAcceptanceRoundTripUsesCallerPool(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "input with spaces")
	dst := filepath.Join(dir, "output")
	payload := bytes.Repeat([]byte("record\x00\n"), 1000)
	if err := os.WriteFile(src, payload, 0600); err != nil {
		t.Fatal(err)
	}
	var acquired, released atomic.Int64
	alloc := func(ctx context.Context, n int) (*uri.Buffer, error) {
		acquired.Add(1)
		return uri.NewBuffer(make([]byte, n), func() { released.Add(1) }), nil
	}
	in, err := uri.OpenInput(context.Background(), fileURI(src), uri.Options{MaxBufferSize: 97, Allocator: alloc})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	out, err := uri.OpenOutput(context.Background(), fileURI(dst), uri.Options{QueueCapacity: 2, MaxBufferSize: 97})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	for b := range in.Data() {
		if err := out.Send(waitContext(t), b); err != nil {
			b.Release()
			t.Fatal(err)
		}
	}
	if err := in.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if err := out.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("byte order/content changed")
	}
	if acquired.Load() != released.Load() {
		t.Fatalf("pool imbalance acquired=%d released=%d", acquired.Load(), released.Load())
	}
}

func TestAcceptanceEmptyInputAndExclusiveOutput(t *testing.T) {
	p := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(p, nil, 0600); err != nil {
		t.Fatal(err)
	}
	in, err := uri.OpenInput(context.Background(), fileURI(p), uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	select {
	case b, ok := <-in.Data():
		if ok {
			b.Release()
			t.Fatal("empty file delivered data")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("EOF did not complete")
	}
	if err := in.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := uri.OpenOutput(context.Background(), fileURI(p), uri.Options{})
	if out != nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("exclusive output: out=%v err=%v", out, err)
	}
	data, _ := os.ReadFile(p)
	if string(data) != "preserve" {
		t.Fatal("existing file changed")
	}
}

func TestAcceptanceDeliveredBufferSurvivesCancelAndWait(t *testing.T) {
	p := filepath.Join(t.TempDir(), "input")
	os.WriteFile(p, []byte("abcdef"), 0600)
	var released atomic.Int64
	in, err := uri.OpenInput(context.Background(), fileURI(p), uri.Options{MaxBufferSize: 6, Allocator: func(ctx context.Context, n int) (*uri.Buffer, error) {
		return uri.NewBuffer(make([]byte, n), func() { released.Add(1) }), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	b := receive(t, in.Data())
	defer b.Release()
	in.Cancel()
	err = in.Wait(waitContext(t))
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// Another buffer may have been acquired for the EOF read. Only this lease's
	// contents/own release are relevant; the endpoint must not recycle it.
	if string(b.Data) != "abcdef" {
		t.Fatal("delivered buffer invalidated")
	}
	before := released.Load()
	b.Release()
	if released.Load() != before+1 {
		t.Fatal("delivered buffer was already released")
	}
	b.Release()
	if released.Load() != before+1 {
		t.Fatal("release callback called twice")
	}
}

func TestAcceptanceBackpressureCancellationAndQueueRelease(t *testing.T) {
	p := filepath.Join(t.TempDir(), "output")
	out, err := uri.OpenOutput(context.Background(), fileURI(p), uri.Options{QueueCapacity: 1, MaxBufferSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	entered := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(unblock) })
	var first, second, rejected atomic.Int64
	b1 := uri.NewBuffer([]byte("a"), func() { first.Add(1); close(entered); <-unblock })
	if err := out.Send(waitContext(t), b1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first write not released")
	}
	// Holding the release callback is deterministic coordination, not evidence
	// about cancellation of a kernel-blocked write.
	b2 := uri.NewBuffer([]byte("b"), func() { second.Add(1) })
	if err := out.Send(waitContext(t), b2); err != nil {
		t.Fatal(err)
	}
	b3 := uri.NewBuffer([]byte("c"), func() { rejected.Add(1) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := out.Send(ctx, b3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full queue did not backpressure: %v", err)
	}
	if rejected.Load() != 0 {
		t.Fatal("failed Send released caller buffer")
	}
	b3.Release()
	out.Cancel()
	short, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	if err := out.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup claimed complete while callback owns buffer: %v", err)
	}
	once.Do(func() { close(unblock) })
	if err := out.Wait(waitContext(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel outcome: %v", err)
	}
	if first.Load() != 1 || second.Load() != 1 || rejected.Load() != 1 {
		t.Fatalf("release counts %d/%d/%d", first.Load(), second.Load(), rejected.Load())
	}
	got, _ := os.ReadFile(p)
	if string(got) != "a" {
		t.Fatalf("queued work written after abort: %q", got)
	}
}

func TestAcceptanceInvalidURIsOptionsAndBuffers(t *testing.T) {
	for _, raw := range []string{"", "input.txt", "unsupported://127.0.0.1:1", "file://remote/tmp/x", "file:///tmp/x?append=true"} {
		if h, err := uri.OpenInput(context.Background(), raw, uri.Options{}); err == nil {
			h.Cancel()
			t.Fatalf("URI accepted: %q", raw)
		}
	}
	for _, options := range []uri.Options{{QueueCapacity: -1}, {MaxBufferSize: -1}, {QueueCapacity: 1 << 30}, {MaxBufferSize: 1 << 30}} {
		p := filepath.Join(t.TempDir(), "must-not-create")
		if h, err := uri.OpenOutput(context.Background(), fileURI(p), options); err == nil {
			h.Cancel()
			t.Fatal("invalid limits accepted")
		}
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid options caused filesystem effect")
		}
	}
	out, err := uri.OpenOutput(context.Background(), fileURI(filepath.Join(t.TempDir(), "out")), uri.Options{MaxBufferSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	var releases atomic.Int64
	b := uri.NewBuffer([]byte("too large"), func() { releases.Add(1) })
	if err := out.Send(context.Background(), b); err == nil {
		t.Fatal("oversized item accepted")
	}
	if releases.Load() != 0 {
		t.Fatal("rejected buffer recycled")
	}
	b.Release()
	if err := out.Send(context.Background(), nil); err == nil {
		t.Fatal("nil item accepted")
	}
}

func TestAcceptanceConcurrentCloseAndRejectedSend(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out")
	out, err := uri.OpenOutput(context.Background(), fileURI(p), uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	var released atomic.Int64
	for i := 0; i < 10; i++ {
		if err := out.Send(waitContext(t), uri.NewBuffer([]byte("x"), func() { released.Add(1) })); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); out.Close() }()
	}
	wg.Wait()
	if err := out.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
	if released.Load() != 10 {
		t.Fatalf("accepted releases=%d", released.Load())
	}
	b := uri.NewBuffer([]byte("late"), func() { released.Add(1) })
	if err := out.Send(context.Background(), b); err == nil {
		t.Fatal("closed endpoint accepted buffer")
	}
	if released.Load() != 10 {
		t.Fatal("closed Send stole buffer")
	}
	b.Release()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "xxxxxxxxxx" {
		t.Fatalf("graceful close lost bytes: %q", data)
	}
}
