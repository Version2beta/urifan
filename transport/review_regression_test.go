package urihandler_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	uri "github.com/Version2beta/urifan/transport"
)

func TestDecoderCancelledCallDoesNotConsumeHeldRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records")
	os.WriteFile(path, []byte{0, 1, 0, 2}, 0600)
	var allocations, firstRelease atomic.Int64
	opts := uri.Options{MaxBufferSize: 8, Allocator: func(ctx context.Context, n int) (*uri.Buffer, error) {
		id := allocations.Add(1)
		return uri.NewBuffer(make([]byte, n), func() {
			if id == 1 {
				firstRelease.Add(1)
			}
		}), nil
	}}
	in, err := uri.OpenInput(context.Background(), fileURI(path), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	callbacks := 0
	decoder, err := uri.NewDecoder(in, func(dst *number, src []byte) error { callbacks++; return decodeNumber(dst, src) }, uri.Framing{FixedSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	var value number
	if err := decoder.Receive(waitContext(t), &value); err != nil || value.Value != 1 {
		t.Fatalf("first record %v %+v", err, value)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	value.Value = 99
	if err := decoder.Receive(cancelled, &value); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled decoder returned %v", err)
	}
	if callbacks != 1 || value.Value != 99 {
		t.Fatal("cancelled callback mutated caller object")
	}
	if firstRelease.Load() != 1 {
		t.Fatal("held lease not released once on terminal cancellation")
	}
	if err := decoder.Receive(waitContext(t), &value); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled partial stream resumed: %v", err)
	}
}
func TestListenerCompletedWaitWinsOverExpiredContext(t *testing.T) {
	listener, err := uri.Listen(context.Background(), "tcp://127.0.0.1:0", uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	listener.Close()
	if err := listener.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The former select was nondeterministic when both channels were ready.
	for i := 0; i < 128; i++ {
		if err := listener.Wait(ctx); err != nil {
			t.Fatalf("completed cleanup lost to waiting-context error: %v", err)
		}
	}
}
func TestEmptyURIFragmentRejectedButEscapedHashAllowed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent")
	out, err := uri.OpenOutput(context.Background(), fileURI(path)+"#", uri.Options{})
	if out != nil {
		out.Cancel()
		out.Wait(waitContext(t))
	}
	if err == nil {
		t.Fatal("empty fragment delimiter accepted")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid URI created a file")
	}
	hashPath := filepath.Join(t.TempDir(), "name#suffix")
	out, err = uri.OpenOutput(context.Background(), fileURI(hashPath), uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	sendBytes(t, out, []byte("hash"))
	finishOutput(t, out)
	got, err := os.ReadFile(hashPath)
	if err != nil || string(got) != "hash" {
		t.Fatalf("escaped hash path: %q %v", got, err)
	}
}
