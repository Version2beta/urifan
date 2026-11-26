package urifan_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	fan "github.com/Version2beta/urifan"
	uri "github.com/Version2beta/urifan/transport"
)

func TestAllocatorChargesCapacityAndReturnsPoolLease(t *testing.T) {
	for _, capacity := range []int{8, 16} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			d := t.TempDir()
			in := filepath.Join(d, "in")
			os.WriteFile(in, []byte("data"), 0600)
			var release atomic.Int64
			r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: uriFile(filepath.Join(d, "out"))}}, PacketSize: 4, MaxMemoryBytes: 8, Allocator: func(context.Context, int) (*uri.Buffer, error) {
				return uri.NewBuffer(make([]byte, 0, capacity), func() { release.Add(1) }), nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Cancel()
			err = r.Wait(deadline(t))
			if capacity == 8 {
				if err != nil || r.Stats().PeakRetainedBytes != 8 {
					t.Fatalf("capacity charge %+v %v", r.Stats(), err)
				}
			} else if !errors.Is(err, fan.ErrMemoryLimit) {
				t.Fatalf("oversized allocator lease %v", err)
			}
			if release.Load() != 1 || r.Stats().RetainedBytes != 0 {
				t.Fatal("caller pool lease leaked")
			}
		})
	}
}
func TestRejectsFileSourceAliasAndInvalidFIFOWithoutOverwriting(t *testing.T) {
	d := t.TempDir()
	in, link := filepath.Join(d, "in"), filepath.Join(d, "link")
	os.WriteFile(in, []byte("keep"), 0600)
	if err := os.Link(in, link); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []uri.FileMode{uri.Append, uri.Truncate} {
		if r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: uriFile(link), FileMode: mode}}}); r != nil || !errors.Is(err, fan.ErrInvalidConfig) {
			t.Fatalf("source alias accepted: %v", err)
		}
	}
	if r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: "fifo://" + link, CreateFIFO: true}}}); r != nil || !errors.Is(err, fan.ErrInvalidConfig) {
		t.Fatalf("ordinary file accepted as FIFO: %v", err)
	}
	b, _ := os.ReadFile(in)
	if string(b) != "keep" {
		t.Fatal("preflight overwrote source")
	}
}
