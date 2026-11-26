package urifan

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"testing"

	uri "github.com/Version2beta/urifan/transport"
)

// These unit tests hold queue custody explicitly to make pressure deterministic;
// public transport tests separately exercise real writer shutdown and fan-out.
func TestGlobalMemoryPressureRetiresOldestAndNeverExceedsBudget(t *testing.T) {
	c, err := normalize(Config{Source: Endpoint{URI: "file:///unused"}, PacketSize: 4, MaxMemoryBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	r := &Relay{config: c, ctx: context.Background(), destinations: make(map[string]*destination), all: make(map[string]*destination), events: make(chan Event, 16), released: make(chan struct{}, 1)}
	add := func(id string) *destination {
		raw := (&url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), id)}).String()
		out, err := uri.OpenOutput(context.Background(), raw, uri.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { out.Cancel(); out.Wait(context.Background()) })
		r.install(Endpoint{ID: id, URI: raw}, out, nil, false)
		return r.destinations[id]
	}
	old, newer := add("old"), add("newer")
	first, err := r.newPacket(4)
	if err != nil {
		t.Fatal(err)
	}
	r.dispatch(first, []*destination{old})
	second, err := r.newPacket(4)
	if err != nil {
		t.Fatal(err)
	}
	r.dispatch(second, []*destination{newer})
	next, err := r.newPacket(4)
	if err != nil {
		t.Fatal(err)
	}
	if r.destinations["old"] != nil || r.destinations["newer"] == nil {
		t.Fatal("did not evict oldest outstanding destination")
	}
	if r.Stats().PeakRetainedBytes > 8 {
		t.Fatal("global budget exceeded")
	}
	next.release()
	newer.abort()
	if r.Stats().RetainedBytes != 0 {
		t.Fatal("retained storage leak")
	}
}
func TestSinglePacketOverGlobalBudgetIsRejectedWithoutAllocation(t *testing.T) {
	r := &Relay{config: Config{MaxMemoryBytes: 4}, ctx: context.Background()}
	if _, err := r.newPacket(5); !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("oversized allocation %v", err)
	}
	if r.counters.memory.Load() != 0 {
		t.Fatal("charged failed allocation")
	}
}
