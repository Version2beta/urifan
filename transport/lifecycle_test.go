package urihandler

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

// This development regression checks the cleanup obligation found in the
// independent review; it is not part of the frozen external acceptance suite.
func TestNormalCompletionCancelsOwnedChildContext(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	address := func(name string) string { return (&url.URL{Scheme: "file", Path: filepath.Join(dir, name)}).String() }
	if err := os.WriteFile(filepath.Join(dir, "empty"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	input, err := OpenInput(parent, address("empty"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer input.Cancel()
	for b := range input.Data() {
		b.Release()
	}
	if err := input.Wait(parent); err != nil {
		t.Fatal(err)
	}
	select {
	case <-input.life.ctx.Done():
	default:
		t.Fatal("completed input retained uncancelled child context")
	}
	output, err := OpenOutput(parent, address("new"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer output.Cancel()
	output.Close()
	if err := output.Wait(parent); err != nil {
		t.Fatal(err)
	}
	select {
	case <-output.life.ctx.Done():
	default:
		t.Fatal("completed output retained uncancelled child context")
	}
	if err := output.Wait(parent); err != nil {
		t.Fatalf("cleanup cancellation changed successful outcome: %v", err)
	}
	if parent.Err() != nil {
		t.Fatal("child cleanup cancelled caller's context")
	}
}
