package cli_test

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Version2beta/urifan/internal/cli"
)

func file(p string) string { return (&url.URL{Scheme: "file", Path: p}).String() }
func TestCLIVersionDoesNotOpenEndpoints(t *testing.T) {
	var out, err bytes.Buffer
	if code := cli.Run(context.Background(), []string{"--version"}, &out, &err); code != 0 || !strings.Contains(out.String(), "urifan dev (unknown)") || err.Len() != 0 {
		t.Fatalf("version: %d %q %q", code, out.String(), err.String())
	}
}

func TestCLIFileFanoutAndDiagnosticsSeparation(t *testing.T) {
	d := t.TempDir()
	in, out := filepath.Join(d, "in"), filepath.Join(d, "out")
	os.WriteFile(in, []byte("payload"), 0600)
	var stdout, stderr bytes.Buffer
	args := []string{"--source", file(in), "--destination", file(out), "--packet-size", "3"}
	if status := cli.Run(context.Background(), args, &stdout, &stderr); status != 0 {
		t.Fatalf("exit %d: %s", status, &stderr)
	}
	got, _ := os.ReadFile(out)
	if string(got) != "payload" || stdout.Len() != 0 {
		t.Fatal("payload/diagnostic separation")
	}
	if !strings.Contains(stderr.String(), "destination_connected") {
		t.Fatal("missing lifecycle diagnostics")
	}
	if status := cli.Run(context.Background(), args, &stdout, &stderr); status != 1 {
		t.Fatalf("existing file exit %d", status)
	}
	args = append(args, "--append")
	if status := cli.Run(context.Background(), args, &stdout, &stderr); status != 0 {
		t.Fatal(stderr.String())
	}
	got, _ = os.ReadFile(out)
	if string(got) != "payloadpayload" {
		t.Fatalf("append %q", got)
	}
}
func TestCLIHelpAndInvalidArguments(t *testing.T) {
	var out, err bytes.Buffer
	if code := cli.Run(context.Background(), []string{"--help"}, &out, &err); code != 0 || !strings.Contains(out.String(), "listen-source") {
		t.Fatal("help")
	}
	for _, args := range [][]string{
		{}, {"--source", "tcp://127.0.0.1:9000"},
		{"--source", "file:///absent", "--packet-size", "-1"},
		{"--source", "file:///absent", "--append", "--overwrite"},
		{"--source", "file:///absent", "--destination-packet-size", "missing=100"},
		{"--source", "file:///absent", "--source", "file:///other"},
	} {
		if code := cli.Run(context.Background(), args, &out, &err); code != 2 {
			t.Fatalf("arguments %v exit %d: %s", args, code, &err)
		}
	}
}
func TestCLIParseErrorsDoNotEchoCredentialURI(t *testing.T) {
	var diagnostic bytes.Buffer
	// Synthetic fixture, not a real credential. Invalid role must not make flag's
	// default error formatting echo the supplied authority into diagnostics.
	code := cli.Run(context.Background(), []string{"--source", "tcp://example:synthetic-password@127.0.0.1:9"}, io.Discard, &diagnostic)
	if code != 2 || strings.Contains(diagnostic.String(), "synthetic-password") {
		t.Fatalf("unsafe parse diagnostic: %q", diagnostic.String())
	}
}

func TestCLIAlreadyCancelledDoesNotCreateOutput(t *testing.T) {
	d := t.TempDir()
	in, out := filepath.Join(d, "in"), filepath.Join(d, "out")
	os.WriteFile(in, []byte("x"), 0600)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if status := cli.Run(ctx, []string{"--source", file(in), "--destination", file(out)}, io.Discard, io.Discard); status != 1 {
		t.Fatal(status)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("cancelled CLI created output")
	}
}

type readyWriter struct {
	once  sync.Once
	ready chan struct{}
}

func (w *readyWriter) Write(b []byte) (int, error) {
	if bytes.Contains(b, []byte("source_connected")) {
		w.once.Do(func() { close(w.ready) })
	}
	return len(b), nil
}
func TestCLIContextRequestsGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &readyWriter{ready: make(chan struct{})}
	done := make(chan int, 1)
	go func() { done <- cli.Run(ctx, []string{"--source", "udp://127.0.0.1:0"}, io.Discard, writer) }()
	select {
	case <-writer.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("CLI not ready")
	}
	cancel()
	select {
	case status := <-done:
		if status != 0 {
			t.Fatal(status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CLI failed to shut down")
	}
}
