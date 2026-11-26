package urifan_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fan "github.com/Version2beta/urifan"
	uri "github.com/Version2beta/urifan/transport"
)

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	ctx := deadline(t)
	for !f() {
		select {
		case <-ctx.Done():
			t.Fatal("condition not met")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestFIFOReaderWaitDoesNotBlockAndHasNoHistory(t *testing.T) {
	d := t.TempDir()
	pipe, path := filepath.Join(d, "pipe"), filepath.Join(d, "file")
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "tcp://127.0.0.1:0", Listen: true}, Destinations: []fan.Endpoint{{URI: uriFile(path)}, {ID: "pipe", URI: "fifo://" + pipe, CreateFIFO: true}}, PacketSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	source := tcp(t, r.Addresses()["source"])
	source.Write([]byte("abcd"))
	eventually(t, func() bool { b, _ := os.ReadFile(path); return string(b) == "abcd" })
	eventually(t, func() bool { _, e := os.Stat(pipe); return e == nil })
	reader, err := uri.OpenInput(context.Background(), "fifo://"+pipe, uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Cancel()
	event(t, r, "destination_connected", "pipe")
	source.Write([]byte("efgh"))
	var got []byte
	fifoDeadline := deadline(t)
	for len(got) < 4 {
		select {
		case b := <-reader.Data():
			if b == nil {
				t.Fatal("FIFO ended")
			}
			got = append(got, b.Data...)
			b.Release()
		case <-fifoDeadline.Done():
			t.Fatal("FIFO output timeout")
		}
	}
	if string(got) != "efgh" {
		t.Fatalf("replayed preconnection bytes %q", got)
	}
	r.Close()
	if err = r.Wait(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(pipe); err != nil || st.Mode()&os.ModeNamedPipe == 0 {
		t.Fatal("FIFO path removed")
	}
	b, _ := os.ReadFile(path)
	if string(b) != "abcdefgh" {
		t.Fatalf("healthy file output %q", b)
	}
}
func TestFIFOSourceWriterSessionsContinuePartialPacket(t *testing.T) {
	d := t.TempDir()
	pipe, path := filepath.Join(d, "pipe"), filepath.Join(d, "file")
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "fifo://" + pipe, CreateFIFO: true}, Destinations: []fan.Endpoint{{URI: uriFile(path)}}, PacketSize: 4, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	write := func(s string) {
		out, e := uri.OpenOutput(deadline(t), "fifo://"+pipe, uri.Options{})
		if e != nil {
			t.Fatal(e)
		}
		defer out.Cancel()
		if e = out.Send(deadline(t), uri.NewBuffer([]byte(s), nil)); e != nil {
			t.Fatal(e)
		}
		out.Close()
		if e = out.Wait(deadline(t)); e != nil {
			t.Fatal(e)
		}
	}
	write("ab")
	event(t, r, "source_disconnected", "")
	write("cd")
	eventually(t, func() bool { b, _ := os.ReadFile(path); return string(b) == "abcd" })
	r.Close()
	if err = r.Wait(deadline(t)); err != nil {
		t.Fatal(err)
	}
}
func TestUnixListenersAndCallerPathsPreserved(t *testing.T) {
	d, err := os.MkdirTemp("/tmp", "urifan-unix-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(d)
	a, b := filepath.Join(d, "source"), filepath.Join(d, "sink")
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "unix://" + a, Listen: true}, Destinations: []fan.Endpoint{{ID: "sink", URI: "unix://" + b, Listen: true}}, PacketSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	sink, err := net.Dial("unix", b)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	sink.SetDeadline(time.Now().Add(3 * time.Second))
	event(t, r, "destination_connected", "")
	source, err := net.Dial("unix", a)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	source.Write([]byte("unix"))
	if got := read(t, sink, 4); got != "unix" {
		t.Fatal(got)
	}
	r.Close()
	if err = r.Wait(deadline(t)); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{a, b} {
		if _, err := os.Lstat(p); err != nil {
			t.Fatal("socket path removed")
		}
	}
}
func TestCompetingSourcesAndDestinationLimit(t *testing.T) {
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "tcp://127.0.0.1:0", Listen: true}, Destinations: []fan.Endpoint{{ID: "sink", URI: "tcp://127.0.0.1:0", Listen: true}}, PacketSize: 4, MaxDestinations: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	sink := tcp(t, r.Addresses()["sink"])
	event(t, r, "destination_connected", "")
	extraSink := tcp(t, r.Addresses()["sink"])
	event(t, r, "destination_rejected", "")
	var v [1]byte
	if n, e := extraSink.Read(v[:]); n != 0 || e == nil {
		t.Fatal("extra destination survived")
	}
	first := tcp(t, r.Addresses()["source"])
	event(t, r, "source_connected", "")
	extra := tcp(t, r.Addresses()["source"])
	event(t, r, "source_rejected", "")
	if n, e := extra.Read(v[:]); n != 0 || e == nil {
		t.Fatal("extra source survived")
	}
	first.Write([]byte("only"))
	if got := read(t, sink, 4); got != "only" {
		t.Fatal(got)
	}
	r.Close()
	if err = r.Wait(deadline(t)); err != nil {
		t.Fatal(err)
	}
}
func TestSlowDestinationRemovalDoesNotLoseHealthyOutput(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, e := listener.Accept()
		if e == nil {
			accepted <- c
		}
	}()
	d := t.TempDir()
	in, out := filepath.Join(d, "in"), filepath.Join(d, "out")
	data := strings.Repeat("x", 8<<20)
	os.WriteFile(in, []byte(data), 0600)
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{ID: "slow", URI: "tcp://" + listener.Addr().String(), QueuePackets: 1}, {ID: "healthy", URI: uriFile(out)}}, PacketSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	c := <-accepted
	defer c.Close()
	if tcp, ok := c.(*net.TCPConn); ok {
		tcp.SetReadBuffer(1024)
	}
	if err = r.Wait(deadline(t)); !errors.Is(err, fan.ErrDestinationFailed) {
		t.Fatalf("slow destination outcome %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil || string(b) != data {
		t.Fatalf("healthy bytes %d want %d: %v", len(b), len(data), err)
	}
	if stats := r.Stats(); stats.RetainedBytes != 0 || stats.PeakRetainedBytes > 64<<20 || stats.ActiveDestinations != 0 {
		t.Fatalf("cleanup/bounds %+v", stats)
	}
}
func TestZeroDestinationsDiscardAndShutdown(t *testing.T) {
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "tcp://127.0.0.1:0", Listen: true}, Destinations: []fan.Endpoint{{ID: "sink", URI: "tcp://127.0.0.1:0", Listen: true}}, PacketSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	c := tcp(t, r.Addresses()["source"])
	c.Write([]byte("past"))
	eventually(t, func() bool { return r.Stats().SourceBytes == 4 })
	sink := tcp(t, r.Addresses()["sink"])
	event(t, r, "destination_connected", "")
	c.Write([]byte("next"))
	if got := read(t, sink, 4); got != "next" {
		t.Fatalf("history %q", got)
	}
	r.Close()
	if err = r.Wait(deadline(t)); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err = r.Wait(cancelled); err != nil {
		t.Fatal("completed Wait lost to expired context")
	}
}
func TestInvalidConfigurationHasNoFileEffects(t *testing.T) {
	for _, source := range []fan.Endpoint{{URI: "http://example.test"}, {URI: "file:///bad%00path"}, {URI: "tcp://127.0.0.1:bad", Listen: true}} {
		path := filepath.Join(t.TempDir(), "absent")
		_, err := fan.Start(context.Background(), fan.Config{Source: source, Destinations: []fan.Endpoint{{URI: uriFile(path)}}})
		if !errors.Is(err, fan.ErrInvalidConfig) {
			t.Fatalf("invalid source %v", err)
		}
		if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid config created output")
		}
	}
}
func TestStartupFailureAndExplicitFileModes(t *testing.T) {
	d := t.TempDir()
	in, out := filepath.Join(d, "in"), filepath.Join(d, "out")
	os.WriteFile(in, []byte("new"), 0600)
	os.WriteFile(out, []byte("old"), 0600)
	for _, mode := range []uri.FileMode{uri.Append, uri.Truncate} {
		r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: uriFile(out), FileMode: mode}}})
		if err != nil {
			t.Fatal(err)
		}
		if err = r.Wait(deadline(t)); err != nil {
			t.Fatal(err)
		}
		b, _ := os.ReadFile(out)
		want := "oldnew"
		if mode == uri.Truncate {
			want = "new"
		}
		if string(b) != want {
			t.Fatal(string(b))
		}
	}
	// A refused direct destination must fail startup before the source starts.
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := refused.Addr().String()
	refused.Close()
	if r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: "tcp://" + address}}}); r != nil || err == nil {
		t.Fatal("silent omission of failed destination")
	}
}
func TestParentCancellationOfIdleNetworkInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r, err := fan.Start(ctx, fan.Config{Source: fan.Endpoint{URI: "udp://127.0.0.1:0"}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err = r.Wait(deadline(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel %v", err)
	}
}
