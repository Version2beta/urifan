package urifan_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fan "github.com/Version2beta/urifan"
	uri "github.com/Version2beta/urifan/transport"
)

func TestShutdownDeadlineCancelsBlockedDestinationWithoutPrematureRelease(t *testing.T) {
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
	var acquired, released atomic.Int64
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "tcp://127.0.0.1:0", Listen: true}, Destinations: []fan.Endpoint{{URI: "tcp://" + listener.Addr().String()}}, PacketSize: 1 << 20, QueueBytes: 16 << 20, ShutdownTimeout: 20 * time.Millisecond, Allocator: func(ctx context.Context, n int) (*uri.Buffer, error) {
		acquired.Add(1)
		return uri.NewBuffer(make([]byte, n), func() { released.Add(1) }), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	c := <-accepted
	defer c.Close()
	c.(*net.TCPConn).SetReadBuffer(1024)
	source := tcp(t, r.Addresses()["source"])
	data := strings.Repeat("x", 8<<20)
	if _, err := source.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return r.Stats().SourceBytes == uint64(len(data)) })
	if released.Load() >= acquired.Load() {
		t.Fatal("blocked destination had already recycled all payloads")
	}
	r.Close()
	if err = r.Wait(deadline(t)); !errors.Is(err, fan.ErrShutdownTimeout) {
		t.Fatalf("shutdown deadline: %v", err)
	}
	if acquired.Load() != released.Load() || r.Stats().RetainedBytes != 0 {
		t.Fatalf("lease leak %d/%d %+v", released.Load(), acquired.Load(), r.Stats())
	}
}
func TestFailedWriteIsReportedWithoutAnotherSourcePacket(t *testing.T) {
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
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "tcp://127.0.0.1:0", Listen: true}, Destinations: []fan.Endpoint{{ID: "sink", URI: "tcp://" + listener.Addr().String()}}, PacketSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	c := <-accepted
	c.(*net.TCPConn).SetLinger(0)
	c.Close()
	source := tcp(t, r.Addresses()["source"])
	source.Write([]byte("once"))
	e := event(t, r, "destination_failed", "sink")
	if e.Error == nil {
		t.Fatal("lost output error")
	}
	r.Close()
	if err = r.Wait(deadline(t)); !errors.Is(err, fan.ErrDestinationFailed) {
		t.Fatal(err)
	}
}
func TestSourceReadErrorContinuesAcrossReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out")
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "tcp://127.0.0.1:0", Listen: true}, Destinations: []fan.Endpoint{{URI: uriFile(path)}}, PacketSize: 4, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	source := tcp(t, r.Addresses()["source"])
	source.Write([]byte("ab"))
	eventually(t, func() bool { return r.Stats().SourceBytes == 2 })
	source.(*net.TCPConn).SetLinger(0)
	source.Close()
	e := event(t, r, "source_disconnected", "")
	if e.Error == nil {
		t.Fatal("expected source reset error")
	}
	next := tcp(t, r.Addresses()["source"])
	next.Write([]byte("cd"))
	eventually(t, func() bool { b, _ := os.ReadFile(path); return string(b) == "abcd" })
	r.Close()
	if err = r.Wait(deadline(t)); err != nil {
		t.Fatal(err)
	}
}
func TestAllocatorErrorReleasesReturnedLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "in")
	os.WriteFile(path, []byte("data"), 0600)
	expected := errors.New("allocator failure")
	var released atomic.Int64
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(path)}, Destinations: []fan.Endpoint{{URI: uriFile(filepath.Join(t.TempDir(), "out"))}}, Allocator: func(context.Context, int) (*uri.Buffer, error) {
		return uri.NewBuffer(make([]byte, 1200), func() { released.Add(1) }), expected
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	if err = r.Wait(deadline(t)); !errors.Is(err, expected) {
		t.Fatalf("allocator result %v", err)
	}
	if released.Load() != 1 || r.Stats().RetainedBytes != 0 {
		t.Fatal("allocator lease leaked")
	}
}
func TestConcurrentCloseCancelAndSnapshot(t *testing.T) {
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "udp://127.0.0.1:0"}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.Stats(); r.Addresses(); r.Close(); r.Cancel() }()
	}
	wg.Wait()
	if err = r.Wait(deadline(t)); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
