package urifan_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	fan "github.com/Version2beta/urifan"
)

func uriFile(p string) string { return (&url.URL{Scheme: "file", Path: p}).String() }
func deadline(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return c
}
func event(t *testing.T, r *fan.Relay, kind, id string) fan.Event {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case e, ok := <-r.Events():
			if !ok {
				t.Fatal("events closed")
			}
			if e.Kind == kind && (id == "" || e.ID == id) {
				return e
			}
		case <-timer.C:
			t.Fatalf("missing %s %s", kind, id)
		}
	}
}
func tcp(t *testing.T, address string) net.Conn {
	t.Helper()
	c, e := net.DialTimeout("tcp", address, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(3 * time.Second))
	return c
}
func read(t *testing.T, c net.Conn, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, e := io.ReadFull(c, b); e != nil {
		t.Fatal(e)
	}
	return string(b)
}
func TestAcceptanceFileFanoutAndExclusiveCreation(t *testing.T) {
	d := t.TempDir()
	in := filepath.Join(d, "in")
	os.WriteFile(in, []byte("abcdefghijklmnopq"), 0600)
	a, b := filepath.Join(d, "a"), filepath.Join(d, "b")
	r, e := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: uriFile(a)}, {URI: uriFile(b)}}, PacketSize: 4})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Cancel()
	if e = r.Wait(deadline(t)); e != nil {
		t.Fatal(e)
	}
	for _, p := range []string{a, b} {
		v, _ := os.ReadFile(p)
		if string(v) != "abcdefghijklmnopq" {
			t.Fatalf("fanout %q", v)
		}
	}
	if _, e = fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: uriFile(a)}}}); e == nil {
		t.Fatal("replaced existing output")
	}
}
func TestAcceptanceSourceContinuationAndNoDestinationReplay(t *testing.T) {
	r, e := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "tcp://127.0.0.1:0", Listen: true}, Destinations: []fan.Endpoint{{ID: "sink", URI: "tcp://127.0.0.1:0", Listen: true}}, PacketSize: 4, FlushInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Cancel()
	dst := tcp(t, r.Addresses()["sink"])
	event(t, r, "destination_connected", "")
	first := tcp(t, r.Addresses()["source"])
	event(t, r, "source_connected", "")
	first.Write([]byte("ab"))
	first.Close()
	event(t, r, "source_disconnected", "")
	second := tcp(t, r.Addresses()["source"])
	event(t, r, "source_connected", "")
	second.Write([]byte("cd"))
	if got := read(t, dst, 4); got != "abcd" {
		t.Fatal(got)
	}
	second.Write([]byte("ef")) // Force admission of the partial packet before joining.
	admissionDeadline := deadline(t)
	for r.Stats().SourceBytes < 6 {
		select {
		case <-admissionDeadline.Done():
			t.Fatal("source admission")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	late := tcp(t, r.Addresses()["sink"])
	event(t, r, "destination_connected", "")
	second.Write([]byte("ghijkl"))
	if got := read(t, dst, 8); got != "efghijkl" {
		t.Fatal(got)
	}
	if got := read(t, late, 4); got != "ijkl" {
		t.Fatalf("replayed partial packet: %q", got)
	}
	r.Close()
	if e = r.Wait(deadline(t)); e != nil {
		t.Fatal(e)
	}
}
func TestAcceptanceUDPBoundariesAndEmptyDatagram(t *testing.T) {
	recv, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer recv.Close()
	r, e := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "udp://127.0.0.1:0"}, Destinations: []fan.Endpoint{{URI: "udp://" + recv.LocalAddr().String()}}, PacketSize: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Cancel()
	send, e := net.Dial("udp", r.Addresses()["source"])
	if e != nil {
		t.Fatal(e)
	}
	defer send.Close()
	for _, s := range []string{"abc", "", "defgh"} {
		send.Write([]byte(s))
		recv.SetReadDeadline(time.Now().Add(time.Second))
		b := make([]byte, 32)
		n, _, err := recv.ReadFrom(b)
		if err != nil || string(b[:n]) != s {
			t.Fatalf("datagram %q %v", b[:n], err)
		}
	}
	r.Close()
	if e = r.Wait(deadline(t)); e != nil {
		t.Fatal(e)
	}
}
func TestAcceptanceStreamPacketizationAndTimedPartial(t *testing.T) {
	recv, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer recv.Close()
	r, e := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "tcp://127.0.0.1:0", Listen: true}, Destinations: []fan.Endpoint{{URI: "udp://" + recv.LocalAddr().String()}}, PacketSize: 4, FlushInterval: 30 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Cancel()
	c := tcp(t, r.Addresses()["source"])
	c.Write([]byte("a"))
	c.Write([]byte("bcdef"))
	for _, want := range []string{"abcd", "ef"} {
		recv.SetReadDeadline(time.Now().Add(time.Second))
		b := make([]byte, 32)
		n, _, err := recv.ReadFrom(b)
		if err != nil || string(b[:n]) != want {
			t.Fatalf("packet %q want %q: %v", b[:n], want, err)
		}
	}
	r.Close()
	if e = r.Wait(deadline(t)); e != nil {
		t.Fatal(e)
	}
}
func TestAcceptanceOversizedDestinationIsolatedAndFailureSticky(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "output")
	recv, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer recv.Close()
	r, e := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "udp://127.0.0.1:0"}, Destinations: []fan.Endpoint{{URI: uriFile(path)}, {ID: "small", URI: "udp://" + recv.LocalAddr().String(), PacketSize: 2}}, EventCapacity: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Cancel()
	c, e := net.Dial("udp", r.Addresses()["source"])
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.Write([]byte("abc"))
	limit := deadline(t)
	for r.Stats().SourceBytes < 3 {
		select {
		case <-limit.Done():
			t.Fatal("source not received")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	r.Close()
	if e = r.Wait(deadline(t)); !errors.Is(e, fan.ErrDestinationFailed) {
		t.Fatalf("lost failure outcome: %v", e)
	}
	got, _ := os.ReadFile(path)
	if string(got) != "abc" {
		t.Fatalf("healthy destination %q", got)
	}
	if r.Stats().LostEvents == 0 {
		t.Fatal("expected bounded event overflow")
	}
}
