package urifan_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	fan "github.com/Version2beta/urifan"
)

// ns/op measures loopback source submission through receipt at every sink. It
// excludes setup and shutdown and is informational, not a network latency SLA.
func BenchmarkPacketLatency(b *testing.B) {
	for _, size := range []int{1200, 8192} {
		for _, count := range []int{1, 4} {
			b.Run(fmt.Sprintf("bytes=%d/sinks=%d", size, count), func(b *testing.B) {
				destinations := make([]fan.Endpoint, count)
				receivers := make([]net.PacketConn, count)
				for i := range receivers {
					c, err := net.ListenPacket("udp", "127.0.0.1:0")
					if err != nil {
						b.Fatal(err)
					}
					defer c.Close()
					receivers[i] = c
					destinations[i] = fan.Endpoint{URI: "udp://" + c.LocalAddr().String()}
				}
				r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "udp://127.0.0.1:0"}, Destinations: destinations})
				if err != nil {
					b.Fatal(err)
				}
				defer r.Cancel()
				sender, err := net.Dial("udp", r.Addresses()["source"])
				if err != nil {
					b.Fatal(err)
				}
				defer sender.Close()
				payload, buffer := make([]byte, size), make([]byte, size)
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if _, err := sender.Write(payload); err != nil {
						b.Fatal(err)
					}
					for _, c := range receivers {
						c.SetReadDeadline(time.Now().Add(time.Second))
						n, _, err := c.ReadFrom(buffer)
						if err != nil || n != size {
							b.Fatalf("datagram %d %v", n, err)
						}
					}
				}
				b.StopTimer()
				r.Close()
				if err = r.Wait(context.Background()); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(r.Stats().PeakRetainedBytes), "peak-live-B")
			})
		}
	}
}
