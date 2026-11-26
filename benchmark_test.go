package urifan_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	fan "github.com/Version2beta/urifan"
)

// End-to-end local page-cache/file benchmark, including setup, forwarding and
// drain, not durable-storage or WAN throughput. Use -benchtime=3x to bound I/O.
func BenchmarkFanout(b *testing.B) {
	for _, size := range []int{1200, 65536} {
		for _, sinks := range []int{1, 4} {
			b.Run(fmt.Sprintf("packet=%d/sinks=%d", size, sinks), func(b *testing.B) {
				d := b.TempDir()
				in := filepath.Join(d, "in")
				payload := make([]byte, 256<<10)
				os.WriteFile(in, payload, 0600)
				var peak int64
				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					outputs := make([]fan.Endpoint, sinks)
					for i := range outputs {
						outputs[i] = fan.Endpoint{URI: uriFile(filepath.Join(d, fmt.Sprint(i)))}
					}
					r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: outputs, PacketSize: size})
					if err != nil {
						b.Fatal(err)
					}
					if err = r.Wait(context.Background()); err != nil {
						b.Fatal(err)
					}
					if p := r.Stats().PeakRetainedBytes; p > peak {
						peak = p
					}
					b.StopTimer()
					for i := range outputs {
						path := filepath.Join(d, fmt.Sprint(i))
						st, err := os.Stat(path)
						if err != nil || st.Size() != int64(len(payload)) {
							b.Fatal("incomplete benchmark output")
						}
						os.Remove(path)
					}
					b.StartTimer()
				}
				b.ReportMetric(float64(peak), "peak-live-B")
			})
		}
	}
}
func BenchmarkFanoutWithNonreadingSink(b *testing.B) {
	d := b.TempDir()
	in, out := filepath.Join(d, "in"), filepath.Join(d, "out")
	payload := make([]byte, 8<<20)
	os.WriteFile(in, payload, 0600)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		func() {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				c, e := listener.Accept()
				if e == nil {
					accepted <- c
				}
			}()
			r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(in)}, Destinations: []fan.Endpoint{{URI: uriFile(out), QueuePackets: 1024, QueueBytes: 16 << 20}, {URI: "tcp://" + listener.Addr().String(), QueuePackets: 1}}, PacketSize: 65536})
			if err != nil {
				b.Fatal(err)
			}
			defer r.Cancel()
			peer := <-accepted
			defer peer.Close()
			peer.(*net.TCPConn).SetReadBuffer(1024)
			if err = r.Wait(context.Background()); !errors.Is(err, fan.ErrDestinationFailed) {
				b.Fatalf("nonreading sink not isolated: %v", err)
			}
			b.StopTimer()
			st, err := os.Stat(out)
			if err != nil || st.Size() != int64(len(payload)) {
				b.Fatal("healthy destination incomplete")
			}
			os.Remove(out)
			b.StartTimer()
		}()
	}
}
