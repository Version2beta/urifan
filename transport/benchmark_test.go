package urihandler_test

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"

	uri "github.com/Version2beta/urifan/transport"
)

// This measures local file writes through a warmable byte pool, including drain;
// it is not network throughput or durable-storage performance.
func BenchmarkOutputReusableBuffers(b *testing.B) {
	for _, size := range []int{1316, 64 * 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "output")
			address := (&url.URL{Scheme: "file", Path: path}).String()
			out, err := uri.OpenOutput(context.Background(), address, uri.Options{MaxBufferSize: size})
			if err != nil {
				b.Fatal(err)
			}
			defer out.Cancel()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buffer, err := out.Acquire(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if err := out.Send(context.Background(), buffer); err != nil {
					buffer.Release()
					b.Fatal(err)
				}
			}
			out.Close()
			if err := out.Wait(context.Background()); err != nil {
				b.Fatal(err)
			}
		})
	}
}
