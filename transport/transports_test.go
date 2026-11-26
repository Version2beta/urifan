package urihandler_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

func allBytes(t *testing.T, in *uri.Input) []byte {
	t.Helper()
	ctx := waitContext(t)
	var result []byte
	for {
		select {
		case b, ok := <-in.Data():
			if !ok {
				if err := in.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				return result
			}
			result = append(result, b.Data...)
			b.Release()
		case <-ctx.Done():
			t.Fatal("input did not finish")
			return nil
		}
	}
}
func sendBytes(t *testing.T, out *uri.Output, data []byte) {
	t.Helper()
	b := uri.NewBuffer(data, nil)
	if err := out.Send(waitContext(t), b); err != nil {
		b.Release()
		t.Fatal(err)
	}
}
func finishOutput(t *testing.T, out *uri.Output) {
	t.Helper()
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if err := out.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
}
func shortSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "uh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return "unix://" + filepath.Join(dir, "socket")
}

func TestTCPAndUnixStreamEndpoints(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			address := "tcp://127.0.0.1:0"
			if network == "unix" {
				address = shortSocket(t)
			}
			listener, err := uri.Listen(context.Background(), address, uri.Options{MaxBufferSize: 7})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if network == "tcp" {
				address = "tcp://" + listener.Addr().String()
			}
			out, err := uri.OpenOutput(waitContext(t), address, uri.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer out.Cancel()
			in, err := listener.AcceptInput(waitContext(t))
			if err != nil {
				t.Fatal(err)
			}
			defer in.Cancel()
			sendBytes(t, out, []byte("first"))
			sendBytes(t, out, []byte("second"))
			finishOutput(t, out)
			if got := allBytes(t, in); string(got) != "firstsecond" {
				t.Fatalf("stream bytes %q", got)
			}
			listener.Close()
			if err := listener.Wait(waitContext(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestListenerConnectionsRemainSeparateAndSurviveListenerClose(t *testing.T) {
	l, err := uri.Listen(context.Background(), "tcp://127.0.0.1:0", uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a, err := uri.OpenOutput(waitContext(t), "tcp://"+l.Addr().String(), uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Cancel()
	first, err := l.AcceptInput(waitContext(t))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Cancel()
	b, err := uri.OpenOutput(waitContext(t), "tcp://"+l.Addr().String(), uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Cancel()
	second, err := l.AcceptInput(waitContext(t))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Cancel()
	l.Close()
	if err := l.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
	sendBytes(t, a, []byte("A"))
	sendBytes(t, b, []byte("B"))
	finishOutput(t, a)
	finishOutput(t, b)
	if string(allBytes(t, first)) != "A" || string(allBytes(t, second)) != "B" {
		t.Fatal("connections mixed or closed with listener")
	}
}
func TestListenerOutputAndAcceptCancellation(t *testing.T) {
	l, err := uri.Listen(context.Background(), "tcp://127.0.0.1:0", uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.AcceptOutput(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled accept %v", err)
	}
	client, err := net.DialTimeout("tcp", l.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	output, err := l.AcceptOutput(waitContext(t))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Cancel()
	sendBytes(t, output, []byte("server output"))
	finishOutput(t, output)
	got, err := io.ReadAll(client)
	if err != nil || string(got) != "server output" {
		t.Fatalf("server write %q %v", got, err)
	}
}
func TestNetworkReadCancellationAndTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[timeout], func(t *testing.T) {
			started := make(chan struct{})
			var released atomic.Int64
			opts := uri.Options{Allocator: func(ctx context.Context, n int) (*uri.Buffer, error) {
				close(started)
				return uri.NewBuffer(make([]byte, n), func() { released.Add(1) }), nil
			}}
			if timeout {
				opts.IOTimeout = 30 * time.Millisecond
			}
			l, err := uri.Listen(context.Background(), "tcp://127.0.0.1:0", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			client, err := net.DialTimeout("tcp", l.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			in, err := l.AcceptInput(waitContext(t))
			if err != nil {
				t.Fatal(err)
			}
			defer in.Cancel()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("read not started")
			}
			if !timeout {
				in.Cancel()
			}
			err = in.Wait(waitContext(t))
			if timeout {
				var ne net.Error
				if !errors.As(err, &ne) || !ne.Timeout() {
					t.Fatalf("timeout not reported: %v", err)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel not reported: %v", err)
			}
			if released.Load() != 1 {
				t.Fatalf("in-flight release %d", released.Load())
			}
		})
	}
}
func TestUDPDatagramBoundariesAndEmptyDatagram(t *testing.T) {
	in, err := uri.OpenInput(context.Background(), "udp://127.0.0.1:0", uri.Options{MaxBufferSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	out, err := uri.OpenOutput(waitContext(t), "udp://"+in.Addr().String(), uri.Options{MaxBufferSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	for _, s := range []string{"one", "", "two-two"} {
		sendBytes(t, out, []byte(s))
	}
	finishOutput(t, out)
	for _, s := range []string{"one", "", "two-two"} {
		b := receive(t, in.Data())
		if string(b.Data) != s {
			t.Fatalf("datagram %q want %q", b.Data, s)
		}
		b.Release()
	}
	in.Cancel()
	if err := in.Wait(waitContext(t)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestUDPTruncationIsAnErrorNotPartialDelivery(t *testing.T) {
	in, err := uri.OpenInput(context.Background(), "udp://127.0.0.1:0", uri.Options{MaxBufferSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	out, err := uri.OpenOutput(waitContext(t), "udp://"+in.Addr().String(), uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	sendBytes(t, out, []byte("too long"))
	finishOutput(t, out)
	if err := in.Wait(waitContext(t)); !errors.Is(err, uri.ErrDatagramTruncated) {
		t.Fatalf("truncation %v", err)
	}
	if b, ok := <-in.Data(); ok {
		b.Release()
		t.Fatal("truncated payload delivered")
	}
}
func TestUDPPackingFullAndTimedPartialBatch(t *testing.T) {
	in, err := uri.OpenInput(context.Background(), "udp://127.0.0.1:0", uri.Options{MaxBufferSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	out, err := uri.OpenOutput(waitContext(t), "udp://"+in.Addr().String(), uri.Options{MaxBufferSize: 16, DatagramPayloadSize: 6, FlushInterval: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	for _, s := range []string{"ab", "cd", "ef", "gh"} {
		sendBytes(t, out, []byte(s))
	}
	for _, want := range []string{"abcdef", "gh"} {
		b := receive(t, in.Data())
		if string(b.Data) != want {
			t.Fatalf("packing %q want %q", b.Data, want)
		}
		b.Release()
	}
	finishOutput(t, out)
}
func TestFIFOTransferEmptyPeerAndCancelledOpen(t *testing.T) {
	for _, payload := range []string{"fifo bytes", ""} {
		t.Run(map[bool]string{true: "data", false: "empty"}[payload != ""], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pipe")
			address := "fifo://" + path
			in, err := uri.OpenInput(context.Background(), address, uri.Options{CreateFIFO: true})
			if err != nil {
				t.Fatal(err)
			}
			defer in.Cancel()
			out, err := uri.OpenOutput(waitContext(t), address, uri.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer out.Cancel()
			if payload != "" {
				sendBytes(t, out, []byte(payload))
			}
			finishOutput(t, out)
			if string(allBytes(t, in)) != payload {
				t.Fatal("FIFO bytes changed")
			}
			if st, err := os.Stat(path); err != nil || st.Mode()&os.ModeNamedPipe == 0 {
				t.Fatal("FIFO removed")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "no-reader")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if out, err := uri.OpenOutput(ctx, "fifo://"+path, uri.Options{}); out != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("peer wait %v %v", out, err)
	}
}
func TestFileAppendTruncateAndFollow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	os.WriteFile(path, []byte("a"), 0600)
	for _, step := range []struct {
		mode        uri.FileMode
		bytes, want string
	}{{uri.Append, "b", "ab"}, {uri.Truncate, "c", "c"}} {
		out, err := uri.OpenOutput(context.Background(), fileURI(path), uri.Options{FileMode: step.mode})
		if err != nil {
			t.Fatal(err)
		}
		sendBytes(t, out, []byte(step.bytes))
		finishOutput(t, out)
		got, _ := os.ReadFile(path)
		if string(got) != step.want {
			t.Fatalf("file mode %q: %q", step.mode, got)
		}
	}
	in, err := uri.OpenInput(context.Background(), fileURI(path), uri.Options{Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Cancel()
	b := receive(t, in.Data())
	if string(b.Data) != "c" {
		t.Fatal("initial follow read")
	}
	b.Release()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("d"))
	f.Close()
	b = receive(t, in.Data())
	if string(b.Data) != "d" {
		t.Fatal("appended read")
	}
	b.Release()
	in.Close()
	if err := in.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
}
func TestDirectChannelGracefulCompletionAndInvalidItem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "channel")
	out, err := uri.OpenOutput(context.Background(), fileURI(path), uri.Options{QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	var releases atomic.Int64
	channel := out.Channel()
	for i := 0; i < 50; i++ {
		b := uri.NewBuffer([]byte("x"), func() { releases.Add(1) })
		select {
		case channel <- b:
		case <-time.After(3 * time.Second):
			b.Release()
			t.Fatal("channel blocked")
		}
	}
	close(channel)
	if err := out.Wait(waitContext(t)); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, bytes.Repeat([]byte("x"), 50)) || releases.Load() != 50 {
		t.Fatalf("channel drain bytes=%d releases=%d", len(got), releases.Load())
	}
	invalid, err := uri.OpenOutput(context.Background(), fileURI(filepath.Join(t.TempDir(), "invalid")), uri.Options{MaxBufferSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer invalid.Cancel()
	b := uri.NewBuffer([]byte("xx"), func() { releases.Add(1) })
	select {
	case invalid.Channel() <- b:
	case <-time.After(3 * time.Second):
		t.Fatal("invalid channel stalled")
	}
	if err := invalid.Wait(waitContext(t)); !errors.Is(err, uri.ErrInvalidBuffer) {
		t.Fatalf("invalid channel item %v", err)
	}
	if releases.Load() != 51 {
		t.Fatal("invalid channel buffer not released")
	}
}
