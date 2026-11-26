package urifan_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	fan "github.com/Version2beta/urifan"
	uri "github.com/Version2beta/urifan/transport"
)

func TestFIFOSourceDoesNotSpinSessionsWithoutWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	r, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: "fifo://" + path, CreateFIFO: true}, EventCapacity: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Cancel()
	out, err := uri.OpenOutput(deadline(t), "fifo://"+path, uri.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Cancel()
	out.Close()
	if err = out.Wait(deadline(t)); err != nil {
		t.Fatal(err)
	}
	event(t, r, "source_disconnected", "")
	// An idle FIFO must await the next writer, not continually reopen/report EOF.
	timer := time.NewTimer(80 * time.Millisecond)
	defer timer.Stop()
	count := 0
loop:
	for {
		select {
		case <-timer.C:
			break loop
		case e := <-r.Events():
			if e.Kind == "source_disconnected" {
				count++
			}
		}
	}
	if count != 0 {
		t.Fatalf("invented %d idle writer sessions", count)
	}
	r.Close()
	if err = r.Wait(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("FIFO path removed")
	}
}
