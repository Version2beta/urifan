package urihandler_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

func Example_fileTransfer() {
	err := func() error {
		dir, err := os.MkdirTemp("", "urihandler-example-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		source := filepath.Join(dir, "input")
		destination := filepath.Join(dir, "output")
		if err := os.WriteFile(source, []byte("queued bytes, preserved order"), 0600); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		address := func(path string) string { return (&url.URL{Scheme: "file", Path: path}).String() }
		input, err := uri.OpenInput(ctx, address(source), uri.Options{})
		if err != nil {
			return err
		}
		defer func() { input.Cancel(); input.Wait(context.Background()) }()
		output, err := uri.OpenOutput(ctx, address(destination), uri.Options{})
		if err != nil {
			return err
		}
		defer func() { output.Cancel(); output.Wait(context.Background()) }()
		for buffer := range input.Data() {
			if err := output.Send(ctx, buffer); err != nil {
				buffer.Release()
				return err
			}
			// Custody has transferred: do not access or release buffer here.
		}
		if err := input.Wait(ctx); err != nil {
			return err
		}
		if err := output.Close(); err != nil {
			return err
		}
		if err := output.Wait(ctx); err != nil {
			return err
		}
		data, err := os.ReadFile(destination)
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}()
	if err != nil {
		fmt.Println("error:", err)
	}
	// Output: queued bytes, preserved order
}
