package urifan_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	fan "github.com/Version2beta/urifan"
)

func ExampleStart() {
	dir, err := os.MkdirTemp("", "urifan-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	input, output := filepath.Join(dir, "input"), filepath.Join(dir, "output")
	if err := os.WriteFile(input, []byte("hello"), 0600); err != nil {
		panic(err)
	}
	relay, err := fan.Start(context.Background(), fan.Config{Source: fan.Endpoint{URI: uriFile(input)}, Destinations: []fan.Endpoint{{URI: uriFile(output)}}})
	if err != nil {
		panic(err)
	}
	defer relay.Cancel()
	if err := relay.Wait(context.Background()); err != nil {
		panic(err)
	}
	bytes, err := os.ReadFile(output)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(bytes))
	// Output: hello
}
