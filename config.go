// Package urifan forwards one logical source to independent URI destinations.
package urifan

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	uri "github.com/Version2beta/urifan/transport"
)

var (
	ErrInvalidConfig     = errors.New("urifan: invalid configuration")
	ErrDestinationFailed = errors.New("urifan: unexpected destination loss")
	ErrBacklog           = errors.New("urifan: destination backlog exceeded")
	ErrPacketTooLarge    = errors.New("urifan: packet exceeds destination limit")
	ErrMemoryLimit       = errors.New("urifan: retained packet memory limit")
	ErrShutdownTimeout   = errors.New("urifan: graceful shutdown timed out")
	ErrLimit             = errors.New("urifan: connection limit")
)

// Endpoint describes a source, direct destination, or TCP/Unix listener.
// PacketSize is a UDP destination ceiling, not a receive truncation setting.
type Endpoint struct {
	ID           string
	URI          string
	Listen       bool
	PacketSize   int
	QueuePackets int
	QueueBytes   int64
	FileMode     uri.FileMode
	CreateFIFO   bool
}

// Config zero fields select documented defaults. Allocator optionally lends
// packet storage; callbacks must observe context and return promptly. Pools own
// their cached storage; only live leases count toward MaxMemoryBytes.
type Config struct {
	Source          Endpoint
	Destinations    []Endpoint
	PacketSize      int
	FlushInterval   time.Duration
	QueuePackets    int
	QueueBytes      int64
	MaxDestinations int
	MaxEndpoints    int
	MaxMemoryBytes  int64
	DialTimeout     time.Duration
	ShutdownTimeout time.Duration
	EventCapacity   int
	Allocator       uri.Allocator
}

func normalize(c Config) (Config, error) {
	if c.PacketSize == 0 {
		c.PacketSize = 1200
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 10 * time.Millisecond
	}
	if c.QueuePackets == 0 {
		c.QueuePackets = 256
	}
	if c.QueueBytes == 0 {
		c.QueueBytes = 4 << 20
	}
	if c.MaxDestinations == 0 {
		c.MaxDestinations = 256
	}
	if c.MaxEndpoints == 0 {
		c.MaxEndpoints = 64
	}
	if c.MaxMemoryBytes == 0 {
		c.MaxMemoryBytes = 64 << 20
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 5 * time.Second
	}
	if c.EventCapacity == 0 {
		c.EventCapacity = 128
	}
	if c.PacketSize < 1 || c.PacketSize > 1<<20 || c.FlushInterval < 0 || c.QueuePackets < 1 || c.QueuePackets > 65536 || c.QueueBytes < 1 || c.QueueBytes > 1<<30 || c.MaxDestinations < 1 || c.MaxDestinations > 4096 || c.MaxEndpoints < 1 || c.MaxEndpoints > 1024 || len(c.Destinations)+1 > c.MaxEndpoints || c.MaxMemoryBytes < int64(c.PacketSize) || c.MaxMemoryBytes > 1<<34 || c.DialTimeout < 0 || c.ShutdownTimeout < 0 || c.EventCapacity < 1 || c.EventCapacity > 65536 {
		return c, ErrInvalidConfig
	}
	c.Destinations = append([]Endpoint(nil), c.Destinations...)
	if c.Source.ID == "" {
		c.Source.ID = "source"
	}
	ids := map[string]bool{c.Source.ID: true}
	if _, err := validateEndpoint(c.Source, true); err != nil {
		return c, err
	}
	direct := 0
	for i := range c.Destinations {
		e := &c.Destinations[i]
		if e.ID == "" {
			e.ID = fmt.Sprintf("destination-%d", i+1)
		}
		if ids[e.ID] {
			return c, fmt.Errorf("%w: duplicate endpoint ID", ErrInvalidConfig)
		}
		ids[e.ID] = true
		scheme, err := validateEndpoint(*e, false)
		if err != nil {
			return c, err
		}
		if !e.Listen {
			direct++
		}
		if scheme == "udp" {
			max := e.PacketSize
			if max == 0 {
				max = 65507
			}
			srcScheme, _ := validateEndpoint(c.Source, true)
			if srcScheme != "udp" && c.PacketSize > max {
				return c, fmt.Errorf("%w: stream packet exceeds UDP destination ceiling", ErrInvalidConfig)
			}
		}
	}
	if direct > c.MaxDestinations {
		return c, fmt.Errorf("%w: too many direct destinations", ErrInvalidConfig)
	}
	return c, nil
}
func validateEndpoint(e Endpoint, source bool) (string, error) {
	fail := func() (string, error) { return "", fmt.Errorf("%w: endpoint %q", ErrInvalidConfig, e.ID) }
	if e.QueuePackets < 0 || e.QueuePackets > 65536 || e.QueueBytes < 0 || e.QueueBytes > 1<<30 || (source && (e.QueuePackets != 0 || e.QueueBytes != 0)) {
		return fail()
	}
	if len(e.ID) > 128 || strings.Contains(e.ID, "/") || len(e.URI) > 4096 || strings.Contains(e.URI, "#") {
		return fail()
	}
	u, err := url.Parse(e.URI)
	if err != nil || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return fail()
	}
	switch u.Scheme {
	case "file", "fifo", "unix":
		if u.Host != "" || !filepath.IsAbs(u.Path) || strings.ContainsRune(u.Path, 0) {
			return fail()
		}
	case "tcp", "udp":
		if u.Path != "" || u.Host == "" {
			return fail()
		}
		_, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			return fail()
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 0 || n > 65535 || (n == 0 && !(source && (e.Listen || u.Scheme == "udp")) && !(e.Listen && !source)) {
			return fail()
		}
	default:
		return fail()
	}
	if e.Listen && u.Scheme != "tcp" && u.Scheme != "unix" && !(source && u.Scheme == "udp") {
		return fail()
	}
	if (e.FileMode != uri.Exclusive && e.FileMode != uri.Append && e.FileMode != uri.Truncate) || (e.FileMode != uri.Exclusive && (source || u.Scheme != "file")) || (e.CreateFIFO && u.Scheme != "fifo") || e.PacketSize < 0 || e.PacketSize > 65507 || (e.PacketSize != 0 && (source || u.Scheme != "udp")) {
		return fail()
	}
	return u.Scheme, nil
}

// Preflight known filesystem failures before opening any outputs. These checks
// do not provide path confinement or eliminate filesystem races.
func preflight(c Config) error {
	var source os.FileInfo
	if endpointScheme(c.Source) == "file" {
		u, _ := url.Parse(c.Source.URI)
		var err error
		source, err = os.Stat(u.Path)
		if err != nil {
			return err
		}
		if !source.Mode().IsRegular() {
			return fmt.Errorf("%w: regular source file required", ErrInvalidConfig)
		}
	}
	for _, e := range c.Destinations {
		u, _ := url.Parse(e.URI)
		if u.Scheme == "fifo" {
			st, err := os.Lstat(u.Path)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) || !e.CreateFIFO {
					return err
				}
				parent, err := os.Stat(filepath.Dir(u.Path))
				if err != nil {
					return err
				}
				if !parent.IsDir() {
					return ErrInvalidConfig
				}
			} else if st.Mode()&os.ModeNamedPipe == 0 {
				return fmt.Errorf("%w: named pipe required", ErrInvalidConfig)
			}
		}
		if source != nil && u.Scheme == "file" {
			if dest, err := os.Stat(u.Path); err == nil && os.SameFile(source, dest) {
				return fmt.Errorf("%w: source file is also a destination", ErrInvalidConfig)
			}
		}
	}
	return nil
}
func endpointScheme(e Endpoint) string { u, _ := url.Parse(e.URI); return u.Scheme }
func transportOptions(c Config, e Endpoint) uri.Options {
	max := 65535
	if c.PacketSize > max {
		max = c.PacketSize
	}
	return uri.Options{QueueCapacity: 1, MaxBufferSize: max, FileMode: e.FileMode, CreateFIFO: e.CreateFIFO, DialTimeout: c.DialTimeout}
}
