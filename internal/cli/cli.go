// Package cli implements the thin urifan command-line adapter.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	fan "github.com/Version2beta/urifan"
	uri "github.com/Version2beta/urifan/transport"
)

// Version and Revision are populated by the release linker flags.
var Version = "dev"
var Revision = "unknown"

// Run parses args, forwards using the library, and returns an exit status:
// 0 normal completion, 1 runtime/startup failure, 2 invalid arguments.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("urifan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c fan.Config
	sourceSet := false
	source := func(role string) func(string) error {
		return func(raw string) error {
			if sourceSet {
				return errors.New("exactly one source is required")
			}
			e, err := endpoint(raw, role, true)
			if err != nil {
				return err
			}
			c.Source = e
			sourceSet = true
			return nil
		}
	}
	dest := func(role string) func(string) error {
		return func(raw string) error {
			e, err := endpoint(raw, role, false)
			if err != nil {
				return err
			}
			e.ID = fmt.Sprintf("destination-%d", len(c.Destinations)+1)
			c.Destinations = append(c.Destinations, e)
			return nil
		}
	}
	fs.Func("source", "URI: file/FIFO input or UDP bind", source("open"))
	fs.Func("connect-source", "URI: TCP/Unix source to dial", source("connect"))
	fs.Func("listen-source", "URI: TCP/Unix source listener or UDP bind", source("listen"))
	fs.Func("destination", "URI: file/FIFO output or UDP destination; repeatable", dest("open"))
	fs.Func("connect-destination", "URI: TCP/Unix destination to dial; repeatable", dest("connect"))
	fs.Func("listen-destination", "URI: TCP/Unix destination listener; repeatable", dest("listen"))
	fs.IntVar(&c.PacketSize, "packet-size", 0, "stream packet payload bytes (default 1200)")
	fs.DurationVar(&c.FlushInterval, "flush-interval", 0, "partial packet delay (default 10ms)")
	fs.IntVar(&c.QueuePackets, "queue-packets", 0, "outstanding packets per destination (default 256)")
	fs.Int64Var(&c.QueueBytes, "queue-bytes", 0, "outstanding payload bytes per destination (default 4194304)")
	fs.IntVar(&c.MaxDestinations, "max-destinations", 0, "active destination limit (default 256)")
	fs.IntVar(&c.MaxEndpoints, "max-endpoints", 0, "configured endpoints including source (default 64)")
	fs.Int64Var(&c.MaxMemoryBytes, "max-memory-bytes", 0, "live packet storage limit (default 67108864)")
	fs.IntVar(&c.EventCapacity, "event-capacity", 0, "bounded event queue (default 128)")
	fs.DurationVar(&c.DialTimeout, "dial-timeout", 0, "outgoing connection deadline (default 10s)")
	fs.DurationVar(&c.ShutdownTimeout, "shutdown-timeout", 0, "graceful draining deadline (default 5s)")
	var overrides []string
	fs.Func("destination-packet-size", "ID=BYTES: UDP destination ceiling; repeatable", func(s string) error { overrides = append(overrides, s); return nil })
	createFIFO := fs.Bool("create-fifo", false, "allow creation of configured FIFO paths; never remove them")
	appendFiles := fs.Bool("append", false, "explicitly append to file destinations")
	overwrite := fs.Bool("overwrite", false, "explicitly truncate file destinations")
	quiet := fs.Bool("quiet", false, "suppress normal events; failures remain on stderr")
	version := fs.Bool("version", false, "print version and source revision")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: urifan SOURCE [DESTINATION ...] [OPTIONS]\nLibrary-backed URI fan-out. All diagnostics use stderr.")
		fs.PrintDefaults()
	}
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		fs.SetOutput(stdout)
		fs.Usage()
		return 0
	}
	// flag's default errors echo raw argument values, potentially including an
	// accidentally supplied credential URI. Report parse failures without values.
	fs.SetOutput(io.Discard)
	parseErr := fs.Parse(args)
	fs.SetOutput(stderr)
	if parseErr != nil {
		if errors.Is(parseErr, flag.ErrHelp) {
			fs.SetOutput(stdout)
			fs.Usage()
			return 0
		}
		fmt.Fprintln(stderr, "urifan: invalid command-line arguments; see --help")
		return 2
	}
	if *version {
		fmt.Fprintf(stdout, "urifan %s (%s)\n", Version, Revision)
		return 0
	}
	invalid := func(err error) int { fmt.Fprintln(stderr, "urifan:", err); return 2 }
	if !sourceSet || fs.NArg() != 0 {
		return invalid(errors.New("exactly one source and no positional arguments are required"))
	}
	if *appendFiles && *overwrite {
		return invalid(errors.New("--append and --overwrite are mutually exclusive"))
	}
	mode := uri.Exclusive
	if *appendFiles {
		mode = uri.Append
	}
	if *overwrite {
		mode = uri.Truncate
	}
	for i := range c.Destinations {
		e := &c.Destinations[i]
		u, _ := url.Parse(e.URI)
		if u.Scheme == "file" {
			e.FileMode = mode
		}
		if u.Scheme == "fifo" {
			e.CreateFIFO = *createFIFO
		}
	}
	u, _ := url.Parse(c.Source.URI)
	if u.Scheme == "fifo" {
		c.Source.CreateFIFO = *createFIFO
	}
	for _, override := range overrides {
		id, value, ok := strings.Cut(override, "=")
		n, err := strconv.Atoi(value)
		if !ok || err != nil || n <= 0 {
			return invalid(errors.New("destination-packet-size requires ID=positive-integer"))
		}
		found := false
		for i := range c.Destinations {
			if c.Destinations[i].ID == id {
				c.Destinations[i].PacketSize = n
				found = true
			}
		}
		if !found {
			return invalid(fmt.Errorf("unknown destination ID %q", id))
		}
	}
	if err := ctx.Err(); err != nil {
		fmt.Fprintln(stderr, "urifan:", err)
		return 1
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stopStartup := context.AfterFunc(ctx, cancel)
	r, err := fan.Start(life, c)
	stopStartup()
	if err != nil {
		fmt.Fprintln(stderr, "urifan:", err)
		if errors.Is(err, fan.ErrInvalidConfig) {
			return 2
		}
		return 1
	}
	stopSignal := context.AfterFunc(ctx, func() { r.Close() })
	defer stopSignal()
	defer r.Cancel()
	var observedLost uint64
	for e := range r.Events() {
		if !*quiet || e.Error != nil {
			fmt.Fprintf(stderr, "%s id=%q uri=%q address=%q", e.Kind, e.ID, e.URI, e.Address)
			if e.Error != nil {
				fmt.Fprintf(stderr, " error=%q", e.Error.Error())
			}
			fmt.Fprintln(stderr)
		}
		lost := r.Stats().LostEvents
		if lost > observedLost {
			fmt.Fprintf(stderr, "events_lost total=%d\n", lost)
			observedLost = lost
		}
	}
	if lost := r.Stats().LostEvents; lost > observedLost {
		fmt.Fprintf(stderr, "events_lost total=%d\n", lost)
	}
	if err := r.Wait(context.Background()); err != nil {
		fmt.Fprintln(stderr, "urifan:", err)
		return 1
	}
	return 0
}
func endpoint(raw, role string, source bool) (fan.Endpoint, error) {
	e := fan.Endpoint{URI: raw, Listen: role == "listen"}
	u, err := url.Parse(raw)
	if err != nil {
		return e, errors.New("invalid endpoint URI")
	}
	stream := u.Scheme == "tcp" || u.Scheme == "unix"
	if (role == "connect" && !stream) || (role == "listen" && !stream && !(source && u.Scheme == "udp")) || (role == "open" && stream) {
		return e, errors.New("endpoint role incompatible with URI scheme")
	}
	return e, nil
}
