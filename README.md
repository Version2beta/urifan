# urifan

**One source, multiple URI-addressed destinations. Bounded buffering. Independent
connection lifetimes.**

Urifan is a Go library and CLI that distribute one source's data to multiple
connected destinations. Slow or failed destinations are removed rather than
stalling healthy destinations. New connections receive future packets, never
replayed history.

Files, FIFOs, TCP, Unix stream sockets and UDP share the forwarding model.
[the bundled urihandler transport](transport/ORIGIN.md) owns endpoint I/O and buffer leases; urifan
owns packet assembly, membership, fan-out, resource limits and failure policy.
It does not parse media, infer messages, authenticate peers or guarantee remote
consumption or durable storage.

## Status, ownership and build

**Experimental; feedback welcome.** Download binaries from
[GitHub Releases](https://github.com/Version2beta/urifan/releases), or build from
source below. Please [open an issue](https://github.com/Version2beta/urifan/issues)
with `urifan --version`, OS/architecture, configuration, expected behavior and observed result;
redact private addresses or credentials.

[CI](https://github.com/Version2beta/urifan/actions/workflows/ci.yml) tests Linux and
macOS on Go 1.24.7 and stable, exercises the compiled CLI, and builds all four
binary variants. Version tags publish experimental releases only after those
checks pass. See the actual run results, not just the workflow definition.
The original single-agent implementation took 47 minutes 25 seconds;
[BUILD.md](BUILD.md) preserves that historical evidence and its limits.

- Owner/copyright holder: Rob Martin. License: [MIT](LICENSE).
- Module: `github.com/Version2beta/urifan`; CLI binary: `urifan`.
- Go baseline: **1.24.7** in `go.mod`; release binaries require no Go installation.
- Platforms: Linux and macOS, amd64 and arm64. CI executes on its native runner
  architectures; cross-built variants are not separately runtime-qualified.
- Self-contained standard-library-only module: the transport source and tests are
  included under this module, originating at revision
  `941401e062cd93280e8de869651aa22078df3f27`. No sibling checkout, module replacement
  or external Go dependency is required. See [origin and license](transport/ORIGIN.md).

```sh
git clone https://github.com/Version2beta/urifan.git
cd urifan
GOPROXY=off GOTOOLCHAIN=local GOWORK=off go test -race -count=1 -timeout 40s ./...
GOPROXY=off GOTOOLCHAIN=local GOWORK=off go vet ./...
mkdir -p bin
GOPROXY=off GOTOOLCHAIN=local GOWORK=off go build -o bin/urifan ./cmd/urifan
./bin/urifan --help
```

Alternatively, with a suitable Go toolchain: `go install
github.com/Version2beta/urifan/cmd/urifan@v0.1.0`.

Binary archives contain `urifan`, documentation and MIT notices. Choose your asset:

| System | Archive suffix |
| --- | --- |
| Linux Intel/AMD 64-bit | `linux_amd64.tar.gz` |
| Linux ARM64 | `linux_arm64.tar.gz` |
| macOS Intel | `darwin_amd64.tar.gz` |
| macOS Apple Silicon | `darwin_arm64.tar.gz` |

Download the archive and `SHA256SUMS` from the same release. Verify its SHA-256
against the listed value (`sha256sum` on Linux or `shasum -a 256` on macOS), extract
it, and run `./urifan --help`. Checksums verify integrity, not a signed publisher
attestation. Binaries are not notarized; build from source if your security policy
requires it rather than disabling security controls.

`scripts/package.sh v0.1.0 OUTPUT_DIRECTORY` reproduces the four cross-builds into
a new/empty directory from a Git checkout. `urifan --version` identifies a packaged
binary's version and source commit; ordinary source builds report `dev`. CI also retains snapshot archives for 14 days on main/PR
runs. Release archives are built from tagged commits, not committed binaries.

The earlier TCP/Zig comparison is separately published as
[Version2beta/tcpfan-experiment](https://github.com/Version2beta/tcpfan-experiment).
It is not this implementation or its oracle.

## CLI examples

```sh
# Accept one source; write a new file and every accepted destination connection.
./bin/urifan \
  --listen-source tcp://127.0.0.1:9000 \
  --destination file:///tmp/capture.bin \
  --listen-destination tcp://127.0.0.1:9001 \
  --packet-size 1200

# Dial both ends. Source EOF drains destinations and exits.
./bin/urifan \
  --connect-source tcp://127.0.0.1:8000 \
  --connect-destination tcp://127.0.0.1:8001

# Preserve UDP datagrams, including their individual sizes.
./bin/urifan \
  --source udp://127.0.0.1:5000 \
  --destination udp://127.0.0.1:5001 \
  --destination udp://127.0.0.1:5002
```

Exactly one source is required. Zero destinations are allowed: data is consumed
and discarded. Destinations are repeatable and assigned IDs `destination-1`,
`destination-2`, etc., in flag order. Accepted clients get `/N` suffixes.
`--destination-packet-size destination-2=1400` overrides a UDP destination's
payload ceiling. `--quiet` suppresses normal events, not errors or overflow reports.
Diagnostics go to stderr, never into the payload or stdout; help uses stdout.

## Endpoint roles and filesystem effects

The URI identifies an address; configuration explicitly selects its role.

| Transport | Source | Destination |
| --- | --- | --- |
| File | `--source`: read regular file | `--destination`: write regular file |
| FIFO | `--source`: read successive writer sessions | `--destination`: wait independently for a reader |
| TCP / Unix stream | `--connect-source` or `--listen-source` | `--connect-destination` or `--listen-destination` |
| UDP | `--source` or `--listen-source`: bind and receive | `--destination`: send to configured address |

Use absolute local `file:///`, `fifo:///` and `unix:///` paths, and numeric
`tcp://host:port` / `udp://host:port` addresses (IPv6 uses brackets). Port 0 is
available for binds/listeners, not dial destinations. URI credentials, query
strings and fragments are unsupported; escape special path characters, e.g. `%23`.
UDP has no connection acceptance, peer EOF or reliable disappearance signal.
A listening UDP destination would need a registration protocol and is not supported.

Output files default to **exclusive creation**, mode 0600 subject to umask.
Existing files are preserved unless `--append` or `--overwrite` explicitly permits
append or truncation. These flags apply only to file destinations and are mutually
exclusive. Source/output aliases detected by file identity are rejected, including
hard links; these checks do not eliminate filesystem races or provide confinement.

`--create-fifo` explicitly permits creation of configured FIFO paths with 0600.
Existing non-FIFOs are rejected. Unix listeners create their socket node but never
replace an occupied path. Neither caller paths nor library-created FIFO/socket
paths are automatically unlinked, even on shutdown. Parent directories must exist.
File closure is not an fsync/durable-storage guarantee.

## Startup and source continuation

Configuration and known filesystem errors are checked before opening outputs.
Direct destinations are prepared, destination listeners are bound, then the source
is opened. Direct-open or listener-bind failure fails startup and closes acquired
resources. Cleanup does not undo authorized file creation/truncation or delete
created paths. There is no implicit retry or omission of a failed direct output.

A FIFO destination awaiting a reader is the deliberate exception: prepare it in
its own bounded worker and leave it inactive without delaying startup or forwarding.
It receives no backlog for the time before a reader arrives. A later preparation
failure is an unexpected destination failure. Default outgoing TCP/Unix connection
establishment timeout is 10 seconds; client absence on listeners/FIFOs is not a
connection timeout.

For a source listener:

- Wait when no source is connected; allow one active source and reject competitors.
- Source EOF/disconnection does not close destinations or finish the relay.
- Each replacement source continues the same logical stream. No boundary markers,
  replay or loss recovery are inserted.
- Preserve received bytes and partial packet assembly across connection changes.
  The flush timer keeps running; destination queues can drain during the gap.
- An accepted source read error is logged, that connection is discarded, and the
  listener continues. Listener failure itself terminates the run.

FIFO writer EOF similarly ends a writer session, not the logical stream. Urifan
reopens the FIFO for the next writer while keeping assembly state. This is explicit
urifan policy, not an assumption that urihandler's ordinary EOF does continuation.
A fatal FIFO error terminates the run.

Final EOF from a directly opened finite file/TCP/Unix source drains output and
completes. An unexpected direct-source error completes with an error after draining
accepted work. UDP input remains active until stopped or it reports an error.
Continuation is concatenation, not proof that successive clients are the same
producer. Use trusted configuration and appropriate network/path access controls.

## Membership and packet assembly

A destination listener is a connection factory, **not a destination with a queue**.
No connection means no sends and no buffering for an eventual connection. Each
accepted client becomes an independent active destination; client failure leaves
its listener available. No automatic destination reconnect or replay is performed.

A packet's recipient set is captured **when stream packet assembly starts**, or
when a UDP datagram is admitted. A client activated during partial assembly starts
with the next packet. It cannot inherit that partial packet or another client's
queue. “Future data” refers to this admission boundary, not physical arrival time
inside the operating system. If no destinations are active, consume/discard data
without retaining history.

A packet is a forwarding/ownership unit, not necessarily an application message:

- **UDP source:** one datagram becomes one packet with its actual size, including
  zero bytes. Receive capacity is 65,535 bytes; truncation is an error, never a
  successful partial packet. `packet-size` does not truncate incoming datagrams.
- **Stream/file/FIFO source:** assemble up to `packet-size` bytes from successive
  reads. A short read alone does not determine a packet boundary. Flush partial
  data after the configured delay from its first byte, at final EOF, or on graceful
  stop. Source gaps do not suspend the timer. Cancellation may abandon partial data.
- **UDP destination:** send one packet as one datagram, without merging/splitting.
  Default ceiling is 65,507 payload bytes, independently of stream packet size;
  a per-destination `packet-size` can lower it. An oversized packet closes only
  that destination with an error event; healthy destinations continue.
- **Stream/file/FIFO destination:** concatenate packet payloads in order. Boundaries
  are not encoded; empty packets write no bytes. UDP boundaries cannot be recovered
  from that stream without a separately selected framing protocol.

Defaults: **1,200-byte stream packets and 10 ms partial flush**. `packet-size` counts
payload, not IP/UDP headers; it is not MTU discovery or a fragmentation guarantee.
Known stream-size/UDP-ceiling incompatibility fails configuration validation.
Network limits can still cause send failure below the configured ceiling.

## Bounds, backpressure and custody

| Configuration / CLI flag | Default |
| --- | --- |
| `PacketSize` / `--packet-size` | 1,200 bytes, maximum 1 MiB |
| `FlushInterval` / `--flush-interval` | 10 ms |
| `QueuePackets` / `--queue-packets` | 256 per destination |
| `QueueBytes` / `--queue-bytes` | 4 MiB per destination |
| `MaxDestinations` / `--max-destinations` | 256 owned destination connections |
| `MaxEndpoints` / `--max-endpoints` | 64 configured endpoints, source included |
| `MaxMemoryBytes` / `--max-memory-bytes` | 64 MiB live packet storage |
| `DialTimeout` / `--dial-timeout` | 10 s |
| `ShutdownTimeout` / `--shutdown-timeout` | 5 s |
| `EventCapacity` / `--event-capacity` | 128 events |

Zero-valued Go fields/CLI numeric defaults select these values; negative values
are rejected. Limits are finite: at most 4,096 destination slots, 1,024 configured
endpoints, 65,536 outstanding packets per destination, 1 GiB destination byte
allowance, 16 GiB live packet storage and 65,536 queued events. The byte allowance
and packet limit apply together. Duration values must be positive after defaulting.

Outstanding counts include the urifan queue, the sender's current packet,
urihandler's one-slot queue and its in-flight write. The forwarder never waits
for destination queue capacity. Exceed either limit and that destination is removed.
The library allows per-endpoint queue and UDP ceiling overrides. Slots remain
occupied during retirement until cleanup finishes, preventing an unbounded tail
of failed connections. Extra accepted clients are rejected at the connection cap.

Configured endpoint count bounds preparers/listeners. Each listener has at most
one admission candidate being decided/cleaned up and one raw accepted connection
held by urihandler; the kernel also has its own bounded listen backlog. There is
one FIFO preparer per configured FIFO destination and one logical source reader.
No unbounded per-client rejection goroutines or pending-open queues are created.

Canonical packet storage is copied once from input, then shared immutably through
separate destination leases. A packet allocation is charged by its full capacity,
not just its current slice length. Assembly is included in the global live-storage
budget. On pressure, evict destinations retaining the oldest outstanding packet,
tie-breaking by connection ID. Use a shared 50 ms waiting budget per allocation
for in-flight reclamation; if safe reclamation cannot make room, fail the run with
`ErrMemoryLimit`. This is not a hard CPU/callback execution deadline.
Never reuse storage still used by an interrupted write. Global resource exhaustion
can therefore terminate the run rather than violate the budget or hang admission.

The global budget is **not an RSS/whole-heap cap**: input I/O buffers, bounded
connection/queue metadata, kernel buffers, GC retirement and pool caches are
separate. Custom `Allocator` hooks lend complete allocation views, must observe
context if blocking, and must return/release promptly without waiting on the relay.
Retained pools remain caller-owned. Oversized/invalid leases are released and
reported; arbitrary caller allocation behavior cannot be constrained by this budget.
No zero-allocation or kernel-zero-copy claim is made.

## Library API

```go
relay, err := urifan.Start(ctx, urifan.Config{
    Source: urifan.Endpoint{URI: "tcp://127.0.0.1:9000", Listen: true},
    Destinations: []urifan.Endpoint{
        {ID: "clients", URI: "tcp://127.0.0.1:9001", Listen: true},
    },
    PacketSize: 1200,
})
// Handle err before using relay.
```

`Endpoint` exposes `ID`, `URI`, `Listen`, UDP `PacketSize`, `QueuePackets`,
`QueueBytes`, urihandler `FileMode`, and `CreateFIFO`. Source-only incompatible
options are rejected. `Config` contains the table's controls, source/destinations,
and optional `urihandler.Allocator` for canonical packet storage. It is copied on
start; destination IDs must be unique and cannot contain `/`.

- `Addresses() map[string]string`: copied configured local network addresses;
  includes assigned listener/UDP port 0 addresses. Accepted clients have distinct
  event IDs, not new listener ports.
- `Events() <-chan Event`: structured `Kind`, `ID`, `URI`, local `Address`, `Error`.
- `Stats() Stats`: concurrent-safe source bytes admitted (including discarded data),
  packets assembled/admitted for dispatch, active destinations, live/peak packet
  storage, cumulative lost events. These are not delivery acknowledgements.
- `Close() error`: request normal stop/drain, without waiting; idempotent.
- `Cancel()`: request abort, without rollback; idempotent.
- `Done() <-chan struct{}`: all owned cleanup finished.
- `Wait(ctx) error`: observe cleanup/result. Its context affects only that wait;
  completed cleanup wins over an expired waiting context.

The opening context governs lifetime and cancellation aborts the relay. Graceful
stop ends admission/listening, flushes the partial packet to its original surviving
recipients, drains accepted output and closes resources. After the shutdown deadline,
cancel remaining work and report `ErrShutdownTimeout`. Close's deadline can cancel
a context-aware allocator even if the forwarding actor is waiting in that allocator.
No hard termination guarantee exists for OS-stuck I/O or noncooperating callbacks;
caller-bounded `Wait` does not authorize early storage reuse.

The transport types are available as
`import uri "github.com/Version2beta/urifan/transport"`; for example,
`uri.NewBuffer`, `uri.Allocator`, `uri.Append` and `uri.Truncate`.

See [the runnable example](example_test.go). Public error sentinels preserve
`errors.Is`: `ErrInvalidConfig`, `ErrDestinationFailed`, `ErrBacklog`,
`ErrPacketTooLarge`, `ErrMemoryLimit`, `ErrShutdownTimeout`, `ErrLimit`.

## Events, failures and CLI exit status

Normal events include listening, connection admission, source EOF/session changes
and rejected extra clients. Source listener peer EOF and FIFO writer EOF are normal.
Read errors on accepted source connections are unexpected/logged but recoverable.
A destination disappearing during forwarding is unexpected even if its peer closes
cleanly. Idle closure may not be detected until I/O; no heartbeat is invented.

The library never prints. Event delivery is bounded/nonblocking; overload can lose
individual events. `Stats().LostEvents` reports the cumulative count independently
of that queue. The CLI warns about lost events and logs unexpected failures when
logging can proceed. Stalled diagnostics cannot block library forwarding. Aggregate
failure status is retained separately, so losing events cannot hide destination loss.

CLI exit statuses: **0** normal completion/requested graceful stop; **1** startup
or runtime failure, fatal source error, timeout, or any unexpected destination loss;
**2** invalid command/configuration. Isolated destination failure does not stop
healthy forwarding, but makes the eventual CLI result nonzero. Recoverable source
connection errors alone do not make an otherwise normal final result fail.
SIGINT/SIGTERM request graceful stop. Successful local writes cannot prove receiver
presence, complete remote consumption, durable storage or exactly-once delivery.

## Acceptance and benchmarks

Tests cover actual temporary files/FIFOs, local TCP/Unix streams and UDP, source
continuation, late membership, read-independent packetization/timed partial flush,
empty datagrams, isolated failures, slow-client removal, safe leases, memory limits,
startup failures, FIFO sessions, shutdown deadlines, event loss and CLI behavior.
Before-code acceptance and subsequent regression evidence are distinguished in
[BUILD.md](BUILD.md). Work is single-agent; there is no independent reviewer or
private reference implementation. Native tests are not deployment qualification.

```sh
# Bounded informational runs: throughput, allocation and retained-memory metrics;
# ns/op in PacketLatency measures loopback submission through receipt at every sink.
go test -run '^$' -bench 'Benchmark(Fanout|PacketLatency)' -benchtime=3x -benchmem ./...
```

Benchmarks vary packet size and destination count and include a nonreading TCP
sink alongside a healthy output. Local page-cache throughput is not durable disk
or WAN performance; these small samples establish no performance threshold or
comparative advantage.

## Boundaries

Not included: automatic reconnect/replay, historical buffering, arbitrary message
framing, media parsing, routing by payload, UDP recipient registration, TLS/auth,
full-duplex proxying, stdin/stdout URI schemes, durable spooling or remote publication.
Opposite-direction destination traffic is never forwarded to the source; no active
reverse-protocol processing is promised. Real deployment conditions, path-MTU
behavior and native execution on every released architecture remain separate from
CI's runner coverage. Minimum-Go/Linux execution evidence is provided by the CI
matrix when those jobs pass.
