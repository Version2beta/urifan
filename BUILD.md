# Initial development build record

This is the historical record of the first local implementation. Public packaging
now uses the self-contained transport snapshot described in
[transport/ORIGIN.md](transport/ORIGIN.md). Current platform/minimum-Go results
and binary artifacts are available in [GitHub Actions](https://github.com/Version2beta/urifan/actions).
The CI and release work happened after the timed interval below.

## Outcome and time

Implemented the whole local README scope: Go library, CLI, files/FIFOs/TCP/Unix/UDP,
source continuation, packetization, membership, isolated failures, bounded queues,
shared packet leases, memory pressure, shutdown, structured events and benchmarks.

- Started: **2026-11-26 08:40:24 UTC**.
- Completed validation and handoff bookkeeping: **2026-11-26 09:27:49 UTC**.
- Measured elapsed: **47 minutes 25 seconds**, within the requested one-hour limit.
- This interval includes skill/README/dependency inspection, acceptance preparation,
  implementation, testing, self-review/corrections, benchmarks and documentation.
  Earlier product discussion and README drafting are excluded; their effort is not zero.

One agent/session, no delegated implementation/review or additional model calls.
Actual session billing is unavailable. No economic advantage or independent review
is claimed. No private reference implementation, SPEC kit, controller or legacy
loop was created. No competing implementation was inspected for this build.

## Source and dependency

There was no implementation or Git repository initially, only the owner's README.
No Git repository, commit or remote mutation was created during this build. Sources,
MIT license, the original local dependency replacement and an ignored `bin/urifan`
executable were present at that point. The readme-loop skill source was read at
its `dd35f99` revision. Unrelated working files were left untouched.

The urihandler source was inspected and integrated at clean revision
`941401e062cd93280e8de869651aa22078df3f27`; its checkout was not modified.
Relative `go.mod` replacement selects that sibling working tree, not an immutable
version pin. The final source manifest records this build's file identities.
No dependency or toolchain was downloaded/installed. `GOPROXY=off`,
`GOTOOLCHAIN=local`, `GOWORK=off` were used for Go validation.

## Acceptance provenance

Five public-interface acceptance tests and the starting README were hashed outside
the candidate before production code. The five behaviors are byte-identical file
fan-out/exclusive creation; source continuation and no late-client replay; UDP
boundaries/empty datagrams; stream packetization/timed partial flush; isolated
oversize failure with sticky outcome despite event loss. The original test source
was subsequently gofmt-formatted, not semantically relaxed. Additional tests were
written during implementation and correction.

The acceptance/test author and implementer share this session. The external record
is provenance, not a filesystem sandbox, withheld judge or independent author.
Owner requirements remain the authority; the implementer chose documented routine
defaults/API/resource policy under the instruction to finish without another gate.

## Executed checks

Actual environment: **Go 1.27.2, darwin/arm64, Apple M5**.

- `go test -race -count=1 -timeout 40s -json ./...`: **30 top-level tests**,
  **33 passed events including subtests and runnable example**, exit 0.
  No acceptance skip or empty collection was counted as success.
- `go vet ./...`: exit 0; `gofmt -l .`: empty; Markdown code fences/whitespace checked.
- Four concurrency-sensitive public tests repeated 20 times under race detection:
  close/cancel snapshots, competing clients, source continuation/late joins and
  write-failure notification. This investigates interleavings, not extra feature count.
- Actual compiled `bin/urifan`: file transfer with binary bytes and empty stdout;
  two live UDP sinks receiving exact datagrams including empty and non-stream-sized
  payloads; actual SIGTERM handler draining and exiting 0.
- Linux amd64 CLI cross-build and Linux arm64 root test-binary cross-build: exit 0.
  These are **compilation only**, not Linux execution.

Real transport tests include FIFO reader waiting without delaying healthy output,
no pre-reader history, successive/empty FIFO writer sessions, persistent FIFO/Unix
paths, TCP/Unix listeners, source read reset/replacement, per-destination limits,
refused startup connections, explicit append/truncate, source hard-link rejection,
idle cancellation and a nonreading TCP sink with an actually retained output lease.
The shutdown test uses a 20 ms deadline after eight MiB are admitted and verifies
all acquired storage is released only after canceled output cleanup.

Other checks exercise global pressure deterministically with held queue custody,
capacity-based allocator accounting, rejected oversized leases, allocator errors,
event overflow/sticky result, help/configuration errors, diagnostic/payload separation
and no credential-URI echo on parse errors. Unit pressure fixtures are not presented
as kernel behavior; real blocked TCP tests supply the separate I/O evidence.

## Corrections and challenges

Self-review found a completed output could go unnoticed while its sender awaited
another source packet; senders now observe endpoint completion directly. Admission
handoff explicitly acknowledges ownership so rejected clients are cleaned up by
bounded existing preparers, not unbounded cleanup goroutines. Retiring connections
occupy their slot until cleanup actually finishes. Listener cleanup is joined.

A new regression reproduced a context-aware allocator preventing Close from ever
starting its own cancellation deadline. It failed with a waiting-context timeout
before repair. A separately owned deadline watcher now cancels that allocator and
is itself joined before Done; the regression passes with `ErrShutdownTimeout`.

The first blocked-writer fixture hit its default four-MiB backlog limit before its
intended eight-MiB shutdown check. The fixture's allowance was increased to 16 MiB
so it tests shutdown, rather than legitimate earlier queue eviction. Required
shutdown behavior was not weakened.

Three isolated defective copies were rejected by intended behavioral assertions,
not compiler/setup failures: omitted UDP payload copy; omitted destination lease
reference; bypassed global-pressure admission. This selected challenge set does
not establish general mutation effectiveness or prove all possible defects absent.

## Informational performance

Command: `go test -run '^$' -bench 'Benchmark(Fanout|PacketLatency)'
-benchtime=3x -benchmem ./...`.

Five file/nonreading-sink scenarios and four loopback UDP latency scenarios passed.
File benchmarks vary 1,200/65,536-byte packets and one/four destinations; they report
throughput, allocations, batch latency and peak live packet storage. Packet latency
uses 1,200/8,192-byte datagrams and one/four sinks, measuring submission through
receipt at every sink, excluding setup/drain. Each scenario has only three measured
iterations; see raw output, not a performance SLA or a comparative claim.

An initial 64,000-byte UDP latency case failed at the host sender with EMSGSIZE
before urifan received data. It was changed to 8,192 bytes to measure a supported
local size; this does not change forwarding acceptance or promise 64-KiB UDP support
on every socket/path. The README explicitly allows underlying send limits below
the configured payload ceiling. File/page-cache throughput is not durable storage
or WAN throughput, and allocation-free forwarding is not claimed.

## Evidence and limits

Original logs, initial/final hashes, negative controls, compiled-CLI records,
cross-built artifacts and the initial README draft were retained privately by
the owner, outside the source tree. They are not part of the public distribution.
No credentials, transcripts or generated binaries are committed. Public CI now
provides reproducible execution logs for the packaged source.

At the end of that initial local build, Go 1.24.7 execution, Linux runtime,
production scale/network conditions, path-MTU behavior and dependency release
packaging were unqualified. The later CI matrix addresses minimum-Go and native
runner coverage; consult its actual results for the current revision. No absolute
RSS bound, forced kernel/callback termination, durable delivery, authentication or
remote-receiver health guarantee is made. No independent review was authorized;
self-review and test authorship remain correlated evidence. The next useful action
is to run `./bin/urifan --help` or the README examples, not another implementation gate.
