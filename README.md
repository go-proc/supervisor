<p align="center"><img src="https://raw.githubusercontent.com/go-proc/brand/main/social/go-proc-supervisor.png" alt="go-proc/supervisor" width="720"></p>

# supervisor — go-proc

[![Go Reference](https://pkg.go.dev/badge/github.com/go-proc/supervisor.svg)](https://pkg.go.dev/github.com/go-proc/supervisor)
[![Docs](https://img.shields.io/badge/docs-mkdocs--material-B45309)](https://go-proc.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.27.1%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**Pure-Go (no cgo) PID-1 process supervision** — a subreaper that collects
every orphaned child the kernel reparents to a process-1 init, and a
supervisor that starts a declared set of processes through a pluggable
runtime, correlates their exits to the reaper, and restarts them per policy.

Extracted from a microVM init and generalized: nothing here is tied to any
particular workload model. The supervisor drives an abstract `Runtime`
(an OCI runtime like crun/runc, a raw fork+exec launcher, or a test fake)
over a small `Spec` of `Proc`s.

> **Two responsibilities, split on purpose.** The **reaper** must run
> unconditionally — PID 1 is the kernel's reaper of last resort for the whole
> orphan tree, including helpers a runtime forks that we never tracked. The
> **supervisor** only cares about the PIDs it launched, correlating reaper
> events back to the owning process to apply its restart policy.

## Features

- **Subreaper** — `signal.Notify(SIGCHLD)` + non-blocking `Wait4(-1, WNOHANG)`
  drains every zombie, publishing a portable `Reaped{PID, ExitStatus, Signaled,
  Signal}` event per collection (no `syscall` types leak into the API).
- **Supervisor** — start each `Proc` via the `Runtime`, index its pid, and on
  exit apply `RestartNever` / `RestartOnFailure` / `RestartAlways`.
- **Clean shutdown** — `Stop` sends `SIGTERM`, waits a grace period, then
  `SIGKILL`s survivors, disabling restart for the rest of its life.
- **Pluggable runtime** — implement the six-method `Runtime` interface and the
  supervisor drives anything: OCI runtimes, plain processes, fakes.
- **Portable** — the Linux-only subreaper sits behind a build tag; on
  macOS/Windows `Reaper.Run` is a no-op stub so `go build`/`go vet`/`go test`
  stay green everywhere. The supervisor logic itself is platform-independent.

CGO-free, dependency-free, **100% test coverage** (measured on the Linux lane
where the reaper is built), `gofmt` + `go vet` clean, race-tested, and green
across the six 64-bit Go targets (amd64, arm64, riscv64, loong64, ppc64le, s390x).

## Install

```sh
go get github.com/go-proc/supervisor
```

## Usage

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/go-proc/supervisor"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// 1. The subreaper — run it for the life of the process (PID 1).
	reaper := supervisor.NewReaper(log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reaper.Run(ctx)

	// 2. The supervisor — drive a Runtime over a Spec of Procs.
	spec := &supervisor.Spec{Procs: []supervisor.Proc{
		{ID: "web", Target: "/bundles/web", Restart: supervisor.RestartAlways},
		{ID: "job", Target: "/bundles/job", Restart: supervisor.RestartOnFailure},
	}}
	sup := supervisor.New(log, myRuntime, reaper, spec)

	go func() {
		time.Sleep(30 * time.Second)
		sup.Stop(ctx, 5*time.Second) // SIGTERM, grace, then SIGKILL
	}()

	if err := sup.Run(ctx); err != nil { // blocks until all done or ctx cancel
		log.Error("supervisor exited", "err", err)
	}
}
```

`myRuntime` is any value implementing the `Runtime` interface below.

## API

```go
type RestartPolicy string
const (
	RestartNever     RestartPolicy = ""            // never restart (zero value)
	RestartOnFailure RestartPolicy = "on-failure"  // restart on non-zero exit or signal
	RestartAlways    RestartPolicy = "always"      // restart on any exit
)

type Proc struct { ID, Target string; Restart RestartPolicy }
type Spec struct { Procs []Proc }
type Stdio struct { Stdin, Stdout, Stderr *os.File }

// Runtime is the process backend the supervisor drives.
type Runtime interface {
	Name() string
	Create(ctx context.Context, id, target string, stdio Stdio) error
	Start(ctx context.Context, id string) error
	State(ctx context.Context, id string) (status string, pid int, err error)
	Kill(ctx context.Context, id, signal string) error
	Delete(ctx context.Context, id string) error
}

// Reaper: PID-1 subreaper.
type Reaped struct { PID, ExitStatus int; Signaled bool; Signal int }
func NewReaper(log *slog.Logger) *Reaper
func (r *Reaper) Run(ctx context.Context)          // Linux: SIGCHLD+Wait4 ; else no-op stub
func (r *Reaper) Events() <-chan Reaped

// Supervisor.
func New(log *slog.Logger, rt Runtime, reaper *Reaper, spec *Spec) *Supervisor
func (s *Supervisor) Run(ctx context.Context) error
func (s *Supervisor) Stop(ctx context.Context, grace time.Duration)
```

## Tests & coverage

The supervisor is driven end-to-end against a programmable fake `Runtime` plus
synthetic reaper events, covering every restart policy and shutdown branch on
all platforms. The Linux subreaper's `drain` loop is exercised through a
`wait4` seam (deterministic exit/signal/error/buffer-full cases) plus a real
`SIGCHLD` delivery test — the latter skips itself under qemu-user.

```sh
COVERPKG=$(go list ./... | paste -sd, -)
go test -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%
```

Coverage is gated at 100% on the native Linux lane, where the reaper is both
compiled and run.

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-proc/supervisor authors.
