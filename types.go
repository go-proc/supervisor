// Package supervisor implements the two PID-1 responsibilities of a
// process-1 init: (1) a subreaper that Wait4()s every orphaned child the
// kernel reparents to this process, and (2) a supervisor that starts a
// declared set of processes through a pluggable Runtime, watches them via
// the reaper, and restarts them according to a per-process RestartPolicy.
//
// The two concerns are split on purpose. The reaper must run
// unconditionally, even for processes this program never started —
// container runtimes and shells fork helpers we do not track, and PID 1
// is the kernel's reaper of last resort for the whole orphan tree. The
// supervisor only cares about the PIDs it launched, correlating reaper
// events back to the process that owns them.
//
// The package is decoupled from any particular workload model: it drives
// an abstract Runtime (an OCI runtime such as crun/runc, a raw fork+exec
// launcher, a test fake, …) over a small Spec of Procs. Nothing here
// interprets what a Proc actually is beyond its identity and restart
// policy.
//
// Portability: the subreaper (SIGCHLD + syscall.Wait4) is Linux-specific
// and lives behind a build tag; on other platforms Reaper.Run is a stub
// that simply blocks until its context is cancelled, so consumers build
// and vet cleanly everywhere.
package supervisor

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// RestartPolicy governs whether a supervised process is restarted after
// it exits.
type RestartPolicy string

const (
	// RestartNever never restarts the process (the zero value).
	RestartNever RestartPolicy = ""
	// RestartOnFailure restarts only when the process exits non-zero or
	// is terminated by a signal.
	RestartOnFailure RestartPolicy = "on-failure"
	// RestartAlways restarts the process whenever it exits, regardless
	// of exit status.
	RestartAlways RestartPolicy = "always"
)

// Proc is one supervised process. Target is an opaque handle the Runtime
// understands — an OCI bundle directory, a command line, a unit name —
// the supervisor never interprets it.
type Proc struct {
	// ID uniquely identifies the process within a Spec. It is the key
	// the Runtime is addressed by (Create/Start/Kill/Delete) and the key
	// reaper exit events are correlated against.
	ID string
	// Target is the Runtime-specific descriptor of what to run.
	Target string
	// Restart selects the restart behaviour after the process exits.
	Restart RestartPolicy
}

// Spec is the ordered set of processes a Supervisor drives.
type Spec struct {
	Procs []Proc
}

// Stdio wires a process's standard streams. A nil stream means the
// Runtime chooses a default (typically inherit or discard).
type Stdio struct {
	Stdin  *os.File
	Stdout *os.File
	Stderr *os.File
}

// Runtime is the process backend the supervisor drives. It abstracts a
// create/start/observe/signal/delete lifecycle so an OCI runtime, a raw
// process launcher, or a test fake are all interchangeable.
type Runtime interface {
	// Name is a short identifier ("crun", "exec", "fake") used in logs.
	Name() string
	// Create readies the process identified by id from target without
	// starting it.
	Create(ctx context.Context, id, target string, stdio Stdio) error
	// Start transitions a created process to running.
	Start(ctx context.Context, id string) error
	// State reports the runtime status string and OS pid of a process.
	State(ctx context.Context, id string) (status string, pid int, err error)
	// Kill delivers signal (e.g. "TERM", "KILL") to the process.
	Kill(ctx context.Context, id, signal string) error
	// Delete frees the runtime state of an exited process.
	Delete(ctx context.Context, id string) error
}

// Reaped is one child-exit event published by the Reaper. It deliberately
// carries no platform syscall types so the whole package stays portable;
// the Linux reaper translates a WaitStatus into these fields.
type Reaped struct {
	// PID is the reaped child's process id.
	PID int
	// ExitStatus is the exit code (0 = clean). It is -1 when the process
	// was terminated by a signal rather than exiting normally.
	ExitStatus int
	// Signaled reports that the process was terminated by a signal.
	Signaled bool
	// Signal is the terminating signal number when Signaled is true.
	Signal int
}

// Reaper consumes SIGCHLD and reaps every child the kernel hands this
// process, publishing a Reaped per collected zombie on a channel the
// Supervisor drains to correlate exits. The channel is closed when Run
// returns.
type Reaper struct {
	log    *slog.Logger
	events chan Reaped
}

// NewReaper builds a Reaper. A nil log is replaced by a discard logger.
func NewReaper(log *slog.Logger) *Reaper {
	if log == nil {
		log = discardLogger()
	}
	return &Reaper{log: log, events: make(chan Reaped, 64)}
}

// Events returns the channel of reaped children. Receivers must drain it;
// a full buffer drops the correlation record (the zombie is still reaped)
// and logs a warning. The channel is closed when Run returns.
func (r *Reaper) Events() <-chan Reaped { return r.events }

// discardLogger returns a slog.Logger that throws everything away.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}
