//go:build linux

package supervisor

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// wait4 is a seam over syscall.Wait4 so the drain loop can be driven
// deterministically in tests without spawning real zombies.
var wait4 = syscall.Wait4

// Run subscribes to SIGCHLD and reaps every child the kernel reparents to
// this process (PID 1 in a microVM). It installs the signal handler
// before the initial drain so no exit that races startup is missed, then
// blocks until ctx is cancelled, does a final drain, and closes Events.
// Run must be called exactly once.
func (r *Reaper) Run(ctx context.Context) {
	defer close(r.events)
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGCHLD)
	defer signal.Stop(sigCh)

	r.drain() // children that exited before we subscribed

	for {
		select {
		case <-ctx.Done():
			r.drain()
			return
		case <-sigCh:
			r.drain()
		}
	}
}

// drain Wait4()s every reapable child without blocking, publishing one
// Reaped per collected zombie. A full Events buffer drops the correlation
// record (the zombie is still reaped) and logs it.
func (r *Reaper) drain() {
	for {
		var ws syscall.WaitStatus
		var ru syscall.Rusage
		pid, err := wait4(-1, &ws, syscall.WNOHANG, &ru)
		if err != nil || pid <= 0 {
			// ECHILD (no children), EINTR, or 0 (no state change) all
			// mean nothing more to reap right now.
			return
		}
		ev := Reaped{PID: pid, ExitStatus: ws.ExitStatus(), Signaled: ws.Signaled()}
		if ev.Signaled {
			ev.Signal = int(ws.Signal())
		}
		select {
		case r.events <- ev:
		default:
			r.log.Warn("reaper events buffer full, dropping exit correlation", "pid", pid)
		}
	}
}
