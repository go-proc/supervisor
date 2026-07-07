//go:build linux

package supervisor

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// scriptWait4 installs a deterministic wait4 that returns the scripted
// (pid, WaitStatus, err) triples in order, then ECHILD forever. It
// restores the original on test cleanup. Because it mutates a package
// global, callers must not run in parallel.
func scriptWait4(t *testing.T, calls []struct {
	pid int
	ws  syscall.WaitStatus
	err error
}) {
	t.Helper()
	orig := wait4
	var i int
	wait4 = func(_ int, ws *syscall.WaitStatus, _ int, _ *syscall.Rusage) (int, error) {
		if i >= len(calls) {
			return 0, syscall.ECHILD
		}
		c := calls[i]
		i++
		*ws = c.ws
		return c.pid, c.err
	}
	t.Cleanup(func() { wait4 = orig })
}

func TestDrain_PublishesExitAndSignal(t *testing.T) {
	// ws 0x0300 → exited, code 3 ; ws 0x0002 → signaled by signal 2.
	scriptWait4(t, []struct {
		pid int
		ws  syscall.WaitStatus
		err error
	}{
		{11, 0x0300, nil},
		{12, 0x0002, nil},
		{0, 0, nil}, // no more state changes
	})
	r := NewReaper(nil)
	r.drain()

	e1 := <-r.Events()
	if e1.PID != 11 || e1.ExitStatus != 3 || e1.Signaled {
		t.Fatalf("event1 = %+v ; want pid 11 exit 3 unsignaled", e1)
	}
	e2 := <-r.Events()
	if e2.PID != 12 || !e2.Signaled || e2.Signal != 2 || e2.ExitStatus != -1 {
		t.Fatalf("event2 = %+v ; want pid 12 signaled by 2", e2)
	}
	select {
	case extra := <-r.Events():
		t.Fatalf("unexpected extra event %+v", extra)
	default:
	}
}

func TestDrain_ErrorReturns(t *testing.T) {
	scriptWait4(t, []struct {
		pid int
		ws  syscall.WaitStatus
		err error
	}{
		{-1, 0, syscall.EINTR},
	})
	r := NewReaper(nil)
	r.drain()
	select {
	case ev := <-r.Events():
		t.Fatalf("error path published event %+v", ev)
	default:
	}
}

func TestDrain_NoChildReturns(t *testing.T) {
	scriptWait4(t, nil) // immediately ECHILD
	r := NewReaper(nil)
	r.drain()
	select {
	case ev := <-r.Events():
		t.Fatalf("no-child path published event %+v", ev)
	default:
	}
}

func TestDrain_DropsWhenBufferFull(t *testing.T) {
	scriptWait4(t, []struct {
		pid int
		ws  syscall.WaitStatus
		err error
	}{
		{99, 0x0100, nil}, // exit 1 ; will be dropped (buffer full)
		{0, 0, nil},
	})
	r := NewReaper(nil) // nil log → discard, must not panic on the drop
	for len(r.events) < cap(r.events) {
		r.events <- Reaped{}
	}
	r.drain() // must not block and must not panic
	if len(r.events) != cap(r.events) {
		t.Fatalf("buffer len = %d ; want cap %d (the reap was dropped)", len(r.events), cap(r.events))
	}
}

// TestRun_SigchldDrains proves a delivered SIGCHLD triggers a drain that
// publishes an event, and that ctx cancellation stops Run and closes
// Events. Uses real signal delivery, so it is gated to native arches
// (qemu-user does not reliably deliver self-signals).
func TestRun_SigchldDrains(t *testing.T) {
	switch runtime.GOARCH {
	case "amd64", "arm64":
	default:
		t.Skip("needs native SIGCHLD delivery; skipped under qemu-user")
	}

	var armed atomic.Bool
	orig := wait4
	wait4 = func(_ int, ws *syscall.WaitStatus, _ int, _ *syscall.Rusage) (int, error) {
		if armed.CompareAndSwap(true, false) {
			*ws = 0x0500 // exit 5
			return 55, nil
		}
		return 0, syscall.ECHILD
	}
	t.Cleanup(func() { wait4 = orig })

	r := NewReaper(nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	// Let Run install its SIGCHLD handler + finish the initial drain
	// (which finds nothing, armed is still false).
	time.Sleep(30 * time.Millisecond)
	armed.Store(true)
	if err := syscall.Kill(os.Getpid(), syscall.SIGCHLD); err != nil {
		t.Fatalf("kill self SIGCHLD: %v", err)
	}

	select {
	case ev := <-r.Events():
		if ev.PID != 55 || ev.ExitStatus != 5 {
			t.Fatalf("event = %+v ; want pid 55 exit 5", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SIGCHLD did not trigger a drain")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
