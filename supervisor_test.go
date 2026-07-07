package supervisor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// logger returns a discard slog.Logger for tests that want a non-nil one.
func logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeRuntime is a programmable Runtime. Nil hooks fall back to a
// succeed-with-a-fresh-pid default so most tests need only override the
// behaviour they exercise.
type fakeRuntime struct {
	mu       sync.Mutex
	pid      atomic.Int64
	onCreate func(id string) error
	onStart  func(id string) error
	onState  func(id string) (string, int, error)
	onKill   func(id, sig string) error
	creates  []string
	starts   []string
	deletes  []string
	kills    []string
}

func (f *fakeRuntime) Name() string { return "fake" }

func (f *fakeRuntime) Create(_ context.Context, id, _ string, _ Stdio) error {
	f.mu.Lock()
	f.creates = append(f.creates, id)
	h := f.onCreate
	f.mu.Unlock()
	if h != nil {
		return h(id)
	}
	return nil
}

func (f *fakeRuntime) Start(_ context.Context, id string) error {
	f.mu.Lock()
	f.starts = append(f.starts, id)
	h := f.onStart
	f.mu.Unlock()
	if h != nil {
		return h(id)
	}
	return nil
}

func (f *fakeRuntime) State(_ context.Context, id string) (string, int, error) {
	f.mu.Lock()
	h := f.onState
	f.mu.Unlock()
	if h != nil {
		return h(id)
	}
	return "running", int(f.pid.Add(1)) + 1000, nil
}

func (f *fakeRuntime) Kill(_ context.Context, id, sig string) error {
	f.mu.Lock()
	f.kills = append(f.kills, id+":"+sig)
	h := f.onKill
	f.mu.Unlock()
	if h != nil {
		return h(id, sig)
	}
	return nil
}

func (f *fakeRuntime) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	f.deletes = append(f.deletes, id)
	f.mu.Unlock()
	return nil
}

func (f *fakeRuntime) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.creates)
}

func (f *fakeRuntime) killList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.kills))
	copy(out, f.kills)
	return out
}

func waitFor(t *testing.T, max time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition never reached")
}

func (s *Supervisor) hasPID(pid int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pidIndex[pid]
	return ok
}

// --- Reaper: portable construction + Run-close contract ---------------

func TestReaper_NewNilLog(t *testing.T) {
	r := NewReaper(nil)
	if r.log == nil {
		t.Fatal("nil log not replaced with discard logger")
	}
	if r.Events() == nil {
		t.Fatal("Events() returned nil channel")
	}
}

// TestReaper_RunClosesEventsOnCancel exercises Reaper.Run on every OS: on
// Linux it runs the real subreaper loop (initial drain finds no child,
// then ctx cancellation drains + closes); elsewhere it is the stub that
// blocks then closes. Either way, cancelling ctx must close Events.
func TestReaper_RunClosesEventsOnCancel(t *testing.T) {
	r := NewReaper(logger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	cancel()
	// Drain until the channel is closed.
	for range r.Events() {
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// --- Supervisor: New ---------------------------------------------------

func TestNew_NilLogUsesDiscard(t *testing.T) {
	s := New(nil, &fakeRuntime{}, NewReaper(nil), &Spec{})
	if s.log == nil {
		t.Fatal("nil log not replaced")
	}
}

// --- Supervisor: startOne error + state branches -----------------------

func TestRun_CreateErrorIsFatal(t *testing.T) {
	boom := errors.New("create boom")
	fake := &fakeRuntime{onCreate: func(string) error { return boom }}
	s := New(logger(), fake, NewReaper(nil), &Spec{Procs: []Proc{{ID: "a"}}})
	err := s.Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Run err = %v ; want create boom", err)
	}
}

func TestRun_StartErrorDeletesAndIsFatal(t *testing.T) {
	boom := errors.New("start boom")
	fake := &fakeRuntime{onStart: func(string) error { return boom }}
	s := New(logger(), fake, NewReaper(nil), &Spec{Procs: []Proc{{ID: "a"}}})
	err := s.Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Run err = %v ; want start boom", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletes) != 1 || fake.deletes[0] != "a" {
		t.Fatalf("failed start did not Delete: %v", fake.deletes)
	}
}

func TestStartOne_StateErrorAndZeroPID(t *testing.T) {
	fake := &fakeRuntime{onState: func(string) (string, int, error) {
		return "", 0, errors.New("state boom")
	}}
	spec := &Spec{Procs: []Proc{{ID: "a"}}}
	s := New(logger(), fake, NewReaper(nil), spec)
	st := &procState{spec: &spec.Procs[0], done: make(chan struct{})}
	if err := s.startOne(context.Background(), st); err != nil {
		t.Fatalf("startOne err = %v ; want nil (state error is non-fatal)", err)
	}
	if st.pid != 0 {
		t.Fatalf("pid = %d ; want 0", st.pid)
	}
	if s.hasPID(0) {
		t.Fatal("pid 0 must not be indexed")
	}
}

// --- Supervisor: shouldRestart (all policy branches) -------------------

func TestShouldRestart_AllBranches(t *testing.T) {
	s := New(logger(), &fakeRuntime{}, NewReaper(nil), &Spec{})
	mk := func(p RestartPolicy, stopped bool) *procState {
		return &procState{spec: &Proc{Restart: p}, stopped: stopped}
	}
	ok := Reaped{ExitStatus: 0}
	fail := Reaped{ExitStatus: 3}
	sig := Reaped{ExitStatus: -1, Signaled: true, Signal: 9}

	cases := []struct {
		name string
		st   *procState
		ev   Reaped
		want bool
	}{
		{"stopped-short-circuit", mk(RestartAlways, true), fail, false},
		{"always", mk(RestartAlways, false), ok, true},
		{"onfailure-nonzero", mk(RestartOnFailure, false), fail, true},
		{"onfailure-signaled", mk(RestartOnFailure, false), sig, true},
		{"onfailure-clean", mk(RestartOnFailure, false), ok, false},
		{"never", mk(RestartNever, false), fail, false},
	}
	for _, c := range cases {
		if got := s.shouldRestart(c.st, c.ev); got != c.want {
			t.Errorf("%s: shouldRestart = %v ; want %v", c.name, got, c.want)
		}
	}
}

// --- Supervisor: Run happy path + reaper correlation -------------------

func TestRun_ReapWithoutRestartReturns(t *testing.T) {
	fake := &fakeRuntime{onState: func(string) (string, int, error) { return "running", 100, nil }}
	reaper := NewReaper(logger())
	spec := &Spec{Procs: []Proc{{ID: "a", Restart: RestartNever}}}
	s := New(logger(), fake, reaper, spec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	waitFor(t, time.Second, func() bool { return s.hasPID(100) })

	reaper.events <- Reaped{PID: 999}                // unknown pid: ignored
	reaper.events <- Reaped{PID: 100, ExitStatus: 0} // owned: closes done

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Run err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after reap")
	}
	close(reaper.events)
}

func TestRun_RestartAlwaysRelaunches(t *testing.T) {
	var stateCalls atomic.Int64
	fake := &fakeRuntime{onState: func(string) (string, int, error) {
		// 200 for the first start, 201 for the relaunch.
		return "running", 200 + int(stateCalls.Add(1)) - 1, nil
	}}
	reaper := NewReaper(logger())
	spec := &Spec{Procs: []Proc{{ID: "a", Restart: RestartAlways}}}
	s := New(logger(), fake, reaper, spec)
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	waitFor(t, time.Second, func() bool { return s.hasPID(200) })

	reaper.events <- Reaped{PID: 200, ExitStatus: 1} // triggers restart → pid 201
	waitFor(t, time.Second, func() bool { return s.hasPID(201) })

	cancel() // end Run (RestartAlways would loop forever otherwise)
	if err := <-errc; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
	close(reaper.events)
}

func TestRun_RestartFailurePathClosesDone(t *testing.T) {
	var n atomic.Int64
	fake := &fakeRuntime{
		onCreate: func(string) error {
			if n.Add(1) == 2 { // fail the restart's Create
				return errors.New("relaunch boom")
			}
			return nil
		},
		onState: func(string) (string, int, error) { return "running", 300, nil },
	}
	reaper := NewReaper(logger())
	spec := &Spec{Procs: []Proc{{ID: "a", Restart: RestartAlways}}}
	s := New(logger(), fake, reaper, spec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	waitFor(t, time.Second, func() bool { return s.hasPID(300) })

	reaper.events <- Reaped{PID: 300, ExitStatus: 1} // restart fails → done closed
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Run err = %v ; want nil (restart failure closes done)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after failed restart")
	}
	close(reaper.events)
}

func TestRun_ContextCancelReturnsErr(t *testing.T) {
	reaper := NewReaper(logger())
	spec := &Spec{Procs: []Proc{{ID: "a", Restart: RestartNever}}}
	s := New(logger(), &fakeRuntime{}, reaper, spec)
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	waitFor(t, time.Second, func() bool { return started(s) })
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v ; want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on ctx cancel")
	}
	close(reaper.events)
}

func started(s *Supervisor) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.states) > 0 && len(s.pidIndex) > 0
}

// --- Supervisor: Stop --------------------------------------------------

func seed(s *Supervisor, spec *Spec, exited bool) *procState {
	st := &procState{spec: &spec.Procs[0], done: make(chan struct{})}
	if exited {
		st.lastExit = &Reaped{}
	}
	s.states[spec.Procs[0].ID] = st
	return st
}

func TestStop_AllExitedReturnsEarly(t *testing.T) {
	fake := &fakeRuntime{}
	spec := &Spec{Procs: []Proc{{ID: "a"}}}
	s := New(logger(), fake, NewReaper(nil), spec)
	st := seed(s, spec, true)
	s.Stop(context.Background(), 5*time.Second)
	if !st.stopped {
		t.Fatal("Stop did not mark process stopped")
	}
	if kl := fake.killList(); len(kl) != 1 || kl[0] != "a:TERM" {
		t.Fatalf("kills = %v ; want [a:TERM]", kl)
	}
}

func TestStop_DeadlineSendsKill(t *testing.T) {
	fake := &fakeRuntime{}
	spec := &Spec{Procs: []Proc{{ID: "a"}}}
	s := New(logger(), fake, NewReaper(nil), spec)
	seed(s, spec, false) // never exits → deadline path
	s.Stop(context.Background(), 200*time.Millisecond)
	kl := fake.killList()
	var sawKill bool
	for _, k := range kl {
		if k == "a:KILL" {
			sawKill = true
		}
	}
	if !sawKill {
		t.Fatalf("deadline did not SIGKILL: kills = %v", kl)
	}
}

func TestStop_KillTermErrorWarns(t *testing.T) {
	fake := &fakeRuntime{onKill: func(_, sig string) error {
		if sig == "TERM" {
			return errors.New("term boom")
		}
		return nil
	}}
	spec := &Spec{Procs: []Proc{{ID: "a"}}}
	s := New(logger(), fake, NewReaper(nil), spec)
	seed(s, spec, true) // already exited → returns after TERM
	s.Stop(context.Background(), time.Second)
}

func TestStop_KillTermESRCHSuppressed(t *testing.T) {
	fake := &fakeRuntime{onKill: func(_, sig string) error {
		if sig == "TERM" {
			return syscall.ESRCH // process already gone: not a warning
		}
		return nil
	}}
	spec := &Spec{Procs: []Proc{{ID: "a"}}}
	s := New(logger(), fake, NewReaper(nil), spec)
	seed(s, spec, true)
	s.Stop(context.Background(), time.Second)
}
