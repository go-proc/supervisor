package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"time"
)

// Supervisor drives the lifecycle of a Spec's processes through a
// Runtime: it starts each one, correlates reaper exit events back to the
// process that owns them, applies the restart policy, and stops the set
// cleanly. It does not prepare the Runtime's targets (bundle unpacking,
// command resolution, …) — it receives ready-to-run descriptors.
type Supervisor struct {
	log    *slog.Logger
	rt     Runtime
	reaper *Reaper
	spec   *Spec

	mu       sync.Mutex
	states   map[string]*procState
	pidIndex map[int]string // runtime-reported pid → process ID
}

type procState struct {
	spec        *Proc
	starts      int
	lastStart   time.Time
	lastExit    *Reaped
	pid         int
	stopped     bool
	stoppedOnce sync.Once
	done        chan struct{}
}

// New builds a Supervisor for spec, driving rt and consuming reaper. A
// nil log is replaced by a discard logger.
func New(log *slog.Logger, rt Runtime, reaper *Reaper, spec *Spec) *Supervisor {
	if log == nil {
		log = discardLogger()
	}
	return &Supervisor{
		log:      log,
		rt:       rt,
		reaper:   reaper,
		spec:     spec,
		states:   make(map[string]*procState, len(spec.Procs)),
		pidIndex: make(map[int]string),
	}
}

// Run starts every process and blocks until ctx is cancelled or all
// processes have exited with no further restart. It returns the first
// fatal error (a failed initial start, or ctx.Err on cancellation);
// per-process exits handled by the restart policy are not fatal.
func (s *Supervisor) Run(ctx context.Context) error {
	for i := range s.spec.Procs {
		p := &s.spec.Procs[i]
		st := &procState{spec: p, done: make(chan struct{})}
		s.mu.Lock()
		s.states[p.ID] = st
		s.mu.Unlock()

		if err := s.startOne(ctx, st); err != nil {
			return fmt.Errorf("start %s: %w", p.ID, err)
		}
	}

	go s.consumeReaper(ctx)

	for i := range s.spec.Procs {
		st := s.states[s.spec.Procs[i].ID]
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-st.done:
		}
	}
	return nil
}

// startOne creates, starts, and resolves the pid of one process, indexing
// it for reaper correlation.
func (s *Supervisor) startOne(ctx context.Context, st *procState) error {
	p := st.spec
	s.log.Info("runtime create starting", "id", p.ID, "target", p.Target, "runtime", s.rt.Name())
	if err := s.rt.Create(ctx, p.ID, p.Target, Stdio{}); err != nil {
		return err
	}
	s.log.Info("runtime create done; starting", "id", p.ID)
	if err := s.rt.Start(ctx, p.ID); err != nil {
		_ = s.rt.Delete(ctx, p.ID)
		return err
	}
	s.log.Info("runtime start done; resolving state", "id", p.ID)
	_, pid, err := s.rt.State(ctx, p.ID)
	if err != nil {
		s.log.Warn("state lookup after start failed", "id", p.ID, "err", err)
	}
	s.mu.Lock()
	st.pid = pid
	st.starts++
	st.lastStart = time.Now()
	if pid > 0 {
		s.pidIndex[pid] = p.ID
	}
	s.mu.Unlock()
	s.log.Info("process started", "id", p.ID, "pid", pid, "starts", st.starts)
	return nil
}

// consumeReaper correlates reaped pids to processes and applies each
// one's restart policy. Events for pids we do not own (untracked runtime
// helpers) are ignored — the reap already collected the zombie.
func (s *Supervisor) consumeReaper(ctx context.Context) {
	for ev := range s.reaper.Events() {
		s.mu.Lock()
		id, ok := s.pidIndex[ev.PID]
		if !ok {
			s.mu.Unlock()
			continue
		}
		delete(s.pidIndex, ev.PID)
		st := s.states[id]
		exit := ev
		st.lastExit = &exit
		s.mu.Unlock()

		s.log.Info("process exited",
			"id", id, "pid", ev.PID, "exit", ev.ExitStatus, "signaled", ev.Signaled)

		_ = s.rt.Delete(ctx, id)

		if s.shouldRestart(st, ev) {
			if err := s.startOne(ctx, st); err != nil {
				s.log.Error("restart failed", "id", id, "err", err)
				close(st.done)
			}
			continue
		}
		close(st.done)
	}
}

// shouldRestart applies the process's RestartPolicy to an exit event.
func (s *Supervisor) shouldRestart(st *procState, ev Reaped) bool {
	if st.stopped {
		return false
	}
	switch st.spec.Restart {
	case RestartAlways:
		return true
	case RestartOnFailure:
		return ev.ExitStatus != 0 || ev.Signaled
	default:
		return false
	}
}

// Stop sends SIGTERM to every still-running process, waits up to grace,
// then SIGKILLs any survivors. It disables restart for the remainder of
// the Supervisor's life.
func (s *Supervisor) Stop(ctx context.Context, grace time.Duration) {
	s.mu.Lock()
	ids := make([]string, 0, len(s.states))
	for id, st := range s.states {
		st.stoppedOnce.Do(func() { st.stopped = true })
		ids = append(ids, id)
	}
	s.mu.Unlock()

	for _, id := range ids {
		if err := s.rt.Kill(ctx, id, "TERM"); err != nil && !errors.Is(err, syscall.ESRCH) {
			s.log.Warn("SIGTERM failed", "id", id, "err", err)
		}
	}

	deadline := time.After(grace)
	for {
		if s.allExited() {
			return
		}
		select {
		case <-deadline:
			for _, id := range ids {
				_ = s.rt.Kill(ctx, id, "KILL")
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// allExited reports whether every supervised process has recorded an exit.
func (s *Supervisor) allExited() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.states {
		if st.lastExit == nil {
			return false
		}
	}
	return true
}
