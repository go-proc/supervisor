//go:build !linux

package supervisor

import "context"

// Run is a no-op subreaper on non-Linux platforms: PID-1 subreaping via
// SIGCHLD + Wait4 is Linux-specific. It blocks until ctx is cancelled and
// then closes Events, so cross-platform builds of code that wires a Reaper
// stay green — the supervisor simply never receives OS exit events here
// (tests feed the supervisor synthetic events directly). Run must be
// called exactly once.
func (r *Reaper) Run(ctx context.Context) {
	defer close(r.events)
	<-ctx.Done()
}
