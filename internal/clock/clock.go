// Package clock provides an injectable time abstraction so the lease manager
// can be tested deterministically without sleeping. The real server uses
// RealClock; tests and the smoke self-check use FakeClock to advance time in
// discrete steps and observe expiry behaviour exactly.
package clock

import (
	"sync"
	"time"
)

// Clock reports the current instant. Implementations must be safe for
// concurrent use.
type Clock interface {
	// Now returns the current time as seen by this clock.
	Now() time.Time
}

// RealClock returns the wall-clock time. It is the clock used by the live
// HTTP server.
type RealClock struct{}

// Now reports the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// FakeClock is a controllable clock whose time only advances when the test
// calls Advance or Set. It is safe for concurrent use.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock creates a FakeClock anchored at the given instant.
func NewFakeClock(at time.Time) *FakeClock {
	return &FakeClock{now: at}
}

// Now returns the clock's current instant.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the clock forward by d. d may be zero or negative (rewinding
// is allowed for tests that want to inspect the past); neither affects any
// persisted state, only the instant reported by subsequent Now calls.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set repositions the clock to the given instant.
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}
