package clock

import (
	"testing"
	"time"
)

func TestRealClockNow(t *testing.T) {
	c := RealClock{}
	before := time.Now()
	got := c.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("RealClock.Now = %v not in [%v,%v]", got, before, after)
	}
}

func TestFakeClockStartsAtAnchor(t *testing.T) {
	anchor := time.Unix(42, 0)
	c := NewFakeClock(anchor)
	if !c.Now().Equal(anchor) {
		t.Fatalf("Now = %v want %v", c.Now(), anchor)
	}
}

func TestFakeClockAdvanceMonotonic(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	c.Advance(5 * time.Second)
	if got := c.Now(); got.Unix() != 5 {
		t.Fatalf("after +5s Now = %v want unix=5", got)
	}
	c.Advance(3 * time.Second)
	if got := c.Now(); got.Unix() != 8 {
		t.Fatalf("after +3s more Now = %v want unix=8", got)
	}
}

func TestFakeClockSetRepositions(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	c.Set(time.Unix(123, 0))
	if got := c.Now(); got.Unix() != 123 {
		t.Fatalf("Set: Now = %v want 123", got)
	}
}

func TestFakeClockAdvanceZeroIsNoop(t *testing.T) {
	anchor := time.Unix(7, 0)
	c := NewFakeClock(anchor)
	c.Advance(0)
	if !c.Now().Equal(anchor) {
		t.Fatalf("0 advance moved clock to %v", c.Now())
	}
}

func TestFakeClockConcurrent(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	done := make(chan struct{})
	// Reader and writer racing must not race-fault; FakeClock holds its mutex.
	go func() {
		defer close(done)
		for i := 0; i <1000; i++ {
			c.Advance(time.Millisecond)
		}
	}()
	for i := 0; i <1000; i++ {
		_ = c.Now()
	}
	<-done
}
