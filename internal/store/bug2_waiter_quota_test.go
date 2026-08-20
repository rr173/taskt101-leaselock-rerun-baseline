package store

import (
	"testing"
	"time"

	"task101-leaselock/internal/lease"
)

func TestWaiterPromotionDoesNotBreakHolderQuota(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(200, 0)
	if err := s.PutHolder(lease.Holder{ID: "H2", MaxConcurrent: 1, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire("other", "H2", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	owner, _ := s.Acquire("X", "H1", now, time.Minute)
	w, _ := s.EnqueueWaiter("X", "H2", time.Minute, now.Add(time.Second))
	if err := s.Release("X", "H1", owner.Token, now.Add(2*time.Second)); err != nil {
		t.Fatalf("release should succeed even when oldest waiter is over quota: %v", err)
	}
	if _, active, ok := s.Inspect("X", now.Add(2*time.Second)); ok && active {
		t.Fatalf("over-quota waiter must not receive the lease")
	}
	pending, _ := s.ListWaiters("X", lease.WaiterPending)
	if len(pending) != 1 || pending[0].ID != w.ID {
		t.Fatalf("waiter=%+v want pending waiter %s", pending, w.ID)
	}
}
