package store

import (
	"testing"
	"time"

	"task101-leaselock/internal/lease"
)

func TestExpiredResourceKeepsFIFOWaiterPriority(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	owner, _ := s.Acquire("X", "owner", now, 5*time.Second)
	first, _ := s.EnqueueWaiter("X", "first", 20*time.Second, now.Add(time.Second))
	second, err := s.EnqueueWaiter("X", "second", 20*time.Second, now.Add(6*time.Second))
	if err != nil {
		t.Fatalf("enqueue after expiry: %v", err)
	}
	got, active, ok := s.Inspect("X", now.Add(6*time.Second))
	if !ok || !active || got.Holder != "first" {
		t.Fatalf("expired resource was not handed to oldest waiter: lease=%+v active=%v ok=%v", got, active, ok)
	}
	if got.Token <= owner.Token {
		t.Fatalf("promoted token=%d must exceed owner token=%d", got.Token, owner.Token)
	}
	waiters, _ := s.ListWaiters("X", lease.WaiterPending)
	if len(waiters) != 1 || waiters[0].ID != second.ID {
		t.Fatalf("pending waiters=%+v want only newer waiter %s", waiters, second.ID)
	}
	granted, _ := s.ListWaiters("X", lease.WaiterGranted)
	if len(granted) != 1 || granted[0].ID != first.ID {
		t.Fatalf("granted waiters=%+v want oldest waiter %s", granted, first.ID)
	}
}
