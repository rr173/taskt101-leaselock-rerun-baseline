package store

import (
	"errors"
	"testing"
	"time"

	"task101-leaselock/internal/lease"
)

func TestDeleteHolderRejectsPendingWaiters(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(800, 0)
	if err := s.PutHolder(lease.Holder{ID: "queued", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Acquire("X", "owner", now, time.Minute)
	if _, err := s.EnqueueWaiter("X", "queued", time.Minute, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteHolder("queued", now.Add(2*time.Second)); !errors.Is(err, lease.ErrHolderHasWaiters) {
		t.Fatalf("delete holder error=%v want ErrHolderHasWaiters", err)
	}
	if _, ok, _ := s.GetHolder("queued"); !ok {
		t.Fatal("holder was deleted while a waiter still referenced it")
	}
}
