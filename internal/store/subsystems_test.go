package store

import (
	"errors"
	"testing"
	"time"

	"task101-leaselock/internal/lease"
)

// TestTransferExpiredReturnsErrExpired: transferring an expired lease is
// forbidden, matching renew/release semantics.
func TestTransferExpiredReturnsErrExpired(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, _ := s.Acquire("X", "H1", now, 5*time.Second)
	later := now.Add(6 * time.Second)
	_, err := s.Transfer("X", "H1", l.Token, "H2", later)
	if !errors.Is(err, lease.ErrExpired) {
		t.Fatalf("Transfer expired = %v want ErrExpired", err)
	}
}

// TestTransferStaleTokenReturnsErrNotHolder: a holder whose token no longer
// matches cannot transfer.
func TestTransferStaleTokenReturnsErrNotHolder(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l1, _ := s.Acquire("X", "H1", now, 5*time.Second)
	later := now.Add(6 * time.Second)
	s.Acquire("X", "H2", later, 5*time.Second) // re-acquired under new token
	_, err := s.Transfer("X", "H1", l1.Token, "H3", later)
	if !errors.Is(err, lease.ErrNotHolder) {
		t.Fatalf("stale Transfer = %v want ErrNotHolder", err)
	}
}

// TestBulkReleaseAllOrNothing: a bad token in one entry rolls back the whole
// batch (A remains held).
func TestBulkReleaseAllOrNothing(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	bar, _ := s.BulkAcquire("H", []string{"A", "B"}, now, 50*time.Second)
	entries := []lease.ReleaseEntry{
		{Resource: "A", Token: bar[0].Token},
		{Resource: "B", Token: 99999},
	}
	_, err := s.BulkRelease("H", entries, now)
	if !errors.Is(err, lease.ErrNotHolder) {
		t.Fatalf("bad bulk release = %v want ErrNotHolder", err)
	}
	// A still held.
	_, _, ok := s.Inspect("A", now)
	if !ok {
		t.Fatalf("A should still be held after rolled-back bulk release")
	}
}

// TestWaiterPromotionOnExpire: expiring a lease promotes the oldest waiter.
func TestWaiterPromotionOnExpire(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, _ := s.Acquire("X", "H1", now, 5*time.Second)
	s.EnqueueWaiter("X", "H2", 5*time.Second, now)
	later := now.Add(6 * time.Second) // X expires
	s.ExpireAll(later)
	// H2 promoted.
	got, active, ok := s.Inspect("X", later)
	if !ok || !active || got.Holder != "H2" || got.Token <= l.Token {
		t.Fatalf("after expire X = %+v active=%v want H2 token>%d", got, active, l.Token)
	}
}

// TestWaiterFIFOOrder: the oldest pending waiter is promoted first.
func TestWaiterFIFOOrder(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	t0 := time.Unix(100, 0)
	la, _ := s.Acquire("X", "H1", t0, 50*time.Second)
	wA, _ := s.EnqueueWaiter("X", "HA", 5*time.Second, t0.Add(1*time.Second))
	wB, _ := s.EnqueueWaiter("X", "HB", 5*time.Second, t0.Add(2*time.Second))
	if wB.ID == wA.ID {
		t.Fatalf("waiter ids collided")
	}
	// Release -> HA (oldest) promoted.
	s.Release("X", "H1", la.Token, t0)
	got, _, _ := s.Inspect("X", t0)
	if got.Holder != "HA" {
		t.Fatalf("first promotion = %s want HA", got.Holder)
	}
}

// TestAcquireExpiredWithWaiterQueuesCallerAndPromotesOldest: when a lease
// expires while waiters are pending, a fresh Acquire must NOT cut in line.
// The oldest waiter is promoted (FIFO) and the caller is appended to the tail
// (ErrQueued, no lease granted to the caller).
func TestAcquireExpiredWithWaiterQueuesCallerAndPromotesOldest(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	t0 := time.Unix(100, 0)
	l1, _ := s.Acquire("X", "H1", t0, 5*time.Second) // deadline 105
	wA, _ := s.EnqueueWaiter("X", "HA", 5*time.Second, t0.Add(1*time.Second))
	wB, _ := s.EnqueueWaiter("X", "HB", 5*time.Second, t0.Add(2*time.Second))
	if wA.Status != lease.WaiterPending || wB.Status != lease.WaiterPending {
		t.Fatalf("setup waiters = %v %v want pending", wA.Status, wB.Status)
	}
	later := t0.Add(6 * time.Second) // X expired
	// Fresh Acquire from HZ while HA/HB are queued: must be queued, not granted.
	_, err := s.Acquire("X", "HZ", later, 5*time.Second)
	if !errors.Is(err, lease.ErrQueued) {
		t.Fatalf("Acquire with queued waiters = %v want ErrQueued", err)
	}
	// HA (oldest) was promoted to holder with a strictly larger token.
	got, active, ok := s.Inspect("X", later)
	if !ok || !active || got.Holder != "HA" || got.Token <= l1.Token {
		t.Fatalf("after expired Acquire X = %+v active=%v want HA token>%d", got, active, l1.Token)
	}
	// The caller HZ is now a pending waiter at the tail, behind HB.
	pending, _ := s.ListWaiters("X", lease.WaiterPending)
	if len(pending) != 2 {
		t.Fatalf("pending waiters = %d want 2 (HB then HZ)", len(pending))
	}
	if pending[0].Holder != "HB" || pending[1].Holder != "HZ" {
		t.Fatalf("pending order = %s %s want HB then HZ", pending[0].Holder, pending[1].Holder)
	}
}

// TestAcquireExpiredNoWaiterGrants: with no queued waiters, an expired lease
// is reaped and replaced with a fresh grant carrying a strictly larger token
// (the pre-fix behavior is preserved when the queue is empty).
func TestAcquireExpiredNoWaiterGrants(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l1, _ := s.Acquire("X", "H1", now, 5*time.Second)
	later := now.Add(6 * time.Second)
	l2, err := s.Acquire("X", "H2", later, 5*time.Second)
	if err != nil {
		t.Fatalf("re-Acquire expired no-waiter: %v", err)
	}
	if l2.Token <= l1.Token {
		t.Fatalf("re-acquire token %d not > %d", l2.Token, l1.Token)
	}
}

// TestBulkAcquireExpiredWithWaiterConflicts: BulkAcquire is all-or-nothing and
// never queues, so a resource whose lease expired while waiters are pending
// causes the whole batch to roll back with ErrConflict rather than cutting in
// line.
func TestBulkAcquireExpiredWithWaiterConflicts(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	t0 := time.Unix(100, 0)
	s.Acquire("Y", "H1", t0, 5*time.Second) // Y expires at 105
	s.EnqueueWaiter("Y", "HA", 5*time.Second, t0.Add(1*time.Second))
	later := t0.Add(6 * time.Second) // Y expired, HA pending
	_, err := s.BulkAcquire("H2", []string{"X", "Y", "Z"}, later, 5*time.Second)
	if !errors.Is(err, lease.ErrConflict) {
		t.Fatalf("BulkAcquire over expired-with-waiter = %v want ErrConflict", err)
	}
	// Nothing granted: X and Z do not exist; Y still has its expired record
	// (no reap, no grant) and HA remains pending.
	if _, _, ok := s.Inspect("X", later); ok {
		t.Fatalf("X should not exist after failed bulk")
	}
	if _, _, ok := s.Inspect("Z", later); ok {
		t.Fatalf("Z should not exist after failed bulk")
	}
	ws, _ := s.ListWaiters("Y", lease.WaiterPending)
	if len(ws) != 1 || ws[0].Holder != "HA" {
		t.Fatalf("HA pending = %+v want 1 HA pending", ws)
	}
}

// TestCancelWaiterNotPromoted: a cancelled waiter is skipped on promotion.
func TestCancelWaiterNotPromoted(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, _ := s.Acquire("X", "H1", now, 50*time.Second)
	wA, _ := s.EnqueueWaiter("X", "HA", 5*time.Second, now.Add(1*time.Second))
	s.EnqueueWaiter("X", "HB", 5*time.Second, now.Add(2*time.Second))
	s.CancelWaiter(wA.ID)
	s.Release("X", "H1", l.Token, now)
	got, _, _ := s.Inspect("X", now)
	if got.Holder != "HB" {
		t.Fatalf("promotion = %s want HB (cancelled HA skipped)", got.Holder)
	}
}

// TestListResourcesNoTagFilter: listing with no tag returns the union of all
// tagged resources.
func TestListResourcesNoTagFilter(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	s.AddTag("A", "prod")
	s.AddTag("B", "web")
	res, _ := s.ListResources("")
	if len(res) != 2 || res[0] != "A" || res[1] != "B" {
		t.Fatalf("all resources = %v want [A B]", res)
	}
}

// TestAuditFilteredByResource: audit entries for an unrelated resource are
// excluded.
func TestAuditFilteredByResource(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.Acquire("X", "H1", now, 5*time.Second)
	s.Acquire("Y", "H2", now, 5*time.Second)
	entries, _ := s.ListAudit("X", 100, 0)
	for _, e := range entries {
		if e.Resource != "X" {
			t.Fatalf("audit for X includes %q", e.Resource)
		}
	}
}

// TestStatsAfterExpiry: expired leases are counted as expired, not active.
func TestStatsAfterExpiry(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.Acquire("A", "H1", now, 5*time.Second)
	s.Acquire("B", "H1", now, 100*time.Second)
	later := now.Add(6 * time.Second)
	s.ExpireAll(later)
	st, _ := s.Stats(later)
	if st.TotalLeases != 1 || st.ActiveLeases != 1 || st.ExpiredLeases != 0 {
		t.Fatalf("stats after expiry = %+v want total=1 active=1", st)
	}
	if st.AuditEntries < 1 {
		t.Fatalf("audit entries = %d want >=1", st.AuditEntries)
	}
}
