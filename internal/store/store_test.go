package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"task101-leaselock/internal/lease"
)

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s, path
}

func TestAcquireThenInspect(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, err := s.Acquire("X", "H1", now, 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if l.Token != 1 || l.Deadline.Unix() != 105 {
		t.Fatalf("lease = %+v", l)
	}
	got, active, ok := s.Inspect("X", now)
	if !ok || !active || got.Token != 1 {
		t.Fatalf("Inspect = %+v active=%v ok=%v", got, active, ok)
	}
}

func TestAcquireHeldReturnsErrHeld(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.Acquire("X", "H1", now, 5*time.Second)
	_, err := s.Acquire("X", "H2", now, 5*time.Second)
	if !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("second Acquire = %v want ErrHeld", err)
	}
}

func TestAcquireOverExpiredReapsAndLargerToken(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l1, _ := s.Acquire("X", "H1", now, 5*time.Second)
	later := now.Add(6 * time.Second)
	l2, err := s.Acquire("X", "H2", later, 5*time.Second)
	if err != nil {
		t.Fatalf("re-Acquire expired: %v", err)
	}
	if l2.Token <= l1.Token {
		t.Fatalf("re-acquire token %d not > %d", l2.Token, l1.Token)
	}
}

func TestRenewExpiredReturnsErrExpired(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, _ := s.Acquire("X", "H1", now, 5*time.Second)
	later := now.Add(6 * time.Second)
	_, err := s.Renew("X", "H1", l.Token, later, 5*time.Second)
	if !errors.Is(err, lease.ErrExpired) {
		t.Fatalf("Renew expired = %v want ErrExpired", err)
	}
}

func TestRenewStaleTokenReturnsErrNotHolder(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l1, _ := s.Acquire("X", "H1", now, 5*time.Second)
	later := now.Add(6 * time.Second)
	l2, _ := s.Acquire("X", "H2", later, 5*time.Second)
	_, err := s.Renew("X", "H1", l1.Token, later, 5*time.Second)
	if !errors.Is(err, lease.ErrNotHolder) {
		t.Fatalf("stale Renew = %v want ErrNotHolder", err)
	}
	if l2.Token <= l1.Token {
		t.Fatalf("re-acquire token %d not > %d", l2.Token, l1.Token)
	}
}

func TestReleaseExpiredReturnsErrExpired(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, _ := s.Acquire("X", "H1", now, 5*time.Second)
	later := now.Add(6 * time.Second)
	err := s.Release("X", "H1", l.Token, later)
	if !errors.Is(err, lease.ErrExpired) {
		t.Fatalf("Release expired = %v want ErrExpired", err)
	}
}

func TestExpireAllReapsDeadLeases(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.Acquire("A", "H1", now, 5*time.Second)
	s.Acquire("B", "H1", now, 50*time.Second)
	s.Acquire("C", "H1", now, 5*time.Second)
	n, _ := s.ExpireAll(now)
	if n != 0 {
		t.Fatalf("ExpireAll pre = %d want 0", n)
	}
	n, _ = s.ExpireAll(now.Add(6 * time.Second))
	if n != 2 {
		t.Fatalf("ExpireAll post = %d want 2", n)
	}
	_, active, ok := s.Inspect("B", now.Add(6*time.Second))
	if !ok || !active {
		t.Fatalf("B should still be active")
	}
}

func TestListSortedByResource(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.Acquire("C", "H1", now, 5*time.Second)
	s.Acquire("A", "H1", now, 5*time.Second)
	s.Acquire("B", "H1", now, 5*time.Second)
	views, _ := s.List(now)
	got := []string{views[0].Lease.Resource, views[1].Lease.Resource, views[2].Lease.Resource}
	want := []string{"A", "B", "C"}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("order = %v want %v", got, want)
		}
	}
}

func TestNextTokenMonotonic(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	if tok, _ := s.NextToken(); tok != 1 {
		t.Fatalf("initial NextToken = %d want 1", tok)
	}
	s.Acquire("A", "H1", now, 5*time.Second)
	if tok, _ := s.NextToken(); tok != 2 {
		t.Fatalf("after 1 grant NextToken = %d want 2", tok)
	}
}

func TestRestartRecoversLeasesAndCounter(t *testing.T) {
	s, path := newTestStore(t)
	now := time.Unix(100, 0)
	_, _ = s.Acquire("X", "H1", now, 5*time.Second)
	ly, _ := s.Acquire("Y", "H2", now, 100*time.Second)
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	restartAt := now.Add(10 * time.Second)
	n, _ := s2.ExpireAll(restartAt)
	if n != 1 {
		t.Fatalf("restart ExpireAll = %d want 1", n)
	}
	got, active, ok := s2.Inspect("Y", restartAt)
	if !ok || !active || got.Token != ly.Token {
		t.Fatalf("Y recovered = %+v active=%v want token=%d", got, active, ly.Token)
	}
	l3, err := s2.Acquire("Z", "H3", restartAt, 5*time.Second)
	if err != nil {
		t.Fatalf("post-restart Acquire Z: %v", err)
	}
	if l3.Token <= ly.Token {
		t.Fatalf("post-restart token %d not > %d", l3.Token, ly.Token)
	}
}

func TestHolderCRUD(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	if err := s.PutHolder(lease.Holder{ID: "H1", Description: "d", MaxConcurrent: 2, CreatedAt: now}); err != nil {
		t.Fatalf("PutHolder: %v", err)
	}
	if err := s.PutHolder(lease.Holder{ID: "H1", MaxConcurrent: 1, CreatedAt: now}); !errors.Is(err, lease.ErrHolderExists) {
		t.Fatalf("dup PutHolder = %v want ErrHolderExists", err)
	}
	h, ok, _ := s.GetHolder("H1")
	if !ok || h.MaxConcurrent != 2 {
		t.Fatalf("GetHolder = %+v ok=%v", h, ok)
	}
	if err := s.SetQuota("H1", 5); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	q, ok, _ := s.GetQuota("H1")
	if !ok || q.MaxConcurrent != 5 {
		t.Fatalf("GetQuota = %+v", q)
	}
	// Delete holder with active lease fails.
	s.Acquire("X", "H1", now, 5*time.Second)
	if err := s.DeleteHolder("H1", now); !errors.Is(err, lease.ErrHolderHasLeases) {
		t.Fatalf("DeleteHolder with leases = %v want ErrHolderHasLeases", err)
	}
	// After release, delete succeeds.
	s.Release("X", "H1", 1, now)
	if err := s.DeleteHolder("H1", now); err != nil {
		t.Fatalf("DeleteHolder after release: %v", err)
	}
}

func TestQuotaEnforcedOnAcquire(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.PutHolder(lease.Holder{ID: "H1", MaxConcurrent: 2, CreatedAt: now})
	s.Acquire("A", "H1", now, 5*time.Second)
	s.Acquire("B", "H1", now, 5*time.Second)
	_, err := s.Acquire("C", "H1", now, 5*time.Second)
	if !errors.Is(err, lease.ErrQuotaExceeded) {
		t.Fatalf("over-quota Acquire = %v want ErrQuotaExceeded", err)
	}
	// Unregistered holder (no cap) can acquire freely.
	if _, err := s.Acquire("D", "H2", now, 5*time.Second); err != nil {
		t.Fatalf("unregistered holder Acquire = %v", err)
	}
}

func TestTransferKeepsTokenChangesHolder(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, _ := s.Acquire("X", "H1", now, 50*time.Second)
	out, err := s.Transfer("X", "H1", l.Token, "H2", now)
	if err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	if out.Token != l.Token || out.Holder != "H2" {
		t.Fatalf("Transfer result = %+v want token=%d holder=H2", out, l.Token)
	}
	// Old holder cannot release.
	if err := s.Release("X", "H1", l.Token, now); !errors.Is(err, lease.ErrNotHolder) {
		t.Fatalf("old holder Release = %v want ErrNotHolder", err)
	}
}

func TestTransferQuotaOnNewHolder(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	// H2 capped at 1, already holding Y.
	s.PutHolder(lease.Holder{ID: "H2", MaxConcurrent: 1, CreatedAt: now})
	s.Acquire("Y", "H2", now, 50*time.Second)
	// H1 holds X, transfers to H2 -> H2 would exceed quota (1+1 > 1).
	l, _ := s.Acquire("X", "H1", now, 50*time.Second)
	_, err := s.Transfer("X", "H1", l.Token, "H2", now)
	if !errors.Is(err, lease.ErrQuotaExceeded) {
		t.Fatalf("Transfer over quota = %v want ErrQuotaExceeded", err)
	}
}

func TestBulkAcquireAllOrNothing(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.Acquire("Y", "H", now, 5*time.Second) // Y held
	_, err := s.BulkAcquire("H", []string{"X", "Y", "Z"}, now, 5*time.Second)
	if !errors.Is(err, lease.ErrConflict) {
		t.Fatalf("bulk with held member = %v want ErrConflict", err)
	}
	// X and Z not acquired.
	if _, _, ok := s.Inspect("X", now); ok {
		t.Fatalf("X should not exist after failed bulk")
	}
	granted, err := s.BulkAcquire("H", []string{"X", "Z"}, now, 5*time.Second)
	if err != nil || len(granted) != 2 {
		t.Fatalf("clean bulk = %v granted=%d", err, len(granted))
	}
	if granted[0].Token >= granted[1].Token {
		t.Fatalf("bulk tokens not increasing")
	}
}

func TestBulkAcquireQuota(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.PutHolder(lease.Holder{ID: "H", MaxConcurrent: 2, CreatedAt: now})
	_, err := s.BulkAcquire("H", []string{"A", "B", "C"}, now, 5*time.Second)
	if !errors.Is(err, lease.ErrQuotaExceeded) {
		t.Fatalf("bulk over quota = %v want ErrQuotaExceeded", err)
	}
}

func TestWaiterImmediateGrant(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	w, err := s.EnqueueWaiter("X", "H1", 5*time.Second, now)
	if err != nil {
		t.Fatalf("EnqueueWaiter free: %v", err)
	}
	if w.Status != lease.WaiterGranted {
		t.Fatalf("status = %v want granted", w.Status)
	}
	if w.GrantedToken != 1 {
		t.Fatalf("granted token = %d want 1", w.GrantedToken)
	}
}

func TestWaiterPromotionOnRelease(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, _ := s.Acquire("X", "H1", now, 50*time.Second)
	w, _ := s.EnqueueWaiter("X", "H2", 5*time.Second, now)
	if w.Status != lease.WaiterPending {
		t.Fatalf("waiter status = %v want pending", w.Status)
	}
	if err := s.Release("X", "H1", l.Token, now); err != nil {
		t.Fatalf("Release: %v", err)
	}
	// H2 now holds X with a new token > l.Token.
	got, active, ok := s.Inspect("X", now)
	if !ok || !active || got.Holder != "H2" || got.Token <= l.Token {
		t.Fatalf("after release X = %+v active=%v want H2 token>%d", got, active, l.Token)
	}
	// Waiter now granted.
	ws, _ := s.ListWaiters("X", "")
	if len(ws) != 1 || ws[0].Status != lease.WaiterGranted {
		t.Fatalf("waiters = %+v want 1 granted", ws)
	}
}

func TestWaiterCancel(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.Acquire("X", "H1", now, 50*time.Second)
	w, _ := s.EnqueueWaiter("X", "H2", 5*time.Second, now)
	if err := s.CancelWaiter(w.ID); err != nil {
		t.Fatalf("CancelWaiter: %v", err)
	}
	// Cancelling twice -> not found (no longer pending).
	if err := s.CancelWaiter(w.ID); !errors.Is(err, lease.ErrWaiterNotFound) {
		t.Fatalf("second CancelWaiter = %v want ErrWaiterNotFound", err)
	}
}

func TestTagsCRUD(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	s.AddTag("A", "prod")
	s.AddTag("A", "web")
	s.AddTag("B", "prod")
	// Idempotent add.
	if err := s.AddTag("A", "prod"); err != nil {
		t.Fatalf("idempotent AddTag: %v", err)
	}
	res, _ := s.ListResources("prod")
	if len(res) != 2 || res[0] != "A" || res[1] != "B" {
		t.Fatalf("tag=prod resources = %v want [A B]", res)
	}
	tags, _ := s.GetTags("A")
	if len(tags) != 2 || tags[0] != "prod" || tags[1] != "web" {
		t.Fatalf("A tags = %v want [prod web]", tags)
	}
	if err := s.RemoveTag("A", "web"); err != nil {
		t.Fatalf("RemoveTag: %v", err)
	}
	tags, _ = s.GetTags("A")
	if len(tags) != 1 || tags[0] != "prod" {
		t.Fatalf("A tags after remove = %v want [prod]", tags)
	}
	if err := s.RemoveTag("A", "nope"); !errors.Is(err, lease.ErrTagNotFound) {
		t.Fatalf("RemoveTag missing = %v want ErrTagNotFound", err)
	}
}

func TestPolicyCRUD(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	if err := s.PutPolicy(lease.Policy{Name: "short", TTLSeconds: 10}); err != nil {
		t.Fatalf("PutPolicy: %v", err)
	}
	if err := s.PutPolicy(lease.Policy{Name: "short", TTLSeconds: 20}); !errors.Is(err, lease.ErrPolicyExists) {
		t.Fatalf("dup PutPolicy = %v want ErrPolicyExists", err)
	}
	p, ok, _ := s.GetPolicy("short")
	if !ok || p.TTLSeconds != 10 {
		t.Fatalf("GetPolicy = %+v ok=%v", p, ok)
	}
	_, ok, _ = s.GetPolicy("nope")
	if ok {
		t.Fatalf("unknown policy should not exist")
	}
	ps, _ := s.ListPolicies()
	if len(ps) != 1 {
		t.Fatalf("policies = %d want 1", len(ps))
	}
}

func TestAuditAppendedOnMutations(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	l, _ := s.Acquire("X", "H1", now, 5*time.Second)
	s.Renew("X", "H1", l.Token, now, 5*time.Second)
	s.Release("X", "H1", l.Token, now)
	entries, _ := s.ListAudit("X", 100, 0)
	// acquire + renew + release = 3 (the release does not trigger a waiter
	// promotion here, so no waiter_grant entry).
	if len(entries) != 3 {
		t.Fatalf("audit entries = %d want 3: %+v", len(entries), entries)
	}
	// Token history: acquire + renew.
	th, _ := s.TokenHistory("X")
	if len(th) != 2 {
		t.Fatalf("token history = %d want 2", len(th))
	}
}

func TestStatsCounts(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(100, 0)
	s.Acquire("A", "H1", now, 5*time.Second)
	s.Acquire("B", "H1", now, 100*time.Second)
	s.PutHolder(lease.Holder{ID: "H1", MaxConcurrent: 5, CreatedAt: now})
	s.PutPolicy(lease.Policy{Name: "p", TTLSeconds: 10})
	st, _ := s.Stats(now)
	if st.TotalLeases != 2 || st.ActiveLeases != 2 || st.ExpiredLeases != 0 || st.NextFencingToken != 3 || st.RegisteredHolders != 1 || st.PoliciesCount != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestOpenMissingDirIsError(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "no", "such", "dir", "x.db"))
	if err == nil {
		t.Fatalf("Open in missing dir should error")
	}
}
