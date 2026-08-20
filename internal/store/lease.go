package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// persistedLease is the on-disk shape of a lease. Times are stored as UnixNano
// integers for compact, unambiguous storage.
type persistedLease struct {
	Resource   string `json:"resource"`
	Holder     string `json:"holder"`
	Token      int64  `json:"token"`
	Deadline   int64  `json:"deadline_nano"`
	Acquired   int64  `json:"acquired_nano"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

func toPersisted(l lease.Lease) persistedLease {
	return persistedLease{
		Resource: l.Resource, Holder: l.Holder, Token: int64(l.Token),
		Deadline: l.Deadline.UnixNano(), Acquired: l.Acquired.UnixNano(),
		TTLSeconds: l.TTLSeconds,
	}
}

func fromPersisted(p persistedLease) lease.Lease {
	return lease.Lease{
		Resource: p.Resource, Holder: p.Holder, Token: lease.Token(p.Token),
		Deadline: time.Unix(0, p.Deadline), Acquired: time.Unix(0, p.Acquired),
		TTLSeconds: p.TTLSeconds,
	}
}

func readLease(tx *bbolt.Tx, resource string) (lease.Lease, bool, error) {
	b := tx.Bucket(bucketLeases)
	if b == nil {
		return lease.Lease{}, false, errors.New("leases bucket missing")
	}
	raw := b.Get([]byte(resource))
	if raw == nil {
		return lease.Lease{}, false, nil
	}
	var p persistedLease
	if err := json.Unmarshal(raw, &p); err != nil {
		return lease.Lease{}, false, fmt.Errorf("decode lease %q: %w", resource, err)
	}
	return fromPersisted(p), true, nil
}

func putLease(tx *bbolt.Tx, l lease.Lease) error {
	b := tx.Bucket(bucketLeases)
	if b == nil {
		return errors.New("leases bucket missing")
	}
	raw, err := json.Marshal(toPersisted(l))
	if err != nil {
		return fmt.Errorf("encode lease %q: %w", l.Resource, err)
	}
	return b.Put([]byte(l.Resource), raw)
}

func deleteLease(tx *bbolt.Tx, resource string) error {
	b := tx.Bucket(bucketLeases)
	if b == nil {
		return errors.New("leases bucket missing")
	}
	return b.Delete([]byte(resource))
}

// holderQuotaCap returns the MaxConcurrent for holder, or 0 (unlimited) if the
// holder is not registered or has no cap.
func holderQuotaCap(tx *bbolt.Tx, holder string) int {
	h, ok, err := readHolder(tx, holder)
	if err != nil || !ok {
		return 0
	}
	return h.MaxConcurrent
}

// countActiveByHolder scans the leases bucket and counts active leases held by
// holder at now.
func countActiveByHolder(tx *bbolt.Tx, holder string, now time.Time) (int, error) {
	b := tx.Bucket(bucketLeases)
	if b == nil {
		return 0, errors.New("leases bucket missing")
	}
	var n int
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var p persistedLease
		if err := json.Unmarshal(v, &p); err != nil {
			return 0, fmt.Errorf("decode lease %q: %w", string(k), err)
		}
		if p.Holder == holder && fromPersisted(p).Active(now) {
			n++
		}
	}
	return n, nil
}

// enforceQuota returns ErrQuotaExceeded if the holder is capped and already at
// the limit (counting the about-to-be-acquired lease via add).
func enforceQuota(tx *bbolt.Tx, holder string, now time.Time, add int) error {
	cap := holderQuotaCap(tx, holder)
	if cap <= 0 {
		return nil
	}
	n, err := countActiveByHolder(tx, holder, now)
	if err != nil {
		return err
	}
	if n+add > cap {
		return lease.ErrQuotaExceeded
	}
	return nil
}

// promoteWaiterFor grants the resource's oldest pending waiter a fresh lease.
// It returns the granted token (0 if no waiter was promoted). Called inside
// the releasing transaction so the promotion commits atomically with the
// release/reap.
func promoteWaiterFor(tx *bbolt.Tx, resource string, now time.Time) (lease.Token, error) {
	w, ok, err := oldestPendingWaiter(tx, resource)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	if err := enforceQuota(tx, w.Holder, now, 1); err != nil {
		if errors.Is(err, lease.ErrQuotaExceeded) {
			return 0, nil
		}
		return 0, err
	}
	token, err := allocToken(tx)
	if err != nil {
		return 0, err
	}
	l := lease.Lease{
		Resource: resource, Holder: w.Holder, Token: token,
		Deadline: now.Add(time.Duration(w.TTLSeconds) * time.Second),
		Acquired: now, TTLSeconds: w.TTLSeconds,
	}
	if err := putLease(tx, l); err != nil {
		return 0, err
	}
	if err := setWaiterStatus(tx, w.ID, lease.WaiterGranted, token); err != nil {
		return 0, err
	}
	if err := appendAudit(tx, now, lease.ActionWaiterGrant, resource, w.Holder, token, "promoted waiter "+w.ID); err != nil {
		return 0, err
	}
	return token, nil
}

// Acquire grants a fresh lease atomically.
func (s *Store) Acquire(resource, holder string, now time.Time, ttl time.Duration) (lease.Lease, error) {
	var granted lease.Lease
	err := s.db.Update(func(tx *bbolt.Tx) error {
		existing, exists, err := readLease(tx, resource)
		if err != nil {
			return err
		}
		if exists && existing.Active(now) {
			return lease.ErrHeld
		}
		if err := enforceQuota(tx, holder, now, 1); err != nil {
			return err
		}
		token, err := allocToken(tx)
		if err != nil {
			return err
		}
		granted = lease.Lease{
			Resource: resource, Holder: holder, Token: token,
			Deadline: now.Add(ttl), Acquired: now, TTLSeconds: int64(ttl / time.Second),
		}
		if err := putLease(tx, granted); err != nil {
			return err
		}
		return appendAudit(tx, now, lease.ActionAcquire, resource, holder, token, "")
	})
	if err != nil {
		return lease.Lease{}, err
	}
	return granted, nil
}

// Renew refreshes the caller's lease deadline.
func (s *Store) Renew(resource, holder string, token lease.Token, now time.Time, ttl time.Duration) (lease.Lease, error) {
	var updated lease.Lease
	err := s.db.Update(func(tx *bbolt.Tx) error {
		current, exists, err := readLease(tx, resource)
		if err != nil {
			return err
		}
		if !exists {
			return lease.ErrNotHeld
		}
		if !current.Active(now) {
			return lease.ErrExpired
		}
		if current.Holder != holder || current.Token != token {
			return lease.ErrNotHolder
		}
		updated = current
		updated.Deadline = now.Add(ttl)
		if err := putLease(tx, updated); err != nil {
			return err
		}
		return appendAudit(tx, now, lease.ActionRenew, resource, holder, token, "")
	})
	if err != nil {
		return lease.Lease{}, err
	}
	return updated, nil
}

// Release drops the caller's active lease and promotes the oldest waiter.
func (s *Store) Release(resource, holder string, token lease.Token, now time.Time) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		current, exists, err := readLease(tx, resource)
		if err != nil {
			return err
		}
		if !exists {
			return lease.ErrNotHeld
		}
		if current.Holder != holder || current.Token != token {
			return lease.ErrNotHolder
		}
		if !current.Active(now) {
			return lease.ErrExpired
		}
		if err := deleteLease(tx, resource); err != nil {
			return err
		}
		if err := appendAudit(tx, now, lease.ActionRelease, resource, holder, token, ""); err != nil {
			return err
		}
		_, err = promoteWaiterFor(tx, resource, now)
		return err
	})
}

// Transfer hands the lease to newHolder, keeping token and deadline.
func (s *Store) Transfer(resource, holder string, token lease.Token, newHolder string, now time.Time) (lease.Lease, error) {
	var updated lease.Lease
	err := s.db.Update(func(tx *bbolt.Tx) error {
		current, exists, err := readLease(tx, resource)
		if err != nil {
			return err
		}
		if !exists {
			return lease.ErrNotHeld
		}
		if current.Holder != holder || current.Token != token {
			return lease.ErrNotHolder
		}
		if !current.Active(now) {
			return lease.ErrExpired
		}
		// The new holder gains one active lease; enforce its quota. The old
		// holder loses one, so only the net-new (1) is checked.
		if err := enforceQuota(tx, newHolder, now, 1); err != nil {
			return err
		}
		updated = current
		updated.Holder = newHolder
		if err := putLease(tx, updated); err != nil {
			return err
		}
		return appendAudit(tx, now, lease.ActionTransfer, resource, newHolder, token, "from "+holder)
	})
	if err != nil {
		return lease.Lease{}, err
	}
	return updated, nil
}

// BulkAcquire acquires leases on every resource atomically (all-or-nothing).
func (s *Store) BulkAcquire(holder string, resources []string, now time.Time, ttl time.Duration) ([]lease.Lease, error) {
	granted := make([]lease.Lease, 0, len(resources))
	err := s.db.Update(func(tx *bbolt.Tx) error {
		// Pre-check: none held active, and holder quota covers the whole batch.
		for _, r := range resources {
			existing, exists, err := readLease(tx, r)
			if err != nil {
				return err
			}
			if exists && existing.Active(now) {
				return lease.ErrConflict
			}
		}
		if err := enforceQuota(tx, holder, now, len(resources)); err != nil {
			return err
		}
		for _, r := range resources {
			token, err := allocToken(tx)
			if err != nil {
				return err
			}
			l := lease.Lease{
				Resource: r, Holder: holder, Token: token,
				Deadline: now.Add(ttl), Acquired: now, TTLSeconds: int64(ttl / time.Second),
			}
			if err := putLease(tx, l); err != nil {
				return err
			}
			if err := appendAudit(tx, now, lease.ActionBulkAcquire, r, holder, token, ""); err != nil {
				return err
			}
			granted = append(granted, l)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return granted, nil
}

// BulkRelease releases multiple leases atomically (all-or-nothing), promoting
// waiters for each freed resource.
func (s *Store) BulkRelease(holder string, entries []lease.ReleaseEntry, now time.Time) (int, error) {
	var released int
	err := s.db.Update(func(tx *bbolt.Tx) error {
		// Pre-check all entries match and are active.
		for _, e := range entries {
			current, exists, err := readLease(tx, e.Resource)
			if err != nil {
				return err
			}
			if !exists {
				return lease.ErrNotHeld
			}
			if current.Holder != holder || current.Token != e.Token {
				return lease.ErrNotHolder
			}
			if !current.Active(now) {
				return lease.ErrExpired
			}
		}
		for _, e := range entries {
			if err := deleteLease(tx, e.Resource); err != nil {
				return err
			}
			if err := appendAudit(tx, now, lease.ActionBulkRelease, e.Resource, holder, e.Token, ""); err != nil {
				return err
			}
			if _, err := promoteWaiterFor(tx, e.Resource, now); err != nil {
				return err
			}
			released++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return released, nil
}

// Inspect returns the stored lease and its liveness at now.
func (s *Store) Inspect(resource string, now time.Time) (lease.Lease, bool, bool) {
	var l lease.Lease
	var active, ok bool
	_ = s.db.View(func(tx *bbolt.Tx) error {
		got, gotOK, err := readLease(tx, resource)
		if err != nil || !gotOK {
			return nil
		}
		l, ok, active = got, true, got.Active(now)
		return nil
	})
	return l, active, ok
}

// List returns every lease with its active flag, sorted by resource.
func (s *Store) List(now time.Time) ([]lease.LeaseView, error) {
	var views []lease.LeaseView
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketLeases)
		if b == nil {
			return errors.New("leases bucket missing")
		}
		c := b.Cursor()
		var collected []lease.LeaseView
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedLease
			if err := json.Unmarshal(v, &p); err != nil {
				return fmt.Errorf("decode lease %q: %w", string(k), err)
			}
			l := fromPersisted(p)
			collected = append(collected, lease.LeaseView{Lease: l, Active: l.Active(now)})
		}
		sort.Slice(collected, func(i, j int) bool {
			return collected[i].Lease.Resource < collected[j].Lease.Resource
		})
		views = collected
		return nil
	})
	if err != nil {
		return nil, err
	}
	return views, nil
}

// ExpireAll reaps every lease whose deadline is at or before now, promoting
// waiters for each reaped resource.
func (s *Store) ExpireAll(now time.Time) (int, error) {
	var reaped int
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketLeases)
		if b == nil {
			return errors.New("leases bucket missing")
		}
		var reapedResources []string
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedLease
			if err := json.Unmarshal(v, &p); err != nil {
				return fmt.Errorf("decode lease %q: %w", string(k), err)
			}
			l := fromPersisted(p)
			if !l.Active(now) {
				key := make([]byte, len(k))
				copy(key, k)
				if err := appendAudit(tx, now, lease.ActionExpire, l.Resource, l.Holder, l.Token, "reaped"); err != nil {
					return err
				}
				if err := b.Delete(key); err != nil {
					return err
				}
				reapedResources = append(reapedResources, l.Resource)
			}
		}
		for _, r := range reapedResources {
			if _, err := promoteWaiterFor(tx, r, now); err != nil {
				return err
			}
		}
		reaped = len(reapedResources)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return reaped, nil
}

// keep binary referenced (used in seq keys elsewhere).
var _ = binary.BigEndian
