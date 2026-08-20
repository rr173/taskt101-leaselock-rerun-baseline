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

// persistedWaiter is the on-disk shape of a waiter.
type persistedWaiter struct {
	ID           string             `json:"id"`
	Resource     string             `json:"resource"`
	Holder       string             `json:"holder"`
	TTLSeconds   int64              `json:"ttl_seconds"`
	CreatedAt    int64              `json:"created_at_nano"`
	Status       lease.WaiterStatus `json:"status"`
	GrantedToken int64              `json:"granted_token,omitempty"`
}

// newWaiterID produces a deterministic-ish id from the waiter sequence. It is
// unique because the sequence is monotonic.
func newWaiterID(seq int64) string {
	return fmt.Sprintf("w-%d", seq)
}

func waiterKey(id string) []byte { return []byte(id) }

func nextWaiterSeq(tx *bbolt.Tx) (int64, error) {
	b := tx.Bucket(bucketWaitSeq)
	if b == nil {
		return 0, errors.New("waiter_seq bucket missing")
	}
	var seq int64 = 1
	if raw := b.Get([]byte("n")); len(raw) == 8 {
		seq = int64(binary.BigEndian.Uint64(raw)) + 1
	}
	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, uint64(seq))
	if err := b.Put([]byte("n"), seqBytes); err != nil {
		return 0, fmt.Errorf("write waiter seq: %w", err)
	}
	return seq, nil
}

func putWaiter(tx *bbolt.Tx, w lease.Waiter) error {
	b := tx.Bucket(bucketWaiters)
	if b == nil {
		return errors.New("waiters bucket missing")
	}
	p := persistedWaiter{
		ID: w.ID, Resource: w.Resource, Holder: w.Holder, TTLSeconds: w.TTLSeconds,
		CreatedAt: w.CreatedAt.UnixNano(), Status: w.Status, GrantedToken: int64(w.GrantedToken),
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode waiter: %w", err)
	}
	return b.Put(waiterKey(w.ID), raw)
}

func readWaiter(tx *bbolt.Tx, id string) (lease.Waiter, bool, error) {
	b := tx.Bucket(bucketWaiters)
	if b == nil {
		return lease.Waiter{}, false, errors.New("waiters bucket missing")
	}
	raw := b.Get(waiterKey(id))
	if raw == nil {
		return lease.Waiter{}, false, nil
	}
	var p persistedWaiter
	if err := json.Unmarshal(raw, &p); err != nil {
		return lease.Waiter{}, false, fmt.Errorf("decode waiter %q: %w", id, err)
	}
	return lease.Waiter{
		ID: p.ID, Resource: p.Resource, Holder: p.Holder, TTLSeconds: p.TTLSeconds,
		CreatedAt: time.Unix(0, p.CreatedAt), Status: p.Status, GrantedToken: lease.Token(p.GrantedToken),
	}, true, nil
}

// oldestPendingWaiter scans waiters in creation (seq) order and returns the
// oldest one still pending for resource.
func oldestPendingWaiter(tx *bbolt.Tx, resource string) (lease.Waiter, bool, error) {
	b := tx.Bucket(bucketWaiters)
	if b == nil {
		return lease.Waiter{}, false, errors.New("waiters bucket missing")
	}
	var found lease.Waiter
	var ok bool
	// Waiter ids are "w-<seq>"; bbolt sorts keys lexicographically, which does
	// NOT preserve numeric order across differing widths. Instead, iterate and
	// pick the minimum seq among pending waiters for the resource.
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var p persistedWaiter
		if err := json.Unmarshal(v, &p); err != nil {
			return lease.Waiter{}, false, fmt.Errorf("decode waiter %q: %w", string(k), err)
		}
		if p.Resource != resource || p.Status != lease.WaiterPending {
			continue
		}
		w := lease.Waiter{
			ID: p.ID, Resource: p.Resource, Holder: p.Holder, TTLSeconds: p.TTLSeconds,
			CreatedAt: time.Unix(0, p.CreatedAt), Status: p.Status, GrantedToken: lease.Token(p.GrantedToken),
		}
		if !ok || w.CreatedAt.Before(found.CreatedAt) {
			found = w
			ok = true
		}
	}
	return found, ok, nil
}

// setWaiterStatus updates a waiter's status and granted token.
func setWaiterStatus(tx *bbolt.Tx, id string, status lease.WaiterStatus, token lease.Token) error {
	w, ok, err := readWaiter(tx, id)
	if err != nil || !ok {
		return err
	}
	w.Status = status
	w.GrantedToken = token
	return putWaiter(tx, w)
}

func countPendingWaitersByHolder(tx *bbolt.Tx, holder string) (int, error) {
	b := tx.Bucket(bucketWaiters)
	if b == nil {
		return 0, errors.New("waiters bucket missing")
	}
	var count int
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var p persistedWaiter
		if err := json.Unmarshal(v, &p); err != nil {
			return 0, fmt.Errorf("decode waiter %q: %w", string(k), err)
		}
		if p.Holder == holder && p.Status == lease.WaiterPending {
			count++
		}
	}
	return count, nil
}

// EnqueueWaiter grants a lease immediately if the resource is free; otherwise
// queues a pending waiter.
func (s *Store) EnqueueWaiter(resource, holder string, ttl time.Duration, now time.Time) (lease.Waiter, error) {
	var w lease.Waiter
	err := s.db.Update(func(tx *bbolt.Tx) error {
		existing, exists, err := readLease(tx, resource)
		if err != nil {
			return err
		}
		if exists && !existing.Active(now) {
			if token, err := promoteWaiterFor(tx, resource, now); err != nil {
				return err
			} else if token != 0 {
				seq, err := nextWaiterSeq(tx)
				if err != nil {
					return err
				}
				w = lease.Waiter{
					ID: newWaiterID(seq), Resource: resource, Holder: holder,
					TTLSeconds: int64(ttl / time.Second), CreatedAt: now,
					Status: lease.WaiterPending,
				}
				return putWaiter(tx, w)
			}
		}
		if !exists || !existing.Active(now) {
			// Resource is free: grant a lease now and return a granted waiter.
			if err := enforceQuota(tx, holder, now, 1); err != nil {
				return err
			}
			token, err := allocToken(tx)
			if err != nil {
				return err
			}
			l := lease.Lease{
				Resource: resource, Holder: holder, Token: token,
				Deadline: now.Add(ttl), Acquired: now, TTLSeconds: int64(ttl / time.Second),
			}
			if err := putLease(tx, l); err != nil {
				return err
			}
			if err := appendAudit(tx, now, lease.ActionAcquire, resource, holder, token, "waiter immediate"); err != nil {
				return err
			}
			seq, err := nextWaiterSeq(tx)
			if err != nil {
				return err
			}
			w = lease.Waiter{
				ID: newWaiterID(seq), Resource: resource, Holder: holder, TTLSeconds: int64(ttl / time.Second),
				CreatedAt: now, Status: lease.WaiterGranted, GrantedToken: token,
			}
			return putWaiter(tx, w)
		}
		// Resource held: queue a pending waiter.
		seq, err := nextWaiterSeq(tx)
		if err != nil {
			return err
		}
		w = lease.Waiter{
			ID: newWaiterID(seq), Resource: resource, Holder: holder, TTLSeconds: int64(ttl / time.Second),
			CreatedAt: now, Status: lease.WaiterPending,
		}
		return putWaiter(tx, w)
	})
	if err != nil {
		return lease.Waiter{}, err
	}
	return w, nil
}

// CancelWaiter cancels a pending waiter.
func (s *Store) CancelWaiter(id string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		w, ok, err := readWaiter(tx, id)
		if err != nil || !ok {
			return lease.ErrWaiterNotFound
		}
		if w.Status != lease.WaiterPending {
			return lease.ErrWaiterNotFound
		}
		w.Status = lease.WaiterCancelled
		return putWaiter(tx, w)
	})
}

// ListWaiters lists waiters, optionally filtered by resource and status.
func (s *Store) ListWaiters(resource string, status lease.WaiterStatus) ([]lease.Waiter, error) {
	var out []lease.Waiter
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketWaiters)
		if b == nil {
			return errors.New("waiters bucket missing")
		}
		var collected []lease.Waiter
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedWaiter
			if err := json.Unmarshal(v, &p); err != nil {
				return fmt.Errorf("decode waiter %q: %w", string(k), err)
			}
			if resource != "" && p.Resource != resource {
				continue
			}
			if status != "" && p.Status != status {
				continue
			}
			collected = append(collected, lease.Waiter{
				ID: p.ID, Resource: p.Resource, Holder: p.Holder, TTLSeconds: p.TTLSeconds,
				CreatedAt: time.Unix(0, p.CreatedAt), Status: p.Status, GrantedToken: lease.Token(p.GrantedToken),
			})
		}
		// Sort by CreatedAt then ID for stable ordering.
		sort.Slice(collected, func(i, j int) bool {
			if !collected[i].CreatedAt.Equal(collected[j].CreatedAt) {
				return collected[i].CreatedAt.Before(collected[j].CreatedAt)
			}
			return collected[i].ID < collected[j].ID
		})
		out = collected
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
