package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// persistedHolder is the on-disk shape of a holder.
type persistedHolder struct {
	ID            string `json:"id"`
	Description   string `json:"description"`
	MaxConcurrent int    `json:"max_concurrent"`
	CreatedAt     int64  `json:"created_at_nano"`
}

func toPersistedHolder(h lease.Holder) persistedHolder {
	return persistedHolder{ID: h.ID, Description: h.Description, MaxConcurrent: h.MaxConcurrent, CreatedAt: h.CreatedAt.UnixNano()}
}

func fromPersistedHolder(p persistedHolder) lease.Holder {
	return lease.Holder{ID: p.ID, Description: p.Description, MaxConcurrent: p.MaxConcurrent, CreatedAt: time.Unix(0, p.CreatedAt)}
}

func readHolder(tx *bbolt.Tx, id string) (lease.Holder, bool, error) {
	b := tx.Bucket(bucketHolders)
	if b == nil {
		return lease.Holder{}, false, errors.New("holders bucket missing")
	}
	raw := b.Get([]byte(id))
	if raw == nil {
		return lease.Holder{}, false, nil
	}
	var p persistedHolder
	if err := json.Unmarshal(raw, &p); err != nil {
		return lease.Holder{}, false, fmt.Errorf("decode holder %q: %w", id, err)
	}
	return fromPersistedHolder(p), true, nil
}

// PutHolder registers a holder. It returns ErrHolderExists if a holder with the
// same id already exists (registration is create-only; use SetQuota to change
// the cap).
func (s *Store) PutHolder(h lease.Holder) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketHolders)
		if b == nil {
			return errors.New("holders bucket missing")
		}
		if b.Get([]byte(h.ID)) != nil {
			return lease.ErrHolderExists
		}
		raw, err := json.Marshal(toPersistedHolder(h))
		if err != nil {
			return fmt.Errorf("encode holder: %w", err)
		}
		return b.Put([]byte(h.ID), raw)
	})
}

// GetHolder returns a holder.
func (s *Store) GetHolder(id string) (lease.Holder, bool, error) {
	var h lease.Holder
	var ok bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		got, gotOK, err := readHolder(tx, id)
		if err != nil {
			return err
		}
		h, ok = got, gotOK
		return nil
	})
	return h, ok, err
}

// DeleteHolder removes a holder that holds no active leases.
func (s *Store) DeleteHolder(id string, now time.Time) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketHolders)
		if b == nil {
			return errors.New("holders bucket missing")
		}
		if b.Get([]byte(id)) == nil {
			return lease.ErrHolderNotFound
		}
		n, err := countActiveByHolder(tx, id, now)
		if err != nil {
			return err
		}
		if n > 0 {
			return lease.ErrHolderHasLeases
		}
		pending, err := countPendingWaitersByHolder(tx, id)
		if err != nil {
			return err
		}
		if pending > 0 {
			return lease.ErrHolderHasWaiters
		}
		return b.Delete([]byte(id))
	})
}

// ListHolders returns all registered holders sorted by id.
func (s *Store) ListHolders() ([]lease.Holder, error) {
	var out []lease.Holder
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketHolders)
		if b == nil {
			return errors.New("holders bucket missing")
		}
		c := b.Cursor()
		var collected []lease.Holder
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedHolder
			if err := json.Unmarshal(v, &p); err != nil {
				return fmt.Errorf("decode holder %q: %w", string(k), err)
			}
			collected = append(collected, fromPersistedHolder(p))
		}
		sort.Slice(collected, func(i, j int) bool { return collected[i].ID < collected[j].ID })
		out = collected
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SetQuota updates a holder's MaxConcurrent.
func (s *Store) SetQuota(id string, max int) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		h, ok, err := readHolder(tx, id)
		if err != nil {
			return err
		}
		if !ok {
			return lease.ErrHolderNotFound
		}
		h.MaxConcurrent = max
		raw, err := json.Marshal(toPersistedHolder(h))
		if err != nil {
			return fmt.Errorf("encode holder: %w", err)
		}
		return tx.Bucket(bucketHolders).Put([]byte(id), raw)
	})
}

// GetQuota returns a holder's quota.
func (s *Store) GetQuota(id string) (lease.Quota, bool, error) {
	h, ok, err := s.GetHolder(id)
	if err != nil || !ok {
		return lease.Quota{}, false, err
	}
	return lease.Quota{HolderID: h.ID, MaxConcurrent: h.MaxConcurrent}, true, nil
}
