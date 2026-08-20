package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// PutPolicy creates a policy. Fails if the name exists.
func (s *Store) PutPolicy(p lease.Policy) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPolicies)
		if b == nil {
			return errors.New("policies bucket missing")
		}
		if b.Get([]byte(p.Name)) != nil {
			return lease.ErrPolicyExists
		}
		raw, err := json.Marshal(p)
		if err != nil {
			return fmt.Errorf("encode policy: %w", err)
		}
		return b.Put([]byte(p.Name), raw)
	})
}

// GetPolicy returns a policy by name.
func (s *Store) GetPolicy(name string) (lease.Policy, bool, error) {
	var p lease.Policy
	var ok bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPolicies)
		if b == nil {
			return errors.New("policies bucket missing")
		}
		raw := b.Get([]byte(name))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			return fmt.Errorf("decode policy %q: %w", name, err)
		}
		ok = true
		return nil
	})
	return p, ok, err
}

// ListPolicies returns all policies sorted by name.
func (s *Store) ListPolicies() ([]lease.Policy, error) {
	var out []lease.Policy
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPolicies)
		if b == nil {
			return errors.New("policies bucket missing")
		}
		var collected []lease.Policy
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p lease.Policy
			if err := json.Unmarshal(v, &p); err != nil {
				return fmt.Errorf("decode policy %q: %w", string(k), err)
			}
			collected = append(collected, p)
		}
		sort.Slice(collected, func(i, j int) bool { return collected[i].Name < collected[j].Name })
		out = collected
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
