package store

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// ResourceStates reads the complete lease bucket in one transaction. The
// endpoint uses this instead of composing Inspect calls that could observe
// different fencing generations.
func (s *Store) ResourceStates(now time.Time) ([]lease.ResourceState, error) {
	var out []lease.ResourceState
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketLeases)
		if b == nil {
			return errors.New("leases bucket missing")
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedLease
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			item := fromPersisted(p)
			out = append(out, lease.ResourceState{Resource: item.Resource, Holder: item.Holder, Token: item.Token, Active: item.Active(now), Deadline: item.Deadline})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Resource < out[j].Resource })
	return out, nil
}
