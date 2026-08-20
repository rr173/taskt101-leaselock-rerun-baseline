package store

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// HolderUsage computes quota pressure from both active leases and pending
// waiters in one read transaction.
func (s *Store) HolderUsage(now time.Time) ([]lease.HolderUsage, error) {
	counts := map[string]*lease.HolderUsage{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		lb := tx.Bucket(bucketLeases)
		wb := tx.Bucket(bucketWaiters)
		if lb == nil || wb == nil {
			return errors.New("lease or waiter bucket missing")
		}
		for _, bucket := range []*bbolt.Bucket{lb} {
			c := bucket.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var p persistedLease
				if err := json.Unmarshal(v, &p); err != nil {
					return err
				}
				if !fromPersisted(p).Active(now) {
					continue
				}
				item := counts[p.Holder]
				if item == nil {
					item = &lease.HolderUsage{HolderID: p.Holder}
					counts[p.Holder] = item
				}
				item.ActiveLeases++
			}
		}
		c := wb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedWaiter
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			if p.Status != lease.WaiterPending {
				continue
			}
			item := counts[p.Holder]
			if item == nil {
				item = &lease.HolderUsage{HolderID: p.Holder}
				counts[p.Holder] = item
			}
			item.QueuedWaiters++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]lease.HolderUsage, 0, len(counts))
	for _, item := range counts {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].HolderID < out[j].HolderID })
	return out, nil
}
