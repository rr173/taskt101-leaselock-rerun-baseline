package store

import (
	"encoding/json"
	"errors"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// WaiterSummary counts every persisted queue record, including completed
// records, so operators can distinguish current pressure from queue history.
func (s *Store) WaiterSummary() (lease.WaiterSummary, error) {
	var out lease.WaiterSummary
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketWaiters)
		if b == nil {
			return errors.New("waiters bucket missing")
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedWaiter
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			out.Total++
			switch p.Status {
			case lease.WaiterPending:
				out.Pending++
			case lease.WaiterGranted:
				out.Granted++
			case lease.WaiterCancelled:
				out.Cancelled++
			}
		}
		return nil
	})
	return out, err
}
