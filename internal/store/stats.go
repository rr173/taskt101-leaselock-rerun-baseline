package store

import (
	"encoding/json"
	"errors"
	"time"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// Stats returns the aggregate snapshot computed at now.
func (s *Store) Stats(now time.Time) (lease.Stats, error) {
	var st lease.Stats
	err := s.db.View(func(tx *bbolt.Tx) error {
		lb := tx.Bucket(bucketLeases)
		if lb == nil {
			return errors.New("leases bucket missing")
		}
		c := lb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedLease
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			st.TotalLeases++
			if fromPersisted(p).Active(now) {
				st.ActiveLeases++
			} else {
				st.ExpiredLeases++
			}
		}
		st.NextFencingToken = readNextToken(tx)

		hb := tx.Bucket(bucketHolders)
		if hb == nil {
			return errors.New("holders bucket missing")
		}
		hc := hb.Cursor()
		for k, _ := hc.First(); k != nil; k, _ = hc.Next() {
			st.RegisteredHolders++
		}

		wb := tx.Bucket(bucketWaiters)
		if wb == nil {
			return errors.New("waiters bucket missing")
		}
		wc := wb.Cursor()
		for k, v := wc.First(); k != nil; k, v = wc.Next() {
			var p persistedWaiter
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			st.TotalWaiters++
			if p.Status == lease.WaiterPending {
				st.PendingWaiters++
			}
		}

		pb := tx.Bucket(bucketPolicies)
		if pb == nil {
			return errors.New("policies bucket missing")
		}
		pc := pb.Cursor()
		for k, _ := pc.First(); k != nil; k, _ = pc.Next() {
			st.PoliciesCount++
		}

		st.AuditEntries = auditCount(tx)
		return nil
	})
	if err != nil {
		return lease.Stats{}, err
	}
	return st, nil
}
