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

// persistedAuditEntry is the on-disk shape of an audit entry. The seq is the
// key (big-endian) so bbolt's sorted iteration is ascending.
type persistedAuditEntry struct {
	Seq      int64             `json:"seq"`
	TimeNano int64             `json:"time_nano"`
	Action   lease.AuditAction `json:"action"`
	Resource string            `json:"resource,omitempty"`
	Holder   string            `json:"holder,omitempty"`
	Token    int64             `json:"token,omitempty"`
	Detail   string            `json:"detail,omitempty"`
}

func fromPersistedAudit(p persistedAuditEntry) lease.AuditEntry {
	return lease.AuditEntry{
		Seq: p.Seq, Time: time.Unix(0, p.TimeNano), Action: p.Action,
		Resource: p.Resource, Holder: p.Holder, Token: lease.Token(p.Token), Detail: p.Detail,
	}
}

// ListAudit returns audit entries ascending by seq, optionally filtered by
// resource, with limit and offset.
func (s *Store) ListAudit(resource string, limit, offset int) ([]lease.AuditEntry, error) {
	var out []lease.AuditEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketAudit)
		if b == nil {
			return errors.New("audit bucket missing")
		}
		var collected []lease.AuditEntry
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedAuditEntry
			if err := json.Unmarshal(v, &p); err != nil {
				return fmt.Errorf("decode audit %q: %w", string(k), err)
			}
			if resource != "" && p.Resource != resource {
				continue
			}
			collected = append(collected, fromPersistedAudit(p))
		}
		// Entries are already ascending by seq (key order); apply offset+limit.
		if offset >= len(collected) {
			out = []lease.AuditEntry{}
			return nil
		}
		collected = collected[offset:]
		if limit > 0 && limit < len(collected) {
			collected = collected[:limit]
		}
		out = collected
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TokenHistory returns the token-issuance events (acquire, renew, bulk_acquire,
// waiter_grant, transfer) for a resource, ascending by seq.
func (s *Store) TokenHistory(resource string) ([]lease.AuditEntry, error) {
	var out []lease.AuditEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketAudit)
		if b == nil {
			return errors.New("audit bucket missing")
		}
		issuance := map[lease.AuditAction]bool{
			lease.ActionAcquire:     true,
			lease.ActionRenew:       true,
			lease.ActionBulkAcquire: true,
			lease.ActionWaiterGrant: true,
			lease.ActionTransfer:    true,
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p persistedAuditEntry
			if err := json.Unmarshal(v, &p); err != nil {
				return fmt.Errorf("decode audit %q: %w", string(k), err)
			}
			if resource != "" && p.Resource != resource {
				continue
			}
			if !issuance[p.Action] {
				continue
			}
			out = append(out, fromPersistedAudit(p))
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// keep binary referenced (used for audit seq keys).
var _ = binary.BigEndian
