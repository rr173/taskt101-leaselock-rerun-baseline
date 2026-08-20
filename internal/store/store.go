package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// Bucket names.
var (
	bucketLeases  = []byte("leases")
	bucketMeta    = []byte("meta")
	bucketHolders = []byte("holders")
	bucketPolicies = []byte("policies")
	bucketWaiters = []byte("waiters")
	bucketWaitSeq = []byte("waiter_seq")
	bucketTags    = []byte("tags")    // key=resource -> JSON []string
	bucketTagIdx  = []byte("tag_idx")  // key=tag -> JSON []string
	bucketAudit   = []byte("audit")
	bucketAuditSeq = []byte("audit_seq")
	keyFencing    = []byte("fencing_next")
)

// allBuckets is the list of buckets created at open time.
var allBuckets = [][]byte{
	bucketLeases, bucketMeta, bucketHolders, bucketPolicies,
	bucketWaiters, bucketWaitSeq, bucketTags, bucketTagIdx, bucketAudit, bucketAuditSeq,
}

// Store is a bbolt-backed lease.Store. All mutating methods run their work in a
// single read-write transaction, including the audit append and any waiter
// promotion, so partial state never commits.
type Store struct {
	db *bbolt.DB
}

// Open opens (or creates) the bbolt file at path and ensures all buckets exist.
// It does not run a recovery sweep; callers should invoke ExpireAll via the
// Service's Recover at process start to clear leases that expired while down.
func Open(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open store %q: %w", path, err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, b := range allBuckets {
			if _, e := tx.CreateBucketIfNotExists(b); e != nil {
				return fmt.Errorf("create bucket %s: %w", string(b), e)
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the underlying file handle.
func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// NextToken reports the fencing token the next Acquire would allocate, without
// allocating it.
func (s *Store) NextToken() (lease.Token, error) {
	var tok lease.Token
	err := s.db.View(func(tx *bbolt.Tx) error {
		tok = readNextToken(tx)
		return nil
	})
	return tok, err
}

// readNextToken reads the persisted fencing counter (defaulting to 1).
func readNextToken(tx *bbolt.Tx) lease.Token {
	b := tx.Bucket(bucketMeta)
	if b == nil {
		return 1
	}
	if raw := b.Get(keyFencing); len(raw) == 8 {
		return lease.Token(int64(binary.BigEndian.Uint64(raw)))
	}
	return 1
}

// allocToken reads the counter, returns it, and persists counter+1.
func allocToken(tx *bbolt.Tx) (lease.Token, error) {
	b := tx.Bucket(bucketMeta)
	if b == nil {
		return 0, errors.New("meta bucket missing")
	}
	next := readNextToken(tx)
	written := make([]byte, 8)
	binary.BigEndian.PutUint64(written, uint64(int64(next)+1))
	if err := b.Put(keyFencing, written); err != nil {
		return 0, fmt.Errorf("write fencing counter: %w", err)
	}
	return next, nil
}

// appendAudit appends one entry to the audit log inside the given transaction.
func appendAudit(tx *bbolt.Tx, at time.Time, action lease.AuditAction, resource, holder string, token lease.Token, detail string) error {
	b := tx.Bucket(bucketAudit)
	if b == nil {
		return errors.New("audit bucket missing")
	}
	sb := tx.Bucket(bucketAuditSeq)
	if sb == nil {
		return errors.New("audit_seq bucket missing")
	}
	var seq int64 = 1
	if raw := sb.Get([]byte("n")); len(raw) == 8 {
		seq = int64(binary.BigEndian.Uint64(raw)) + 1
	}
	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, uint64(seq))
	if err := sb.Put([]byte("n"), seqBytes); err != nil {
		return fmt.Errorf("write audit seq: %w", err)
	}
	e := lease.AuditEntry{Seq: seq, Time: at, Action: action, Resource: resource, Holder: holder, Token: token, Detail: detail}
	raw, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode audit: %w", err)
	}
	// Key by 8-byte big-endian seq so bbolt's sorted iteration is ascending.
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(seq))
	return b.Put(key, raw)
}

// auditCount returns the number of audit entries.
func auditCount(tx *bbolt.Tx) int64 {
	sb := tx.Bucket(bucketAuditSeq)
	if sb == nil {
		return 0
	}
	if raw := sb.Get([]byte("n")); len(raw) == 8 {
		return int64(binary.BigEndian.Uint64(raw))
	}
	return 0
}
