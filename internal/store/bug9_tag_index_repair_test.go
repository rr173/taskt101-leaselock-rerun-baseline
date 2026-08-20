package store

import (
	"testing"

	"go.etcd.io/bbolt"
)

func TestIdempotentAddTagRepairsMissingReverseIndex(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	if err := s.AddTag("resource-a", "prod"); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketTagIdx).Delete([]byte("prod"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTag("resource-a", "prod"); err != nil {
		t.Fatal(err)
	}
	resources, err := s.ListResources("prod")
	if err != nil {
		t.Fatal(err)
	}
	if len(resources) != 1 || resources[0] != "resource-a" {
		t.Fatalf("repaired resources=%v", resources)
	}
}
