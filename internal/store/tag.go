package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"go.etcd.io/bbolt"

	"task101-leaselock/internal/lease"
)

// persistedTags holds the tag set for a resource.
type persistedTags []string

func readTags(tx *bbolt.Tx, resource string) ([]string, bool, error) {
	b := tx.Bucket(bucketTags)
	if b == nil {
		return nil, false, errors.New("tags bucket missing")
	}
	raw := b.Get([]byte(resource))
	if raw == nil {
		return nil, false, nil
	}
	var tags persistedTags
	if err := json.Unmarshal(raw, &tags); err != nil {
		return nil, false, fmt.Errorf("decode tags for %q: %w", resource, err)
	}
	return tags, true, nil
}

func putTags(tx *bbolt.Tx, resource string, tags []string) error {
	b := tx.Bucket(bucketTags)
	if b == nil {
		return errors.New("tags bucket missing")
	}
	raw, err := json.Marshal(tags)
	if err != nil {
		return fmt.Errorf("encode tags: %w", err)
	}
	return b.Put([]byte(resource), raw)
}

func readTagIndex(tx *bbolt.Tx, tag string) ([]string, bool, error) {
	b := tx.Bucket(bucketTagIdx)
	if b == nil {
		return nil, false, errors.New("tag_idx bucket missing")
	}
	raw := b.Get([]byte(tag))
	if raw == nil {
		return nil, false, nil
	}
	var resources []string
	if err := json.Unmarshal(raw, &resources); err != nil {
		return nil, false, fmt.Errorf("decode tag index %q: %w", tag, err)
	}
	return resources, true, nil
}

func putTagIndex(tx *bbolt.Tx, tag string, resources []string) error {
	b := tx.Bucket(bucketTagIdx)
	if b == nil {
		return errors.New("tag_idx bucket missing")
	}
	raw, err := json.Marshal(resources)
	if err != nil {
		return fmt.Errorf("encode tag index: %w", err)
	}
	return b.Put([]byte(tag), raw)
}

// AddTag adds a tag to a resource (idempotent).
func (s *Store) AddTag(resource, tag string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		tags, _, err := readTags(tx, resource)
		if err != nil {
			return err
		}
		alreadyPresent := false
		for _, t := range tags {
			if t == tag {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			tags = append(tags, tag)
			sort.Strings(tags)
			if err := putTags(tx, resource, tags); err != nil {
				return err
			}
		}
		// Update the tag->resources index.
		resources, _, err := readTagIndex(tx, tag)
		if err != nil {
			return err
		}
		for _, r := range resources {
			if r == resource {
				return nil
			}
		}
		resources = append(resources, resource)
		sort.Strings(resources)
		return putTagIndex(tx, tag, resources)
	})
}

// RemoveTag removes a tag from a resource.
func (s *Store) RemoveTag(resource, tag string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		tags, ok, err := readTags(tx, resource)
		if err != nil {
			return err
		}
		if !ok {
			return lease.ErrTagNotFound
		}
		idx := -1
		for i, t := range tags {
			if t == tag {
				idx = i
				break
			}
		}
		if idx < 0 {
			return lease.ErrTagNotFound
		}
		updated := append(tags[:idx], tags[idx+1:]...)
		if err := putTags(tx, resource, updated); err != nil {
			return err
		}
		// Update the tag->resources index.
		resources, _, err := readTagIndex(tx, tag)
		if err != nil {
			return err
		}
		for i, r := range resources {
			if r == resource {
				resources = append(resources[:i], resources[i+1:]...)
				break
			}
		}
		return putTagIndex(tx, tag, resources)
	})
}

// ListResources returns resources carrying tag (or all tagged resources if tag
// is empty), sorted ascending.
func (s *Store) ListResources(tag string) ([]string, error) {
	var out []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		if tag != "" {
			resources, ok, err := readTagIndex(tx, tag)
			if err != nil {
				return err
			}
			if ok {
				out = append(out, resources...)
			}
			return nil
		}
		// No tag filter: union of all tag index values.
		b := tx.Bucket(bucketTagIdx)
		if b == nil {
			return errors.New("tag_idx bucket missing")
		}
		seen := map[string]bool{}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var resources []string
			if err := json.Unmarshal(v, &resources); err != nil {
				return fmt.Errorf("decode tag index %q: %w", string(k), err)
			}
			for _, r := range resources {
				if !seen[r] {
					seen[r] = true
					out = append(out, r)
				}
			}
		}
		sort.Strings(out)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if tag != "" {
		sort.Strings(out)
		return out, nil
	}
	if out == nil {
		out = []string{}
	}
	return out, nil
}

// GetTags returns the tags on a resource.
func (s *Store) GetTags(resource string) ([]string, error) {
	var out []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		tags, ok, err := readTags(tx, resource)
		if err != nil {
			return err
		}
		if ok {
			out = tags
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = []string{}
	}
	return out, nil
}
