// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package tests contains high-level tests for LFDB ensuring robustness across edge cases.
package tests

import (
	"bytes"
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"testing"
)

// Constants used in snapshot mutation tests.
const (
	// testCapacity allocates extra capacity so appends reuse the original backing array.
	testCapacity = 16
	// addedByte is appended to mutate the original slice in place.
	addedByte = byte('2')
	// valueKeyByte is the key used for value-mutation tests.
	valueKeyByte = byte('k')
	// initialValueByte is the initial stored value for mutation tests.
	initialValueByte = byte('v')
	// initialKeyByte seeds the key slice with a deterministic value for key-mutation tests.
	initialKeyByte = byte('a')
	// mutatedKeyByte is written to the key slice to verify immutability.
	mutatedKeyByte = byte('b')
)

// TestSnapshotIsolationAfterValueMutation verifies that modifying the original value
// slice after storing it does not alter data observed by existing snapshots.
func TestSnapshotIsolationAfterValueMutation(t *testing.T) {
	ctx := context.Background()
	db := core.New[[]byte, []byte]()
	defer db.Close(ctx)

	key := []byte{valueKeyByte}
	val := make([]byte, 0, testCapacity)
	val = append(val, initialValueByte)
	db.Put(ctx, key, val)

	snap := db.Snapshot(ctx)
	defer snap.Close(ctx)

	val = append(val, addedByte)
	db.Put(ctx, key, val)

	got, _ := snap.Get(ctx, key)
	if !bytes.Equal(got, []byte{initialValueByte}) {
		t.Fatalf("snapshot saw mutated value: %q", got)
	}
}

// TestSnapshotIsolationAfterKeyMutation ensures that mutating the original key slice
// after insertion does not affect lookups using the pre-mutation key.
func TestSnapshotIsolationAfterKeyMutation(t *testing.T) {
	ctx := context.Background()
	db := core.New[[]byte, []byte]()
	defer db.Close(ctx)

	key := []byte{initialKeyByte}
	val := []byte{initialValueByte}
	db.Put(ctx, key, val)

	snap := db.Snapshot(ctx)
	defer snap.Close(ctx)

	key[0] = mutatedKeyByte
	got, ok := snap.Get(ctx, []byte{initialKeyByte})
	if !ok || !bytes.Equal(got, val) {
		t.Fatalf("snapshot failed to retrieve value with original key; got %q, exists %t", got, ok)
	}
}
