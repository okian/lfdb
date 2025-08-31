// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	core "github.com/kianostad/lfdb/internal/core"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTTLInTransaction(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When performing TTL operations within a transaction", func() {
			// Test TTL operations within a transaction
			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Put with TTL
				tx.PutWithTTL(ctx, []byte("key1"), "value1", 100*time.Millisecond)

				// Check TTL
				ttl, exists := tx.GetTTL(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))

				// Extend TTL
				extended := tx.ExtendTTL(ctx, []byte("key1"), 50*time.Millisecond)
				So(extended, ShouldBeTrue)

				// Check extended TTL
				ttl, exists = tx.GetTTL(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then TTL should be valid after transaction commit", func() {
				// After transaction commit, check that TTL is still valid
				ttl, exists := db.GetTTL(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))
			})

			Convey("Then value should expire after TTL duration", func() {
				// Wait for expiration
				time.Sleep(200 * time.Millisecond)

				// Should not be able to get the value after expiration
				_, exists := db.Get(ctx, []byte("key1"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestTTLRemovalInTransaction(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When removing TTL within a transaction", func() {
			// First put a key with TTL
			db.PutWithTTL(ctx, []byte("key2"), "value2", 50*time.Millisecond)

			// Test TTL removal within a transaction
			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Remove TTL
				removed := tx.RemoveTTL(ctx, []byte("key2"))
				So(removed, ShouldBeTrue)

				// Check that TTL is removed
				_, exists := tx.GetTTL(ctx, []byte("key2"))
				So(exists, ShouldBeFalse)

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then TTL should remain removed after transaction commit", func() {
				// After transaction commit, check that TTL is still removed
				_, exists := db.GetTTL(ctx, []byte("key2"))
				So(exists, ShouldBeFalse)
			})

			Convey("Then value should survive past original expiration", func() {
				// Wait past original expiration
				time.Sleep(100 * time.Millisecond)

				// Should still be able to get the value (no TTL)
				val, exists := db.Get(ctx, []byte("key2"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")
			})
		})
	})
}

func TestTTLWithExpiryInTransaction(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When using absolute expiry within a transaction", func() {
			// Test absolute expiry within a transaction
			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				expiresAt := time.Now().Add(100 * time.Millisecond)
				tx.PutWithExpiry(ctx, []byte("key3"), "value3", expiresAt)

				// Check TTL
				ttl, exists := tx.GetTTL(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then TTL should be valid after transaction commit", func() {
				// After transaction commit, check that TTL is still valid
				ttl, exists := db.GetTTL(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))
			})

			Convey("Then value should expire after expiry time", func() {
				// Wait for expiration
				time.Sleep(150 * time.Millisecond)

				// Should not be able to get the value after expiration
				_, exists := db.Get(ctx, []byte("key3"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestTTLTransactionAbort(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When aborting a transaction with TTL operations", func() {
			// Test that TTL operations are not applied when transaction aborts
			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Put with TTL
				tx.PutWithTTL(ctx, []byte("key4"), "value4", 100*time.Millisecond)

				// Extend TTL
				tx.ExtendTTL(ctx, []byte("key4"), 50*time.Millisecond)

				// Abort transaction by returning error
				return fmt.Errorf("intentional abort")
			})

			So(err, ShouldNotBeNil)

			Convey("Then key should not exist after transaction abort", func() {
				// After transaction abort, key should not exist
				_, exists := db.Get(ctx, []byte("key4"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestTTLTransactionReadYourWrites(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When performing TTL operations within a transaction", func() {
			// Test that TTL operations are visible within the same transaction
			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Put with TTL
				tx.PutWithTTL(ctx, []byte("key5"), "value5", 100*time.Millisecond)

				// Read the value within transaction
				val, exists := tx.Get(ctx, []byte("key5"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value5")

				// Check TTL within transaction
				ttl, exists := tx.GetTTL(ctx, []byte("key5"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))

				// Extend TTL
				extended := tx.ExtendTTL(ctx, []byte("key5"), 50*time.Millisecond)
				So(extended, ShouldBeTrue)

				// Check extended TTL within transaction
				ttl, exists = tx.GetTTL(ctx, []byte("key5"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))

				return nil
			})

			So(err, ShouldBeNil)
		})
	})
}
