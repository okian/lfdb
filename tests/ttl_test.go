// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTTLBasic(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When putting a value with TTL", func() {
			// Test PutWithTTL
			db.PutWithTTL(ctx, []byte("key1"), "value1", 100*time.Millisecond)

			Convey("Then should be able to get the value immediately", func() {
				val, exists := db.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")
			})

			Convey("Then should have a positive TTL", func() {
				ttl, exists := db.GetTTL(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))
			})

			Convey("Then should expire after TTL duration", func() {
				// Wait for expiration
				time.Sleep(150 * time.Millisecond)

				// Should not be able to get the value after expiration
				_, exists := db.Get(ctx, []byte("key1"))
				So(exists, ShouldBeFalse)

				// TTL should be 0 after expiration
				_, exists = db.GetTTL(ctx, []byte("key1"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestTTLWithExpiry(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When putting a value with expiry time", func() {
			// Test PutWithExpiry
			expiresAt := time.Now().Add(100 * time.Millisecond)
			db.PutWithExpiry(ctx, []byte("key2"), "value2", expiresAt)

			Convey("Then should be able to get the value immediately", func() {
				val, exists := db.Get(ctx, []byte("key2"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")
			})

			Convey("Then should expire after expiry time", func() {
				// Wait for expiration
				time.Sleep(150 * time.Millisecond)

				// Should not be able to get the value after expiration
				_, exists := db.Get(ctx, []byte("key2"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestTTLExtension(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When extending TTL", func() {
			// Put with short TTL
			db.PutWithTTL(ctx, []byte("key3"), "value3", 50*time.Millisecond)

			Convey("Then should have initial TTL", func() {
				ttl, exists := db.GetTTL(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))
			})

			Convey("Then should be able to extend TTL", func() {
				extended := db.ExtendTTL(ctx, []byte("key3"), 100*time.Millisecond)
				So(extended, ShouldBeTrue)

				// Check TTL after extension
				ttl, exists := db.GetTTL(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))
			})

			Convey("Then should survive past original expiration", func() {
				// Extend TTL
				db.ExtendTTL(ctx, []byte("key3"), 100*time.Millisecond)

				// Wait past original expiration but before extended expiration
				time.Sleep(75 * time.Millisecond)

				// Check TTL after waiting
				ttl, exists := db.GetTTL(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(ttl, ShouldBeGreaterThan, time.Duration(0))

				// Should still be able to get the value
				val, exists := db.Get(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value3")
			})

			Convey("Then should expire after extended TTL", func() {
				// Extend TTL
				db.ExtendTTL(ctx, []byte("key3"), 100*time.Millisecond)

				// Wait for extended expiration
				time.Sleep(100 * time.Millisecond)

				// Should not be able to get the value after extended expiration
				_, exists := db.Get(ctx, []byte("key3"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestTTLRemoval(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When removing TTL", func() {
			// Put with TTL
			db.PutWithTTL(ctx, []byte("key4"), "value4", 50*time.Millisecond)

			Convey("Then should be able to remove TTL", func() {
				removed := db.RemoveTTL(ctx, []byte("key4"))
				So(removed, ShouldBeTrue)
			})

			Convey("Then should survive past original expiration", func() {
				// Remove TTL
				db.RemoveTTL(ctx, []byte("key4"))

				// Wait past original expiration
				time.Sleep(75 * time.Millisecond)

				// Should still be able to get the value (no TTL)
				val, exists := db.Get(ctx, []byte("key4"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value4")

				// TTL should be 0 (no TTL)
				_, exists = db.GetTTL(ctx, []byte("key4"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestTTLWithRegularPut(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()

		Convey("When overwriting TTL value with regular Put", func() {
			// Put with TTL
			db.PutWithTTL(ctx, []byte("key5"), "value5", 50*time.Millisecond)

			// Overwrite with regular Put (should remove TTL)
			db.Put(ctx, []byte("key5"), "value6")

			Convey("Then should survive past original expiration", func() {
				// Wait past original expiration
				time.Sleep(75 * time.Millisecond)

				// Should still be able to get the new value (no TTL)
				val, exists := db.Get(ctx, []byte("key5"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value6")

				// TTL should be 0 (no TTL)
				_, exists = db.GetTTL(ctx, []byte("key5"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}
