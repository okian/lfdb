// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAtomicOperations(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When performing atomic operations", func() {
			// Test Add operation
			newVal, success := database.Add(ctx, []byte("counter"), 5)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 5)

			// Test Add to existing value
			newVal, success = database.Add(ctx, []byte("counter"), 3)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 8)

			// Test Increment
			newVal, success = database.Increment(ctx, []byte("counter"))
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 9)

			// Test Decrement
			newVal, success = database.Decrement(ctx, []byte("counter"))
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 8)

			// Test Multiply
			newVal, success = database.Multiply(ctx, []byte("counter"), 2)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 16)

			// Test Divide
			newVal, success = database.Divide(ctx, []byte("counter"), 4)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 4)

			Convey("Then final state should be correct", func() {
				// Verify final state
				val, exists := database.Get(ctx, []byte("counter"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, 4)
			})
		})
	})
}

func TestAtomicOperationsWithFloat64(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, float64]()
		defer database.Close(ctx)

		Convey("When performing atomic operations with float64", func() {
			// Test Add operation with float64
			newVal, success := database.Add(ctx, []byte("score"), 10.5)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 10.5)

			// Test Add to existing value
			newVal, success = database.Add(ctx, []byte("score"), 2.5)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 13.0)

			// Test Multiply
			newVal, success = database.Multiply(ctx, []byte("score"), 2.0)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 26.0)

			// Test Divide
			newVal, success = database.Divide(ctx, []byte("score"), 4.0)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 6.5)
		})
	})
}

func TestAtomicOperationsInTransactions(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When performing atomic operations in transactions", func() {
			// Put initial value
			database.Put(ctx, []byte("balance"), 100)

			// Execute transaction with atomic operations
			err := database.Txn(ctx, func(tx core.Txn[[]byte, int]) error {
				// Read current balance
				val, exists := tx.Get(ctx, []byte("balance"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, 100)

				// Add 50 to balance
				newVal, success := tx.Add(ctx, []byte("balance"), 50)
				So(success, ShouldBeTrue)
				So(newVal, ShouldEqual, 150)

				// Increment balance
				newVal, success = tx.Increment(ctx, []byte("balance"))
				So(success, ShouldBeTrue)
				So(newVal, ShouldEqual, 151)

				// Multiply balance by 2
				newVal, success = tx.Multiply(ctx, []byte("balance"), 2)
				So(success, ShouldBeTrue)
				So(newVal, ShouldEqual, 302)

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then final state should be correct", func() {
				// Verify final state
				val, exists := database.Get(ctx, []byte("balance"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, 302)
			})
		})
	})
}

func TestAtomicOperationsWithNonExistentKeys(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When performing atomic operations on non-existent keys", func() {
			// Test Increment on non-existent key
			newVal, success := database.Increment(ctx, []byte("new_counter"))
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 1)

			// Test Decrement on non-existent key
			newVal, success = database.Decrement(ctx, []byte("new_counter2"))
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, -1)

			// Test Multiply on non-existent key
			newVal, success = database.Multiply(ctx, []byte("new_factor"), 5)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 5)

			// Test Divide on non-existent key
			newVal, success = database.Divide(ctx, []byte("new_quotient"), 3)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 0)
		})
	})
}

func TestAtomicOperationsWithZeroDivision(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When performing division by zero", func() {
			// Put a value
			database.Put(ctx, []byte("number"), 10)

			// Test division by zero
			newVal, success := database.Divide(ctx, []byte("number"), 0)
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, 0)
		})
	})
}

func TestAtomicOperationsWithStringValues(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When performing atomic operations with strings", func() {
			// Test that atomic operations work with strings (should use fallback behavior)
			newVal, success := database.Add(ctx, []byte("text"), "world")
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, "world")

			// Test Add to existing string (should use fallback)
			newVal, success = database.Add(ctx, []byte("text"), "hello")
			So(success, ShouldBeTrue)
			So(newVal, ShouldEqual, "hello")
		})
	})
}
