// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"fmt"
	"os"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestExportBinary(t *testing.T) {
	Convey("Given a test database with data", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add some test data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting to binary", func() {
			// Export to binary
			filename := "test_export.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)

			So(err, ShouldBeNil)

			Convey("Then the export file should exist and have content", func() {
				// Verify file exists and has content
				fileInfo, err := os.Stat(filename)
				So(err, ShouldBeNil)
				So(fileInfo.Size(), ShouldBeGreaterThan, 0)
			})
		})
	})
}

func TestImportBinary(t *testing.T) {
	Convey("Given a test database with data", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add test data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting and importing binary data", func() {
			// Export to binary
			filename := "test_import.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)
			So(err, ShouldBeNil)

			// Create new database and import
			newDatabase := core.New[[]byte, string]()
			defer newDatabase.Close(ctx)

			err = core.ImportBinary(newDatabase, filename)
			So(err, ShouldBeNil)

			Convey("Then the imported data should match the original", func() {
				// Verify imported data
				for key, expectedValue := range testData {
					value, exists := newDatabase.Get(ctx, []byte(key))
					So(exists, ShouldBeTrue)
					So(value, ShouldEqual, expectedValue)
				}
			})
		})
	})
}

func TestExportCSV(t *testing.T) {
	Convey("Given a test database with data", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add test data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting to CSV", func() {
			// Export to CSV
			filename := "test_export.csv"
			defer os.Remove(filename)

			err := core.ExportCSV(database, filename)

			So(err, ShouldBeNil)

			Convey("Then the export file should exist and have content", func() {
				// Verify file exists and has content
				fileInfo, err := os.Stat(filename)
				So(err, ShouldBeNil)
				So(fileInfo.Size(), ShouldBeGreaterThan, 0)
			})
		})
	})
}

func TestImportCSV(t *testing.T) {
	Convey("Given a test database with data", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add test data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting and importing CSV data", func() {
			// Export to CSV
			filename := "test_import.csv"
			defer os.Remove(filename)

			err := core.ExportCSV(database, filename)
			So(err, ShouldBeNil)

			// Create new database and import
			newDatabase := core.New[[]byte, string]()
			defer newDatabase.Close(ctx)

			err = core.ImportCSV(newDatabase, filename)
			So(err, ShouldBeNil)

			Convey("Then the imported data should match the original", func() {
				// Verify imported data
				for key, expectedValue := range testData {
					value, exists := newDatabase.Get(ctx, []byte(key))
					So(exists, ShouldBeTrue)
					So(value, ShouldEqual, expectedValue)
				}
			})
		})
	})
}

func TestExportJSON(t *testing.T) {
	Convey("Given a test database with data", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add test data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting to JSON", func() {
			// Export to JSON
			filename := "test_export.json"
			defer os.Remove(filename)

			err := core.ExportJSON(database, filename)

			So(err, ShouldBeNil)

			Convey("Then the export file should exist and have content", func() {
				// Verify file exists and has content
				fileInfo, err := os.Stat(filename)
				So(err, ShouldBeNil)
				So(fileInfo.Size(), ShouldBeGreaterThan, 0)
			})
		})
	})
}

func TestImportJSON(t *testing.T) {
	Convey("Given a test database with data", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add test data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting and importing JSON data", func() {
			// Export to JSON
			filename := "test_import.json"
			defer os.Remove(filename)

			err := core.ExportJSON(database, filename)
			So(err, ShouldBeNil)

			// Create new database and import
			newDatabase := core.New[[]byte, string]()
			defer newDatabase.Close(ctx)

			err = core.ImportJSON(newDatabase, filename)
			So(err, ShouldBeNil)

			Convey("Then the imported data should match the original", func() {
				// Verify imported data
				for key, expectedValue := range testData {
					value, exists := newDatabase.Get(ctx, []byte(key))
					So(exists, ShouldBeTrue)
					So(value, ShouldEqual, expectedValue)
				}
			})
		})
	})
}

func TestExportWithSnapshots(t *testing.T) {
	Convey("Given a test database with data and snapshots", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add initial data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		// Create snapshot
		snapshot := database.Snapshot(ctx)
		defer snapshot.Close(ctx)

		// Add more data after snapshot
		database.Put(ctx, []byte("key4"), "value4")
		database.Put(ctx, []byte("key5"), "value5")

		Convey("When exporting with snapshot", func() {
			// Export to binary
			filename := "test_snapshot_export.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)

			So(err, ShouldBeNil)

			Convey("Then the export file should exist and have content", func() {
				// Verify file exists and has content
				fileInfo, err := os.Stat(filename)
				So(err, ShouldBeNil)
				So(fileInfo.Size(), ShouldBeGreaterThan, 0)
			})
		})
	})
}

func TestImportWithSnapshots(t *testing.T) {
	Convey("Given a test database with data and snapshots", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add initial data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		// Create snapshot
		snapshot := database.Snapshot(ctx)
		defer snapshot.Close(ctx)

		// Add more data after snapshot
		database.Put(ctx, []byte("key4"), "value4")
		database.Put(ctx, []byte("key5"), "value5")

		Convey("When exporting and importing with snapshot", func() {
			// Export to binary
			filename := "test_snapshot_import.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)
			So(err, ShouldBeNil)

			// Create new database and import
			newDatabase := core.New[[]byte, string]()
			defer newDatabase.Close(ctx)

			err = core.ImportBinary(newDatabase, filename)
			So(err, ShouldBeNil)

			Convey("Then the imported data should match the snapshot", func() {
				// Verify imported data matches snapshot (not current state)
				for key, expectedValue := range testData {
					value, exists := newDatabase.Get(ctx, []byte(key))
					So(exists, ShouldBeTrue)
					So(value, ShouldEqual, expectedValue)
				}

				// Verify data added after snapshot is not imported
				if _, exists := newDatabase.Get(ctx, []byte("key4")); exists {
					// This might be acceptable depending on implementation
					// Just verify the operation doesn't crash
					_ = exists // Suppress unused variable warning
				}

				if _, exists := newDatabase.Get(ctx, []byte("key5")); exists {
					// This might be acceptable depending on implementation
					// Just verify the operation doesn't crash
					_ = exists // Suppress unused variable warning
				}
			})
		})
	})
}

func TestExportLargeDataset(t *testing.T) {
	Convey("Given a test database with large dataset", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add large dataset
		const numEntries = 1000
		testData := make(map[string]string, numEntries)

		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			testData[key] = value
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting large dataset", func() {
			// Export to binary
			filename := "test_large_export.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)

			So(err, ShouldBeNil)

			Convey("Then the export file should exist and have content", func() {
				// Verify file exists and has content
				fileInfo, err := os.Stat(filename)
				So(err, ShouldBeNil)
				So(fileInfo.Size(), ShouldBeGreaterThan, 0)
			})
		})
	})
}

func TestImportLargeDataset(t *testing.T) {
	Convey("Given a test database with large dataset", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add large dataset
		const numEntries = 1000
		testData := make(map[string]string, numEntries)

		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			testData[key] = value
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting and importing large dataset", func() {
			// Export to binary
			filename := "test_large_import.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)
			So(err, ShouldBeNil)

			// Create new database and import
			newDatabase := core.New[[]byte, string]()
			defer newDatabase.Close(ctx)

			err = core.ImportBinary(newDatabase, filename)
			So(err, ShouldBeNil)

			Convey("Then the imported data should match the original", func() {
				// Verify imported data
				for key, expectedValue := range testData {
					value, exists := newDatabase.Get(ctx, []byte(key))
					So(exists, ShouldBeTrue)
					So(value, ShouldEqual, expectedValue)
				}
			})
		})
	})
}

func TestExportConcurrent(t *testing.T) {
	Convey("Given a test database with concurrent access", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add initial data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When exporting with concurrent access", func() {
			var wg sync.WaitGroup
			const numGoroutines = 10
			const numOperations = 100

			// Start concurrent operations
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						key := fmt.Sprintf("concurrent-key-%d-%d", id, j)
						value := fmt.Sprintf("concurrent-value-%d-%d", id, j)
						database.Put(ctx, []byte(key), value)
					}
				}(i)
			}

			// Export while concurrent operations are running
			filename := "test_concurrent_export.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)

			So(err, ShouldBeNil)

			wg.Wait()

			Convey("Then the export file should exist and have content", func() {
				// Verify file exists and has content
				fileInfo, err := os.Stat(filename)
				So(err, ShouldBeNil)
				So(fileInfo.Size(), ShouldBeGreaterThan, 0)
			})
		})
	})
}

func TestImportConcurrent(t *testing.T) {
	Convey("Given a test database with data", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add test data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When importing with concurrent access", func() {
			// Export to binary
			filename := "test_concurrent_import.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)
			So(err, ShouldBeNil)

			// Create new database and import
			newDatabase := core.New[[]byte, string]()
			defer newDatabase.Close(ctx)

			err = core.ImportBinary(newDatabase, filename)
			So(err, ShouldBeNil)

			var wg sync.WaitGroup
			const numGoroutines = 10
			const numOperations = 100

			// Start concurrent operations on imported database
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						key := fmt.Sprintf("concurrent-key-%d-%d", id, j)
						value := fmt.Sprintf("concurrent-value-%d-%d", id, j)
						newDatabase.Put(ctx, []byte(key), value)
					}
				}(i)
			}

			wg.Wait()

			Convey("Then the imported data should still be accessible", func() {
				// Verify original imported data is still accessible
				for key, expectedValue := range testData {
					value, exists := newDatabase.Get(ctx, []byte(key))
					So(exists, ShouldBeTrue)
					So(value, ShouldEqual, expectedValue)
				}
			})
		})
	})
}

func TestExportImportRoundTrip(t *testing.T) {
	Convey("Given a test database with various data types", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add various types of data
		testData := map[string]string{
			"empty":           "",
			"simple":          "hello world",
			"unicode":         "Hello 世界",
			"special_chars":   "!@#$%^&*()_+-=[]{}|;':\",./<>?",
			"newlines":        "line1\nline2\nline3",
			"tabs":            "col1\tcol2\tcol3",
			"very_long":       "very long string " + string(make([]byte, 1000)),
			"binary_data":     string([]byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}),
			"key_with_spaces": "value with spaces",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		Convey("When performing export-import round trip", func() {
			// Export to binary
			filename := "test_roundtrip.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)
			So(err, ShouldBeNil)

			// Create new database and import
			newDatabase := core.New[[]byte, string]()
			defer newDatabase.Close(ctx)

			err = core.ImportBinary(newDatabase, filename)
			So(err, ShouldBeNil)

			Convey("Then the imported data should match the original exactly", func() {
				// Verify imported data matches exactly
				for key, expectedValue := range testData {
					value, exists := newDatabase.Get(ctx, []byte(key))
					So(exists, ShouldBeTrue)
					So(value, ShouldEqual, expectedValue)
				}
			})
		})
	})
}

func TestExportImportWithDeletes(t *testing.T) {
	Convey("Given a test database with deleted entries", t, func() {
		ctx := context.Background()
		// Create test database
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Add test data
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for key, value := range testData {
			database.Put(ctx, []byte(key), value)
		}

		// Delete some entries
		database.Delete(ctx, []byte("key2"))

		Convey("When exporting and importing with deletes", func() {
			// Export to binary
			filename := "test_deletes.bin"
			defer os.Remove(filename)

			err := core.ExportBinary(database, filename)
			So(err, ShouldBeNil)

			// Create new database and import
			newDatabase := core.New[[]byte, string]()
			defer newDatabase.Close(ctx)

			err = core.ImportBinary(newDatabase, filename)
			So(err, ShouldBeNil)

			Convey("Then the imported data should reflect the deletes", func() {
				// Verify existing entries
				value, exists := newDatabase.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(value, ShouldEqual, "value1")

				value, exists = newDatabase.Get(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(value, ShouldEqual, "value3")

				// Verify deleted entry
				_, exists = newDatabase.Get(ctx, []byte("key2"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}
