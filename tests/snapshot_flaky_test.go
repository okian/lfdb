package tests

import (
	"bytes"
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	core "github.com/kianostad/lfdb/internal/core"
)

// TestSnapshotIsolationStress tries to reproduce the observed fuzz flake deterministically
func TestSnapshotIsolationStress(t *testing.T) {
	t.Parallel()
	const workers = 16
	const iters = 500
	var wg sync.WaitGroup
	// Collect errors from worker goroutines to report from the main test goroutine.
	errCh := make(chan string, workers)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(rand.Int())))
			for i := 0; i < iters; i++ {
				// Random short key/value
				key := make([]byte, 4)
				r.Read(key)
				val := make([]byte, 8)
				r.Read(val)
				ctx := context.Background()
				db := core.New[[]byte, []byte]()
				// Put initial
				db.Put(ctx, key, val)
				// Snapshot
				snap := db.Snapshot(ctx)
				// Verify snapshot sees initial
				got, ok := snap.Get(ctx, key)
				if !ok || !bytes.Equal(got, val) {
					// Report error from worker and stop this worker.
					select {
					case errCh <- "snapshot miss on initial":
					default:
					}
					snap.Close(ctx)
					db.Close(ctx)
					return
				}
				// Modify after snapshot
				newVal := append(val, []byte("-modified")...)
				db.Put(ctx, key, newVal)
				// Try to trigger scheduling differences
				runtime.Gosched()
				// Snapshot should still see old
				got2, ok2 := snap.Get(ctx, key)
				if !ok2 || !bytes.Equal(got2, val) {
					// Report error from worker and stop this worker.
					select {
					case errCh <- "snapshot saw wrong value/miss":
					default:
					}
					snap.Close(ctx)
					db.Close(ctx)
					return
				}
				snap.Close(ctx)
				db.Close(ctx)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	// If any worker reported an error, fail the test from the main goroutine.
	if len(errCh) > 0 {
		// Drain to get a representative message.
		var first string
		for msg := range errCh {
			first = msg
			break
		}
		t.Fatalf("snapshot isolation stress detected error: %s", first)
	}
}
