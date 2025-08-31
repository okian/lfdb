package db

import (
	"runtime"
	"testing"
	"time"
)

// testValue is a large struct used to detect lingering references after clearing a batch.
type testValue struct {
	_ [1 << 20]byte // 1 MiB to ensure noticeable memory usage
}

const (
	// gcMaxAttempts is the maximum number of garbage collection attempts to wait for finalization.
	gcMaxAttempts = 10
	// gcPause defines the delay between garbage collection attempts.
	gcPause = 10 * time.Millisecond
)

// TestBatchClearReleasesMemory verifies that Clear removes all references allowing GC to reclaim memory.
func TestBatchClearReleasesMemory(t *testing.T) {
	batch := NewBatch[[]byte, *testValue]()
	val := &testValue{}
	finalized := make(chan struct{})
	runtime.SetFinalizer(val, func(*testValue) { close(finalized) })

	batch.Put([]byte("k"), val)
	batch.Clear()

	// Remove our own reference and trigger GC repeatedly until finalizer runs.
	val = nil
	for i := 0; i < gcMaxAttempts; i++ {
		runtime.GC()
		runtime.Gosched()
		select {
		case <-finalized:
			return // Success: finalizer executed
		default:
			time.Sleep(gcPause)
		}
	}

	t.Fatalf("object was not garbage collected after %d attempts", gcMaxAttempts)
}
