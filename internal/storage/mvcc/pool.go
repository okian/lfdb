// Licensed under the MIT License. See LICENSE file in the project root for details.

package mvcc

import (
	"sync"
)

// VersionPool provides object pooling for Version structs to reduce memory allocations
type VersionPool[V any] struct {
	pool sync.Pool
}

// NewVersionPool creates a new VersionPool
func NewVersionPool[V any]() *VersionPool[V] {
	return &VersionPool[V]{
		pool: sync.Pool{
			New: func() interface{} {
				return &Version[V]{
					begin:     ^uint64(0), // initially invisible
					end:       ^uint64(0), // open
					tomb:      false,
					expiresAt: nil,
				}
			},
		},
	}
}

// Get retrieves a Version from the pool or creates a new one
func (p *VersionPool[V]) Get() *Version[V] {
	return p.pool.Get().(*Version[V])
}

// Put returns a Version to the pool after resetting its fields
func (p *VersionPool[V]) Put(v *Version[V]) {
	// Reset the version to its initial state
	v.begin = ^uint64(0) // initially invisible
	v.end = ^uint64(0)   // open
	var zero V
	v.val = zero
	v.tomb = false
	v.expiresAt = nil
	v.next.Store(nil)

	p.pool.Put(v)
}
