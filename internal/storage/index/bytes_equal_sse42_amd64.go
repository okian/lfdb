// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego && (amd64.v3 || amd64.v4)

package index

// bytesEqualSSE42 uses SSE4.2 instructions for 16-byte aligned comparisons.
// The implementation resides in bytes_equal_sse42_amd64.s.
func bytesEqualSSE42(a, b []byte) bool
