// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego

package index

// bytesEqualSSE2 uses SSE2 instructions for 16-byte aligned comparisons.
// The implementation resides in bytes_equal_sse2_amd64.s.
func bytesEqualSSE2(a, b []byte) bool
