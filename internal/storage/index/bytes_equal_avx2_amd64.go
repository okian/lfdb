// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego && goamd64.v4

package index

// bytesEqualAVX2 uses AVX2 instructions for 32-byte aligned comparisons.
// The implementation resides in bytes_equal_avx2_amd64.s.
func bytesEqualAVX2(a, b []byte) bool
