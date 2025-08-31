// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego && !amd64.v4

package index

// bytesEqualAVX2 is a stub used when the compiler target lacks AVX2 support
// (GOAMD64 < v4). It delegates to bytesEqualScalar, ensuring the build
// succeeds even though AVX2 code paths are unavailable.
func bytesEqualAVX2(a, b []byte) bool {
	return bytesEqualScalar(a, b)
}
