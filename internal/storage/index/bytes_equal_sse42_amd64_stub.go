// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego && !(goamd64.v3 || goamd64.v4)

package index

// bytesEqualSSE42 is a stub used when the compiler target lacks SSE4.2
// support (GOAMD64 < v3). It falls back to bytesEqualScalar so builds remain
// functional on lower feature levels.
func bytesEqualSSE42(a, b []byte) bool {
	return bytesEqualScalar(a, b)
}
