// Licensed under the MIT License. See LICENSE file in the project root for details.

// +build amd64

#include "textflag.h"

// bytesEqualAVX2 compares two byte slices using optimized scalar instructions
// func bytesEqualAVX2(a, b []byte) bool
TEXT ·bytesEqualAVX2(SB), NOSPLIT, $0-49
	MOVQ a_base+0(FP), SI    // a.ptr
	MOVQ a_len+8(FP), CX     // a.len
	MOVQ b_base+24(FP), DI   // b.ptr
	MOVQ b_len+32(FP), DX    // b.len
	
	// Check lengths are equal
	CMPQ CX, DX
	JNE  not_equal
	
	// Check if length is zero
	CMPQ CX, $0
	JE   equal
	
	// Use optimized scalar comparison for 8-byte chunks
	CMPQ CX, $8
	JL   scalar_compare
	
	// Compare 8-byte chunks
	MOVQ CX, AX
	SHRQ $3, AX              // AX = len / 8
	SHLQ $3, AX              // AX = (len / 8) * 8
	SUBQ AX, CX              // CX = remaining bytes
	
	// Compare 8-byte chunks
chunk_loop:
	MOVQ (SI), AX            // Load 8 bytes from a
	CMPQ AX, (DI)            // Compare with b
	JNE  not_equal
	ADDQ $8, SI
	ADDQ $8, DI
	SUBQ $8, AX
	JNZ  chunk_loop
	
	// Handle remaining bytes
	CMPQ CX, $0
	JE   equal
	
scalar_compare:
	// Compare remaining bytes one by one
	MOVQ CX, AX
	REP; CMPSB
	JNE  not_equal
	
equal:
	MOVB $1, ret+48(FP)
	RET
	
not_equal:
	MOVB $0, ret+48(FP)
	RET

// bytesEqualSSE42 compares two byte slices using optimized scalar instructions
// func bytesEqualSSE42(a, b []byte) bool
TEXT ·bytesEqualSSE42(SB), NOSPLIT, $0-49
	MOVQ a_base+0(FP), SI    // a.ptr
	MOVQ a_len+8(FP), CX     // a.len
	MOVQ b_base+24(FP), DI   // b.ptr
	MOVQ b_len+32(FP), DX    // b.len
	
	// Check lengths are equal
	CMPQ CX, DX
	JNE  not_equal
	
	// Check if length is zero
	CMPQ CX, $0
	JE   equal
	
	// Use optimized scalar comparison for 8-byte chunks
	CMPQ CX, $8
	JL   scalar_compare
	
	// Compare 8-byte chunks
	MOVQ CX, AX
	SHRQ $3, AX              // AX = len / 8
	SHLQ $3, AX              // AX = (len / 8) * 8
	SUBQ AX, CX              // CX = remaining bytes
	
	// Compare 8-byte chunks
chunk_loop:
	MOVQ (SI), AX            // Load 8 bytes from a
	CMPQ AX, (DI)            // Compare with b
	JNE  not_equal
	ADDQ $8, SI
	ADDQ $8, DI
	SUBQ $8, AX
	JNZ  chunk_loop
	
	// Handle remaining bytes
	CMPQ CX, $0
	JE   equal
	
scalar_compare:
	// Compare remaining bytes one by one
	MOVQ CX, AX
	REP; CMPSB
	JNE  not_equal
	
equal:
	MOVB $1, ret+48(FP)
	RET
	
not_equal:
	MOVB $0, ret+48(FP)
	RET

// bytesEqualSSE2 compares two byte slices using optimized scalar instructions
// func bytesEqualSSE2(a, b []byte) bool
TEXT ·bytesEqualSSE2(SB), NOSPLIT, $0-49
	MOVQ a_base+0(FP), SI    // a.ptr
	MOVQ a_len+8(FP), CX     // a.len
	MOVQ b_base+24(FP), DI   // b.ptr
	MOVQ b_len+32(FP), DX    // b.len
	
	// Check lengths are equal
	CMPQ CX, DX
	JNE  not_equal
	
	// Check if length is zero
	CMPQ CX, $0
	JE   equal
	
	// Use optimized scalar comparison for 8-byte chunks
	CMPQ CX, $8
	JL   scalar_compare
	
	// Compare 8-byte chunks
	MOVQ CX, AX
	SHRQ $3, AX              // AX = len / 8
	SHLQ $3, AX              // AX = (len / 8) * 8
	SUBQ AX, CX              // CX = remaining bytes
	
	// Compare 8-byte chunks
chunk_loop:
	MOVQ (SI), AX            // Load 8 bytes from a
	CMPQ AX, (DI)            // Compare with b
	JNE  not_equal
	ADDQ $8, SI
	ADDQ $8, DI
	SUBQ $8, AX
	JNZ  chunk_loop
	
	// Handle remaining bytes
	CMPQ CX, $0
	JE   equal
	
scalar_compare:
	// Compare remaining bytes one by one
	MOVQ CX, AX
	REP; CMPSB
	JNE  not_equal
	
equal:
	MOVB $1, ret+48(FP)
	RET
	
not_equal:
	MOVB $0, ret+48(FP)
	RET
