// Licensed under the MIT License. See LICENSE file in the project root for details.

// +build amd64

#include "textflag.h"

// bytesEqualAVX2 compares two byte slices using AVX2 instructions
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
	
	// Check if length is less than 32 bytes
	CMPQ CX, $32
	JL   scalar_compare
	
	// Use AVX2 for 32-byte chunks
	MOVQ CX, AX
	SHRQ $5, AX              // AX = len / 32
	SHLQ $5, AX              // AX = (len / 32) * 32
	SUBQ AX, CX              // CX = remaining bytes
	
	// Compare 32-byte chunks
chunk_loop:
	VMOVDQU (SI), Y0         // Load 32 bytes from a
	VPCMPEQB (DI), Y0, Y1    // Compare with b
	VPMOVMSKB Y1, AX         // Get mask of equal bytes
	CMPL AX, $0xFFFFFFFF     // Check if all 32 bytes are equal
	JNE  not_equal
	ADDQ $32, SI
	ADDQ $32, DI
	SUBQ $32, AX
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

// bytesEqualSSE42 compares two byte slices using SSE4.2 instructions
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
	
	// Check if length is less than 16 bytes
	CMPQ CX, $16
	JL   scalar_compare
	
	// Use SSE4.2 for 16-byte chunks
	MOVQ CX, AX
	SHRQ $4, AX              // AX = len / 16
	SHLQ $4, AX              // AX = (len / 16) * 16
	SUBQ AX, CX              // CX = remaining bytes
	
	// Compare 16-byte chunks
chunk_loop:
	MOVDQU (SI), X0          // Load 16 bytes from a
	PCMPEQB (DI), X0         // Compare with b
	PMOVMSKB X0, AX          // Get mask of equal bytes
	CMPL AX, $0xFFFF         // Check if all 16 bytes are equal
	JNE  not_equal
	ADDQ $16, SI
	ADDQ $16, DI
	SUBQ $16, AX
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

// bytesEqualSSE2 compares two byte slices using SSE2 instructions
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
	
	// Check if length is less than 16 bytes
	CMPQ CX, $16
	JL   scalar_compare
	
	// Use SSE2 for 16-byte chunks
	MOVQ CX, AX
	SHRQ $4, AX              // AX = len / 16
	SHLQ $4, AX              // AX = (len / 16) * 16
	SUBQ AX, CX              // CX = remaining bytes
	
	// Compare 16-byte chunks
chunk_loop:
	MOVDQU (SI), X0          // Load 16 bytes from a
	MOVDQU (DI), X1          // Load 16 bytes from b
	PCMPEQB X1, X0           // Compare bytes
	PMOVMSKB X0, AX          // Get mask of equal bytes
	CMPL AX, $0xFFFF         // Check if all 16 bytes are equal
	JNE  not_equal
	ADDQ $16, SI
	ADDQ $16, DI
	SUBQ $16, AX
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
