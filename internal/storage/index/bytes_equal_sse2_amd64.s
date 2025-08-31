// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego

// This function is the SSE2-capable variant that compares two byte slices.
// It mirrors the structure of the SSE4.2 version but omits instructions not
// available on plain SSE2. It still uses 16-byte XMM loads and compares.
//
// The strategy:
// - Compare as many 16-byte chunks as possible using XOR + compare-with-zero.
// - If any chunk differs, short-circuit to false.
// - Compare remaining tail bytes with a scalar loop.

#include "textflag.h"

// sseChunkSize defines the number of bytes processed per SSE2 iteration.
// sseChunkShift is log2(16) and sseChunkMask computes the remainder bytes.
#define sseChunkSize 16
#define sseChunkShift 4
#define sseChunkMask 15

// bytesEqualSSE2 compares two byte slices using SSE2 instructions.
// It processes 16 bytes per iteration and safely handles remaining tail bytes.
// func bytesEqualSSE2(a, b []byte) bool
TEXT ·bytesEqualSSE2(SB), NOSPLIT, $0-49
        MOVQ a_base+0(FP), SI   // Load a's data pointer into SI
        MOVQ a_len+8(FP), CX    // Load len(a) into CX
        MOVQ b_base+24(FP), DI  // Load b's data pointer into DI
        MOVQ b_len+32(FP), DX   // Load len(b) into DX

        CMPQ CX, DX             // Lengths must match
        JNE  not_equal
        TESTQ CX, CX            // Empty slices are equal
        JE   equal

        MOVQ CX, R8             // Copy length to R8
        SHRQ $sseChunkShift, R8 // Number of 16-byte chunks to compare
        ANDQ $sseChunkMask, CX  // Remainder bytes after 16-byte chunks

        TESTQ R8, R8            // Any full chunks?
        JE   tail               // If none, go to tail

loop:
        MOVOU (SI), X0          // Load 16 bytes from a
        MOVOU (DI), X1          // Load 16 bytes from b
        PXOR  X1, X0            // X0 := a ^ b (0 where equal, nonzero where differ)
        PXOR  X2, X2            // Zero out X2
        PCMPEQB X0, X2          // Compare X2(=0) vs X0(diff) -> 0xFF where equal
        PMOVMSKB X2, AX         // Extract MSBs into AX bitmask
        CMPQ  AX, $0xFFFF       // Expect all 1s for 16 matching bytes
        JNE   not_equal         // Mismatch -> false
        ADDQ  $sseChunkSize, SI // Advance pointers by 16 bytes
        ADDQ  $sseChunkSize, DI
        DECQ  R8                // One chunk processed
        JNZ   loop              // Keep processing chunks

tail:
        TESTQ CX, CX            // Leftover bytes?
        JE    equal             // If none, equal
        MOVQ  CX, R8            // Copy remainder (CMPSB uses CX internally)
        REP; CMPSB              // Compare bytes SI vs DI, CX times
        JNE   not_equal         // Any mismatch -> false
        JMP   equal             // All matched

equal:
        MOVB $1, ret+48(FP)     // Store true
        RET                      // Return

not_equal:
        MOVB $0, ret+48(FP)     // Store false
        RET                      // Return
