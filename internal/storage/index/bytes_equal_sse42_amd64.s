// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego && (amd64.v3 || amd64.v4)

// This function compares two byte slices using SSE4.2-capable XMM instructions
// (though the sequence below uses operations available since SSE2 and is valid
// on SSE4.2-capable CPUs as well). It processes 16 bytes per iteration, then
// falls back to a scalar loop for leftover bytes.
//
// Quick references:
// - XMM registers (X0..X15) are 128-bit wide = 16 bytes.
// - MOVOU loads 16 bytes without alignment requirements (unaligned OK).
// - PXOR computes bitwise XOR; we XOR the two blocks to get a diff vector.
// - PCMPEQB compares bytes against zero to convert zeros->0xFF and nonzeros->0x00.
//   Here we zero X2, then compare diff against zero; only equal bytes become 0xFF.
// - PMOVMSKB extracts the MSB of each byte into a bitmask we can test in a GPR.

#include "textflag.h"

// sseChunkSize defines the number of bytes processed per SSE iteration.
// sseChunkShift is log2(16) and sseChunkMask is used for remainder math.
#define sseChunkSize 16
#define sseChunkShift 4
#define sseChunkMask 15

// bytesEqualSSE42 compares two byte slices using SSE4.2-capable instructions.
// It processes 16 bytes per iteration and safely handles any remaining tail bytes.
// func bytesEqualSSE42(a, b []byte) bool
TEXT ·bytesEqualSSE42(SB), NOSPLIT, $0-49
        MOVQ a_base+0(FP), SI   // Load a's data pointer into SI
        MOVQ a_len+8(FP), CX    // Load len(a) into CX
        MOVQ b_base+24(FP), DI  // Load b's data pointer into DI
        MOVQ b_len+32(FP), DX   // Load len(b) into DX

        CMPQ CX, DX             // If lengths differ, cannot be equal
        JNE  not_equal
        TESTQ CX, CX            // If length is zero, slices are trivially equal
        JE   equal

        MOVQ CX, R8             // Copy length to R8
        SHRQ $sseChunkShift, R8 // R8 = number of 16-byte chunks (len / 16)
        ANDQ $sseChunkMask, CX  // CX = remaining bytes (len % 16)

        TESTQ R8, R8            // Any 16B blocks to compare?
        JE   tail               // If none, skip block loop

loop:
        MOVOU (SI), X0          // Load 16 bytes from a into X0
        MOVOU (DI), X1          // Load 16 bytes from b into X1
        PXOR  X1, X0            // X0 := a ^ b (zero where equal, nonzero where differ)
        PXOR  X2, X2            // Zero X2 to create a 16B vector of zeros
        PCMPEQB X0, X2          // Compare X2(=0) vs X0(diff): lanes become 0xFF where diff==0
        PMOVMSKB X2, AX         // Pack MSBs of all 16 lanes into AX (bitmask)
        CMPQ  AX, $0xFFFF       // 0xFFFF indicates all 16 lanes matched (all MSBs set)
        JNE   not_equal         // If any bit is 0, there was a mismatch
        ADDQ  $sseChunkSize, SI // Advance a pointer by 16 bytes
        ADDQ  $sseChunkSize, DI // Advance b pointer by 16 bytes
        DECQ  R8                // One chunk consumed
        JNZ   loop              // Loop until no chunks remain

tail:
        TESTQ CX, CX            // Any leftover bytes?
        JE    equal             // If none, we are equal
tail_loop:
        MOVB  (SI), AL          // Load next byte from a
        MOVB  (DI), DL          // Load next byte from b
        CMPB  AL, DL            // Compare bytes
        JNE   not_equal         // Any mismatch -> false
        INCQ  SI                // Advance pointers
        INCQ  DI
        DECQ  CX                // Decrement remaining count
        JNZ   tail_loop         // Continue until CX == 0
        JMP   equal             // All remaining bytes matched

equal:
        MOVB $1, ret+48(FP)     // Store true in return slot
        RET                      // Return

not_equal:
        MOVB $0, ret+48(FP)     // Store false in return slot
        RET                      // Return
