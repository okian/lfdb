// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build arm64 && !purego

// This function implements bytes-equality for ARM64 using NEON SIMD.
// It compares two slices a and b and returns true if all bytes match.
//
// Quick NEON/ARM64 notes:
// - V registers (V0..V31) are 128-bit, viewable as B16 (16 bytes), B8 (8 bytes), etc.
// - We XOR the loaded vectors to get a diff; any non-zero lane indicates mismatch.
// - VMOV Vn.D[m], Rx moves a 64-bit lane from a vector to a general-purpose register.
// - We OR the two 64-bit halves; if the result is non-zero, there was a mismatch.
// - The function follows the same structure as the x86 versions: vector blocks,
//   then a partial 8-byte block, then final byte-by-byte tail.

#include "textflag.h"

// neonChunkSize defines the number of bytes processed per NEON iteration (16 bytes).
// neonChunkShift is log2(16) and neonChunkMask computes the remainder bytes.
#define neonChunkSize 16
#define neonChunkShift 4
#define neonChunkMask 15

// bytesEqualNEON compares two byte slices for equality using ARM64 NEON instructions.
// func bytesEqualNEON(a, b []byte) bool
TEXT ·bytesEqualNEON(SB), NOSPLIT, $0-49
        MOVD a_base+0(FP), R0   // Load a's data pointer into R0
        MOVD a_len+8(FP), R1    // Load len(a) into R1
        MOVD b_base+24(FP), R2  // Load b's data pointer into R2
        MOVD b_len+32(FP), R3   // Load len(b) into R3

        CMP  R1, R3             // Compare lengths
        BNE  not_equal          // If different, cannot be equal
        CBZ  R1, equal          // If length is zero, they are equal

        MOVD R1, R4                 // Copy length into R4 (block counter)
        LSR  $neonChunkShift, R4    // R4 = number of 16-byte blocks (len / 16)
        AND  $neonChunkMask, R1, R1 // R1 = remainder bytes (len % 16)

        CBZ  R4, tail            // If no full 16B blocks, jump to tail processing

loop:
        VLD1  (R0), [V0.B16]     // Load 16 bytes from a into V0
        VLD1  (R2), [V1.B16]     // Load 16 bytes from b into V1
        VEOR  V1.B16, V0.B16, V2.B16 // V2 := a ^ b (zero where equal, nonzero where differ)
        VMOV  V2.D[0], R5        // Move lower 64 bits of V2 into R5
        VMOV  V2.D[1], R6        // Move upper 64 bits of V2 into R6
        ORR   R6, R5, R6         // Combine halves; any nonzero means mismatch in block
        CBNZ  R6, not_equal      // If combined is non-zero, a mismatch occurred
        ADD   $neonChunkSize, R0, R0 // Advance a pointer by 16 bytes
        ADD   $neonChunkSize, R2, R2 // Advance b pointer by 16 bytes
        SUBS  $1, R4, R4         // Decrement block count, set flags
        BNE   loop               // If blocks remain, continue loop

tail:
        CMP   $8, R1             // If we have at least 8 bytes leftover, process them
        BLO   byte_tail          // Otherwise, go to final byte-by-byte loop
        VLD1  (R0), [V0.B8]      // Load 8 bytes from a into lower half of V0
        VLD1  (R2), [V1.B8]      // Load 8 bytes from b into lower half of V1
        VEOR  V1.B8, V0.B8, V2.B8 // XOR to detect differences across 8 bytes
        VMOV  V2.D[0], R5        // Move resulting 64 bits to R5
        CBNZ  R5, not_equal      // Non-zero means at least one differing byte
        ADD   $8, R0, R0         // Advance pointers by 8
        ADD   $8, R2, R2
        SUB   $8, R1, R1         // Reduce remainder by 8

byte_tail:
        CBZ  R1, equal          // If no leftover bytes, we are equal
        MOVD R1, R4             // Use R4 as byte-loop counter
byte_loop:
        MOVBU (R0), R5          // Load one byte from a into R5 (zero-extended)
        MOVBU (R2), R6          // Load one byte from b into R6 (zero-extended)
        CMP   R5, R6            // Compare the bytes
        BNE  not_equal          // If differ, return false
        ADD  $1, R0, R0         // Advance a pointer by 1
        ADD  $1, R2, R2         // Advance b pointer by 1
        SUBS $1, R4, R4         // Decrement loop counter; update flags
        BNE  byte_loop          // Continue until all leftover bytes are compared

equal:
        MOVD $1, R0             // Prepare true (1) in R0
        MOVB R0, ret+48(FP)     // Store boolean result
        RET                      // Return
not_equal:
        MOVD $0, R0             // Prepare false (0)
        MOVB R0, ret+48(FP)     // Store boolean result
        RET                      // Return
