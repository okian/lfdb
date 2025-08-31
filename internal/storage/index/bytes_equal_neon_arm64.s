// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build arm64 && !purego
// +build arm64,!purego

#include "textflag.h"

// neonChunkSize defines the number of bytes processed per NEON iteration.
#define neonChunkSize 16
#define neonChunkShift 4
#define neonChunkMask 15

// bytesEqualNEON compares two byte slices for equality using ARM64 NEON instructions.
// func bytesEqualNEON(a, b []byte) bool
TEXT ·bytesEqualNEON(SB), NOSPLIT, $0-49
        MOVD a_base+0(FP), R0
        MOVD a_len+8(FP), R1
        MOVD b_base+24(FP), R2
        MOVD b_len+32(FP), R3

        CMP  R1, R3
        BNE  not_equal
        CBZ  R1, equal

        MOVD R1, R4
        LSR  $neonChunkShift, R4 // blocks = len / 16
        AND  $neonChunkMask, R1, R1  // R1 = len % 16

        CBZ  R4, tail

loop:
        VLD1  (R0), [V0.B16]
        VLD1  (R2), [V1.B16]
        VEOR  V1.B16, V0.B16, V2.B16
        VMOV  V2.D[0], R5                // move lower 64 bits to R5
        VMOV  V2.D[1], R6                // move upper 64 bits to R6
        ORR   R6, R5, R6                 // combine halves into R6
        CBNZ  R6, not_equal              // branch if any byte differs
        ADD   $neonChunkSize, R0, R0
        ADD   $neonChunkSize, R2, R2
        SUBS  $1, R4, R4
        BNE   loop

tail:
        CMP   $8, R1
        BLO   byte_tail
        VLD1  (R0), [V0.B8]
        VLD1  (R2), [V1.B8]
        VEOR  V1.B8, V0.B8, V2.B8
        VMOV  V2.D[0], R5
        CBNZ  R5, not_equal
        ADD   $8, R0, R0
        ADD   $8, R2, R2
        SUB   $8, R1, R1

byte_tail:
        CBZ  R1, equal
        MOVD R1, R4
byte_loop:
        MOVBU (R0), R5
        MOVBU (R2), R6
        CMP   R5, R6
        BNE  not_equal
        ADD  $1, R0, R0
        ADD  $1, R2, R2
        SUBS $1, R4, R4
        BNE  byte_loop

equal:
        MOVD $1, R0
        MOVB R0, ret+48(FP)
        RET
not_equal:
        MOVD $0, R0
        MOVB R0, ret+48(FP)
        RET
