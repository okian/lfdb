// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego && (goamd64.v3 || goamd64.v4)

#include "textflag.h"

// sseChunkSize defines the number of bytes processed per SSE iteration.
#define sseChunkSize 16
#define sseChunkShift 4
#define sseChunkMask 15

// bytesEqualSSE42 compares two byte slices using SSE4.2 instructions.
// It processes 16 bytes per iteration and safely handles any remaining tail bytes.
// func bytesEqualSSE42(a, b []byte) bool
TEXT ·bytesEqualSSE42(SB), NOSPLIT, $0-49
        MOVQ a_base+0(FP), SI
        MOVQ a_len+8(FP), CX
        MOVQ b_base+24(FP), DI
        MOVQ b_len+32(FP), DX

        CMPQ CX, DX
        JNE  not_equal
        TESTQ CX, CX
        JE   equal

        MOVQ CX, R8
        SHRQ $sseChunkShift, R8
        ANDQ $sseChunkMask, CX

        TESTQ R8, R8
        JE   tail

loop:
        MOVOU (SI), X0
        MOVOU (DI), X1
        PXOR  X1, X0           // X0 = diff
        PXOR  X2, X2           // X2 = 0
        PCMPEQB X0, X2
        PMOVMSKB X2, AX
        CMPQ  AX, $0xFFFF
        JNE   not_equal
        ADDQ  $sseChunkSize, SI
        ADDQ  $sseChunkSize, DI
        DECQ  R8
        JNZ   loop

tail:
        TESTQ CX, CX
        JE    equal
        MOVQ  CX, R8
        REP; CMPSB
        JNE   not_equal
        JMP   equal

equal:
        MOVB $1, ret+48(FP)
        RET

not_equal:
        MOVB $0, ret+48(FP)
        RET
