// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build amd64 && !purego && goamd64.v4

#include "textflag.h"

// avx2ChunkSize defines the number of bytes processed per AVX2 iteration.
// A dedicated constant avoids magic numbers and eases future adjustments.
// See https://en.wikipedia.org/wiki/Advanced_Vector_Extensions
#define avx2ChunkSize 32
#define avx2ChunkShift 5
#define avx2ChunkMask 31

// sseChunkSize defines bytes processed per SSE iteration when handling tails.
#define sseChunkSize 16
#define sseChunkShift 4
#define sseChunkMask 15

// bytesEqualAVX2 compares two byte slices for equality using AVX2 instructions.
// It safely handles misaligned tails by falling back to SSE and scalar code paths.
// func bytesEqualAVX2(a, b []byte) bool
TEXT ·bytesEqualAVX2(SB), NOSPLIT, $0-49
        MOVQ a_base+0(FP), SI    // SI = &a[0]
        MOVQ a_len+8(FP), CX     // CX = len(a)
        MOVQ b_base+24(FP), DI   // DI = &b[0]
        MOVQ b_len+32(FP), DX    // DX = len(b)

        // Lengths must match
        CMPQ CX, DX
        JNE  not_equal
        // Zero length is equal
        TESTQ CX, CX
        JE   equal

        MOVQ CX, R8              // R8 = len
        SHRQ $avx2ChunkShift, R8 // R8 = len / 32
        ANDQ $avx2ChunkMask, CX  // CX = len % 32

        TESTQ R8, R8
        JE   avx2_tail

avx2_loop:
        VMOVDQU (SI), Y0        // Load 32 bytes from a
        VMOVDQU (DI), Y1        // Load 32 bytes from b
        VPCMPEQB Y1, Y0, Y2     // Compare bytes
        VPMOVMSKB Y2, AX        // Create bit mask
        CMPL    AX, $0xFFFFFFFF
        JNE     not_equal
        ADDQ    $avx2ChunkSize, SI
        ADDQ    $avx2ChunkSize, DI
        DECQ    R8
        JNZ     avx2_loop

avx2_tail:
        // Handle remaining 16-byte chunk with SSE
        CMPQ CX, $sseChunkSize
        JB   scalar_tail
        MOVOU  (SI), X0
        MOVOU  (DI), X1
        PCMPEQB X1, X0
        PMOVMSKB X0, AX
        CMPL   AX, $0xFFFF
        JNE    not_equal
        ADDQ   $sseChunkSize, SI
        ADDQ   $sseChunkSize, DI
        SUBQ   $sseChunkSize, CX

scalar_tail:
        TESTQ CX, CX
        JE    equal
        MOVQ  CX, R8
        REP; CMPSB
        JNE   not_equal
        JMP   equal

equal:
        MOVB $1, ret+48(FP)
        VZEROUPPER
        RET

not_equal:
        MOVB $0, ret+48(FP)
        VZEROUPPER
        RET
