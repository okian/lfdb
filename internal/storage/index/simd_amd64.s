// Licensed under the MIT License. See LICENSE file in the project root for details.

// +build amd64

#include "textflag.h"

// chunkSize defines the number of bytes processed per iteration. Using a
// constant avoids magic numbers and simplifies adjustments for wider SIMD
// instructions. See https://en.wikipedia.org/wiki/Word_(computer_architecture)
#define chunkSize 8

// chunkShift is log2(chunkSize) and enables efficient division using bit
// shifting. See https://en.wikipedia.org/wiki/Bit_manipulation
#define chunkShift 3

// chunkMask equals chunkSize-1 and isolates the remaining bytes after chunk
// processing. See https://en.wikipedia.org/wiki/Mask_(computing)
#define chunkMask 7

// bytesEqualAVX2 compares two byte slices for equality using scalar
// instructions as a fallback when AVX2 is unavailable. a and b must have the
// same length; nil slices are handled safely when their length is zero.
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

        // Prepare chunk counter and remainder
        MOVQ CX, R8              // R8 = len
        SHRQ $chunkShift, R8     // R8 = len / chunkSize
        ANDQ $chunkMask, CX      // CX = len % chunkSize

        TESTQ R8, R8             // Any full chunks?
        JE   scalar_compare

chunk_loop:
        MOVQ (SI), AX            // Load chunk from a
        CMPQ AX, (DI)            // Compare with b
        JNE  not_equal
        ADDQ $chunkSize, SI      // Advance a
        ADDQ $chunkSize, DI      // Advance b
        DECQ R8                  // Next chunk
        JNZ  chunk_loop

        TESTQ CX, CX             // Any remaining bytes?
        JE   equal

scalar_compare:
        MOVQ CX, AX              // Load remainder length
        REP; CMPSB               // Compare remaining bytes
        JNE  not_equal
        JMP  equal

equal:
        MOVB $1, ret+48(FP)
        RET

not_equal:
        MOVB $0, ret+48(FP)
        RET

// bytesEqualSSE42 compares two byte slices for equality using scalar
// instructions when SSE4.2 is unavailable. a and b must have the same length;
// nil slices are handled safely when their length is zero.
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

        // Prepare chunk counter and remainder
        MOVQ CX, R8              // R8 = len
        SHRQ $chunkShift, R8     // R8 = len / chunkSize
        ANDQ $chunkMask, CX      // CX = len % chunkSize

        TESTQ R8, R8             // Any full chunks?
        JE   scalar_compare

chunk_loop:
        MOVQ (SI), AX            // Load chunk from a
        CMPQ AX, (DI)            // Compare with b
        JNE  not_equal
        ADDQ $chunkSize, SI      // Advance a
        ADDQ $chunkSize, DI      // Advance b
        DECQ R8                  // Next chunk
        JNZ  chunk_loop

        TESTQ CX, CX             // Any remaining bytes?
        JE   equal

scalar_compare:
        MOVQ CX, AX              // Load remainder length
        REP; CMPSB               // Compare remaining bytes
        JNE  not_equal
        JMP  equal

equal:
        MOVB $1, ret+48(FP)
        RET

not_equal:
        MOVB $0, ret+48(FP)
        RET

// bytesEqualSSE2 compares two byte slices for equality using scalar
// instructions when SSE2 is unavailable. a and b must have the same length;
// nil slices are handled safely when their length is zero.
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

        // Prepare chunk counter and remainder
        MOVQ CX, R8              // R8 = len
        SHRQ $chunkShift, R8     // R8 = len / chunkSize
        ANDQ $chunkMask, CX      // CX = len % chunkSize

        TESTQ R8, R8             // Any full chunks?
        JE   scalar_compare

chunk_loop:
        MOVQ (SI), AX            // Load chunk from a
        CMPQ AX, (DI)            // Compare with b
        JNE  not_equal
        ADDQ $chunkSize, SI      // Advance a
        ADDQ $chunkSize, DI      // Advance b
        DECQ R8                  // Next chunk
        JNZ  chunk_loop

        TESTQ CX, CX             // Any remaining bytes?
        JE   equal

scalar_compare:
        MOVQ CX, AX              // Load remainder length
        REP; CMPSB               // Compare remaining bytes
        JNE  not_equal
        JMP  equal

equal:
        MOVB $1, ret+48(FP)
        RET

not_equal:
        MOVB $0, ret+48(FP)
        RET
