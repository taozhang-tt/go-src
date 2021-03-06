// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// func castagnoliSSE42(crc uint32, p []byte) uint32
TEXT ┬ĚcastagnoliSSE42(SB),7,$0
	MOVL crc+0(FP), AX  // CRC value
	MOVQ p+8(FP), SI  // data pointer
	MOVL p+16(FP), CX  // len(p)

	NOTL AX

	/* If there's less than 8 bytes to process, we do it byte-by-byte. */
	CMPL CX, $8
	JL cleanup

	/* Process individual bytes until the input is 8-byte aligned. */
startup:
	MOVQ SI, BX
	ANDQ $7, BX
	JZ aligned

	CRC32B (SI), AX
	DECL CX
	INCQ SI
	JMP startup

aligned:
	/* The input is now 8-byte aligned and we can process 8-byte chunks. */
	CMPL CX, $8
	JL cleanup

	CRC32Q (SI), AX
	ADDQ $8, SI
	SUBQ $8, CX
	JMP aligned

cleanup:
	/* We may have some bytes left over that we process one at a time. */
	CMPL CX, $0
	JE done

	CRC32B (SI), AX
	INCQ SI
	DECQ CX
	JMP cleanup

done:
	NOTL AX
	MOVL AX, ret+24(FP)
	RET

// func haveSSE42() bool
TEXT ┬ĚhaveSSE42(SB),7,$0
	XORQ AX, AX
	INCL AX
	CPUID
	SHRQ $20, CX
	ANDQ $1, CX
	MOVB CX, ret+0(FP)
	RET

