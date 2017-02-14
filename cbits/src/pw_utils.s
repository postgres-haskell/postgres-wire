	.file	"pw_utils.c"
	.text
	.p2align 4,,15
	.globl	scan_datarows
	.type	scan_datarows, @function
scan_datarows:
.LFB18:
	.cfi_startproc
	cmpq	$4, %rsi
	jbe	.L9
	xorl	%eax, %eax
	cmpb	$68, (%rdi)
	movq	%rsi, %r8
	je	.L6
	jmp	.L16
	.p2align 4,,10
	.p2align 3
.L7:
	leaq	1(%rcx,%rax), %rax
	movq	%rsi, %r8
	subq	%rax, %r8
	cmpq	$4, %r8
	jbe	.L13
	cmpb	$68, (%rdi,%rax)
	jne	.L3
.L6:
	movl	1(%rdi,%rax), %ecx
	subq	$1, %r8
	bswap	%ecx
	movl	%ecx, %ecx
	cmpq	%rcx, %r8
	jnb	.L7
.L13:
	movl	$1, (%rdx)
	ret
.L16:
	xorl	%eax, %eax
	.p2align 4,,10
	.p2align 3
.L3:
	movl	$2, (%rdx)
	ret
.L9:
	xorl	%eax, %eax
	jmp	.L13
	.cfi_endproc
.LFE18:
	.size	scan_datarows, .-scan_datarows
	.ident	"GCC: (Ubuntu 4.8.4-2ubuntu1~14.04.3) 4.8.4"
	.section	.note.GNU-stack,"",@progbits
