/*-------------------------------------------------------------------------
 *
 * strlcpy.c
 *      strncpy done right
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL: pgsql/src/port/strlcpy.c,v 1.5 2008/01/01 19:46:00 momjian Exp $
 *
 * This file was taken from OpenBSD and is used on platforms that don't
 * provide strlcpy().  The OpenBSD copyright terms follow.
 *-------------------------------------------------------------------------
 */

/*    $OpenBSD: strlcpy.c,v 1.11 2006/05/05 15:27:38 millert Exp $    */

/*
 * Copyright (c) 1998 Todd C. Miller <Todd.Miller@courtesan.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "gtm/gtm_c.h"

#ifndef HAVE_STRLCPY
/*
 * Copy src to string dst of size siz.    At most siz-1 characters
 * will be copied.    Always NUL terminates (unless siz == 0).
 * Returns strlen(src); if retval >= siz, truncation occurred.
 * Function creation history:  http://www.gratisoft.us/todd/papers/strlcpy.html
 */
size_t
strlcpy(char *dst, const char *src, size_t siz) //安全复制字符串的函数
{
    char       *d = dst; //d是一个指向dst缓冲区的指针，用于在复制过程中更新目标位置
    const char *s = src; //s是一个指向src字符串的指针，用于在复制过程中更新源位置。
    size_t        n = siz;//s是一个指向src字符串的指针，用于在复制过程中更新源位置。

    /* Copy as many bytes as will fit */
    if (n != 0) //如果n（即dst缓冲区的大小）不为0，开始复制字符串。
    {
        while (--n != 0) //while循环用于逐个复制字符，直到遇到源字符串的结尾（空字符'\0'）
        				//或dst缓冲区已满。（--n!=0）
        {
            if ((*d++ = *s++) == '\0') 
                break;
        }
    }

    /* Not enough room in dst, add NUL and traverse rest of src */
    if (n == 0) //如果n为0，这意味着dst缓冲区已满，但字符串复制尚未完成。
    {
        if (siz != 0)
            *d = '\0';            /* NUL-terminate dst */
        while (*s++)	//while循环仅仅是为了遍历完src的剩余部分，不执行任何操作。
        			   //这只是一个简单的技巧，用于确保s指针指向src的末尾。
            ;
    }
								//返回复制的字符数。
    return (s - src - 1);        /* count does not include NUL */
}
#endif
