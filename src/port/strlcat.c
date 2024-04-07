/*
 * src/port/strlcat.c
 *
 *    $OpenBSD: strlcat.c,v 1.13 2005/08/08 08:05:37 espie Exp $    */

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

#include "c.h"


/*
 * Appends src to string dst of size siz (unlike strncat, siz is the
 * full size of dst, not space left).  At most siz-1 characters
 * will be copied.  Always NUL terminates (unless siz <= strlen(dst)).
 * Returns strlen(src) + MIN(siz, strlen(initial dst)).
 * If retval >= siz, truncation occurred.
 */
size_t
strlcat(char *dst, const char *src, size_t siz)
{
    char       *d = dst; // 指向 dst 的指针，用于遍历 dst 字符串  
    const char *s = src; // 指向 src 的指针，用于遍历 src 字符串 
    size_t        n = siz; // 剩余的可用空间大小  
    size_t        dlen;   // dst 字符串的长度  

    /* Find the end of dst and adjust bytes left but don't go past end */
	// 找到 dst 字符串的结尾，并调整剩余空间大小，但不超过 dst 的总大小  

    while (n-- != 0 && *d != '\0')
        d++;
    dlen = d - dst;   // 计算 dst 字符串的长度  
    n = siz - dlen;   // 更新剩余的可用空间大小

    if (n == 0)		 // 如果没有剩余空间，返回 dst 和 src 的总长度  
        return (dlen + strlen(s));
    // 连接 src 到 dst，直到 src 结束或剩余空间用完  
    
    while (*s != '\0')
    {
        if (n != 1)
        {
            *d++ = *s;	// 将 src 的字符复制到 dst  
            n--;		// 剩余空间减一  
        }
        s++;  			// 移动到 src 的下一个字符  
    }
    // 在 dst 的末尾添加空字符（'\0'），确保它是一个有效的 C 字符串  

    *d = '\0';
	
    return (dlen + (s - src));    /* count does not include NUL */
}
