/*-------------------------------------------------------------------------
 *
 * pgstrcasecmp.c
 *       Portable SQL-like case-independent comparisons and conversions.
 *
 * SQL99 specifies Unicode-aware case normalization, which we don't yet
 * have the infrastructure for.  Instead we use tolower() to provide a
 * locale-aware translation.  However, there are some locales where this
 * is not right either (eg, Turkish may do strange things with 'i' and
 * 'I').  Our current compromise is to use tolower() for characters with
 * the high bit set, and use an ASCII-only downcasing for 7-bit
 * characters.
 *
 * NB: this code should match downcase_truncate_identifier() in scansup.c.
 *
 * We also provide strict ASCII-only case conversion functions, which can
 * be used to implement C/POSIX case folding semantics no matter what the
 * C library thinks the locale is.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * src/port/pgstrcasecmp.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include <ctype.h>


/*
 * Case-independent comparison of two null-terminated strings.
 
 pg_strcasecmp函数：比较两个以空字符('\0')结尾的字符串，不考虑它们的大小写。
 如果字符串不相同，返回它们的第一个不同字符的差值（ASCII码值）；如果字符串相同，返回0。  
 */
int
pg_strcasecmp(const char *s1, const char *s2)
{// #lizard forgives
    for (;;)
    {
        unsigned char ch1 = (unsigned char) *s1++;
        unsigned char ch2 = (unsigned char) *s2++;

        if (ch1 != ch2)
        {
            if (ch1 >= 'A' && ch1 <= 'Z')
                ch1 += 'a' - 'A';
            else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
                ch1 = tolower(ch1);

            if (ch2 >= 'A' && ch2 <= 'Z')
                ch2 += 'a' - 'A';
            else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
                ch2 = tolower(ch2);

            if (ch1 != ch2)
                return (int) ch1 - (int) ch2;
        }
        if (ch1 == 0)
            break;
    }
    return 0;
}

/*
 * Case-independent comparison of two not-necessarily-null-terminated strings.
 * At most n bytes will be examined from each string.
 
pg_strncasecmp 函数： 用于比较两个可能不以空字符结尾的字符串，
并且最多只比较前n个字节。如果字符串在n个字节内完全相同，则返回0；
如果不同，则返回它们的第一个不同字符的差值（ASCII码值）。  
 */
int
pg_strncasecmp(const char *s1, const char *s2, size_t n)
{// #lizard forgives
    while (n-- > 0)
    {
        unsigned char ch1 = (unsigned char) *s1++;
        unsigned char ch2 = (unsigned char) *s2++;

        if (ch1 != ch2)
        {
            if (ch1 >= 'A' && ch1 <= 'Z')
                ch1 += 'a' - 'A';
            else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
                ch1 = tolower(ch1);

            if (ch2 >= 'A' && ch2 <= 'Z')
                ch2 += 'a' - 'A';
            else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
                ch2 = tolower(ch2);

            if (ch1 != ch2)
                return (int) ch1 - (int) ch2;
        }
        if (ch1 == 0)
            break;
    }
    return 0;
}

/*
 * Fold a character to upper case.
 *
 * Unlike some versions of toupper(), this is safe to apply to characters
 * that aren't lower case letters.  Note however that the whole thing is
 * a bit bogus for multibyte character sets.


pg_toupper函数：将一个小写字母转换为大写字母。
如果传入的字符不是小写字母，则返回原字符。
需要注意的是，这个函数仅对ASCII字符集有效，并且不处理多字节字符集的情况。  

 */
 
unsigned char
pg_toupper(unsigned char ch)
{
    if (ch >= 'a' && ch <= 'z')
        ch += 'A' - 'a';
    else if (IS_HIGHBIT_SET(ch) && islower(ch))
        ch = toupper(ch);
    return ch;
}

/*
 * Fold a character to lower case.
 *
 * Unlike some versions of tolower(), this is safe to apply to characters
 * that aren't upper case letters.  Note however that the whole thing is
 * a bit bogus for multibyte character sets.
 */
unsigned char
pg_tolower(unsigned char ch)
{
    if (ch >= 'A' && ch <= 'Z')
        ch += 'a' - 'A';
    else if (IS_HIGHBIT_SET(ch) && isupper(ch))	
        ch = tolower(ch);
    return ch;
}

/*
 * Fold a character to upper case, following C/POSIX locale rules.
 */
unsigned char
pg_ascii_toupper(unsigned char ch)
{
    if (ch >= 'a' && ch <= 'z')
        ch += 'A' - 'a';	//通过加减，实则运算ascll码，小写97，大写65
        					//实际就是小写字母-32，完成大小写转换
    return ch;
}

/*
 * Fold a character to lower case, following C/POSIX locale rules.
 */
unsigned char
pg_ascii_tolower(unsigned char ch)
{
    if (ch >= 'A' && ch <= 'Z')
        ch += 'a' - 'A';	//通过加减，实则运算ascll码，小写97，大写65
        					//实际就是大写字母+32，完成大小写转换
    return ch;
}
