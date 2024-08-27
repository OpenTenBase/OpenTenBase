/* src/port/strerror.c */

/*
 * strerror - map error number to descriptive string
 *
 * This version is obviously somewhat Unix-specific.
 *
 * based on code by Henry Spencer
 * modified for ANSI by D'Arcy J.M. Cain
 */

#include "c.h"


extern const char *const sys_errlist[];
extern int    sys_nerr;
/*
该函数用于将错误编号（errnum）转换为描述该错误的字符串。
strerror函数接受一个整数errnum作为参数，并返回一个指向描述错误的字符串的指针。
*/
const char *
strerror(int errnum)
{
    static char buf[24];
	//函数首先检查errnum是否在有效的错误编号范围内。
	//如果errnum小于0或大于sys_nerr，则表明这是一个无法识别的错误编号。
    if (errnum < 0 || errnum > sys_nerr)
    {
        sprintf(buf, _("unrecognized error %d"), errnum);
        return buf;
        //如果errnum不在有效范围内，函数使用sprintf将错误编号格式化为一个字符串，并存储在buf中。
    	//然后，函数返回buf的指针，指向表示未知错误的字符串。
    }
	//如果errnum是一个有效的错误编号，函数直接返回sys_errlist数组中相应索引位置的字符串指针。
    return sys_errlist[errnum];
}
