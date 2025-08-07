#include "common.h"

int enable_elog;

int is_print(void)
{
	return enable_elog;
}

void switch_elog(int on)
{
	enable_elog=on;
}
