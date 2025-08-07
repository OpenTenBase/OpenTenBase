/*
 * support GUC opentenbase_test_flag, only used for opentenbase_test
 */

#ifndef __OPTIMIZE__

#include <unistd.h>
#include <sys/stat.h>

#include "postgres.h"
#include "utils/guc.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/opentenbase_test.h"
#include "utils/varlena.h"
#include "miscadmin.h"

static char **opentenbase_test_flag_list = NULL;
static int opentenbase_test_flag_list_len = 0;
static int opentenbase_test_flag_list_cap = 0;
static MemoryContext opentenbase_test_memoryContext;

static void
opentenbase_test_flag_add(char *input)
{
	MemoryContext old_context = NULL;
	char *keyword = NULL;
	int i = 0;

	for (i = 0; i < opentenbase_test_flag_list_len; i++)
	{
		if (strcmp(opentenbase_test_flag_list[i], input) == 0)
		{
			return;
		}
	}

	if (opentenbase_test_memoryContext == NULL)
	{
		opentenbase_test_memoryContext = AllocSetContextCreate(
				TopMemoryContext, "OpenTenBaseTest", ALLOCSET_DEFAULT_SIZES);
	}

	old_context = MemoryContextSwitchTo(opentenbase_test_memoryContext);

	if (opentenbase_test_flag_list == NULL)
	{
		opentenbase_test_flag_list_cap = 8;
		opentenbase_test_flag_list = palloc(opentenbase_test_flag_list_cap * sizeof(char *));
	}

	keyword = pstrdup(input);

	if (opentenbase_test_flag_list_len >= opentenbase_test_flag_list_cap)
	{
		opentenbase_test_flag_list_cap = opentenbase_test_flag_list_cap * 2;
		opentenbase_test_flag_list = repalloc(opentenbase_test_flag_list, opentenbase_test_flag_list_cap * sizeof(char *));
	}

	opentenbase_test_flag_list[opentenbase_test_flag_list_len] = keyword;
	opentenbase_test_flag_list_len++;

	MemoryContextSwitchTo(old_context);
}

static void
opentenbase_test_flag_remove(char *input)
{
	int i = 0;
	for (i = 0; i < opentenbase_test_flag_list_len; i++)
	{
		if (strcmp(opentenbase_test_flag_list[i], input) == 0)
		{
			pfree(opentenbase_test_flag_list[i]);
			opentenbase_test_flag_list_len--;
			if (opentenbase_test_flag_list_len > 0)
			{
				opentenbase_test_flag_list[i] = opentenbase_test_flag_list[opentenbase_test_flag_list_len];
				opentenbase_test_flag_list[opentenbase_test_flag_list_len] = NULL;
			}
			break;
		}
	}
}

static void
opentenbase_test_flag_clear(void)
{
	int i = 0;
	for (i = 0; i < opentenbase_test_flag_list_len; i++)
	{
		pfree(opentenbase_test_flag_list[i]);
	}
	opentenbase_test_flag_list_len = 0;
}

char *opentenbase_test_flag;

bool
opentenbase_test_flag_contains(char *keyword)
{
	int i = 0;
	for (i = 0; i < opentenbase_test_flag_list_len; i++)
	{
		if (strcmp(opentenbase_test_flag_list[i], keyword) == 0)
		{
			return true;
		}
	}

	return false;
}

bool
opentenbase_test_flag_contains_prefix(char *keyword)
{
	int i = 0;
	for (i = 0; i < opentenbase_test_flag_list_len; i++)
	{
		if (memcmp(opentenbase_test_flag_list[i], keyword, strlen(keyword)) == 0)
		{
			return true;
		}
	}

	return false;
}

char*
get_opentenbase_test_flag_at_prefix(char *keyword)
{
	int i = 0;
	for (i = 0; i < opentenbase_test_flag_list_len; i++)
	{
		if (memcmp(opentenbase_test_flag_list[i], keyword, strlen(keyword)) == 0)
		{
			return opentenbase_test_flag_list[i];
		}
	}

	return NULL;
}

bool
check_opentenbase_test_flag(char **newval, void **extra, GucSource source)
{
	char *input_string = NULL;
	List *params = NIL;
	ListCell *item = NULL;
	bool has_sign = false;
	char firstchar = 0;
	int i = 0;
	int flag_len = 0;
	bool ret = true;

	if (*newval == NULL)
		return ret;

	input_string = guc_strdup(ERROR, *newval);
	flag_len = strlen(input_string);
	for (i = 0; i < flag_len; i ++)
	{
		if (input_string[i] == '"')
		{
			input_string[i] = ' ';
			Assert(i == 0 || i == flag_len - 1);
		}
	}

	if (!SplitIdentifierString(input_string, ';', &params))
	{
		GUC_check_errdetail("Format of opentenbase_test_flag \"%s\" is incorrect.", input_string);
		ret = false;
		goto return_error;
	}
	if (list_length(params) > 0)
	{
		Assert(linitial(params) != NULL);
		firstchar = ((char *)linitial(params))[0];
	}
	has_sign = (firstchar == '+' || firstchar == '-');
	foreach (item, params)
	{
		char *flag = lfirst(item);
		bool tmpsign = (flag[0] == '+' || flag[0] == '-');
		if (tmpsign != has_sign)
		{
			GUC_check_errdetail("all items must begin with a a/r sign, or neither of them begin with a/r sign");
			ret = false;
			goto return_error;
		}
	}

return_error:
	free(input_string);
	list_free_ext(params);
	return ret;
}

void
assign_opentenbase_test_flag(const char *newval, void *extra)
{
	char *input_string = NULL;
	List *params = NIL;
	ListCell *item = NULL;
	bool has_sign = false;
	char firstchar = 0;
	int i = 0;
	int flag_len = 0;

	if (newval == NULL)
		return;

	input_string = guc_strdup(ERROR, newval);
	flag_len = strlen(input_string);
	for (i = 0; i < flag_len; i ++)
	{
		if (input_string[i] == '"')
		{
			input_string[i] = ' ';
			Assert(i == 0 || i == flag_len - 1);
		}
	}

	if (!SplitIdentifierString(input_string, ';', &params))
	{
		/*
		 * this should not happend, because we have checked it in check_opentenbase_test_flag
		 */
		Assert(false);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SplitIdentifierString failed at assign_opentenbase_test_flag")));
	}
	if (list_length(params) > 0)
	{
		Assert(linitial(params) != NULL);
		firstchar = ((char *)linitial(params))[0];
	}
	has_sign = (firstchar == '+' || firstchar == '-');
	if (has_sign == false)
	{
		opentenbase_test_flag_clear();
		foreach (item, params)
		{
			char *flag = lfirst(item);
			opentenbase_test_flag_add(flag);
		}
	}
	else
	{
		foreach (item, params)
		{
			char *flag = lfirst(item);
			if (flag[0] == '+')
			{
				opentenbase_test_flag_add(flag + 1);
			}
			else
			{
				opentenbase_test_flag_remove(flag + 1);
			}
		}
	}

	free(input_string);
	list_free_ext(params);
}


char*
get_valid_opentenbase_test_flag(void)
{
	int i = 0;
	StringInfoData str;
	initStringInfo(&str);
	appendStringInfo(&str, "\"");
	for (i = 0; i < opentenbase_test_flag_list_len; i++)
	{
		if (i > 0)
		{
			appendStringInfo(&str, ";");
		}
		appendStringInfo(&str, "%s", opentenbase_test_flag_list[i]);
	}
	appendStringInfo(&str, "\"");

	return str.data;
}

const char*
show_opentenbase_test_flag(void)
{
	static char buf[4096];
	char* value = get_valid_opentenbase_test_flag();
	if (strlen(value) > 4095)
	{
		elog(ERROR, "opentenbase_test_flag is too long");
	}
	snprintf(buf, sizeof(buf), "%s", value);
	pfree(value);
	return buf;
}

void
opentenbase_test_suspend_if_prefix(char *keyword)
{
	if (opentenbase_test_flag_contains_prefix(keyword)) {
		char tmpfilename[256];
		struct stat st;
		FILE *file = NULL;
		char * value = get_opentenbase_test_flag_at_prefix(keyword);
		sprintf(tmpfilename, "/tmp/%s", value);
		file = fopen(tmpfilename, "w");
		if (file == NULL) {
			elog(ERROR, "failed to activate suspend stub");
		}
		fclose(file);

		while (stat(tmpfilename, &st) == 0) {
			CHECK_FOR_INTERRUPTS();
			sleep(1);
		}
	}
}
#endif
