/*
 * support GUC opentenbase_test_flag, only used for opentenbase_test
 */

#ifndef OPENTENBASE_TEST_H
#define OPENTENBASE_TEST_H

#include "utils/guc.h"

extern char * opentenbase_test_flag;

extern bool check_opentenbase_test_flag(char ** newval, void **extra, GucSource source);
extern void assign_opentenbase_test_flag(const char* newval, void* extra);
extern const char* show_opentenbase_test_flag(void);
extern bool opentenbase_test_flag_contains(char *keyword);
extern char* get_valid_opentenbase_test_flag(void);
extern bool opentenbase_test_flag_contains_prefix(char *keyword);
extern char* get_opentenbase_test_flag_at_prefix(char *keyword);
extern void opentenbase_test_suspend_if_prefix(char *keyword);
#endif
