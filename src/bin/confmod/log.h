/*
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 */
#ifndef __LOG_H__
#define __LOG_H__

#define DEBUG3  10
#define DEBUG2  11
#define DEBUG1  12
#define INFO    13
#define NOTICE2 14
#define NOTICE  15
#define WARNING 16
#define ERROR   17
#define PANIC   18
#define MANDATORY 19
/* related function declaration */
extern void elog_start(const char *file, const char *func, int line);
extern void elogFinish(int level, const char *fmt,...) __attribute__((format(printf, 2, 3)));
#define elog elog_start(__FILE__, __FUNCTION__, __LINE__), elogFinish

#endif /* __LOG_H__ */
