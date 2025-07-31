/*-------------------------------------------------------------------------
 *
 * auditlogger.h
 *      Exports from postmaster/auditlogger.c.
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * src/include/postmaster/auditlogger.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __AUDIT_LOGGER_H__
#define __AUDIT_LOGGER_H__

#include <limits.h>

#define 					AUDIT_COMMON_LOG		(1 << 0)
#define 					AUDIT_FGA_LOG			(1 << 1)
/* size_rotation_for = AUDIT_COMMON_LOG | AUDIT_FGA_LOG | MAINTAIN_TRACE_LOG */
#define 					MAINTAIN_TRACE_LOG      (1 << 2)

extern int                    AuditLog_RotationAge;
extern int                    AuditLog_RotationSize;
extern PGDLLIMPORT char *    AuditLog_filename;
extern bool                 AuditLog_truncate_on_rotation;
extern int                    AuditLog_file_mode;

extern int					AuditLog_max_worker_number;
extern int					AuditLog_common_log_queue_size_kb;
extern int					AuditLog_fga_log_queue_size_kb;
extern int					Maintain_trace_log_queue_size_kb;
extern int					AuditLog_common_log_cache_size_kb;
extern int					AuditLog_fga_log_cacae_size_kb;
extern int					Maintain_trace_log_cache_size_kb;

extern bool                 am_auditlogger;
extern bool                 enable_auditlogger_warning;

extern int                    AuditLogger_Start(void);

#ifdef EXEC_BACKEND
extern void                 AuditLoggerMain(int argc, char *argv[]) pg_attribute_noreturn();
#endif

extern Size                 AuditLoggerShmemSize(void);
extern void                 AuditLoggerShmemInit(void);
extern int                    AuditLoggerQueueAcquire(void);

extern void     alog(int destination, const char *fmt,...) pg_attribute_printf(2, 3);
#define 		audit_log(args...)          alog(AUDIT_COMMON_LOG, ##args)
#define 		audit_log_fga(args...)      alog(AUDIT_FGA_LOG, ##args)
#define 		trace_log(args...)          alog(MAINTAIN_TRACE_LOG, ##args)

#endif                            /* __AUDIT_LOGGER_H__ */
