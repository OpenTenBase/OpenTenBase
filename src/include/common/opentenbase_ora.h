/*-------------------------------------------------------------------------
 *
 * opentenbase_ora.h
 *	  Definitions related Macro and struct for opentenbase_ora compatible.
 *
 * These definitions are used by both frontend and backend code.
 *
 * Copyright (c) 2023, Tencent Inc.
 *
 * src/include/common/opentenbase_ora.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _OPENTENBASE_ORA_H
#define _OPENTENBASE_ORA_H

#define OPENTENBASE_ORA_QQUOTE_DELIM_POS 2

/*
 * Support different databases sql layer.
 */
typedef enum DatabaseSQLMode
{
	SQLMODE_NULL = 0,
	SQLMODE_POSTGRESQL = 'p',
	SQLMODE_OPENTENBASE_ORA = 'o'
} DatabaseSQLMode;

#define ORA_MODE (false || sql_mode == SQLMODE_OPENTENBASE_ORA)
#define PG_MODE (sql_mode == SQLMODE_POSTGRESQL)

extern int sql_mode;

#endif							/* _OPENTENBASE_ORA_H */
