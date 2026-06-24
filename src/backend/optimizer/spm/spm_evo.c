/*------------------------------------------------------------------------
 *
 * spm_evo.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/optimizer/spm/spm_evo.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <float.h>
#include <limits.h>
#include <math.h>
#include "optimizer/spm.h"

int plan_retention_weeks = 0;