/*-------------------------------------------------------------------------
 *
 * predtest.h
 *	  prototypes for predtest.c
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/predtest.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREDTEST_H
#define PREDTEST_H

#include "nodes/primnodes.h"


extern bool predicate_implied_by(List *predicate_list, List *clause_list,
					 bool clause_is_check);
extern bool predicate_refuted_by(List *predicate_list, List *clause_list,
					 bool clause_is_check);

#endif							/* PREDTEST_H */
