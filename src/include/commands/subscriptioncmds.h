/*-------------------------------------------------------------------------
 *
 * subscriptioncmds.h
 *	  prototypes for subscriptioncmds.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/subscriptioncmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SUBSCRIPTIONCMDS_H
#define SUBSCRIPTIONCMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

#ifdef __SUBSCRIPTION__
extern const char * g_opentenbase_subscription_extension;
extern const char * g_opentenbase_subscription_relname;
extern const char * g_opentenbase_subscription_parallel_relname;
#endif

#ifdef __SUBSCRIPTION__
ObjectAddress
CreateSubscription(CreateSubscriptionStmt *stmt, bool isTopLevel, bool force_to_disable);
#else
extern ObjectAddress CreateSubscription(CreateSubscriptionStmt *stmt,
				   bool isTopLevel);
#endif
extern ObjectAddress AlterSubscription(AlterSubscriptionStmt *stmt);
extern void DropSubscription(DropSubscriptionStmt *stmt, bool isTopLevel);

extern ObjectAddress AlterSubscriptionOwner(const char *name, Oid newOwnerId);
extern void AlterSubscriptionOwner_oid(Oid subid, Oid newOwnerId);

#ifdef __SUBSCRIPTION__
extern void check_opentenbase_subscription_extension(void);
extern ObjectAddress CreateOpenTenBaseSubscription(CreateSubscriptionStmt *stmt,
				   			bool isTopLevel);
extern void AlterOpenTenBaseSubscription(AlterSubscriptionStmt *stmt);
extern void DropOpenTenBaseSubscription(DropSubscriptionStmt *stmt, bool isTopLevel);
extern bool IsOpenTenBaseSubscription(Node * stmt);
#endif

#endif							/* SUBSCRIPTIONCMDS_H */
