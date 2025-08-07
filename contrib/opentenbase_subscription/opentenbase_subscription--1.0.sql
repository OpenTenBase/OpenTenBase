/* contrib/opentenbase_subscription/opentenbase_subscription--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION opentenbase_subscription" to load this file. \quit

CREATE TABLE "opentenbase_subscription"
(
	"sub_name"				name,				-- Name of OpenTenBase subscription created on coordinator
	"sub_ignore_pk_conflict"	bool,				-- ignore primary key conflict occurs when apply 
	"sub_parallel_number"		int4,				-- Split OpenTenBase subscription into multiple parallel opentenbase-sub-subscriptions
	"sub_is_all_actived"		bool				-- Whether all parallel opentenbase-sub-subscriptions are actived.
												-- If there are some parallel opentenbase-sub-subscriptions, 
												-- other opentenbase-sub-subscriptions can be activated only after 
												-- the first opentenbase-sub-subscription has completed the data COPY.
												-- And other opentenbase-sub-subscriptions can only be activated by 
												-- the first opentenbase-sub-subscription.
) WITH OIDS;

CREATE TABLE "opentenbase_subscription_parallel"
(
	"sub_parent"				oid,				-- Oid of parent opentenbase subsription stored in opentenbase_subscription above
	"sub_child"				oid,				-- A OpenTenBase subscription may be split into multiple parallel opentenbase-sub-subscriptions,
												-- and each opentenbase-sub-subscription is recorded in pg_subscription with a given oid
	"sub_index"				int4,				-- Index of this opentenbase-sub-subscription in all parallel opentenbase-sub-subscriptions
	"sub_active_state"		bool,				-- Whether the current opentenbase-sub-subscription is activated by the first opentenbase-sub-subscription,
												-- valid only when sub_index > 0
	"sub_active_lsn"			pg_lsn				-- The LSN value that was set when the current opentenbase-sub-subscription was activated by the first
												-- opentenbase-sub-subscription, valid only when sub_index > 0
) WITH OIDS;

CREATE VIEW "OPENTENBASE_SUBSCRIPTION" AS
SELECT
	"oid" as "OID",
	"sub_name" as "SUB_NAME",
	"sub_ignore_pk_conflict" as "SUB_IGNORE_PK_CONFLICT",
	"sub_parallel_number" as "SUB_PARALLEL_NUMBER",
	"sub_is_all_actived" as "SUB_IS_ALL_ACTIVED" from "opentenbase_subscription";

CREATE VIEW "OPENTENBASE_SUBSCRIPTION_PARALLEL" AS
SELECT 
	"oid" as "OID",
	"sub_parent" as "SUB_PARENT",
	"sub_child" as "SUB_CHILD",
	"sub_index" as "SUB_INDEX",
	"sub_active_state" as "SUB_ACTIVE_STATE",
	"sub_active_lsn" as "SUB_ACTIVE_LSN" from "opentenbase_subscription_parallel";

-- -- Don't want this to be available to non-superusers.
REVOKE ALL ON TABLE "opentenbase_subscription" FROM PUBLIC;
REVOKE ALL ON TABLE "opentenbase_subscription_parallel" FROM PUBLIC;
