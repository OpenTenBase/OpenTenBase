/*
 * contrib/opentenbase_subscription/opentenbase_subscription--1.0.sql
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION opentenbase_subscription" to load this file. \quit

CREATE TABLE opentenbase_subscription
(
	sub_name				name,				-- Name of OpenTenBase subscription created on coordinator
	sub_ignore_pk_conflict	bool,				-- ignore primary key conflict occurs when apply 
	sub_manual_hot_date		text,				-- GUC parameter, manual_hot_date
	sub_temp_hot_date		text,				-- GUC parameter, temp_hot_date
	sub_temp_cold_date		text,				-- GUC parameter, temp_cold_date
	sub_parallel_number		int4,				-- Split OpenTenBase subscription into multiple parallel opentenbase-sub-subscriptions
	sub_is_all_actived		bool				-- Whether all parallel opentenbase-sub-subscriptions are actived.
												-- If there are some parallel opentenbase-sub-subscriptions, 
												-- other opentenbase-sub-subscriptions can be activated only after 
												-- the first opentenbase-sub-subscription has completed the data COPY.
												-- And other opentenbase-sub-subscriptions can only be activated by 
												-- the first opentenbase-sub-subscription.
) WITH OIDS;

CREATE TABLE opentenbase_subscription_parallel
(
	sub_parent				oid,				-- Oid of parent opentenbase subsription stored in opentenbase_subscription above
	sub_child				oid,				-- A OpenTenBase subscription may be split into multiple parallel opentenbase-sub-subscriptions,
												-- and each opentenbase-sub-subscription is recorded in pg_subscription with a given oid
	sub_index				int4,				-- Index of this opentenbase-sub-subscription in all parallel opentenbase-sub-subscriptions
	sub_active_state		bool,				-- Whether the current opentenbase-sub-subscription is activated by the first opentenbase-sub-subscription,
												-- valid only when sub_index > 0
	sub_active_lsn			pg_lsn				-- The LSN value that was set when the current opentenbase-sub-subscription was activated by the first
												-- opentenbase-sub-subscription, valid only when sub_index > 0
) WITH OIDS;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON TABLE opentenbase_subscription FROM PUBLIC;
REVOKE ALL ON TABLE opentenbase_subscription_parallel FROM PUBLIC;
