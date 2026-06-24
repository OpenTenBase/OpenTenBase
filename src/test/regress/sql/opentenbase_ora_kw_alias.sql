\c regression_ora
set do_as_alias to on;
set case_as_alias to on;
 select 1 abort from dual;----unreserved
 select 1 absolute from dual;----unreserved
 select 1 access from dual;----unreserved
 select 1 account from dual;----unreserved
 select 1 action from dual;----unreserved
 select 1 add from dual;----unreserved
 select 1 admin from dual;----unreserved
 select 1 after from dual;----unreserved
 select 1 aggregate from dual;----unreserved
 select 1 all from dual;----reserved
 select 1 also from dual;----unreserved
 select 1 alter from dual;----unreserved
 select 1 always from dual;----unreserved
 select 1 analyse from dual;----reserved
 select 1 analyze from dual;----reserved
 select 1 and from dual;----reserved
 select 1 any from dual;----reserved
 select 1 array from dual;----reserved
 select 1 as from dual;----reserved
 select 1 asc from dual;----reserved
 select 1 assertion from dual;----unreserved
 select 1 assignment from dual;----unreserved
 select 1 asymmetric from dual;----reserved
 select 1 at from dual;----unreserved
 select 1 attach from dual;----unreserved
 select 1 attribute from dual;----unreserved
 select 1 audit from dual;----reserved
 select 1 authorization from dual;----reserved (can be function or type name)
 select 1 backward from dual;----unreserved
 select 1 barrier from dual;----unreserved
 select 1 before from dual;----unreserved
 select 1 begin from dual;----unreserved
 select 1 begin_subtxn from dual;----unreserved
 select 1 between from dual;----unreserved (cannot be function or type name)
 select 1 bigint from dual;----unreserved (cannot be function or type name)
 select 1 binary from dual;----reserved (can be function or type name)
 select 1 binary_double from dual;----unreserved (cannot be function or type name)
 select 1 binary_float from dual;----unreserved (cannot be function or type name)
 select 1 bit from dual;----unreserved (cannot be function or type name)
 select 1 body from dual;----unreserved (cannot be package name)
 select 1 boolean from dual;----unreserved (cannot be function or type name)
 select 1 both from dual;----reserved
 select 1 by from dual;----reserved
 select 1 cache from dual;----unreserved
 select 1 call from dual;----unreserved
 select 1 called from dual;----unreserved
 select 1 cascade from dual;----unreserved
 select 1 cascaded from dual;----unreserved
 select 1 case from dual;----reserved
 select 1 cast from dual;----reserved
 select 1 catalog from dual;----unreserved
 select 1 chain from dual;----unreserved
 select 1 char from dual;----unreserved (cannot be function or type name)
 select 1 character from dual;----unreserved (cannot be function or type name)
 select 1 characteristics from dual;----unreserved
 select 1 check from dual;----reserved
 select 1 checkpoint from dual;----unreserved
 select 1 class from dual;----unreserved
 select 1 clean from dual;----unreserved
 select 1 close from dual;----unreserved
 select 1 cluster from dual;----unreserved
 select 1 coalesce from dual;----unreserved (cannot be function or type name)
 select 1 collate from dual;----reserved
 select 1 collation from dual;----reserved (can be function or type name)
 select 1 column from dual;----reserved
 select 1 columns from dual;----unreserved
 select 1 comment from dual;----unreserved
 select 1 comments from dual;----unreserved
 select 1 commit from dual;----unreserved
 select 1 commit_subtxn from dual;----unreserved
 select 1 committed from dual;----unreserved
 select 1 concurrently from dual;----reserved (can be function or type name)
 select 1 configuration from dual;----unreserved
 select 1 conflict from dual;----unreserved
 select 1 connect from dual;----reserved
 select 1 connection from dual;----unreserved
 select 1 constraint from dual;----reserved
 select 1 constraints from dual;----unreserved
 select 1 content from dual;----unreserved
 select 1 continue from dual;----unreserved
 select 1 conversion from dual;----unreserved
 select 1 coordinator from dual;----unreserved
 select 1 copy from dual;----unreserved
 select 1 cost from dual;----unreserved
 select 1 count from dual;----unreserved
 select 1 create from dual;----reserved
 select 1 cross from dual;----reserved (can be function or type name)
 select 1 csv from dual;----unreserved
 select 1 cube from dual;----unreserved
 select 1 current from dual;----unreserved
 select 1 current_catalog from dual;----reserved
 select 1 current_date from dual;----reserved
 select 1 current_role from dual;----reserved
 select 1 current_schema from dual;----reserved (can be function or type name)
 select 1 current_time from dual;----reserved
 select 1 current_timestamp from dual;----reserved
 select 1 current_user from dual;----reserved
 select 1 cursor from dual;----unreserved
 select 1 cycle from dual;----unreserved
 select 1 data from dual;----unreserved
 select 1 database from dual;----unreserved
 select 1 day from dual;----unreserved
 select 1 dbtimezone from dual;----reserved
 select 1 deallocate from dual;----unreserved
 select 1 dec from dual;----unreserved (cannot be function or type name)
 select 1 decimal from dual;----unreserved (cannot be function or type name)
 select 1 declare from dual;----unreserved
 select 1 default from dual;----reserved
 select 1 defaults from dual;----unreserved
 select 1 deferrable from dual;----reserved
 select 1 deferred from dual;----unreserved
 select 1 definer from dual;----unreserved
 select 1 delete from dual;----reserved
 select 1 delimiter from dual;----unreserved
 select 1 delimiters from dual;----unreserved
 select 1 depends from dual;----unreserved
 select 1 desc from dual;----reserved
 select 1 detach from dual;----unreserved
 select 1 dictionary from dual;----unreserved
 select 1 direct from dual;----unreserved
 select 1 disable from dual;----unreserved
 select 1 discard from dual;----unreserved
 select 1 distinct from dual;----reserved
 select 1 distkey from dual;----unreserved
 select 1 distribute from dual;----unreserved
 select 1 distributed from dual;----unreserved
 select 1 diststyle from dual;----unreserved
 select 1 do from dual;----reserved
 select 1 document from dual;----unreserved
 select 1 domain from dual;----unreserved
 select 1 double from dual;----unreserved
 select 1 drop from dual;----unreserved
 select 1 each from dual;----unreserved
 select 1 editionable from dual;----unreserved
 select 1 else from dual;----reserved
 select 1 enable from dual;----unreserved
 select 1 encoding from dual;----unreserved
 select 1 encrypted from dual;----unreserved
 select 1 end from dual;----reserved
 select 1 enum from dual;----unreserved
 select 1 error from dual;----unreserved
 select 1 escape from dual;----unreserved
 select 1 event from dual;----unreserved
 select 1 except from dual;----reserved
 select 1 exchange from dual;----unreserved
 select 1 exclude from dual;----unreserved
 select 1 excluding from dual;----unreserved
 select 1 exclusive from dual;----unreserved
 select 1 exec from dual;----unreserved
 select 1 execute from dual;----unreserved
 select 1 exists from dual;----unreserved (cannot be function or type name)
 select 1 explain from dual;----unreserved
 select 1 extension from dual;----unreserved
 select 1 extent from dual;----unreserved
 select 1 external from dual;----unreserved
 select 1 extract from dual;----unreserved (cannot be function or type name)
 select 1 false from dual;----reserved
 select 1 family from dual;----unreserved
 select 1 fetch from dual;----reserved
 select 1 filter from dual;----unreserved
 select 1 first from dual;----unreserved
 select 1 float from dual;----unreserved (cannot be function or type name)
 select 1 following from dual;----unreserved
 select 1 for from dual;----reserved
 select 1 force from dual;----unreserved
 select 1 foreign from dual;----reserved
 select 1 forward from dual;----unreserved
 select 1 freeze from dual;----reserved (can be function or type name)
 select 1 from from dual;----reserved
 select 1 full from dual;----reserved (can be function or type name)
 select 1 function from dual;----unreserved
 select 1 functions from dual;----unreserved
 select 1 generated from dual;----unreserved
 select 1 global from dual;----unreserved
 select 1 grant from dual;----reserved
 select 1 granted from dual;----unreserved
 select 1 greatest from dual;----unreserved (cannot be function or type name)
 select 1 group from dual;----reserved
 select 1 grouping from dual;----unreserved (cannot be function or type name)
 select 1 gtm from dual;----unreserved
 select 1 handler from dual;----unreserved
 select 1 having from dual;----reserved
 select 1 header from dual;----unreserved
 select 1 hold from dual;----unreserved
 select 1 hour from dual;----unreserved
 select 1 identity from dual;----unreserved
 select 1 if from dual;----unreserved
 select 1 ilike from dual;----reserved (can be function or type name)
 select 1 immediate from dual;----unreserved
 select 1 immutable from dual;----unreserved
 select 1 implicit from dual;----unreserved
 select 1 import from dual;----unreserved
 select 1 in from dual;----reserved
 select 1 including from dual;----unreserved
 select 1 increment from dual;----unreserved
 select 1 index from dual;----unreserved
 select 1 indexes from dual;----unreserved
 select 1 inherit from dual;----unreserved
 select 1 inherits from dual;----unreserved
 select 1 initially from dual;----reserved
 select 1 inline from dual;----unreserved
 select 1 inner from dual;----reserved (can be function or type name)
 select 1 inout from dual;----unreserved (cannot be function or type name)
 select 1 input from dual;----unreserved
 select 1 insensitive from dual;----unreserved
 select 1 insert from dual;----unreserved
 select 1 instead from dual;----unreserved
 select 1 int from dual;----unreserved (cannot be function or type name)
 select 1 integer from dual;----unreserved (cannot be function or type name)
 select 1 intersect from dual;----reserved
 select 1 interval from dual;----unreserved (cannot be function or type name)
 select 1 into from dual;----reserved
 select 1 invoker from dual;----unreserved
 select 1 is from dual;----reserved (can be function or type name)
 select 1 isnull from dual;----reserved (can be function or type name)
 select 1 isolation from dual;----unreserved
 select 1 join from dual;----reserved (can be function or type name)
 select 1 key from dual;----unreserved
 select 1 label from dual;----unreserved
 select 1 language from dual;----unreserved
 select 1 large from dual;----unreserved
 select 1 last from dual;----unreserved
 select 1 lateral from dual;----reserved
 select 1 leading from dual;----reserved
 select 1 leakproof from dual;----unreserved
 select 1 least from dual;----unreserved (cannot be function or type name)
 select 1 left from dual;----reserved (can be function or type name)
 select 1 level from dual;----reserved
 select 1 like from dual;----reserved (can be function or type name)
 select 1 limit from dual;----reserved
 select 1 listen from dual;----unreserved
 select 1 load from dual;----unreserved
 select 1 local from dual;----unreserved
 select 1 localtime from dual;----reserved
 select 1 localtimestamp from dual;----reserved
 select 1 location from dual;----unreserved
 select 1 lock from dual;----unreserved
 select 1 locked from dual;----unreserved
 select 1 logged from dual;----unreserved
 select 1 mapping from dual;----unreserved
 select 1 match from dual;----unreserved
 select 1 matched from dual;----unreserved
 select 1 materialized from dual;----unreserved
 select 1 maxvalue from dual;----unreserved
 select 1 merge from dual;----unreserved
 select 1 method from dual;----unreserved
 select 1 minus from dual;----reserved
 select 1 minute from dual;----unreserved
 select 1 minvalue from dual;----unreserved
 select 1 mode from dual;----unreserved
 select 1 month from dual;----unreserved
 select 1 move from dual;----unreserved
 select 1 name from dual;----unreserved
 select 1 names from dual;----unreserved
 select 1 national from dual;----unreserved (cannot be function or type name)
 select 1 natural from dual;----reserved (can be function or type name)
 select 1 nchar from dual;----unreserved (cannot be function or type name)
 select 1 new from dual;----unreserved
 select 1 next from dual;----unreserved
 select 1 no from dual;----unreserved
 select 1 noaudit from dual;----reserved
 select 1 node from dual;----unreserved
 select 1 none from dual;----unreserved (cannot be function or type name)
 select 1 noneditionable from dual;----unreserved
 select 1 not from dual;----reserved
 select 1 nothing from dual;----unreserved
 select 1 notify from dual;----unreserved
 select 1 notnull from dual;----reserved (can be function or type name)
 select 1 nowait from dual;----unreserved
 select 1 null from dual;----reserved
 select 1 nullif from dual;----unreserved (cannot be function or type name)
 select 1 nulls from dual;----unreserved
 select 1 number from dual;----unreserved (cannot be function or type name)
 select 1 numeric from dual;----unreserved (cannot be function or type name)
 select 1 object from dual;----unreserved
 select 1 of from dual;----unreserved
 select 1 off from dual;----unreserved
 select 1 offset from dual;----reserved
 select 1 oid from dual;----unreserved
 select 1 oids from dual;----unreserved
 select 1 old from dual;----unreserved
 select 1 on from dual;----reserved
 select 1 only from dual;----reserved
 select 1 operator from dual;----unreserved
 select 1 option from dual;----unreserved
 select 1 options from dual;----unreserved
 select 1 or from dual;----reserved
 select 1 order from dual;----reserved
 select 1 ordinality from dual;----unreserved
 select 1 out from dual;----unreserved (cannot be function or type name)
 select 1 outer from dual;----reserved (can be function or type name)
 select 1 over from dual;----unreserved
 select 1 overflow from dual;----unreserved
 select 1 overlaps from dual;----reserved (can be function or type name)
 select 1 overlay from dual;----unreserved (cannot be function or type name)
 select 1 overriding from dual;----unreserved
 select 1 owned from dual;----unreserved
 select 1 owner from dual;----unreserved
 select 1 package from dual;----unreserved
 select 1 parallel from dual;----unreserved
 select 1 parser from dual;----unreserved
 select 1 partial from dual;----unreserved
 select 1 partition from dual;----reserved
 select 1 partitions from dual;----unreserved
 select 1 passing from dual;----unreserved
 select 1 password from dual;----unreserved
 select 1 pause from dual;----unreserved
 select 1 pivot from dual;----reserved
 select 1 placing from dual;----reserved
 select 1 plans from dual;----unreserved
 select 1 policy from dual;----unreserved
 select 1 position from dual;----unreserved (cannot be function or type name)
 select 1 preceding from dual;----unreserved
 select 1 precision from dual;----unreserved (cannot be function or type name)
 select 1 preferred from dual;----unreserved
 select 1 prepare from dual;----unreserved
 select 1 prepared from dual;----unreserved
 select 1 preserve from dual;----unreserved
 select 1 primary from dual;----reserved
 select 1 prior from dual;----reserved
 select 1 privileges from dual;----unreserved
 select 1 procedural from dual;----unreserved
 select 1 procedure from dual;----unreserved
 select 1 procedures from dual;----unreserved
 select 1 profile from dual;----unreserved
 select 1 program from dual;----unreserved
 select 1 publication from dual;----unreserved
 select 1 quote from dual;----unreserved
 select 1 randomly from dual;----unreserved
 select 1 range from dual;----unreserved
 select 1 read from dual;----unreserved
 select 1 real from dual;----unreserved (cannot be function or type name)
 select 1 reassign from dual;----unreserved
 select 1 rebuild from dual;----unreserved
 select 1 recheck from dual;----unreserved
 select 1 record from dual;----unreserved
 select 1 recursive from dual;----unreserved
 select 1 ref from dual;----unreserved
 select 1 references from dual;----reserved
 select 1 referencing from dual;----unreserved
 select 1 refresh from dual;----unreserved
 select 1 reindex from dual;----unreserved
 select 1 relative from dual;----unreserved
 select 1 release from dual;----unreserved
 select 1 rename from dual;----unreserved
 select 1 repeatable from dual;----unreserved
 select 1 replace from dual;----unreserved
 select 1 replica from dual;----unreserved
 select 1 reset from dual;----unreserved
 select 1 resource from dual;----unreserved
 select 1 restart from dual;----unreserved
 select 1 restrict from dual;----unreserved
 select 1 returning from dual;----reserved
 select 1 returns from dual;----unreserved
 select 1 revoke from dual;----unreserved
 select 1 right from dual;----reserved (can be function or type name)
 select 1 role from dual;----unreserved
 select 1 rollback from dual;----unreserved
 select 1 rollback_subtxn from dual;----unreserved
 select 1 rollup from dual;----unreserved
 select 1 routine from dual;----unreserved
 select 1 routines from dual;----unreserved
 select 1 row from dual;----unreserved (cannot be function or type name)
 select 1 rowid from dual;----reserved (can be function or type name)
 select 1 rows from dual;----unreserved
 select 1 rowtype from dual;----unreserved
 select 1 rule from dual;----unreserved
 select 1 sample from dual;----unreserved
 select 1 savepoint from dual;----unreserved
 select 1 schema from dual;----unreserved
 select 1 schemas from dual;----unreserved
 select 1 scroll from dual;----unreserved
 select 1 search from dual;----unreserved
 select 1 second from dual;----unreserved
 select 1 security from dual;----unreserved
 select 1 select from dual;----reserved
 select 1 sequence from dual;----unreserved
 select 1 sequences from dual;----unreserved
 select 1 serializable from dual;----unreserved
 select 1 server from dual;----unreserved
 select 1 session from dual;----unreserved
 select 1 session_user from dual;----reserved
 select 1 sessiontimezone from dual;----reserved
 select 1 set from dual;----unreserved
 select 1 setof from dual;----unreserved (cannot be function or type name)
 select 1 sets from dual;----unreserved
 select 1 sharding from dual;----unreserved
 select 1 share from dual;----unreserved
 select 1 sharing from dual;----unreserved
 select 1 show from dual;----unreserved
 select 1 similar from dual;----reserved (can be function or type name)
 select 1 simple from dual;----unreserved
 select 1 skip from dual;----unreserved
 select 1 slot from dual;----unreserved
 select 1 smallint from dual;----unreserved (cannot be function or type name)
 select 1 snapshot from dual;----unreserved
 select 1 some from dual;----reserved
 select 1 sql from dual;----unreserved
 select 1 stable from dual;----unreserved
 select 1 standalone from dual;----unreserved
 select 1 start from dual;----reserved
 select 1 statement from dual;----unreserved
 select 1 statistics from dual;----unreserved
 select 1 stdin from dual;----unreserved
 select 1 stdout from dual;----unreserved
 select 1 step from dual;----unreserved
 select 1 storage from dual;----unreserved
 select 1 strict from dual;----unreserved
 select 1 strip from dual;----unreserved
 select 1 subscription from dual;----unreserved
 select 1 substring from dual;----unreserved (cannot be function or type name)
 select 1 successful from dual;----reserved
 select 1 symmetric from dual;----reserved
 select 1 sysdate from dual;----reserved
 select 1 sysid from dual;----unreserved
 select 1 system from dual;----unreserved
 select 1 systimestamp from dual;----reserved
 select 1 table from dual;----reserved
 select 1 tables from dual;----unreserved
 select 1 tablesample from dual;----reserved (can be function or type name)
 select 1 tablespace from dual;----unreserved
 select 1 opentenbase from dual;----unreserved
 select 1 temp from dual;----unreserved
 select 1 template from dual;----unreserved
 select 1 temporary from dual;----unreserved
 select 1 text from dual;----unreserved
 select 1 then from dual;----reserved
 select 1 time from dual;----unreserved (cannot be function or type name)
 select 1 timestamp from dual;----unreserved (cannot be function or type name)
 select 1 to from dual;----reserved
 select 1 trailing from dual;----reserved
 select 1 transaction from dual;----unreserved
 select 1 transform from dual;----unreserved
 select 1 treat from dual;----unreserved (cannot be function or type name)
 select 1 trigger from dual;----unreserved
 select 1 trim from dual;----unreserved (cannot be function or type name)
 select 1 true from dual;----reserved
 select 1 truncate from dual;----unreserved
 select 1 trusted from dual;----unreserved
 select 1 type from dual;----unreserved
 select 1 types from dual;----unreserved
 select 1 unbounded from dual;----unreserved
 select 1 uncommitted from dual;----unreserved
 select 1 unencrypted from dual;----unreserved
 select 1 union from dual;----reserved
 select 1 unique from dual;----reserved
 select 1 unknown from dual;----unreserved
 select 1 unlisten from dual;----unreserved
 select 1 unlock from dual;----unreserved
 select 1 unlogged from dual;----unreserved
 select 1 unpause from dual;----unreserved
 select 1 until from dual;----unreserved
 select 1 update from dual;----unreserved
 select 1 user from dual;----reserved
 select 1 using from dual;----reserved
 select 1 vacuum from dual;----unreserved
 select 1 valid from dual;----unreserved
 select 1 validate from dual;----unreserved
 select 1 validator from dual;----unreserved
 select 1 value from dual;----unreserved
 select 1 values from dual;----unreserved (cannot be function or type name)
 select 1 varchar from dual;----unreserved (cannot be function or type name)
 select 1 variadic from dual;----reserved
 select 1 varying from dual;----unreserved
 select 1 verbose from dual;----reserved (can be function or type name)
 select 1 version from dual;----unreserved
 select 1 view from dual;----unreserved
 select 1 views from dual;----unreserved
 select 1 volatile from dual;----unreserved
 select 1 when from dual;----reserved
 select 1 whenever from dual;----reserved
 select 1 where from dual;----reserved
 select 1 whitespace from dual;----unreserved
 select 1 window from dual;----reserved
 select 1 with from dual;----reserved
 select 1 within from dual;----unreserved
 select 1 without from dual;----unreserved
 select 1 work from dual;----unreserved
 select 1 wrapper from dual;----unreserved
 select 1 write from dual;----unreserved
 select 1 xml from dual;----unreserved
 select 1 xmlattributes from dual;----unreserved (cannot be function or type name)
 select 1 xmlconcat from dual;----unreserved (cannot be function or type name)
 select 1 xmlelement from dual;----unreserved (cannot be function or type name)
 select 1 xmlexists from dual;----unreserved (cannot be function or type name)
 select 1 xmlforest from dual;----unreserved (cannot be function or type name)
 select 1 xmlnamespaces from dual;----unreserved (cannot be function or type name)
 select 1 xmlparse from dual;----unreserved (cannot be function or type name)
 select 1 xmlpi from dual;----unreserved (cannot be function or type name)
 select 1 xmlroot from dual;----unreserved (cannot be function or type name)
 select 1 xmlserialize from dual;----unreserved (cannot be function or type name)
 select 1 xmltable from dual;----unreserved (cannot be function or type name)
 select 1 year from dual;----unreserved
 select 1 yes from dual;----unreserved
 select 1 zone from dual;----unreserved

select 1999 year, 12 month, 31 day from dual;

select to_char(1999) year, to_char(12) month, to_char(31) day from dual;

select 31::text day from dual;
 
select '1999' year, '12' month, '31' day from dual;

select date '2001-10-01' day from dual;

select date '2001-10-01' - integer '7' year from dual;

create table test_bb_alias(year integer, month integer, day integer);
insert into test_bb_alias values(2000, 1, 1);
insert into test_bb_alias values(1999, 12, 32);

select year month, month year, day from test_bb_alias;

create table test_aa_alias(aa integer, bb integer);
insert into test_aa_alias values(1999, 11);
select aa year, bb month from test_aa_alias;


select a.year,a.month, b.year, b.month, b.day from (select aa year, bb month from test_aa_alias) as a, test_bb_alias b where a.year=b.year;

select a.year day,a.month year, b.year, b.month, b.day from (select aa year, bb month from test_aa_alias) as a, test_bb_alias b where a.year=b.year;

-- support percent as alias not need 'as' keyword
select 'abc' percent from dual;
select 123 percent from dual;
select 123.456 percent from dual;
select '123' || '456' percent from dual;

drop table if exists t_kw_percent;
create table t_kw_percent (id int, v varchar2(20), n number(4,2), c clob);
select id percent from t_kw_percent;
select v percent from t_kw_percent;
select n percent from t_kw_percent;
select c percent from t_kw_percent;
drop table if exists t_kw_percent;

/* -- BEGIN ID 877553047 --
 *	support case as alias not need 'as' keyword
 */
set case_as_alias to on;
create table t_kw_case_20230423 ("case" int, v varchar2(20), n number(4,2), c clob);
create table t_kw_case_20230516 ("case" int);
select 123 as case from dual;
select 123 case from dual;
select '123' || '456' as case from dual;
select '123' || '456' case from dual;

select case when n = 1 then 1 end case from t_kw_case_20230423;
select case when n = 1 then 1 end as case from t_kw_case_20230423;
select "case" case from t_kw_case_20230423;
select "case" from t_kw_case_20230423 case;

select 'aa' case from dual;
select 123 case from dual;
select 123.456::float case from dual;
select 123.456::number case from dual;
select '123' || '456' case from dual;
-- complex query
select 1 as case from dual order by case limit 1;
select CASE WHEN 1=1 THEN 'not null' ELSE NULL END CASE from dual;
select case."case" from t_kw_case_20230423 case, t_kw_case_20230516 t2 where case."case" = t2."case" and case."case" < 1;
create table t_kw_case_20230614 (id int, v varchar2(20), n number(4,2), c clob);
insert into t_kw_case_20230614 values(1, 'x', 23.06, 'a');
select id case, count(v) from t_kw_case_20230614 group by case order by case limit 1;
select case from (SELECT v as case FROM t_kw_case_20230614 where v = 'x') where case = 'x';
select v case INTO a1_20230725 from t_kw_case_20230614 where n > 2;
SELECT case, n, AVG(n) OVER (PARTITION BY case ORDER BY id) FROM (SELECT id, v case, n FROM t_kw_case_20230614);
SELECT id case FROM t_kw_case_20230614 group by case having sum(id) > 1;
select id case, v case, n case, c case, case when id is null then 'xxx' else 'yyy' end case from t_kw_case_20230614;
SELECT id, COUNT (CASE WHEN n = 1 THEN 1
                       ELSE NULL
                       END) case,
                COUNT (CASE WHEN n = 2 THEN 1
                       ELSE NULL
                       END)
    FROM t_kw_case_20230614 GROUP BY id order by case;

drop table t_kw_case_20230614;
drop table t_kw_case_20230423;
drop table t_kw_case_20230516;
reset case_as_alias;
/* -- BEGIN case TEST --*/


/*
 *	support do as alias not need 'as' keyword
 */
set do_as_alias to on;
create table t_kw_do_20230423 ("do" int, v varchar2(20), n number(4,2), c clob);
create table t_kw_do_20230516 ("do" int);
select 123 as do from dual;
select 123 do from dual;
select '123' || '456' as do from dual;
select '123' || '456' do from dual;

select case when n = 1 then 1 end do from t_kw_do_20230423;
select case when n = 1 then 1 end as do from t_kw_do_20230423;
select "do" do from t_kw_do_20230423;
select "do" from t_kw_do_20230423 do;

select 'aa' do from dual;
select 123 do from dual;
select 123.456::float do from dual;
select 123.456::number do from dual;
select '123' || '456' do from dual;
-- complex query
select 1 as do from dual order by do limit 1;
select CASE WHEN 1=1 THEN 'not null' ELSE NULL END do from dual;
select do."do" from t_kw_do_20230423 do, t_kw_do_20230516 t2 where do."do" = t2."do" and do."do" < 1;
create table t_kw_do_20230614 (id int primary key, v varchar2(20), n number(4,2), c clob);
insert into t_kw_do_20230614 values(1, 'x', 23.06, 'a');
select id do, count(v) from t_kw_do_20230614 group by do order by do limit 1;
select do from (SELECT v as do FROM t_kw_do_20230614 where v = 'x') where do = 'x';

select v do INTO a2_20230725 from t_kw_do_20230614 where n > 2;
SELECT do, n, AVG(n) OVER (PARTITION BY do ORDER BY id) FROM (SELECT id, v do, n FROM t_kw_do_20230614);
select id do, v do, n do, c do, case when id is null then 'xxx' else 'yyy' end do from t_kw_do_20230614;
SELECT id do FROM t_kw_do_20230614 group by do having sum(id) > 1;

do
$$
BEGIN
NULL;
END;
$$
/

CREATE RULE my_rule_20230725 AS
    ON INSERT TO t_kw_do_20230614
    WHERE new.n = 999
    DO INSTEAD
        INSERT INTO t_kw_do_20230614 (id, n)
        VALUES (888, 666);

drop rule my_rule_20230725 on t_kw_do_20230614;

INSERT INTO t_kw_do_20230614 (id, v)
VALUES (1, 'john')
ON CONFLICT (id) DO UPDATE
SET id = 2, v = 'doe';

INSERT INTO t_kw_do_20230614 (id, v)
VALUES (1, 'john')
ON CONFLICT (id) DO NOTHING;

drop table t_kw_do_20230614;
drop table t_kw_do_20230423;
drop table t_kw_do_20230516;
reset do_as_alias;
/* -- BEGIN do TEST --*/

/* -- BEGIN prompt as table/column name TEST --*/
create table t_kw_prompt_20230821 (prompt serial);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt int);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt smallint);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt real);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt binary_float);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt float);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt double precision);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt binary_double);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt decimal);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt dec);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt numeric);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt number);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt boolean);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt bit);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt character(10));
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt national character(10));
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt varchar(10));
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt varchar2(10));
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt nvarchar2(10));
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt raw(100));
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt blob);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt long raw);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt bfile);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt date);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt time);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt timestamp);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt timestamp with time zone);
drop table t_kw_prompt_20230821;
create table t_kw_prompt_20230821 (prompt interval year to month);
drop table t_kw_prompt_20230821;

create table prompt (id int primary key, prompt varchar2(20), n number(4,2), c clob);
insert into prompt values (10, 'good', 3.14, 'clob');
select 123 as prompt from dual;
select 123 prompt from dual;
select '123' || '456' as prompt from dual;
select '123' || '456' prompt from dual;

select case when n = 1 then 1 end prompt from prompt;
select case when n = 1 then 1 end as prompt from prompt;
select prompt from prompt;
select prompt || '++' as prompt from prompt;

-- complex query
select 1 as prompt from prompt order by prompt limit 1;
select prompt from prompt order by prompt limit 1;
select CASE WHEN 1=1 THEN 'not null' ELSE NULL END prompt from dual;
select prompt.id from prompt as prompt, prompt t2 where prompt.id = t2.id and prompt.id < 1; 
select prompt, count(n) from prompt group by prompt order by prompt limit 1;
select prompt from (SELECT prompt FROM prompt where prompt = 'good') where prompt = 'good';
SELECT prompt, max(prompt) OVER (PARTITION BY prompt ORDER BY id) FROM (SELECT id, prompt, n FROM prompt);
select id prompt, prompt, n prompt, c prompt, case when id is null then 'xxx' else 'yyy' end prompt from prompt;

-- opentenbase_ora PROMPT usage
-- tp1: direct use of prompt
prompt    'abcdef'  "1234 "                          ;
prompt select aaa from dual;
prompt abc 1000 1000 1200 1000 ;
prompt "abc 1000 1000 1200 1000";
prompt "abc 1000 1000 1200" 1000   'aaaa'    ;
