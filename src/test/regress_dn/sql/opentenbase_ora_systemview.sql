/*
 * opentenbase_ora System Views
 */

\c regression_ora

\d+ all_col_comments;
select * from all_col_comments limit 0;

\d+ user_col_comments;
select * from user_col_comments limit 0;

\d+ all_cons_columns;
select * from all_cons_columns limit 0;

\d+ user_cons_columns;
select * from user_cons_columns limit 0;

\d+ internal_constraints;
select * from internal_constraints limit 0;

\d+ dba_constraints;
select * from dba_constraints limit 0;

\d+ all_constraints;
select * from all_constraints limit 0;

\d+ user_constraints;
select * from user_constraints limit 0;

\d+ internal_tab_columns;
select * from internal_tab_columns limit 0;

\d+ dba_tab_columns;
select * from dba_tab_columns limit 0;

\d+ all_tab_columns;
select * from all_tab_columns limit 0;

\d+ user_tab_columns;
select * from user_tab_columns limit 0;

\d+ INTERNAL_INDEXES;
select * from INTERNAL_INDEXES limit 0;

\d+ DBA_INDEXES;
select * from DBA_INDEXES limit 0;

\d+ ALL_INDEXES;
select * from ALL_INDEXES limit 0;

\d+ USER_INDEXES;
select * from USER_INDEXES limit 0;

\d+ INTERNAL_IND_COLUMNS;
select * from INTERNAL_IND_COLUMNS limit 0;

\d+ DBA_IND_COLUMNS;
select * from DBA_IND_COLUMNS limit 0;

\d+ ALL_IND_COLUMNS;
select * from ALL_IND_COLUMNS limit 0;

\d+ USER_IND_COLUMNS;
select * from USER_IND_COLUMNS limit 0;

\d+ DBA_USERS;
select * from DBA_USERS limit 0;

\d+ USER_USERS;
select * from USER_USERS limit 0;

\d+ ALL_USERS;
select * from ALL_USERS limit 0;

\d+ DBA_SYNONYMS;
select * from DBA_SYNONYMS limit 0;

\d+ ALL_SYNONYMS;
select * from ALL_SYNONYMS limit 0;

\d+ USER_SYNONYMS;
select * from USER_SYNONYMS limit 0;
