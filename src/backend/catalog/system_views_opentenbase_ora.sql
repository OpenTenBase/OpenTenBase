/*
 * PostgreSQL System Views
 *
 * Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * src/backend/catalog/system_views_opentenbase_ora.sql
 *
 * Note: this file is read in single-user -j mode, which means that the
 * command terminator is semicolon-newline-newline; whenever the backend
 * sees that, it stops and executes what it's got.  If you write a lot of
 * statements without empty lines between, they'll all get quoted to you
 * in any error message about one of them, so don't do that.  Also, you
 * cannot write a semicolon immediately followed by an empty line in a
 * string literal (including a function body!) or a multiline comment.
 */

-- opentenbase_ora: For length(numeric)
CREATE or REPLACE FUNCTION opentenbase_ora.length(numeric) RETURNS integer AS
$$
SELECT pg_catalog.length($1::text);
$$
LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;

/*
 * opentenbase_ora System Views
 */

-- ALL_COL_COMMENTS
CREATE VIEW all_col_comments AS
SELECT u.rolname AS owner,
       nc.nspname AS schema_name,
       c.relname AS table_name,
       a.attname AS column_name,
       pg_catalog.col_description(c.oid, a.attnum) AS comments

FROM pg_attribute a
         JOIN ((pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) JOIN pg_authid u ON c.relowner = u.oid) ON a.attrelid = c.oid
WHERE (NOT pg_is_other_temp_schema(nc.oid))

  AND a.attnum > 0 AND NOT a.attisdropped
  AND c.relkind IN ('r', 'v', 'f', 'p')

  AND (pg_has_role(c.relowner, 'USAGE')
    OR has_column_privilege(c.oid, a.attnum,
                            'SELECT, INSERT, UPDATE, REFERENCES'));
GRANT SELECT ON all_col_comments TO PUBLIC;

-- USER_COL_COMMENTS
CREATE VIEW user_col_comments AS
SELECT schema_name,
       table_name,
       column_name,
       comments
FROM all_col_comments WHERE owner = current_user;
GRANT SELECT ON user_col_comments TO PUBLIC;

-- ALL_CONS_COLUMNS
CREATE VIEW all_cons_columns AS
SELECT tblownername AS owner,
       cstrschema AS constraint_schema,
       cstrname AS constraint_name,
       tblschema AS table_schema,
       tblname AS table_name,
       colname AS column_name,
       colposition AS position

FROM (
         /* check constraints */
         SELECT DISTINCT r.oid, nr.nspname, r.relname, r.relowner, a.attname, nc.nspname, c.conname, a.attnum, u.rolname
         FROM pg_namespace nr, pg_class r, pg_attribute a, pg_depend d, pg_namespace nc, pg_constraint c, pg_authid u
         WHERE nr.oid = r.relnamespace
           AND r.oid = a.attrelid
           AND u.oid = r.relowner
           AND d.refclassid = 'pg_catalog.pg_class'::regclass
           AND d.classid = 'pg_catalog.pg_constraint'::regclass
           AND d.refobjid = r.oid
           AND d.refobjsubid = a.attnum
           AND d.objid = c.oid
           AND c.connamespace = nc.oid
           AND c.contype = 'c'
           AND r.relkind IN ('r', 'p')
           AND NOT a.attisdropped

         UNION ALL

         /* unique/primary key/foreign key constraints */
         SELECT r.oid, nr.nspname, r.relname, r.relowner, a.attname, nc.nspname, c.conname, a.attnum, u.rolname
         FROM pg_namespace nr, pg_class r, pg_attribute a, pg_namespace nc,
              pg_constraint c, pg_authid u
         WHERE nr.oid = r.relnamespace
           AND r.oid = a.attrelid
           AND r.relowner = u.oid
           AND nc.oid = c.connamespace
           AND r.oid = c.conrelid
           AND a.attnum = ANY (c.conkey)
           AND NOT a.attisdropped
           AND c.contype IN ('p', 'u', 'f')
           AND r.relkind IN ('r', 'p')

         UNION ALL

         /* not-null constraints */
         SELECT r.oid, nr.nspname, r.relname, r.relowner, a.attname, nr.nspname, CAST(nr.oid AS text) || '_' || CAST(r.oid AS text) || '_' || CAST(a.attnum AS text) || '_not_null' , -- cons_name
                a.attnum, u.rolname

         FROM pg_namespace nr, pg_class r, pg_attribute a, pg_authid u

         WHERE nr.oid = r.relnamespace
           AND r.oid = a.attrelid
           AND r.relowner = u.oid
           AND a.attnotnull
           AND a.attnum > 0
           AND NOT a.attisdropped
           AND r.relkind IN ('r', 'p')
           AND (NOT pg_is_other_temp_schema(nr.oid))

) AS x (tbloid, tblschema, tblname, tblowneroid, colname, cstrschema, cstrname, colposition, tblownername)

WHERE pg_has_role(x.tblowneroid, 'USAGE')
   OR has_table_privilege(x.tbloid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
   OR has_any_column_privilege(x.tbloid, 'SELECT, INSERT, UPDATE, REFERENCES');
GRANT SELECT ON all_cons_columns TO PUBLIC;

-- USER_CONS_COLUMNS
CREATE VIEW user_cons_columns  AS
SELECT constraint_schema,
       constraint_name,
       table_schema,
       table_name,
       column_name,
       position
FROM all_cons_columns WHERE owner = current_user;
GRANT SELECT ON user_cons_columns TO PUBLIC;

-- INTERNAL_CONSTRAINTS
CREATE VIEW internal_constraints AS
SELECT u.rolname AS owner,
       connsp.nspname AS constraint_schema,
       con.conname AS constraint_name,
       CASE con.contype WHEN 'c' THEN 'C'
                        WHEN 'f' THEN 'R'
                        WHEN 'p' THEN 'P'
                        WHEN 'u' THEN 'U' END AS constraint_type,
       rnsp.nspname AS table_schema,
       r.relname AS table_name,
       CASE con.contype WHEN 'f' THEN fu.rolname END AS r_owner,
       CASE con.contype WHEN 'f' THEN npkc.nspname END AS r_constraint_schema,
       CASE con.contype WHEN 'f' THEN pkc.conname END AS r_constraint_name,
       CASE con.confdeltype WHEN 'c' THEN 'CASCADE'
                            WHEN 'n' THEN 'SET NULL'
                            WHEN 'd' THEN 'SET DEFAULT'
                            WHEN 'r' THEN 'RESTRICT'
                            WHEN 'a' THEN 'NO ACTION' END  AS delete_rule,
       con.condeferrable AS is_deferrable,
        con.condeferred AS deferred,
		r.relowner AS rel_owner,
		r.oid AS rel_oid

    FROM pg_constraint con LEFT JOIN pg_namespace connsp ON connsp.oid = con.connamespace
        LEFT JOIN pg_class r ON r.relkind IN ('r', 'p') AND con.conrelid = r.oid
        LEFT JOIN pg_authid u ON r.relowner = u.oid
        LEFT JOIN pg_namespace rnsp ON rnsp.oid = r.relnamespace
        LEFT JOIN (pg_class fr LEFT JOIN pg_authid fu ON fu.oid = fr.relowner) ON con.confrelid = fr.oid AND con.contype NOT IN ('t', 'x')
        LEFT JOIN pg_depend d1  -- find constraint's dependency on an index
        ON d1.objid = con.oid AND d1.classid = 'pg_constraint'::regclass
            AND d1.refclassid = 'pg_class'::regclass AND d1.refobjsubid = 0
        LEFT JOIN pg_depend d2  -- find pkey/unique constraint for that index
        ON d2.refclassid = 'pg_constraint'::regclass
            AND d2.classid = 'pg_class'::regclass
            AND d2.objid = d1.refobjid AND d2.objsubid = 0
            AND d2.deptype = 'i'
        LEFT JOIN pg_constraint pkc ON pkc.oid = d2.refobjid
            AND pkc.contype IN ('p', 'u')
            AND pkc.conrelid = con.confrelid
        LEFT JOIN pg_namespace npkc ON pkc.connamespace = npkc.oid

    UNION ALL

-- not-null constraints

SELECT u.rolname AS owner,
       nr.nspname AS constraint_schema,
       CAST(nr.oid AS text) || '_' || CAST(r.oid AS text) || '_' || CAST(a.attnum AS text) || '_not_null' AS constraint_name, -- XXX
       'C' AS constraint_type,
       nr.nspname AS table_schema,
       r.relname AS table_name,
       NULL AS r_owner,
       NULL AS r_constraint_schema,
       NULL AS r_constraint_name,
       NULL AS delete_rule,
       'f'AS is_deferrable,
       'f'AS deferred,
       r.relowner AS rel_owner,
       r.oid AS rel_oid

FROM pg_namespace nr,
    pg_class r,
    pg_attribute a,
    pg_authid u

WHERE nr.oid = r.relnamespace
  AND r.oid = a.attrelid
  AND r.relowner = u.oid
  AND a.attnotnull
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND r.relkind IN ('r', 'p')
  AND (NOT pg_is_other_temp_schema(nr.oid));

CREATE VIEW dba_constraints AS
	SELECT owner, constraint_schema, constraint_name, constraint_type, table_schema,
        table_name, r_owner, r_constraint_schema, r_constraint_name, delete_rule,
        is_deferrable, deferred
	FROM internal_constraints;

GRANT SELECT ON dba_constraints TO PUBLIC;

CREATE VIEW all_constraints AS
	SELECT owner, constraint_schema, constraint_name, constraint_type, table_schema,
        table_name, r_owner, r_constraint_schema, r_constraint_name, delete_rule,
        is_deferrable, deferred
	FROM internal_constraints
    WHERE pg_has_role(rel_owner, 'USAGE')
          OR has_table_privilege(rel_oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
          OR has_any_column_privilege(rel_oid, 'SELECT, INSERT, UPDATE, REFERENCES');

GRANT SELECT ON all_constraints TO PUBLIC;

-- USER_CONSTRAINTS
CREATE VIEW user_constraints AS
SELECT constraint_schema,
       constraint_name,
       constraint_type,
       table_schema,
       table_name,
       r_owner,
       r_constraint_name,
       delete_rule,
       is_deferrable,
        deferred
        FROM all_constraints WHERE owner = current_user;
GRANT SELECT ON user_constraints TO PUBLIC;

-- ALL_TAB_COLUMNS
CREATE FUNCTION _pg_truetypid(pg_attribute, pg_type) RETURNS oid
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
AS
$$SELECT CASE WHEN $2.typtype = 'd' THEN $2.typbasetype ELSE $1.atttypid END$$;

CREATE FUNCTION _pg_truetypmod(pg_attribute, pg_type) RETURNS int4
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
AS
$$SELECT CASE WHEN $2.typtype = 'd' THEN $2.typtypmod ELSE $1.atttypmod END$$;

CREATE FUNCTION _pg_char_max_length(typid oid, typmod int4) RETURNS integer
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
AS
$$SELECT
      CASE WHEN $2 = -1 /* default typmod */
               THEN null
           WHEN $1 IN (1042, 1043) /* char, varchar */
               THEN $2 - 4
           WHEN $1 IN (1560, 1562) /* bit, varbit */
               THEN $2
           ELSE null
          END$$;

CREATE FUNCTION _pg_char_octet_length(typid oid, typmod int4) RETURNS integer
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
AS
$$SELECT
      CASE WHEN $1 IN (25, 1042, 1043) /* text, char, varchar */
               THEN CASE WHEN $2 = -1 /* default typmod */
                             THEN CAST(2^30 AS integer)
                         ELSE _pg_char_max_length($1, $2) *
                              pg_catalog.pg_encoding_max_length((SELECT encoding FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()))
END
ELSE null
          END$$;

CREATE FUNCTION _pg_numeric_precision(typid oid, typmod int4) RETURNS integer
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
AS
$$SELECT
      CASE $1
          WHEN 21 /*int2*/ THEN 16
          WHEN 23 /*int4*/ THEN 32
          WHEN 20 /*int8*/ THEN 64
          WHEN 1700 /*numeric*/ THEN
              CASE WHEN $2 = -1
                       THEN null
                   ELSE (($2 - 4) >> 16) & 65535
END
WHEN 700 /*float4*/ THEN 24 /*FLT_MANT_DIG*/
          WHEN 701 /*float8*/ THEN 53 /*DBL_MANT_DIG*/
          ELSE null
          END$$;

CREATE FUNCTION _pg_numeric_scale(typid oid, typmod int4) RETURNS integer
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
AS
$$SELECT
      CASE WHEN $1 IN (21, 23, 20) THEN 0
           WHEN $1 IN (1700) THEN
               CASE WHEN $2 = -1
                        THEN null
                    ELSE ($2 - 4) & 65535
END
ELSE null
          END$$;

CREATE FUNCTION _pg_datetime_precision(typid oid, typmod int4) RETURNS integer
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT
AS
$$SELECT
      CASE WHEN $1 IN (1082) /* date */
               THEN 0
           WHEN $1 IN (1083, 1114, 1184, 1266) /* time, timestamp, same + tz */
               THEN CASE WHEN $2 < 0 THEN 6 ELSE $2 END
           WHEN $1 IN (1186) /* interval */
               THEN CASE WHEN $2 < 0 OR $2 & 65535 = 65535 THEN 6 ELSE $2 & 65535 END
           ELSE null
          END$$;

CREATE VIEW internal_tab_columns AS
SELECT u.rolname AS owner,
       nc.nspname AS table_schema,
       c.relname AS table_name,
       a.attname AS column_name,
       CASE WHEN t.typtype = 'd' THEN
                CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
                     WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, null)
                     ELSE 'USER-DEFINED' END
            ELSE
                CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                     WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
                     ELSE 'USER-DEFINED' END
           END AS data_type,
       tu.rolname AS data_type_owner,
       coalesce(_pg_char_max_length(_pg_truetypid(a, t), _pg_truetypmod(a, t)), _pg_numeric_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t))) AS data_length,
        _pg_numeric_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t)) AS data_precision,
        _pg_numeric_scale(_pg_truetypid(a, t), _pg_truetypmod(a, t)) AS data_scale,
        CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END AS nullable,
           a.attnum AS column_id,
           pg_get_expr(ad.adbin, ad.adrelid) AS data_default,
--            length(pg_get_expr(ad.adbin, ad.adrelid)) AS default_length
--            _pg_datetime_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t)) AS datetime_precision,
--            CASE WHEN c.relkind IN ('r', 'p') OR
--                      (c.relkind IN ('v', 'f') AND
--                       pg_column_is_updatable(c.oid, a.attnum, false))
--                THEN 'YES' ELSE 'NO' END AS data_upgraded
			CASE
				WHEN s.stadistinct >= 0 THEN s.stadistinct
				WHEN s.stadistinct < 0 THEN s.stadistinct * (-1) * c.reltuples
				ELSE NULL
			END :: numeric(38,0) AS NUM_DISTINCT,
			NULL AS LOW_VALUE,
			NULL AS HIGH_VALUE,
			NULL AS DENSITY,
			c.reltuples * s.stanullfrac AS NUM_NULLS,
			CASE
				WHEN s.stakind1 = 2 THEN array_length(stavalues1, 1)
				WHEN s.stakind2 = 2 THEN array_length(stavalues2, 1)
				WHEN s.stakind3 = 2 THEN array_length(stavalues3, 1)
				WHEN s.stakind4 = 2 THEN array_length(stavalues4, 1)
				WHEN s.stakind5 = 2 THEN array_length(stavalues5, 1)
				ELSE NULL
			END AS NUM_BUCKETS,
			NULL AS LAST_ANALYZED,
			NULL AS SAMPLE_SIZE,
			CASE
				WHEN (SELECT count(*) FROM pg_type t
						WHERE oid = a.atttypid AND
								t.typname IN ('char', 'varchar', 'text', 'varchar2',
												'nvarchar2', '_varchar2', '_nvarchar2',
												'_char', '_varchar', '_text')) > 0
					THEN (SELECT setting FROM pg_settings WHERE name = 'server_encoding')
				ELSE NULL
			END AS CHARACTER_SET_NAME,
			CASE
				WHEN (SELECT count(*) FROM pg_type t
						WHERE oid = a.atttypid AND
								t.typname IN ('char', 'varchar', 'varchar2',
												'nvarchar2')) > 0
					THEN a.atttypmod - 4
				ELSE NULL
			END AS CHAR_COL_DECL_LENGTH,
			'NO' AS GLOBAL_STATS,
			'YES' AS USER_STATS,
			s.stawidth AS AVG_COL_LEN,
			CASE
				WHEN (SELECT count(*) FROM pg_type t
						WHERE oid = a.atttypid AND
								t.typname IN ('char', 'varchar', 'varchar2',
												'nvarchar2')) > 0
					THEN a.atttypmod - 4
				ELSE NULL
			END AS CHAR_LENGTH,
			'B' AS CHAR_USED,
			NULL AS V80_FMT_IMAGE,
			NULL AS DATA_UPGRADED,
			CASE
				WHEN s.stakind1 = 2 THEN 'HEIGHT BALANCED'
				WHEN s.stakind2 = 2 THEN 'HEIGHT BALANCED'
				WHEN s.stakind3 = 2 THEN 'HEIGHT BALANCED'
				WHEN s.stakind4 = 2 THEN 'HEIGHT BALANCED'
				WHEN s.stakind5 = 2 THEN 'HEIGHT BALANCED'
				ELSE
					( -- else only have MCV?
						CASE
							WHEN s.stakind1 = 2 THEN 'FREQUENCY'
							WHEN s.stakind2 = 2 THEN 'FREQUENCY'
							WHEN s.stakind3 = 2 THEN 'FREQUENCY'
							WHEN s.stakind4 = 2 THEN 'FREQUENCY'
							WHEN s.stakind5 = 2 THEN 'FREQUENCY'
						ELSE
							'NONE'
						END
					)
			END AS HISTOGRAM,
		   c.relowner AS rel_owner,
		   c.oid AS rel_oid,
		   a.attnum AS rel_attnum
    FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
         JOIN ((pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) JOIN pg_authid u ON u.oid = c.relowner) ON a.attrelid = c.oid
         JOIN ((pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) LEFT JOIN pg_authid tu ON tu.oid = t.typowner) ON a.atttypid = t.oid
         LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
                   ON (t.typtype = 'd' AND t.typbasetype = bt.oid)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
                   ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')
         LEFT JOIN (pg_depend dep JOIN pg_sequence seq ON (dep.classid = 'pg_class'::regclass AND dep.objid = seq.seqrelid AND dep.deptype = 'i'))
                   ON (dep.refclassid = 'pg_class'::regclass AND dep.refobjid = c.oid AND dep.refobjsubid = a.attnum)
         LEFT JOIN pg_statistic s ON (s.starelid = c.oid AND s.staattnum = a.attnum)

    WHERE (NOT pg_is_other_temp_schema(nc.oid))
        AND a.attnum > 0 AND NOT a.attisdropped
        AND c.relkind IN ('r', 'v', 'f', 'p');

CREATE VIEW dba_tab_columns AS
    SELECT owner, table_schema, table_name, column_name, data_type, data_type_owner,
           data_length, data_precision, data_scale, nullable, column_id,
           data_default, num_distinct, low_value, high_value, density, num_nulls,
		   num_buckets, last_analyzed, sample_size, character_set_name, char_col_decl_length,
		   global_stats, user_stats, avg_col_len, char_length, char_used, v80_fmt_image, data_upgraded, histogram
	FROM internal_tab_columns;

GRANT SELECT ON dba_tab_columns TO PUBLIC;

CREATE VIEW all_tab_columns AS
    SELECT owner, table_schema, table_name, column_name, data_type, data_type_owner,
           data_length, data_precision, data_scale, nullable, column_id,
           data_default, num_distinct, low_value, high_value, density, num_nulls,
			num_buckets, last_analyzed, sample_size, character_set_name, char_col_decl_length,
			global_stats, user_stats, avg_col_len, char_length, char_used, v80_fmt_image,
			data_upgraded, histogram
	FROM internal_tab_columns
    WHERE (pg_has_role(rel_owner, 'USAGE')
                 OR has_column_privilege(rel_oid, rel_attnum,
                     'SELECT, INSERT, UPDATE, REFERENCES'));

GRANT SELECT ON all_tab_columns TO PUBLIC;

-- USER_TAB_COLUMNS
CREATE VIEW user_tab_columns AS
	SELECT table_schema, table_name, column_name, data_type, data_type_owner,
	       data_length, data_precision, data_scale, nullable, column_id,
	       data_default, num_distinct, low_value, high_value, density, num_nulls,
		   num_buckets, last_analyzed, sample_size, character_set_name, char_col_decl_length,
		   global_stats, user_stats, avg_col_len, char_length, char_used, v80_fmt_image,
		   data_upgraded, histogram
	FROM all_tab_columns where owner = current_user;

GRANT SELECT ON user_tab_columns TO PUBLIC;

-- *_INDEXES
CREATE VIEW INTERNAL_INDEXES AS
	SELECT
		(SELECT rolname FROM pg_authid WHERE oid = idx_i.relowner) AS OWNER,
		idx_i.relname AS INDEX_NAME,
		(SELECT nspname FROM pg_namespace WHERE oid = idx_i.relnamespace) AS INDEX_NSP,
		(SELECT amname FROM pg_am WHERE oid = idx_i.relam) AS INDEX_TYPE,
		(SELECT rolname FROM pg_authid WHERE oid = idx_r.relowner) AS TABLE_OWNER,
		idx_r.relname AS TABLE_NAME,
		(SELECT nspname FROM pg_namespace WHERE oid = idx_r.relnamespace) AS TABLE_NSP,
		'TABLE' AS TABLE_TYPE,
		CASE WHEN i.indisunique THEN 'UNIQUE'
			ELSE 'NONUNIQUE'
		END AS UNIQUENESS,
		'DISABLED' AS COMPRESSION,
		NULL AS PREFIX_LENGTH,
		(SELECT spcname FROM pg_tablespace WHERE oid = idx_i.reltablespace) AS TABLESPACE_NAME,
		NULL AS INI_TRANS,
		NULL AS MAX_TRANS,
		NULL AS INITIAL_EXTENT,
		NULL AS NEXT_EXTENT,
		NULL AS MIN_EXTENTS,
		NULL AS MAX_EXTENTS,
		NULL AS PCT_INCREASE,
		10 AS PCT_THRESHOLD,
		NULL AS INCLUDE_COLUMN,
		NULL AS FREELISTS,
		NULL AS FREELIST_GROUPS,
		NULL AS PCT_FREE,
		'NO' AS LOGGING,
		0 AS BLEVEL,
		0 AS LEAF_BLOCKS,
		0 AS DISTINCT_KEYS,
		CASE WHEN i.indisunique THEN 1
			ELSE 0
		END AS AVG_LEAF_BLOCKS_PER_KEY,
		0 AS AVG_DATA_BLOCKS_PER_KEY,
		0 AS CLUSTERING_FACTOR,
		CASE WHEN i.indisvalid THEN 'VALID'
			ELSE 'UNUSABLE'
		END AS STATUS,
		idx_r.reltuples as NUM_ROWS,
		0 as SAMPLE_SIZE,
		null as LAST_ANALYZED,
		null as DEGREE,
		null as INSTANCES,
		CASE WHEN idx_i.relkind = 'I' THEN 'YES'
			ELSE 'NO'
		END AS PARTITIONED,
		CASE WHEN idx_i.relkind = 't' THEN 'Y'
			ELSE 'N'
		END AS TEMPORARY,
		'N' AS GENERATED,
		'N' AS SECONDARY,
		'DEFAULT' AS BUFFER_POOL,
		'NO' AS USER_STATS,
		NULL AS DURATION,
		0 AS PCT_DIRECT_ACCESS,
		NULL AS ITYP_OWNER,
		NULL AS ITYP_NAME,
		NULL AS PARAMETERS,
		'NO' AS GLOBAL_STATS,
		NULL AS DOMIDX_STATUS,
		NULL AS DOMIDX_OPSTATUS,
		CASE WHEN i.indexprs IS NOT NULL THEN 'ENABLED'
			ELSE NULL
		END AS FUNCIDX_STATUS,
		'NO' AS JOIN_INDEX,
		NULL AS IOT_REDUNDANT_PKEY_ELIM,
		'NO' AS DROPPED,
		idx_r.oid AS relid
	FROM pg_index i JOIN pg_class idx_i ON (i.indexrelid = idx_i.oid)
		JOIN pg_class idx_r ON (i.indrelid = idx_r.oid)
	WHERE idx_r.relkind IN ('r', 'v', 'f', 'p');

CREATE VIEW DBA_INDEXES AS
	SELECT OWNER, INDEX_NSP, INDEX_NAME, INDEX_TYPE, TABLE_OWNER, TABLE_NSP, TABLE_NAME, TABLE_TYPE, COMPRESSION, PREFIX_LENGTH,
			TABLESPACE_NAME, INI_TRANS,
			MAX_TRANS, INITIAL_EXTENT, NEXT_EXTENT, MIN_EXTENTS, MAX_EXTENTS, PCT_INCREASE, PCT_THRESHOLD,
			INCLUDE_COLUMN, FREELISTS, FREELIST_GROUPS, PCT_FREE, LOGGING, BLEVEL, LEAF_BLOCKS, DISTINCT_KEYS,
			AVG_LEAF_BLOCKS_PER_KEY, AVG_DATA_BLOCKS_PER_KEY,
			CLUSTERING_FACTOR, STATUS, NUM_ROWS, SAMPLE_SIZE, LAST_ANALYZED, DEGREE,
			INSTANCES, PARTITIONED, TEMPORARY, GENERATED, SECONDARY, BUFFER_POOL,
			USER_STATS, DURATION, PCT_DIRECT_ACCESS, ITYP_OWNER, ITYP_NAME, PARAMETERS,
			GLOBAL_STATS, DOMIDX_STATUS, DOMIDX_OPSTATUS, FUNCIDX_STATUS,
			JOIN_INDEX,	IOT_REDUNDANT_PKEY_ELIM, DROPPED
	FROM INTERNAL_INDEXES;

CREATE VIEW ALL_INDEXES AS
	SELECT OWNER, INDEX_NSP, INDEX_NAME, INDEX_TYPE, TABLE_OWNER, TABLE_NSP, TABLE_NAME, TABLE_TYPE, COMPRESSION, PREFIX_LENGTH,
			TABLESPACE_NAME, INI_TRANS, MAX_TRANS, INITIAL_EXTENT, NEXT_EXTENT, MIN_EXTENTS, MAX_EXTENTS, PCT_INCREASE, PCT_THRESHOLD,
			INCLUDE_COLUMN,	FREELISTS, FREELIST_GROUPS, PCT_FREE, LOGGING, BLEVEL, LEAF_BLOCKS, DISTINCT_KEYS,
			AVG_LEAF_BLOCKS_PER_KEY, AVG_DATA_BLOCKS_PER_KEY,
			CLUSTERING_FACTOR, STATUS, NUM_ROWS, SAMPLE_SIZE, LAST_ANALYZED, DEGREE,
			INSTANCES, PARTITIONED, TEMPORARY, GENERATED, SECONDARY, BUFFER_POOL,
			USER_STATS, DURATION, PCT_DIRECT_ACCESS, ITYP_OWNER, ITYP_NAME, PARAMETERS, GLOBAL_STATS,
			DOMIDX_STATUS, DOMIDX_OPSTATUS, FUNCIDX_STATUS, JOIN_INDEX,	IOT_REDUNDANT_PKEY_ELIM, DROPPED
	FROM INTERNAL_INDEXES WHERE has_table_privilege(relid, 'select,update,delete,truncate,references,trigger,rule,select with grant option,insert with grant option,update with grant option,delete with grant option,truncate with grant option,references with grant option,trigger with grant option,rule with grant option');

CREATE VIEW USER_INDEXES AS
	SELECT INDEX_NSP, INDEX_NAME, INDEX_TYPE, TABLE_OWNER, TABLE_NSP, TABLE_NAME, TABLE_TYPE, COMPRESSION, PREFIX_LENGTH,
		TABLESPACE_NAME, INI_TRANS, MAX_TRANS, INITIAL_EXTENT, NEXT_EXTENT, MIN_EXTENTS, MAX_EXTENTS, PCT_INCREASE, PCT_THRESHOLD,
		INCLUDE_COLUMN, FREELISTS, FREELIST_GROUPS, PCT_FREE, LOGGING, BLEVEL, LEAF_BLOCKS, DISTINCT_KEYS,
		AVG_LEAF_BLOCKS_PER_KEY, AVG_DATA_BLOCKS_PER_KEY, CLUSTERING_FACTOR, STATUS,
		NUM_ROWS, SAMPLE_SIZE, LAST_ANALYZED, DEGREE, INSTANCES, PARTITIONED, TEMPORARY, GENERATED,
		SECONDARY, BUFFER_POOL, USER_STATS, DURATION, PCT_DIRECT_ACCESS, ITYP_OWNER, ITYP_NAME,
		PARAMETERS, GLOBAL_STATS, DOMIDX_STATUS, DOMIDX_OPSTATUS, FUNCIDX_STATUS, JOIN_INDEX, IOT_REDUNDANT_PKEY_ELIM, DROPPED
	FROM INTERNAL_INDEXES WHERE OWNER = current_user;

GRANT SELECT ON DBA_INDEXES TO PUBLIC;
GRANT SELECT ON ALL_INDEXES TO PUBLIC;
GRANT SELECT ON USER_INDEXES TO PUBLIC;

-- *_IND_COLUMNS
CREATE VIEW INTERNAL_IND_COLUMNS AS
	SELECT
		(SELECT rolname FROM pg_authid WHERE oid = i_c.relowner) AS INDEX_OWNER,
		(SELECT nspname FROM pg_namespace WHERE oid = i_c.relnamespace) AS INDEX_NSP,
		(SELECT relname FROM pg_class WHERE oid = i.indexrelid) AS INDEX_NAME,
		(SELECT rolname FROM pg_authid WHERE oid = r_c.relowner) AS TABLE_OWNER,
		(SELECT nspname FROM pg_namespace WHERE oid = r_c.relnamespace) AS TABLE_NSP,
		r_c.relname AS TABLE_NAME,
		a.attname AS COLUMN_NAME,
		a.attnum AS COLUMN_POSITION,
		a.attlen AS COLUMN_LENGTH,
		CASE
			WHEN (SELECT count(*) FROM pg_type t
					WHERE oid = a.atttypid AND
							t.typname IN ('char', 'varchar', 'varchar2',
											'nvarchar2')) > 0
				THEN a.atttypmod - 4
			ELSE NULL
		END AS CHAR_LENGTH,
		CASE
			WHEN (c.i_col_opt & 1) = 1 THEN 'Y'
			ELSE 'N'
		END AS DESCEND,
		i.indrelid AS relid
	FROM pg_index i
			JOIN (SELECT pg_catalog.unnest(indkey) i_col, indexrelid, indrelid, pg_catalog.unnest(indoption) i_col_opt FROM pg_index) c ON i.indexrelid = c.indexrelid
			JOIN pg_attribute a ON a.attrelid = c.indrelid AND a.attnum = c.i_col
			JOIN pg_class i_c ON i_c.oid = i.indexrelid
			JOIN pg_class r_c ON r_c.oid = i.indrelid
	WHERE r_c.relkind IN ('r', 'v', 'f', 'p');

CREATE VIEW DBA_IND_COLUMNS AS
	SELECT
		INDEX_OWNER,
		INDEX_NSP,
		INDEX_NAME,
		TABLE_OWNER,
		TABLE_NSP,
		TABLE_NAME,
		COLUMN_NAME,
		COLUMN_POSITION,
		COLUMN_LENGTH,
		CHAR_LENGTH,
		DESCEND
	FROM INTERNAL_IND_COLUMNS;

CREATE VIEW ALL_IND_COLUMNS AS
	SELECT
		INDEX_OWNER,
		INDEX_NSP,
		INDEX_NAME,
		TABLE_OWNER,
		TABLE_NSP,
		TABLE_NAME,
		COLUMN_NAME,
		COLUMN_POSITION,
		COLUMN_LENGTH,
		CHAR_LENGTH,
		DESCEND
	FROM INTERNAL_IND_COLUMNS WHERE has_table_privilege(relid, 'select,update,delete,truncate,references,trigger,rule,select with grant option,insert with grant option,update with grant option,delete with grant option,truncate with grant option,references with grant option,trigger with grant option,rule with grant option');;

CREATE VIEW USER_IND_COLUMNS AS
SELECT
	INDEX_OWNER,
	INDEX_NSP,
	INDEX_NAME,
	TABLE_OWNER,
	TABLE_NSP,
	TABLE_NAME,
	COLUMN_NAME,
	COLUMN_POSITION,
	COLUMN_LENGTH,
	CHAR_LENGTH,
	DESCEND
FROM INTERNAL_IND_COLUMNS WHERE INDEX_OWNER = current_user OR TABLE_OWNER = current_user;

GRANT SELECT ON DBA_IND_COLUMNS TO PUBLIC;
GRANT SELECT ON ALL_IND_COLUMNS TO PUBLIC;
GRANT SELECT ON USER_IND_COLUMNS TO PUBLIC;

-- *_USERS
CREATE VIEW DBA_USERS AS
SELECT
	rolname AS USERNAME,
	oid AS USER_ID,
	NULL AS PASSWORD,
	CASE
		WHEN rolvaliduntil IS NULL OR (rolvaliduntil > now()) THEN 'OPEN'
		ELSE 'EXPIRED'
	END AS ACCOUNT_STATUS,
	rolvaliduntil AS LOCK_DATE,
	rolvaliduntil AS EXPIRY_DATE,
	NULL AS DEFAULT_TABLESPACE,
	NULL AS TEMPORARY_TABLESPACE,
	NULL AS CREATED,
	(SELECT profname FROM pg_profile WHERE oid = rolprofile) AS PROFILE,
	NULL AS INITIAL_RSRC_CONSUMER_GROUP,
	NULL AS EXTERNAL_NAME
FROM pg_authid;

CREATE VIEW USER_USERS AS
SELECT
	rolname AS USERNAME,
	oid AS USER_ID,
	CASE
		WHEN rolvaliduntil IS NULL OR (rolvaliduntil > now()) THEN 'OPEN'
		ELSE 'EXPIRED'
	END AS ACCOUNT_STATUS,
	rolvaliduntil AS LOCK_DATE,
	rolvaliduntil AS EXPIRY_DATE,
	NULL AS DEFAULT_TABLESPACE,
	NULL AS TEMPORARY_TABLESPACE,
	NULL AS CREATED,
	NULL AS INITIAL_RSRC_CONSUMER_GROUP,
	NULL AS EXTERNAL_NAME
FROM pg_authid WHERE rolname = current_user;

CREATE VIEW ALL_USERS AS
SELECT
	rolname AS USERNAME,
	oid AS USER_ID,
	NULL AS CREATED
FROM pg_authid;

-- *_SYNONYMS
CREATE VIEW DBA_SYNONYMS AS
	SELECT
	(SELECT rolname FROM pg_authid WHERE oid = synowner) AS OWNER,
	synname AS SYNONYM_NAME,
	NULL AS TABLE_OWNER,
	objname AS TABLE_NAME,
	objdblink AS DB_LINK
FROM pg_synonym;

CREATE VIEW ALL_SYNONYMS AS
	SELECT
	(SELECT rolname FROM pg_authid WHERE oid = synowner) AS OWNER,
	synname AS SYNONYM_NAME,
	NULL AS TABLE_OWNER,
	objname AS TABLE_NAME,
	objdblink AS DB_LINK
FROM pg_synonym WHERE (SELECT rolname FROM pg_authid WHERE oid = synowner) = current_user;

CREATE VIEW USER_SYNONYMS AS
	SELECT
	synname AS SYNONYM_NAME,
	NULL AS TABLE_OWNER,
	objname AS TABLE_NAME,
	objdblink AS DB_LINK
FROM pg_synonym WHERE (SELECT rolname FROM pg_authid WHERE oid = synowner) = current_user;

GRANT SELECT ON DBA_USERS TO PUBLIC;
GRANT SELECT ON USER_USERS TO PUBLIC;
GRANT SELECT ON ALL_USERS TO PUBLIC;
GRANT SELECT ON DBA_SYNONYMS TO PUBLIC;
GRANT SELECT ON ALL_SYNONYMS TO PUBLIC;
GRANT SELECT ON USER_SYNONYMS TO PUBLIC;


-- opentenbase_ora: For mod(text,integer) and mod(character varying, numeric)
CREATE OR REPLACE FUNCTION opentenbase_ora.mod(text,integer) RETURNS INTEGER AS
$$
SELECT opentenbase_ora.mod($1::character varying,$2);
$$
LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION opentenbase_ora.mod(character varying, numeric) RETURNS NUMERIC AS
$$
SELECT opentenbase_ora.mod($1::text,$2);
$$
LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION opentenbase_ora.mod(float4, numeric) RETURNS NUMERIC AS
$$
SELECT pg_catalog.mod($1, $2);
$$
LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION opentenbase_ora.mod(float8, numeric) RETURNS NUMERIC AS
$$
SELECT pg_catalog.mod($1, $2);
$$
LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION opentenbase_ora.sys_context(namespace varchar2, attribute varchar2, len number default 256)
RETURNS varchar2
AS 'sys_context'
LANGUAGE INTERNAL VOLATILE PARALLEL RESTRICTED STRICT;

CREATE OR REPLACE FUNCTION opentenbase_ora.xs_sys_context(namespace varchar2, attribute varchar2, len number default 256)
RETURNS varchar2
AS 'xs_sys_context'
LANGUAGE INTERNAL VOLATILE PARALLEL RESTRICTED STRICT;

-- We have converted the "char" to "CHAR" in opentenbase_ora mode, make more friendly for the postgresql client.
CREATE DOMAIN "pg_catalog"."char" AS "pg_catalog"."CHAR";

-- If the parameter of the string_to_array function is an empty string,
-- the empty string will become null in opentenbase_ora mode, and result in unexpected results.
CREATE OR REPLACE VIEW "pg_catalog".pg_policies AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        pol.polname AS policyname,
        CASE
            WHEN pol.polpermissive THEN
                'PERMISSIVE'
            ELSE
                'RESTRICTIVE'
        END AS permissive,
        CASE
            WHEN pol.polroles = '{0}' THEN
                string_to_array('public', '''')
            ELSE
                ARRAY
                (
                    SELECT rolname
                    FROM pg_catalog.pg_authid
                    WHERE oid = ANY (pol.polroles) ORDER BY 1
                )
        END AS roles,
        CASE pol.polcmd
            WHEN 'r' THEN 'SELECT'
            WHEN 'a' THEN 'INSERT'
            WHEN 'w' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            WHEN '*' THEN 'ALL'
        END AS cmd,
        pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS qual,
        pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check
    FROM pg_catalog.pg_policy pol
    JOIN pg_catalog.pg_class C ON (C.oid = pol.polrelid)
    LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace);
