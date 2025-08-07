-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION opentenbase_ora_package_function UPDATE TO '3.22'" to load this file. \quit

/* Update dbms_stats package function from 3.17 and fix identifier in the double quotes */
create or replace package body dbms_stats as
$p$
    -- GATHER_TABLE_STATS
    PROCEDURE GATHER_TABLE_STATS (ownname text, tabname text, partname text default null, estimate_percent numeric default null,
        block_sample bool default false , method_opt text default null , degree numeric default null , granularity text default null ,
        cascade bool default true, stattab text default null , statid text default null , statown text default null , no_invalidate bool default false, force bool default true) SECURITY INVOKER AS $$
    DECLARE
        l_cur_user text;
        l_cur_user_is_superuser bool;
        l_tablename text;
        l_tableowner text;
    BEGIN
        IF partname IS NOT NULL THEN
            l_tablename := partname;
        ELSE
            l_tablename := tabname;
        END IF;

        /*
         * In opentenbase_ora, test will be converted to TEST.
         * So we upper the ident while its first character is not double quote.
         */
        IF SUBSTRING(l_tablename FROM 1 FOR 1) != '"' THEN
            l_tablename := upper(l_tablename);
        END IF;

        IF SUBSTRING(ownname FROM 1 FOR 1) != '"' THEN
            ownname := upper(ownname);
        END IF;

        BEGIN
            SELECT * INTO l_cur_user FROM current_user;
            SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;
            SELECT tableowner INTO l_tableowner FROM pg_tables WHERE quote_ident(tablename) = l_tablename;
         /*
          * Add this exception to catch 'no data found' for SELECT INTO. Else the rest
          * statements are not executed.
          */
        EXCEPTION WHEN no_data_found THEN
            NULL;/* May be not found */
        END;

        IF l_tableowner IS NULL or quote_ident(l_tableowner) != ownname THEN
            RAISE EXCEPTION 'table % does not exist', l_tablename;
        ELSEIF l_cur_user_is_superuser = true or quote_ident(l_cur_user) = ownname THEN
            EXECUTE format('analyze %s', l_tablename);
        ELSE
            RAISE EXCEPTION 'User % has no privilege to analyze table %', l_cur_user, l_tablename;
        END IF;
    END;
    $$ LANGUAGE default_plsql;

    -- GATHER_DATABASE_STATS
    PROCEDURE GATHER_DATABASE_STATS (estimate_percent numeric DEFAULT NULL, block_sample bool DEFAULT FALSE, method_opt text DEFAULT NULL, degree numeric DEFAULT NULL,
        granularity text DEFAULT NULL,  cascade bool DEFAULT TRUE, stattab text DEFAULT NULL,  statid text DEFAULT NULL, options text DEFAULT 'GATHER',
        statown text DEFAULT NULL, gather_sys BOOLEAN  DEFAULT TRUE, no_invalidate BOOLEAN DEFAULT FALSE, obj_filter_list text DEFAULT NULL) SECURITY INVOKER AS $$
    DECLARE
        l_cur_user text;
        l_cur_user_is_superuser bool;
        r record;
    BEGIN
        SELECT * INTO l_cur_user FROM current_user;
        SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;

        IF l_cur_user_is_superuser = true THEN
	        analyze;
	    ELSE
	        FOR r IN
	            select tablename FROM pg_tables where tableowner = l_cur_user
            LOOP
                EXECUTE format('analyze %I', r.tablename);
            END LOOP;
        END IF;
    END;
    $$ LANGUAGE default_plsql;

    -- GET_TABLE_STATS
    CREATE OR REPLACE PROCEDURE GET_TABLE_STATS (ownname text, tabname text, partname text DEFAULT NULL, stattab text DEFAULT NULL, statid text DEFAULT NULL, numrows INOUT numeric default 0,
        numblks INOUT numeric default 0, avgrlen INOUT numeric default 0, statown text DEFAULT NULL, cachedblk INOUT numeric default 0, cachehit INOUT numeric default 0) SECURITY INVOKER AS $$
    DECLARE
        l_tableowner text;
        l_tablename text;
        l_cur_user text;
        l_cur_user_is_superuser bool;
    BEGIN
        IF partname IS NOT NULL THEN
            l_tablename := partname;
        ELSE
            l_tablename := tabname;
        END IF;

        /*
         * In opentenbase_ora, test will be converted to TEST.
         * So we upper the ident while its first character is not double quote.
         */
        IF SUBSTRING(l_tablename FROM 1 FOR 1) != '"' THEN
            l_tablename := upper(l_tablename);
        END IF;

        IF SUBSTRING(ownname FROM 1 FOR 1) != '"' THEN
            ownname := upper(ownname);
        END IF;

        BEGIN
            SELECT * INTO l_cur_user FROM current_user;
            SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;
            SELECT tableowner INTO l_tableowner FROM pg_tables WHERE quote_ident(tablename) = l_tablename;
        /*
         * Add this exception to catch 'no data found' for SELECT INTO. Else the rest
         * statements are not executed.
         */
        EXCEPTION WHEN no_data_found THEN
            NULL;/* May be not found */
        END;

        IF l_tableowner IS NULL or quote_ident(l_tableowner) != ownname THEN
            RAISE EXCEPTION 'table % does not exist', l_tablename;
        ELSEIF l_cur_user_is_superuser = true or quote_ident(l_cur_user) = ownname THEN
            select COALESCE(reltuples,0), COALESCE(relpages,0) INTO numrows, numblks from pg_class where quote_ident(relname) = l_tablename and relkind = 'r';
            select COALESCE(sum(COALESCE(avg_width,0)),0) INTO avgrlen from pg_stats where quote_ident(tablename) = l_tablename;
            select COALESCE(heap_blks_hit,0)/COALESCE(NULLIF(heap_blks_hit + heap_blks_read,0), 1) INTO cachehit from pg_statio_user_tables where quote_ident(relname) = l_tablename;
            cachedblk := cachehit * numblks;
        ELSE
            RAISE EXCEPTION 'User % has no privilege to analyze table %', l_cur_user, l_tablename;
        END IF;
    END;
    $$ LANGUAGE default_plsql;

    -- GET_COLUMN_STATS
    CREATE OR REPLACE PROCEDURE GET_COLUMN_STATS (ownname text, tabname text, colname text, partname text DEFAULT NULL, stattab text DEFAULT NULL,
        statid text DEFAULT NULL, distcnt INOUT int DEFAULT 0, density INOUT numeric DEFAULT 0, nullcnt INOUT numeric DEFAULT 0,
        srec INOUT text DEFAULT null, avgclen INOUT numeric DEFAULT 0, statown text DEFAULT NULL) SECURITY INVOKER AS $$
    DECLARE
        l_tablename text;
        l_tableowner text;
        l_cnt int;
        l_numrows int;
        l_distinct numeric;
        l_cur_user text;
        l_cur_user_is_superuser bool;
    BEGIN
        IF partname IS NOT NULL THEN
            l_tablename := partname;
        ELSE
            l_tablename := tabname;
        END IF;

        /*
         * In opentenbase_ora, test will be converted to TEST.
         * So we upper the ident while its first character is not double quote.
         */
        IF SUBSTRING(l_tablename FROM 1 FOR 1) != '"' THEN
            l_tablename := upper(l_tablename);
        END IF;

        IF SUBSTRING(ownname FROM 1 FOR 1) != '"' THEN
            ownname := upper(ownname);
        END IF;

        IF SUBSTRING(colname FROM 1 FOR 1) != '"' THEN
            colname := upper(colname);
        END IF;

        BEGIN
            SELECT * INTO l_cur_user FROM current_user;
            SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;
            SELECT tableowner INTO l_tableowner FROM pg_tables WHERE quote_ident(tablename) = l_tablename;
        /*
         * Add this exception to catch 'no data found' for SELECT INTO. Else the rest
         * statements are not executed.
         */
        EXCEPTION WHEN no_data_found THEN
            NULL;/* May be not found */
        END;

        IF l_tableowner IS NULL or quote_ident(l_tableowner) != ownname THEN
            RAISE EXCEPTION 'table % does not exist', l_tablename;
        ELSEIF l_cur_user_is_superuser = true or quote_ident(l_cur_user) = ownname THEN
            select COALESCE(count(1),0) into l_cnt from pg_stats where quote_ident(tablename) = l_tablename and quote_ident(attname) = colname;
            IF l_cnt = 1 THEN
                select COALESCE(reltuples, 0) INTO l_numrows from pg_class where quote_ident(relname) = l_tablename;
                select COALESCE(n_distinct,0) INTO l_distinct from pg_stats where quote_ident(tablename) = l_tablename and quote_ident(attname) = colname;
                IF l_distinct < 0 THEN
                    distcnt := ABS(l_distinct) * l_numrows;
                    density := 1.0/distcnt::numeric;
                ELSEIF l_distinct > 0 THEN
                    distcnt := l_distinct::int;
                    density := 1.0/distcnt::numeric;
                ELSE
                    density := 0;
                    distcnt := 0;
                END IF;
                select COALESCE(reltuples * null_frac, 0) INTO nullcnt from pg_class c, pg_stats s where c.relname = s.tablename and quote_ident(s.tablename) = l_tablename and quote_ident(s.attname) = colname;
                select COALESCE(avg_width, 0) INTO avgclen from pg_stats where quote_ident(tablename) = l_tablename and quote_ident(attname) = colname;
            ELSE
                RAISE EXCEPTION 'column % does not exist', colname;
            END IF;
        ELSE
            RAISE EXCEPTION 'User % has no privilege to analyze table %', l_cur_user, l_tablename;
        END IF;
    END;
    $$ LANGUAGE default_plsql;

    -- GET_INDEX_STATS
    CREATE OR REPLACE PROCEDURE GET_INDEX_STATS (ownname text, indname text, partname text DEFAULT NULL, stattab text DEFAULT NULL, statid text DEFAULT NULL, numrows INOUT numeric DEFAULT 0,
        numlblks INOUT numeric DEFAULT 0, numdist INOUT numeric DEFAULT 0, avglblk INOUT numeric DEFAULT 0, avgdblk INOUT numeric DEFAULT 0, clstfct INOUT numeric DEFAULT 0,
        indlevel INOUT numeric DEFAULT 0, statown text DEFAULT NULL, cachedblk INOUT numeric DEFAULT 0, cachehit INOUT numeric DEFAULT 0) SECURITY INVOKER AS $$
    DECLARE
        l_tableowner text;
        l_indexname text;
        r record;
        l_cur_user text;
        l_cur_user_is_superuser bool;
        distcnt int;
    BEGIN
        numdist := 1;
        IF partname IS NOT NULL THEN
            l_indexname := partname;
        ELSE
            l_indexname := indname;
        END IF;

        /*
         * In opentenbase_ora, test will be converted to TEST.
         * So we upper the ident while its first character is not double quote.
         */
        IF SUBSTRING(l_indexname FROM 1 FOR 1) != '"' THEN
            l_indexname := upper(l_indexname);
        END IF;

        IF SUBSTRING(ownname FROM 1 FOR 1) != '"' THEN
            ownname := upper(ownname);
        END IF;

        BEGIN
            SELECT * INTO l_cur_user FROM current_user;
            SELECT COALESCE(usesuper,false) INTO l_cur_user_is_superuser FROM pg_user where usename = l_cur_user;
            select tableowner INTO l_tableowner from pg_indexes ix, pg_tables t where ix.tablename = t.tablename and quote_ident(ix.indexname) = l_indexname;

        /*
         * Add this exception to catch 'no data found' for SELECT INTO. Else the rest
         * statements are not executed.
         */
        EXCEPTION WHEN no_data_found THEN
            NULL;/* May be not found */
        END;

        IF l_tableowner IS NULL or quote_ident(l_tableowner) != ownname THEN
            RAISE EXCEPTION 'index % does not exist', l_indexname;
        ELSEIF l_cur_user_is_superuser = true or quote_ident(l_cur_user) = ownname THEN
            select COALESCE(reltuples,0), COALESCE(relpages,0) INTO numrows, numlblks from pg_class where quote_ident(relname) = l_indexname and relkind = 'i';
            FOR r IN select COALESCE(s.n_distinct,0) n_distinct, COALESCE(s.avg_width,0) avg_width from
                            pg_class t,
                            pg_class i,
                            pg_index ix,
                            pg_attribute a,
                            pg_stats s
                        where
                            t.oid = ix.indrelid
                            and ix.indexrelid = i.oid
                            and a.attrelid = t.oid
                            and a.attnum = ANY(ix.indkey)
                            and i.relkind = 'i'
                            and quote_ident(i.relname) = l_indexname
                            and s.tablename = t.relname
                            and s.attname = a.attname
            LOOP
                IF r.n_distinct < 0 THEN
                    distcnt := ABS(r.n_distinct) * numrows;
                ELSEIF r.n_distinct > 0 THEN
                    distcnt := r.n_distinct::int;
                ELSE
                    distcnt := 0;
                END IF;
                -- numdist/avglblk
                numdist := numdist * COALESCE(NULLIF(distcnt,0), 1);
                avglblk := avglblk + r.avg_width;
            END LOOP;
            numdist := LEAST(numrows, numdist);

            -- avgdblk
            SELECT COALESCE(sum(COALESCE(s.avg_width, 0)),0) INTO avgdblk FROM
                            pg_stats s,
                            pg_class t,
                            pg_class i,
                            pg_index ix
                        WHERE
                            quote_ident(i.relname) = l_indexname
                            and i.relkind = 'i'
                            and i.oid = ix.indexrelid
                            and t.oid = ix.indrelid
                            and t.relname = s.tablename;
            select COALESCE(idx_blks_hit/COALESCE(NULLIF(idx_blks_hit + idx_blks_read,0), 1), 0) INTO cachehit from pg_statio_user_indexes where quote_ident(indexrelname) = l_indexname;
            cachedblk := cachehit * numlblks;
        ELSE
            RAISE EXCEPTION 'User % has no privilege to analyze index %', l_cur_user, l_indexname;
        END IF;
    END;
    $$ LANGUAGE default_plsql;
$p$;
GRANT USAGE ON SCHEMA DBMS_STATS TO PUBLIC;
