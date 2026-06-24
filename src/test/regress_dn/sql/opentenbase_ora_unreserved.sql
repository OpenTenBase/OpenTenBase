\c regression_ora
-- check as table name
do
    $$
    declare
    idx int;
    create_str varchar(512) := '';
    drop_str   varchar(200) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        for idx in 1 .. 134
        loop
            create_str = 'create table ';
            drop_str = 'Drop table if exists ';
            create_str = create_str || skeys[idx];
            create_str = create_str || '(i int)';
            drop_str = drop_str || skeys[idx];
            --raise notice '%',create_str;
            --raise notice '%',drop_str;
            begin
                execute(drop_str);
                execute(create_str);
                execute(drop_str);
            exception
                when syntax_error then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
    end;
    $$;

--check as col name
do
    $$
    declare
    idx int;
    create_str varchar(512) := '';
    drop_str   varchar(200) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        for idx in 1 .. 134
        loop
            create_str = 'create table t(';
            drop_str = 'Drop table if exists t';
            create_str = create_str ||  skeys[idx] || ' int)';
            drop_str = drop_str;
            begin
                execute(drop_str);
                execute(create_str);
                execute(drop_str);
            exception
                when syntax_error then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
    end;
    $$;

--check as col alias with as
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        for idx in 1 .. 134
        loop
            sel_str = 'select 1 as ';
            sel_str = sel_str ||  skeys[idx] || ' from dual';
            begin
                execute(sel_str);
            exception
                when others then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
    end;
    $$;

--check as col alias without as
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        for idx in 1 .. 134
        loop
            sel_str = 'select 1 ';
            sel_str = sel_str ||  skeys[idx] || ' from dual';
            begin
                execute(sel_str);
            exception
                when others then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
    end;
    $$;
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        for idx in 1 .. 134
        loop
            sel_str = 'select 1 ';
            sel_str = sel_str ||  skeys[idx] || ' from dual';
            begin
                execute(sel_str);
            exception
                when others then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
    end;
    $$;

--check as table alias with as
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        for idx in 1 .. 134
        loop
            sel_str = 'select 1 from dual as ';
            sel_str = sel_str ||  skeys[idx];
            begin
                execute(sel_str);
            exception
                when others then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
    end;
    $$;

--check as table alias without as
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        for idx in 1 .. 134
        loop
            sel_str = 'select 1 from dual ';
            sel_str = sel_str ||  skeys[idx];
            begin
                execute(sel_str);
            exception
                when syntax_error or reserved_name then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
    end;
    $$;

--check as insert alias with as
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        sel_str = 'drop table if exists test_insert';
        execute sel_str;
        sel_str = 'create table test_insert(i int default 1, j int)';
        execute sel_str;
        for idx in 1 .. 134
        loop
            sel_str = 'insert into test_insert as ';
            sel_str = sel_str ||  skeys[idx] || '(' || skeys[idx] || '.j) values (1)';
            begin
                execute(sel_str);
            exception
                when others then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
        sel_str = 'drop table if exists test_insert';
        execute sel_str;
    end;
    $$;

--check as insert alias without as
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        sel_str = 'drop table if exists test_insert';
        execute sel_str;
        sel_str = 'create table test_insert(i int default 1, j int)';
        execute sel_str;
        for idx in 1 .. 134
        loop
            sel_str = 'insert into test_insert ';
            sel_str = sel_str ||  skeys[idx] || '(' || skeys[idx] || '.j) values (1)';
            begin
                execute(sel_str);
            exception
                when others then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
        sel_str = 'drop table if exists test_insert';
        execute sel_str;
    end;
    $$;

--check as update alias with as
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        sel_str = 'drop table if exists test_insert';
        execute sel_str;
        sel_str = 'create table test_insert(i int default 1, j int)';
        execute sel_str;
        for idx in 1 .. 134
        loop
            sel_str = 'update test_insert as ';
            sel_str = sel_str ||  skeys[idx] || ' set ' || skeys[idx] || '.j=1';
            begin
                execute(sel_str);
            exception
                when others then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
        sel_str = 'drop table if exists test_insert';
        execute sel_str;
    end;
    $$;

--check as update alias without as
do
    $$
    declare
    idx int;
    sel_str varchar(512) := '';
    skeys varchar(64)[] := array[
		'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
        'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
        'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

        'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

         'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                 'overlaps', 'right',

          'body',

          'bigint','bit','boolean','inout','out','overlay','setof','substring',
          'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
          'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
    begin
        sel_str = 'drop table if exists test_insert';
        execute sel_str;
        sel_str = 'create table test_insert(i int default 1, j int)';
        execute sel_str;
        for idx in 1 .. 134
        loop
            sel_str = 'update test_insert ';
            sel_str = sel_str ||  skeys[idx] || ' set ' || skeys[idx] || '.j=1';
            begin
                execute(sel_str);
            exception
                when others then
                raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
            end;
        end loop;
        sel_str = 'drop table if exists test_insert';
        execute sel_str;
    end;
    $$;
do
        $$
        declare
        idx int;
        sel_str varchar(512) := '';
        skeys varchar(64)[] := array[
                    'binary_double', 'binary_float', 'byte', 'character', 'coalesce','dec', 'extract', 'greatest','grouping','grouping_id','int',
            'interval', 'least', 'national','nchar','none','nullif','numeric','position','precision','real','row','time','timestamp','treat','trim',
            'xmlattributes','xmlconcat','xmlelement','xmlexists','xmlforest','xmlnamespaces','xmlparse','xmlpi','xmlroot','xmlserialize','xmltable',

            'analyze', 'array', 'audit', 'authenticated', 'both','case', 'cast', 'collate','column','connect_by_isleaf','connect_by_root',
                    'constraint', 'current_date','current_time','current_timestamp','current_user','dbtimezone','deferrable','end','except',
                    'false', 'fetch', 'foreign','initially','lateral','leading','level','limit','link','localtime','localtimestamp','noaudit',
                    'nocycle','noorder','offset','only','pivot','primary','references','returning','rownum','sessiontimezone','some','successful',
                    'sys_connect_by_path','sysdate','systimestamp','trailing','true','uid','unpivot','user','using','when','whenever',

             'authorization', 'binary', 'collation', 'cross', 'current_schema','full', 'inner', 'join','left','natural','outer',
                     'overlaps', 'right',

              'body',

              'bigint','bit','boolean','inout','out','overlay','setof','substring',
              'analyse','asymmetric','current_catalog','current_role','do','placing','session_user','shardcluster','symmetric','variadic','window',
              'concurrently','freeze','ilike','isnull','notnull','similar','stashmerge','tablesample','verbose'];
        begin
            for idx in 1 .. 134
            loop
                sel_str = 'select '|| skeys[idx] || '.dummy from dual as '|| skeys[idx] ||';';
                begin
                    execute(sel_str);
                exception
                    when syntax_error or reserved_name then
                    raise notice '%', ('Failed keyword:' || skeys[idx] || ',' || sqlerrm );
                end;
            end loop;
        end;
        $$;
-- test cases, duplicate gram
drop table if exists left;
create table left (i int, j int);
select left.i left from left left join left left1 on left.i = left1.j;
select left.i as left from left left left join left left1 on left.i = left1.j;
drop table left;

select * from dual where trim('1    ') = '1';
select trim('1    ') from dual;
--bug 117819729
select ('a' || trim(TRAILING from ' dylan ') || 'a') TRAILING from dual;
select ('a' || trim(LEADING from ' dylan ') || 'a') LEADING from dual;
select ('a' || trim(BOTH from ' dylan ') || 'a') BOTH from dual;

select 1 prompt from dual;
select 1+1 prompt from dual;
select 2! prompt from dual;
CREATE DATABASE LINK t_link_abc CONNECT TO "user1" identified by '' USING '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 127.0.0.1)(PORT = 11000))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = orcl)))';
create database link template template0_ora;
drop database link;
drop database link link;

drop table if exists binary;
create table binary (i int);
copy binary binary from  'test';
copy binary from  'test';
drop table binary;

--bug 120619041
CREATE TABLE keyword_departments_20240117 (
  id int primary key,
  name varchar2(50),
  parent_id int
);

INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (1, 'Head Office', null);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (2, 'Sale', 1);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (3, 'Marketing', 1);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (4, 'IT', 1);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (5, 'Sale East', 2);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (6, 'Sale West', 2);

-- 作为列别名，order by 会报错
select id using from keyword_departments_20240117 order by using;
select id using from keyword_departments_20240117 order by using using >;
select id using from keyword_departments_20240117 order by using using operator(<);
select id using from keyword_departments_20240117 order by using using > nulls last;
select id using from keyword_departments_20240117 order by using using < nulls first;

-- 作为表别名，order by 会报错
select id from keyword_departments_20240117 using order by using.id;

-- 作为对比，若出现在 group by 中就不报错
select id using from keyword_departments_20240117 group by using order by id;
select id from keyword_departments_20240117 using group by using.id order by id;

select (USING.id+123) from keyword_departments_20240117 as USING group by (USING.id+123) order by (USING.id+123) desc;
select (USING.id+123) from keyword_departments_20240117 USING group by (USING.id+123) order by (USING.id+123) desc;

select (USING.id+123) from keyword_departments_20240117 as USING group by (USING.id+123) order by (USING.id+123) using <;
select (USING.id+123) from keyword_departments_20240117 as USING group by (USING.id+123) order by (USING.id+123) using < nulls last;
select (USING.id+123) from keyword_departments_20240117 as USING group by (USING.id+123) order by (USING.id+123) using =;
select (USING.id+123) from keyword_departments_20240117 as USING group by (USING.id+123) order by (USING.id+123) using operator(<) nulls last;
select (USING.id+123) from keyword_departments_20240117 as USING group by (USING.id+123) order by (USING.id+123) using operator(>);

--bug 120604297
drop table if exists t_variadic;
create table t_variadic(c1 int);
insert into t_variadic values(1);
select (VARIADIC.c1+123) from t_variadic VARIADIC;
select AVG(VARIADIC.c1) from t_variadic VARIADIC;

--bug 126367295
--assert core
select * from dual offset (+0);
-- bug offset/array kw in some complex query
drop table keyword_departments_20240117;
drop table keyword_employees_20240117;
drop table keyword_sales_20240117;
CREATE TABLE keyword_departments_20240117 (
  id int primary key,
  name varchar2(50),
  parent_id int
);

CREATE TABLE keyword_employees_20240117 (
    employee_id int primary key,
    first_name varchar2(50) NOT NULL,
    last_name varchar2(50) NOT NULL,
    department_id int,
    salary number,
    FOREIGN KEY (department_id) REFERENCES keyword_departments_20240117(id)
);

CREATE TABLE keyword_sales_20240117 (
  id NUMBER PRIMARY KEY,
  keyword_sales_20240117_rep VARCHAR2(50),
  amount NUMBER
);

INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (1, 'Head Office', null);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (2, 'Sale', 1);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (3, 'Marketing', 1);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (4, 'IT', 1);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (5, 'Sale East', 2);
INSERT INTO keyword_departments_20240117 (id, name, parent_id) VALUES (6, 'Sale West', 2);

INSERT INTO keyword_employees_20240117 (employee_id, first_name, last_name, department_id, salary) VALUES (1, 'John', 'Doe', 1, 50000.00);
INSERT INTO keyword_employees_20240117 (employee_id, first_name, last_name, department_id, salary) VALUES (2, 'Jane', 'Smith', 1, 60000.00);
INSERT INTO keyword_employees_20240117 (employee_id, first_name, last_name, department_id, salary) VALUES (3, 'Bob', 'Johnson', 2, 7000.00);
INSERT INTO keyword_employees_20240117 (employee_id, first_name, last_name, department_id, salary) VALUES (4, 'David', 'Smith', 2, 7500.00);
INSERT INTO keyword_employees_20240117 (employee_id, first_name, last_name, department_id, salary) VALUES (5, 'Alice', 'Williams', 3, 8000.00);
INSERT INTO keyword_employees_20240117 (employee_id, first_name, last_name, department_id, salary) VALUES (6, 'Alice', 'Johnson', 4, 10000.00);

INSERT INTO keyword_sales_20240117 (id, keyword_sales_20240117_rep, amount) VALUES (1, 'Alice', 1000);
INSERT INTO keyword_sales_20240117 (id, keyword_sales_20240117_rep, amount) VALUES (2, 'Alice', 2000);
INSERT INTO keyword_sales_20240117 (id, keyword_sales_20240117_rep, amount) VALUES (3, 'Alice', 3000);
INSERT INTO keyword_sales_20240117 (id, keyword_sales_20240117_rep, amount) VALUES (4, 'Bob', 1500);
INSERT INTO keyword_sales_20240117 (id, keyword_sales_20240117_rep, amount) VALUES (5, 'Bob', 2500);
INSERT INTO keyword_sales_20240117 (id, keyword_sales_20240117_rep, amount) VALUES (6, 'John', 1200);
INSERT INTO keyword_sales_20240117 (id, keyword_sales_20240117_rep, amount) VALUES (7, 'John', 2200);

-- 验证场景如下（需全部通过）：
select offset.salary from keyword_employees_20240117 as offset where offset.salary = 5000;
select offset.salary from keyword_employees_20240117 as offset group by offset.salary order by offset.salary;
select id as offset, id+123 as offset from keyword_departments_20240117 as offset;
select (offset.id+123) from keyword_departments_20240117 as offset group by (offset.id+123) order by (offset.id+123) desc;
select (offset.id+123) from keyword_departments_20240117 as offset group by offset.id order by offset.id desc;
SELECT d.id, d.name, d.offset as offset, (SELECT AVG(offset.salary) as offset FROM keyword_employees_20240117 as offset WHERE offset.department_id = d.id) as offset FROM (select id, name, id+123 as offset from keyword_departments_20240117) as d;
SELECT offset.id, offset.name, offset.offset as offset, (SELECT AVG(c.salary) as offset FROM keyword_employees_20240117 as c WHERE c.department_id = offset.id) as offset FROM (select id, name, id+123 as offset from keyword_departments_20240117) as offset;
SELECT offset.first_name, offset.last_name, d.name, offset.offset FROM (select first_name, last_name, salary as offset, department_id from keyword_employees_20240117) as offset JOIN keyword_departments_20240117 as d ON offset.department_id = d.id where offset.offset > (SELECT AVG(e2.salary) as offset FROM keyword_employees_20240117 as e2 WHERE e2.department_id = offset.department_id);
SELECT c.first_name, c.last_name, offset.name, c.offset FROM (select first_name, last_name, salary as offset, department_id from keyword_employees_20240117) as c JOIN keyword_departments_20240117 as offset ON c.department_id = offset.id where c.offset > (SELECT AVG(e2.salary) as offset FROM keyword_employees_20240117 as e2 WHERE e2.department_id = c.department_id);
SELECT c.first_name, c.last_name, d.name, c.offset FROM (select first_name, last_name, salary as offset, department_id from keyword_employees_20240117) as c JOIN keyword_departments_20240117 as d ON c.department_id = d.id where c.offset > (SELECT AVG(offset.salary) as offset FROM keyword_employees_20240117 as offset WHERE offset.department_id = c.department_id);
explain SELECT d.id, d.name, d.offset as offset, (SELECT AVG(offset.salary) as offset FROM keyword_employees_20240117 as offset WHERE offset.department_id = d.id) as offset FROM (select id, name, id+123 as offset from keyword_departments_20240117) as d;
explain SELECT c.first_name, c.last_name, offset.name, c.offset FROM (select first_name, last_name, salary as offset, department_id from keyword_employees_20240117) as c JOIN keyword_departments_20240117 as offset ON c.department_id = offset.id where c.offset > (SELECT AVG(e2.salary) as offset FROM keyword_employees_20240117 as e2 WHERE e2.department_id = c.department_id);
create view view1 as SELECT offset.first_name, offset.last_name, d.name, offset.offset FROM (select first_name, last_name, salary as offset, department_id from keyword_employees_20240117) as offset JOIN keyword_departments_20240117 as d ON offset.department_id = d.id where offset.offset > (SELECT AVG(e2.salary) as offset FROM keyword_employees_20240117 as e2 WHERE e2.department_id = offset.department_id);
create materialized view mview1 as SELECT c.first_name, c.last_name, d.name, c.offset FROM (select first_name, last_name, salary as offset, department_id from keyword_employees_20240117) as c JOIN keyword_departments_20240117 as d ON c.department_id = d.id where c.offset > (SELECT AVG(offset.salary) as offset FROM keyword_employees_20240117 as offset WHERE offset.department_id = c.department_id);
drop view view1;
drop materialized view mview1;
drop table keyword_departments_20240117;
drop table keyword_employees_20240117;
drop table keyword_sales_20240117;
