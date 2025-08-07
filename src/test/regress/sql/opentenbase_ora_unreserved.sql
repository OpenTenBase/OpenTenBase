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
            sel_str = 'select 1 + 1 ';
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
                sel_str = 'select 1 ' || skeys[idx] || ' from dual ';
                sel_str = sel_str ||  skeys[idx] || '(' ||skeys[idx] || ')' || ' order by ' || skeys[idx] || '.' || skeys[idx];
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

-- test case for CROSS as alias
select * from (table dual) cross cross join (select * from dual) where cross.dummy='X';
select * from (table dual) cross(cross) cross join (select * from dual) where cross.cross='X';
select * from (table dual) cross join (select * from dual);
create table cross(cross int);
select * from cross cross join (table dual);

--test case for FULL/RIGHT
select * from (select * from dual) FULL right outer join (select * from dual) right on full.dummy = right.dummy;
select * from (select * from dual) FULL(full) right outer join (select * from dual) right(right) on full.full = right.right;
select * from (select * from dual) FULL(full) right join (select * from dual) right(right) on full.full = right.right;
select * from (select * from dual) FULL(full) full join (select * from dual) right(right) on full.full = right.right;
select * from (select * from dual) FULL(full) right outer join (select * from dual) right(right) on full.full = right.right;
select * from (select * from dual) FULL full join (select * from dual) right(right) on full.dummy = right.right;
select * from (select * from dual) right right outer join (select * from dual) full on full.dummy = right.dummy;
create table full(full int);
create table right(right int);
insert into full values (1),(2);
insert into right values (1),(3);
select * from full full full outer join right right on full.full = right.right order by 1;
select * from right right right join full full on full.full = right.right order by 1;
drop table full;
drop table right;

--test case for INNER
create table inner(inner int);
create table left(left int);
insert into inner values (1),(2);
insert into left values (2),(3);
select * from inner inner inner join (select * from left) left on inner.inner = left.left;
drop table inner;
drop table left;


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
