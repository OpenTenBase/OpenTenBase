CREATE TABLE test1 (x int, y int[]) DISTRIBUTE BY REPLICATION;
INSERT INTO test1 VALUES (1, ARRAY[11, 111]);
CREATE INDEX test1_y_idx ON test1 USING gin (y) WITH (fastupdate = off);

\x

execute direct on (datanode_1) 'SELECT * FROM gin_metapage_info(get_raw_page(''test1_y_idx'', 0))';
execute direct on (datanode_1) 'SELECT * FROM gin_metapage_info(get_raw_page(''test1_y_idx'', 1))';

execute direct on (datanode_1) 'SELECT * FROM gin_page_opaque_info(get_raw_page(''test1_y_idx'', 1))';

execute direct on (datanode_1) 'SELECT * FROM gin_leafpage_items(get_raw_page(''test1_y_idx'', 1))';

INSERT INTO test1 SELECT x, ARRAY[1,10] FROM generate_series(2,10000) x;

execute direct on (datanode_1) 'SELECT COUNT(*) > 0
FROM gin_leafpage_items(get_raw_page(''test1_y_idx'',
                        (pg_relation_size(''test1_y_idx'') /
                         current_setting(''block_size'')::bigint)::int - 1))';

create database contrib_ora_pageinspect sql mode opentenbase_ora;
\c contrib_ora_pageinspect

CREATE TABLE test1 (x int, y int[]) DISTRIBUTE BY REPLICATION;
INSERT INTO test1 VALUES (1, ARRAY[11, 111]);
CREATE INDEX test1_y_idx ON test1 USING gin (y) WITH (fastupdate = off);

execute direct on (datanode_1) 'SELECT * FROM gin_metapage_info(get_raw_page(''TEST1_Y_IDX'', 0))';
execute direct on (datanode_1) 'SELECT * FROM gin_metapage_info(get_raw_page(''TEST1_Y_IDX'', 1))';

execute direct on (datanode_1) 'SELECT * FROM gin_page_opaque_info(get_raw_page(''TEST1_Y_IDX'', 1))';

execute direct on (datanode_1) 'SELECT * FROM gin_leafpage_items(get_raw_page(''TEST1_Y_IDX'', 1))';

INSERT INTO test1 SELECT x, ARRAY[1,10] FROM generate_series(2,10000) x;

execute direct on (datanode_1) 'SELECT COUNT(*) > 0
FROM gin_leafpage_items(get_raw_page(''TEST1_Y_IDX'',
                        (pg_relation_size(''TEST1_Y_IDX'') /
                         current_setting(''block_size'')::bigint)::int - 1))';
