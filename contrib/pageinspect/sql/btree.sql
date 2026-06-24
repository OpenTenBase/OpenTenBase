CREATE TABLE test1 (a int8, b text) DISTRIBUTE BY REPLICATION;
INSERT INTO test1 VALUES (72057594037927937, 'text');
CREATE INDEX test1_a_idx ON test1 USING btree (a);

\x

execute direct on (datanode_1) 'SELECT * FROM bt_metap(''test1_a_idx'')';

execute direct on (datanode_1) 'SELECT * FROM bt_page_stats(''test1_a_idx'', 0)';
execute direct on (datanode_1) 'SELECT * FROM bt_page_stats(''test1_a_idx'', 1)';
execute direct on (datanode_1) 'SELECT * FROM bt_page_stats(''test1_a_idx'', 2)';

execute direct on (datanode_1) 'SELECT * FROM bt_page_items(''test1_a_idx'', 0)';
execute direct on (datanode_1) 'SELECT * FROM bt_page_items(''test1_a_idx'', 1)';
execute direct on (datanode_1) 'SELECT * FROM bt_page_items(''test1_a_idx'', 2)';

execute direct on (datanode_1) 'SELECT * FROM bt_page_items(get_raw_page(''test1_a_idx'', 0))';
execute direct on (datanode_1) 'SELECT * FROM bt_page_items(get_raw_page(''test1_a_idx'', 1))';
execute direct on (datanode_1) 'SELECT * FROM bt_page_items(get_raw_page(''test1_a_idx'', 2))';

DROP TABLE test1;

create database contrib_ora_pageinspect sql mode opentenbase_ora;
\c contrib_ora_pageinspect

CREATE TABLE test1 (a int8, b text) DISTRIBUTE BY REPLICATION;
INSERT INTO test1 VALUES (72057594037927937, 'text');
CREATE INDEX test1_a_idx ON test1 USING btree (a);

execute direct on (datanode_1) 'SELECT * FROM bt_metap(''TEST1_A_IDX'')';

execute direct on (datanode_1) 'SELECT * FROM bt_page_stats(''TEST1_A_IDX'', 0)';
execute direct on (datanode_1) 'SELECT * FROM bt_page_stats(''TEST1_A_IDX'', 1)';
execute direct on (datanode_1) 'SELECT * FROM bt_page_stats(''TEST1_A_IDX'', 2)';

execute direct on (datanode_1) 'SELECT * FROM bt_page_items(''TEST1_A_IDX'', 0)';
execute direct on (datanode_1) 'SELECT * FROM bt_page_items(''TEST1_A_IDX'', 1)';
execute direct on (datanode_1) 'SELECT * FROM bt_page_items(''TEST1_A_IDX'', 2)';

execute direct on (datanode_1) 'SELECT * FROM bt_page_items(get_raw_page(''TEST1_A_IDX'', 0))';
execute direct on (datanode_1) 'SELECT * FROM bt_page_items(get_raw_page(''TEST1_A_IDX'', 1))';
execute direct on (datanode_1) 'SELECT * FROM bt_page_items(get_raw_page(''TEST1_A_IDX'', 2))';

DROP TABLE test1;
