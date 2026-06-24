CREATE TABLE test_hash (a int, b text) DISTRIBUTE BY REPLICATION;
INSERT INTO test_hash VALUES (1, 'one');
CREATE INDEX test_hash_a_idx ON test_hash USING hash (a);

\x

execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''test_hash_a_idx'', 0))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''test_hash_a_idx'', 1))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''test_hash_a_idx'', 2))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''test_hash_a_idx'', 3))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''test_hash_a_idx'', 4))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''test_hash_a_idx'', 5))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''test_hash_a_idx'', 6))';


execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''test_hash_a_idx'', 0)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''test_hash_a_idx'', 1)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''test_hash_a_idx'', 2)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''test_hash_a_idx'', 3)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''test_hash_a_idx'', 4)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''test_hash_a_idx'', 5)';


execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''test_hash_a_idx'', 0))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''test_hash_a_idx'', 1))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''test_hash_a_idx'', 2))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''test_hash_a_idx'', 3))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''test_hash_a_idx'', 4))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''test_hash_a_idx'', 5))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''test_hash_a_idx'', 0))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''test_hash_a_idx'', 1))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''test_hash_a_idx'', 2))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''test_hash_a_idx'', 3))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''test_hash_a_idx'', 4))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''test_hash_a_idx'', 5))';

execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''test_hash_a_idx'', 0))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''test_hash_a_idx'', 1))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''test_hash_a_idx'', 2))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''test_hash_a_idx'', 3))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''test_hash_a_idx'', 4))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''test_hash_a_idx'', 5))';


DROP TABLE test_hash;

create database contrib_ora_pageinspect sql mode opentenbase_ora;
\c contrib_ora_pageinspect

CREATE TABLE test_hash (a int, b text) DISTRIBUTE BY REPLICATION;
INSERT INTO test_hash VALUES (1, 'one');
CREATE INDEX test_hash_a_idx ON test_hash USING hash (a);

execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''TEST_HASH_A_IDX'', 0))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''TEST_HASH_A_IDX'', 1))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''TEST_HASH_A_IDX'', 2))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''TEST_HASH_A_IDX'', 3))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''TEST_HASH_A_IDX'', 4))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''TEST_HASH_A_IDX'', 5))';
execute direct on (datanode_1) 'SELECT hash_page_type(get_raw_page(''TEST_HASH_A_IDX'', 6))';


execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''TEST_HASH_A_IDX'', 0)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''TEST_HASH_A_IDX'', 1)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''TEST_HASH_A_IDX'', 2)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''TEST_HASH_A_IDX'', 3)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''TEST_HASH_A_IDX'', 4)';
execute direct on (datanode_1) 'SELECT * FROM hash_bitmap_info(''TEST_HASH_A_IDX'', 5)';


execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''TEST_HASH_A_IDX'', 0))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''TEST_HASH_A_IDX'', 1))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''TEST_HASH_A_IDX'', 2))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''TEST_HASH_A_IDX'', 3))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''TEST_HASH_A_IDX'', 4))';

execute direct on (datanode_1) 'SELECT magic, version, ntuples, bsize, bmsize, bmshift, maxbucket, highmask,
lowmask, ovflpoint, firstfree, nmaps, procid, spares, mapp FROM
hash_metapage_info(get_raw_page(''TEST_HASH_A_IDX'', 5))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''TEST_HASH_A_IDX'', 0))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''TEST_HASH_A_IDX'', 1))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''TEST_HASH_A_IDX'', 2))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''TEST_HASH_A_IDX'', 3))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''TEST_HASH_A_IDX'', 4))';

execute direct on (datanode_1) 'SELECT live_items, dead_items, page_size, hasho_prevblkno, hasho_nextblkno,
hasho_bucket, hasho_flag, hasho_page_id FROM
hash_page_stats(get_raw_page(''TEST_HASH_A_IDX'', 5))';

execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''TEST_HASH_A_IDX'', 0))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''TEST_HASH_A_IDX'', 1))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''TEST_HASH_A_IDX'', 2))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''TEST_HASH_A_IDX'', 3))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''TEST_HASH_A_IDX'', 4))';
execute direct on (datanode_1) 'SELECT * FROM hash_page_items(get_raw_page(''TEST_HASH_A_IDX'', 5))';


DROP TABLE test_hash;
