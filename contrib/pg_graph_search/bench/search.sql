-- pgbench 脚本：N 度搜索
\set root random(1, 19999)
SELECT count(*) FROM gbench.b_search(:root, 3, 100, 0);
