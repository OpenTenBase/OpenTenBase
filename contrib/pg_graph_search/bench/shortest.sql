-- pgbench 脚本：最短路径
\set p1 random(1, 19999)
\set p2 random(1, 19999)
SELECT o_depth FROM gbench.b_shortest_path(:p1, :p2, 4);
