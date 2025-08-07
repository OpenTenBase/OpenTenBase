--case_1:
CREATE NODE coord999 WITH (TYPE='coordinator', HOST='172.16.0.3', PORT=15432, CLUSTER='opentenbase_cluster');
CREATE NODE coord999 WITH (TYPE='coordinator', HOST='172.16.0.3', PORT=15433, CLUSTER='opentenbase_cluster_01');
CREATE NODE coord999_1 WITH (TYPE='coordinator', HOST='172.16.0.3', PORT=15431);

--expect:
--coord999_1 and coord999 with(15432) in opentenbase_cluster, and coord999 with(15433) in opentenbase_cluster_01;
select node_name,node_type,node_port,node_host,node_plane_name from pgxc_node where node_name in ('coord999', 'coord999_1')order by oid;

--case_2:
ALTER NODE coord999 WITH (HOST='10.112.111.147', PORT=30003, CLUSTER='opentenbase_cluster');
--expect:
--error, CLUSTER could not be modified.
select node_name,node_type,node_port,node_host,node_plane_name from pgxc_node where node_name in ('coord999', 'coord999_1')order by oid;


--case_3:
ALTER NODE coord999 WITH (HOST='10.112.111.147', PORT=30003);
ALTER NODE coord999 in opentenbase_cluster_01 WITH (HOST='10.112.111.147', PORT=30003);
--expect:
--ok
select node_name,node_type,node_port,node_host,node_plane_name from pgxc_node where node_name in ('coord999', 'coord999_1')order by oid;

--case_4:
drop node coord999;
--expect:
--only coord999 in opentenbase_cluster dropped.
select node_name,node_type,node_port,node_host,node_plane_name from pgxc_node where node_name in ('coord999', 'coord999_1')order by oid;

--case_5:
drop node coord999_1 in opentenbase_cluster;
drop node coord999 in opentenbase_cluster_01;
--expect
--coord999 in opentenbase_cluster_01 dropped, coord999_1 dropped.
select node_name,node_type,node_port,node_host,node_plane_name from pgxc_node where node_name in ('coord999', 'coord999_1')order by oid;
