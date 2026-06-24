--
-- XC_NODE
--

-- Tests involving node DDLs related to Postgres-XL settings

-- Default values
CREATE NODE dummy_node_coordinator WITH (TYPE = 'coordinator');
CREATE NODE dummy_node_datanode WITH (TYPE = 'datanode');
SELECT node_name, node_type, node_port, node_host FROM pgxc_node
WHERE node_name IN ('dummy_node_coordinator',  'dummy_node_datanode')
ORDER BY 1;
-- test to make sure that node_id is generated correctly for the added nodes
select hashname(node_name) = node_id from pgxc_node
WHERE node_name IN ('dummy_node_coordinator',  'dummy_node_datanode');
-- Some modifications
ALTER NODE dummy_node_coordinator WITH (PORT = 5466, HOST = 'target_host_1');
ALTER NODE dummy_node_datanode WITH (PORT = 5689, HOST = 'target_host_2', TYPE = 'datanode', PREFERRED);
SELECT node_name, node_type, node_port, node_host, nodeis_preferred FROM pgxc_node
WHERE node_name IN ('dummy_node_coordinator',  'dummy_node_datanode')
ORDER BY 1;
DROP NODE dummy_node_coordinator;
DROP NODE dummy_node_datanode;

-- Check for error messages
CREATE NODE dummy_node WITH (TYPE = 'dummy'); -- fail
CREATE NODE dummy_node WITH (PORT = 6543, HOST = 'dummyhost'); -- type not specified
CREATE NODE dummy_node WITH (PORT = 99999, TYPE = 'datanode'); -- port value error
CREATE NODE dummy_node WITH (PORT = -1, TYPE = 'datanode'); -- port value error
CREATE NODE dummy_node WITH (TYPE = 'coordinator', PREFERRED = true); -- fail
ALTER NODE dummy_node WITH (PREFERRED = false); -- does not exist
DROP NODE dummy_node; -- does not exist
-- Additinal checks on type and properties
CREATE NODE dummy_node WITH (TYPE = 'datanode');
ALTER NODE dummy_node WITH (TYPE = 'coordinator');
DROP NODE dummy_node;
CREATE NODE dummy_node WITH (TYPE = 'coordinator');
ALTER NODE dummy_node WITH (PREFERRED);
ALTER NODE dummy_node WITH (PRIMARY);
ALTER NODE dummy_node WITH (TYPE = 'datanode');
DROP NODE dummy_node;

-- experiment_feature
set experiment_feature to off;
--expected error
CREATE PUBLICATION mypub_dn001 FOR ALL TABLES;
DROP PUBLICATION mypub_dn001;
CREATE SUBSCRIPTION sub_dn001 CONNECTION 'host=100.105.39.157 port=20008 user=test1 dbname=postgres' PUBLICATION mypub_dn001 WITH (connect=true, enabled=true, create_slot=true, copy_data=true, synchronous_commit=on, ignore_pk_conflict = true, parallel_number=4);
reset experiment_feature;
