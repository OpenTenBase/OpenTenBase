\c opentenbase_ora_package_function_regression_ora
\set ECHO none
set client_min_messages TO error;
select NODE_NAME, NODE_TYPE, node_host, nodeis_primary, nodeis_preferred, node_plane_name from pgxc_node order by 1;
CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;
set client_min_messages TO default;
