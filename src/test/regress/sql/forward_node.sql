-- Verify that FORWARD is stored by CREATE NODE and updated by ALTER NODE.
CREATE NODE regress_forward_node
WITH (TYPE = 'datanode', HOST = 'forward_host_1', PORT = 6546, FORWARD = 6548);

COPY (
    SELECT node_forward_port
      FROM pgxc_node
     WHERE node_name = 'regress_forward_node'
) TO STDOUT;

ALTER NODE regress_forward_node WITH (FORWARD = 6550);

COPY (
    SELECT node_forward_port
      FROM pgxc_node
     WHERE node_name = 'regress_forward_node'
) TO STDOUT;

-- Avoid a non-deterministic cleanup warning in the expected output.
SET client_min_messages = error;
DROP NODE regress_forward_node;
RESET client_min_messages;
