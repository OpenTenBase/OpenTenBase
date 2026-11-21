-- Copyright (c) OpenTenBase contributors.
-- A named partition scan in PostgreSQL compatibility mode.
CREATE TABLE partition_select_parent (id integer, payload text)
  PARTITION BY RANGE (id);
CREATE TABLE partition_select_low PARTITION OF partition_select_parent
  FOR VALUES FROM (0) TO (10);
CREATE TABLE partition_select_high PARTITION OF partition_select_parent
  FOR VALUES FROM (10) TO (20);
CREATE TABLE partition_select_unrelated (id integer, payload text);

INSERT INTO partition_select_parent VALUES (1, 'low'), (11, 'high');
SELECT id, payload FROM partition_select_parent PARTITION (partition_select_low);
SELECT p.id FROM partition_select_parent PARTITION (partition_select_high) AS p;
SELECT count(*) FROM partition_select_parent PARTITION (partition_select_low);
SELECT count(*) FROM partition_select_parent WHERE id = 1;
SELECT partition_select_parent.id FROM partition_select_parent PARTITION (partition_select_low);

-- Reject a table that is not the selected parent's direct partition.
SELECT * FROM partition_select_parent PARTITION (partition_select_unrelated);
SELECT * FROM partition_select_unrelated PARTITION (partition_select_low);

DROP TABLE partition_select_unrelated;
DROP TABLE partition_select_parent;
