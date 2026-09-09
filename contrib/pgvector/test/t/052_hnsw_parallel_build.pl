use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# A parallel build is the risky path for the build report.  In parallel mode
# buildstate->graph points into the DSM segment, which DestroyParallelContext()
# unmaps, so the per-build statistics have to be collected before that happens.
# This test guards exactly that: reading the report after a parallel build must
# not crash, and the numbers must be sane.

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(32));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY(SELECT random() FROM generate_series(1, 32))::vector(32) FROM generate_series(1, 20000) i;"
);
$node->safe_psql("postgres", "ANALYZE tst;");

# The report is backend-local, so CREATE INDEX and the query have to share a
# session.
# Booleans are spelled out with CASE rather than concatenated: PostgreSQL turns
# a boolean into 'true'/'false' when it is concatenated, which is not worth
# depending on.
my ($ret, $stdout, $stderr) = $node->psql("postgres", qq(
SET max_parallel_maintenance_workers = 2;
SET maintenance_work_mem = '256MB';
CREATE INDEX ON tst USING hnsw (v vector_l2_ops);
SELECT CASE WHEN valid THEN 'y' ELSE 'n' END
    || '|' || tuples_done
    || '|' || CASE WHEN total_ms > 0 THEN 'y' ELSE 'n' END
    || '|' || CASE WHEN memory_total > 0 THEN 'y' ELSE 'n' END
    || '|' || CASE WHEN memory_peak > 0 THEN 'y' ELSE 'n' END
    || '|' || CASE WHEN memory_peak <= memory_total THEN 'y' ELSE 'n' END
FROM vector_index_build_report();
));
is($ret, 0, $stderr);
is($stdout, 'y|20000|y|y|y|y', 'report is sane after a parallel build');

# The parallel path also has to survive being asked twice in a row, since the
# shared segment is created and destroyed per build.
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
SET max_parallel_maintenance_workers = 2;
SET maintenance_work_mem = '256MB';
DROP INDEX tst_v_idx;
CREATE INDEX ON tst USING hnsw (v vector_l2_ops);
SELECT CASE WHEN valid THEN 'y' ELSE 'n' END || '|' || tuples_done
FROM vector_index_build_report();
));
is($ret, 0, $stderr);
is($stdout, 'y|20000', 'report is sane after a second parallel build');

# Diagnosing and measuring recall on an index built in parallel must work too.
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
SELECT count(*) FROM vector_index_diagnose('tst_v_idx', true)
WHERE aspect = 'recall (measured)' AND severity IN ('ok', 'info', 'warning');
));
is($ret, 0, $stderr);
is($stdout, '1', 'recall can be measured on a parallel-built index');

done_testing();
