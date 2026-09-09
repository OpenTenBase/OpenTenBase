use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

$node->safe_psql("postgres", "CREATE EXTENSION vector;");

my $hnsw = "(SELECT oid FROM pg_am WHERE amname = 'hnsw')";
my $ivfflat = "(SELECT oid FROM pg_am WHERE amname = 'ivfflat')";

# The phase numbers are reported to pg_stat_progress_create_index through the
# ambuildphasename callback, which pg_indexam_progress_phasename() calls.

my @hnsw_phases = (
	[1, 'initializing'],
	[2, 'loading tuples'],
	[3, 'flushing graph'],
	[4, 'finalizing'],
);

for my $phase (@hnsw_phases)
{
	my ($num, $name) = @{$phase};

	is($node->safe_psql("postgres",
		"SELECT pg_indexam_progress_phasename($hnsw, $num);"),
		$name, "hnsw build phase $num is $name");
}

# An out of range phase has no name
is($node->safe_psql("postgres",
	"SELECT pg_indexam_progress_phasename($hnsw, 99) IS NULL;"),
	't', 'unknown hnsw build phase has no name');

# ivfflat keeps the phases it already had
my @ivfflat_phases = (
	[2, 'performing k-means'],
	[3, 'assigning tuples'],
	[4, 'loading tuples'],
);

for my $phase (@ivfflat_phases)
{
	my ($num, $name) = @{$phase};

	is($node->safe_psql("postgres",
		"SELECT pg_indexam_progress_phasename($ivfflat, $num);"),
		$name, "ivfflat build phase $num is $name");
}

# Create table with enough rows to make the phase timings measurable
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[random(), random(), random()] FROM generate_series(1, 3000) i;"
);

# A build that fits in maintenance_work_mem should report sane numbers and
# should not have flushed the graph to disk.
my ($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET maintenance_work_mem = '32MB';
	CREATE INDEX ON tst USING hnsw (v vector_l2_ops);
	SELECT init_ms >= 0,
	       load_ms >= 0,
	       flush_ms >= 0,
	       finalize_ms >= 0,
	       total_ms >= load_ms,
	       tuples_done = 3000,
	       memory_total > 0,
	       memory_peak <= memory_total,
	       NOT flushed_to_disk,
	       valid
	FROM vector_index_build_report();
));
is($ret, 0, $stderr);
is($stdout, 't|t|t|t|t|t|t|t|t|t', 'report for a build that fits in memory');

# A build that cannot fit in maintenance_work_mem must report that the graph
# was flushed to disk, and must have spent time doing so.
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	DROP INDEX tst_v_idx;
	SET maintenance_work_mem = '64kB';
	CREATE INDEX ON tst USING hnsw (v vector_l2_ops);
	SELECT flushed_to_disk,
	       flush_ms >= 0,
	       tuples_done = 3000,
	       valid
	FROM vector_index_build_report();
));
is($ret, 0, $stderr);
is($stdout, 't|t|t|t', 'report for a build that spills to disk');

# The report is backend-local, so a fresh session has nothing to show yet
is($node->safe_psql("postgres",
	"SELECT valid FROM vector_index_build_report();"),
	'f', 'report is empty in a session that has not built an index');

done_testing();
