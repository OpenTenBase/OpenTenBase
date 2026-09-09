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

# The estimator should behave like the thing it models: linear in the number of
# rows, growing with the dimension and with m.

is($node->safe_psql("postgres", qq(
	SELECT abs(e2.estimated_bytes - 2 * e1.estimated_bytes) <= e1.estimated_bytes / 100
	FROM vector_index_memory_estimate(10000, 128, 16) e1,
	     vector_index_memory_estimate(20000, 128, 16) e2;
)), 't', 'estimate scales linearly with row count');

is($node->safe_psql("postgres", qq(
	SELECT e2.estimated_bytes - e1.estimated_bytes
	       BETWEEN 128 * 4 * 10000 AND 128 * 8 * 10000
	FROM vector_index_memory_estimate(10000, 128, 16) e1,
	     vector_index_memory_estimate(10000, 256, 16) e2;
)), 't', 'estimate grows with dimension, roughly one float4 per dimension');

is($node->safe_psql("postgres", qq(
	SELECT e2.estimated_bytes > e1.estimated_bytes
	FROM vector_index_memory_estimate(10000, 128, 8) e1,
	     vector_index_memory_estimate(10000, 128, 32) e2;
)), 't', 'estimate grows with m');

# The suggestion has to be something a user can paste into a SET
like($node->safe_psql("postgres",
	"SELECT suggested_work_mem FROM vector_index_memory_estimate(100000, 128, 16);"),
	qr/^\d+(MB|GB)$/, 'suggested maintenance_work_mem is a usable setting');

# Nonsense input is rejected rather than silently returning a bogus number
for my $case (
	['row count', 'vector_index_memory_estimate(0, 128, 16)'],
	['row count', 'vector_index_memory_estimate(-1, 128, 16)'],
	['dimension', 'vector_index_memory_estimate(1000, 0, 16)'],
	['dimension', 'vector_index_memory_estimate(1000, 99999, 16)'],
	['m', 'vector_index_memory_estimate(1000, 128, 1)'],
	['m', 'vector_index_memory_estimate(1000, 128, 999)'])
{
	my ($what, $call) = @{$case};

	my ($ret, $stdout, $stderr) =
		$node->psql("postgres", "SELECT * FROM $call;");

	isnt($ret, 0, "invalid $what is rejected: $call");
}

# Table used for the end to end checks
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(128));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY(SELECT random() FROM generate_series(1, 128))::vector(128) FROM generate_series(1, 50000) i;"
);

# The row count advice is based on pg_class.reltuples, so it needs an estimate
$node->safe_psql("postgres", "ANALYZE tst;");

# vector_index_memory_advise() reads the row count and the dimension itself
my @advice = split /\|/, $node->safe_psql("postgres", qq(
	SET maintenance_work_mem = '64MB';
	SELECT rows_est, dims, estimated_bytes > 0, fits
	FROM vector_index_memory_advise('tst');
));
is_deeply(\@advice, [ 50000, 128, 't', 'f' ],
	'advise reports the row count, dimension and that it does not fit');

# A table without a vector column is an error, not a bogus estimate
my ($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	CREATE TABLE novector (i int);
	SELECT * FROM vector_index_memory_advise('novector');
));
isnt($ret, 0, 'advise rejects a table without a vector column');

# The pre-build notice is the point of this whole exercise: the user hears
# about the problem before the build starts, not after it has slowed down.
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET maintenance_work_mem = '1MB';
	CREATE INDEX ON tst USING hnsw (v vector_l2_ops);
));
is($ret, 0, $stderr);
like($stderr, qr/hnsw index build is estimated to need/,
	'pre-build notice when maintenance_work_mem is too small');
like($stderr, qr/Set maintenance_work_mem to at least \d+(MB|GB)/,
	'pre-build notice suggests a concrete setting');
like($stderr, qr/hnsw graph no longer fits into maintenance_work_mem/,
	'the original in-build notice is still emitted as a backstop');

# Following the advice should get rid of both the warning and the spill
my $suggested = $node->safe_psql("postgres", qq(
	SELECT suggested_work_mem FROM vector_index_memory_estimate(50000, 128, 16);
));

($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	DROP INDEX tst_v_idx;
	SET maintenance_work_mem = '$suggested';
	CREATE INDEX ON tst USING hnsw (v vector_l2_ops);
	SELECT NOT flushed_to_disk FROM vector_index_build_report();
));
is($ret, 0, $stderr);
unlike($stderr, qr/hnsw index build is estimated to need/,
	"no pre-build notice once maintenance_work_mem is $suggested as advised");
unlike($stderr, qr/hnsw graph no longer fits into maintenance_work_mem/,
	"the build no longer spills to disk with maintenance_work_mem = $suggested");
is($stdout, 't', 'report confirms the graph was not flushed early');

done_testing();
