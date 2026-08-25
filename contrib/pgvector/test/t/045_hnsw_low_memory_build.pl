use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (v vector(3));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY[random(), random(), random()] FROM generate_series(1, 1000) i;"
);

my ($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET client_min_messages = DEBUG;
	SET maintenance_work_mem = '3073kB';
	ALTER TABLE tst SET (parallel_workers = 1);
	CREATE INDEX ON tst USING hnsw (v vector_l2_ops);
));
is($ret, 0, $stderr);
like($stderr, qr/using \d+ parallel workers/);
like($stderr, qr/hnsw graph no longer fits into maintenance_work_mem after 0 tuples/);

done_testing();
