use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dim = 3;

my $array_sql = join(",", ('random()') x $dim);

# Initialize node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

# Create table and index
$node->safe_psql("postgres", "CREATE EXTENSION vector;");
$node->safe_psql("postgres", "CREATE TABLE tst (i int4, v vector($dim));");
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT i, ARRAY[$array_sql] FROM generate_series(1, 100000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING ivfflat (v vector_l2_ops);");

# Get size
my $size = $node->safe_psql("postgres", "SELECT pg_total_relation_size('tst_v_idx');");

# Store values
$node->safe_psql("postgres", "CREATE TABLE tmp AS SELECT * FROM tst;");

# Delete all, vacuum, and insert same data
$node->safe_psql("postgres", "DELETE FROM tst;");
$node->safe_psql("postgres", "VACUUM tst;");
$node->safe_psql("postgres", "INSERT INTO tst SELECT * FROM tmp;");

# Check size
my $new_size = $node->safe_psql("postgres", "SELECT pg_total_relation_size('tst_v_idx');");
is($size, $new_size, "size does not change");

done_testing();
