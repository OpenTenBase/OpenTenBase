use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

$node->safe_psql("postgres", "CREATE EXTENSION vector;");

# ---------------------------------------------------------------- scripts
#
# Both the install script and the upgrade script must define every function.
# A function that is missing from the upgrade script is the classic way users
# end up with an upgraded extension that has no functions.

my $sharedir = `pg_config --sharedir`;
chomp $sharedir;

my @functions = qw(
	vector_index_build_report
	vector_index_memory_estimate
	vector_index_memory_advise
	vector_index_recall_check
	vector_index_diagnose
);

for my $script (
	["install script", "$sharedir/extension/vector--0.8.7.sql"],
	["upgrade script", "$sharedir/extension/vector--0.8.6--0.8.7.sql"])
{
	my ($label, $path) = @{$script};

	open my $fh, '<', $path or BAIL_OUT("cannot read $path");
	my $content = do { local $/; <$fh> };
	close $fh;

	for my $fn (@functions)
	{
		like($content, qr/CREATE FUNCTION \Q$fn\E\s*\(/, "$label defines $fn");
	}
}

# And they must actually be present in the database after installing.
my $nfuncs = $node->safe_psql("postgres", qq(
	SELECT count(*) FROM pg_proc p
	JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
	JOIN pg_extension e ON d.refobjid = e.oid
	WHERE e.extname = 'vector' AND p.proname LIKE 'vector_index%';
));
is($nfuncs, scalar(@functions), 'all diagnostic functions exist after CREATE EXTENSION');

# --------------------------------------------------------------- halfvec
#
# The tuning guidance recommends halfvec for high dimensions, so the tools have
# to work on it rather than rejecting the column.

$node->safe_psql("postgres", "CREATE TABLE hv (id serial PRIMARY KEY, v halfvec(768));");
$node->safe_psql("postgres",
	"INSERT INTO hv (v) SELECT ARRAY(SELECT random()::real FROM generate_series(1, 768))::halfvec(768) FROM generate_series(1, 2000);");
$node->safe_psql("postgres", "ANALYZE hv;");

my $hv = $node->safe_psql("postgres", qq(
	SELECT dims || '|' || CASE WHEN estimated_bytes > 0 THEN 'y' ELSE 'n' END
	FROM vector_index_memory_advise('hv');
));
is($hv, '768|y', 'memory_advise() works on a halfvec column');

# The same dimensions need about half the vector memory as halfvec.
my $cmp = $node->safe_psql("postgres", qq(
	SELECT CASE WHEN
	       (vector_index_memory_estimate(100000, 768, 16, 2)).estimated_bytes
	     < (vector_index_memory_estimate(100000, 768, 16, 4)).estimated_bytes
	     THEN 'y' ELSE 'n' END;
));
is($cmp, 'y', 'bytes_per_dim = 2 estimates less memory than 4');

my ($ret, $stdout, $stderr) = $node->psql("postgres",
	"SELECT * FROM vector_index_memory_estimate(1000, 128, 16, 3);");
isnt($ret, 0, 'an unsupported bytes_per_dim is rejected');

# --------------------------------------------------- unanalyzed / tiny table

# A table that was never analyzed has no row count, which has to be reported
# rather than silently treated as zero rows. Building an index refreshes
# reltuples, so the "never analyzed" state is simulated explicitly.
$node->safe_psql("postgres", "CREATE TABLE fresh (id serial PRIMARY KEY, v vector(32));");
$node->safe_psql("postgres",
	"INSERT INTO fresh (v) SELECT ARRAY(SELECT random() FROM generate_series(1, 32))::vector(32) FROM generate_series(1, 100);");
$node->safe_psql("postgres", "CREATE INDEX fresh_idx ON fresh USING hnsw (v vector_l2_ops);");
$node->safe_psql("postgres", "UPDATE pg_class SET reltuples = -1 WHERE relname = 'fresh';");

my $stats = $node->safe_psql("postgres", qq(
	SELECT count(*) FROM vector_index_diagnose('fresh_idx')
	WHERE aspect = 'statistics' AND severity = 'warning';
));
is($stats, '1', 'an unanalyzed table is reported as a warning');

# After ANALYZE the row count is known and small enough to be called out.
$node->safe_psql("postgres", "ANALYZE fresh;");
my $tiny = $node->safe_psql("postgres", qq(
	SELECT count(*) FROM vector_index_diagnose('fresh_idx')
	WHERE aspect = 'table size' AND severity = 'info';
));
is($tiny, '1', 'a table too small to benefit is reported as info');

# Building on an empty table is valid, not an error.
$node->safe_psql("postgres", "CREATE TABLE none (id serial PRIMARY KEY, v vector(32));");
$node->safe_psql("postgres", "ANALYZE none;");
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
CREATE INDEX none_idx ON none USING hnsw (v vector_l2_ops);
SELECT CASE WHEN valid THEN 'y' ELSE 'n' END || '|' || tuples_done
FROM vector_index_build_report();
));
is($ret, 0, $stderr);
is($stdout, 'y|0', 'building on an empty table reports zero tuples');

done_testing();
