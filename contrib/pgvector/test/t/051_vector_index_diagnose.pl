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

$node->safe_psql("postgres", "CREATE TABLE tst (v vector(32));");
# 20000 rows are used on purpose: with a tiny ef_search the recall is stably
# low on a graph this size, while on a much smaller table it fluctuates and
# would make the assertions flaky
$node->safe_psql("postgres",
	"INSERT INTO tst SELECT ARRAY(SELECT random() FROM generate_series(1, 32))::vector(32) FROM generate_series(1, 20000) i;"
);
$node->safe_psql("postgres", "CREATE INDEX ON tst USING hnsw (v vector_l2_ops);");

# --- vector_index_recall_check ---

# With the default search width the recall should be healthy
my ($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SELECT recall >= 0.9
	FROM vector_index_recall_check('tst_v_idx');
));
is($ret, 0, $stderr);
is($stdout, 't', 'recall is high with the default ef_search');

# A tiny search width must be caught, and the advice must say what to raise
my $suggestion = $node->safe_psql("postgres", qq(
	SET hnsw.ef_search = 1;
	SELECT suggestion FROM vector_index_recall_check('tst_v_idx');
));
like($suggestion, qr/SET hnsw\.ef_search/, 'low recall advice mentions hnsw.ef_search');

($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET hnsw.ef_search = 1;
	SELECT recall < 0.5 AND assessment = 'low'
	FROM vector_index_recall_check('tst_v_idx');
));
is($ret, 0, $stderr);
is($stdout, 't', 'recall check catches a tiny ef_search');

# Bad input is rejected instead of returning a meaningless number
for my $case (
	['a table', "vector_index_recall_check('tst')"],
	['zero k', "vector_index_recall_check('tst_v_idx', 0, 20)"],
	['zero samples', "vector_index_recall_check('tst_v_idx', 10, 0)"])
{
	my ($what, $call) = @{$case};

	($ret, $stdout, $stderr) = $node->psql("postgres", "SELECT * FROM $call;");
	isnt($ret, 0, "recall check rejects $what");
}

# --- vector_index_diagnose ---

# The tiny ef_search has to show up as a warning
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET hnsw.ef_search = 1;
	SELECT count(*) FROM vector_index_diagnose('tst_v_idx')
	WHERE aspect = 'ef_search' AND severity = 'warning';
));
is($ret, 0, $stderr);
is($stdout, '1', 'diagnose warns about a tiny ef_search');

# A healthy index reports no warnings at all
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SELECT count(*) FROM vector_index_diagnose('tst_v_idx')
	WHERE severity = 'warning';
));
is($ret, 0, $stderr);
cmp_ok($stdout, '==', 0, 'diagnose reports no warnings for a healthy index');

# The same call can also measure recall, so a single query gives the full
# picture: parameters, memory and the actual result quality
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET hnsw.ef_search = 1;
	SELECT count(*) FROM vector_index_diagnose('tst_v_idx', true)
	WHERE aspect = 'recall (measured)' AND severity = 'warning'
	  AND detail LIKE 'recall\@10 = 0.%';
));
is($ret, 0, $stderr);
is($stdout, '1', 'diagnose measures recall in the same call when asked');

($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SELECT count(*) FROM vector_index_diagnose('tst_v_idx', true)
	WHERE aspect = 'recall (measured)' AND severity = 'ok'
	  AND detail LIKE 'recall\@10 = 1.%';
));
is($ret, 0, $stderr);
is($stdout, '1', 'the measured recall is ok once ef_search is healthy again');

# probes = 1 on an ivfflat index is the other classic low recall trap
$node->safe_psql("postgres",
	"CREATE INDEX ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 20);");

($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	SET ivfflat.probes = 1;
	SELECT count(*) FROM vector_index_diagnose('tst_v_idx1')
	WHERE aspect = 'probes' AND severity = 'warning';
));
is($ret, 0, $stderr);
is($stdout, '1', 'diagnose warns about probes = 1');

# Only vector indexes are diagnosed
($ret, $stdout, $stderr) = $node->psql("postgres", qq(
	CREATE TABLE notvec (i int);
	CREATE INDEX ON notvec (i);
	SELECT * FROM vector_index_diagnose('notvec_i_idx');
));
isnt($ret, 0, 'diagnose rejects an index that is not hnsw or ivfflat');

done_testing();
