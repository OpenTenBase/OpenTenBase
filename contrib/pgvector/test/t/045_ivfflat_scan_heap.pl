use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

$node->safe_psql('postgres', q(
	CREATE EXTENSION vector;
	CREATE TABLE tst (i int4 PRIMARY KEY, v vector(8));
	SELECT setseed(0.20260825);
	INSERT INTO tst
	SELECT i, ARRAY[
		random(), random(), random(), random(),
		random(), random(), random(), random()
	]::vector
	FROM generate_series(1, 20000) i;
));

my @operators = ('<->', '<#>', '<=>');
my @opclasses = ('vector_l2_ops', 'vector_ip_ops', 'vector_cosine_ops');
my @query_ids = (7, 113, 997);

for my $metric (0 .. $#operators)
{
	my $operator = $operators[$metric];
	my $opclass = $opclasses[$metric];

	$node->safe_psql('postgres',
		"CREATE INDEX idx ON tst USING ivfflat (v $opclass) WITH (lists = 20); ANALYZE tst;");

	my $plan = $node->safe_psql('postgres', qq(
		SET enable_seqscan = off;
		SET ivfflat.probes = 20;
		EXPLAIN (COSTS OFF)
		SELECT i FROM tst ORDER BY v $operator (SELECT v FROM tst WHERE i = 7) LIMIT 10;
	));
	like($plan, qr/Index Scan using idx on tst/, "$opclass uses IVFFlat");

	for my $query_id (@query_ids)
	{
		my $exact = $node->safe_psql('postgres', qq(
			SET enable_indexscan = off;
			SELECT string_agg(i::text, ',' ORDER BY i)
			FROM (
				SELECT i FROM tst
				ORDER BY v $operator (SELECT v FROM tst WHERE i = $query_id)
				LIMIT 100
			) topk;
		));

		my $heap = $node->safe_psql('postgres', qq(
			SET enable_seqscan = off;
			SET ivfflat.probes = 20;
			SET work_mem = '64MB';
			SELECT string_agg(i::text, ',' ORDER BY i)
			FROM (
				SELECT i FROM tst
				ORDER BY v $operator (SELECT v FROM tst WHERE i = $query_id)
				LIMIT 100
			) topk;
		));
		is($heap, $exact, "$opclass in-memory heap matches exact top 100");

		my $fallback = $node->safe_psql('postgres', qq(
			SET enable_seqscan = off;
			SET ivfflat.probes = 20;
			SET work_mem = '64kB';
			SELECT string_agg(i::text, ',' ORDER BY i)
			FROM (
				SELECT i FROM tst
				ORDER BY v $operator (SELECT v FROM tst WHERE i = $query_id)
				LIMIT 100
			) topk;
		));
		is($fallback, $exact, "$opclass tuplesort fallback matches exact top 100");
	}

	my $queries = $node->safe_psql('postgres', q(
		SELECT i, v FROM tst WHERE i IN (7, 113, 997) ORDER BY i;
	));
	my @query_vectors = map { (split(/\|/, $_, 2))[1] } split(/\n/, $queries);
	my $query_number = 0;
	my $values = join(', ', map { $query_number++; "($query_number, '$_'::vector)" } @query_vectors);

	my $exact_rescan = $node->safe_psql('postgres', qq(
		SET enable_indexscan = off;
		SELECT string_agg(r.i::text, ',' ORDER BY q.n, r.distance, r.i)
		FROM (VALUES $values) AS q(n, v)
		CROSS JOIN LATERAL (
			SELECT i, v $operator q.v AS distance
			FROM tst ORDER BY distance LIMIT 10
		) r;
	));
	my $index_rescan = $node->safe_psql('postgres', qq(
		SET enable_seqscan = off;
		SET ivfflat.probes = 20;
		SET work_mem = '64MB';
		SELECT string_agg(r.i::text, ',' ORDER BY q.n, r.distance, r.i)
		FROM (VALUES $values) AS q(n, v)
		CROSS JOIN LATERAL (
			SELECT i, v $operator q.v AS distance
			FROM tst ORDER BY distance LIMIT 10
		) r;
	));
	is($index_rescan, $exact_rescan, "$opclass nested-loop rescans match exact results");

	$node->safe_psql('postgres', 'DROP INDEX idx;');
}

$node->safe_psql('postgres',
	'CREATE INDEX idx ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 20);');
my ($ret, $stdout, $stderr) = $node->psql('postgres', q(
	SET enable_seqscan = off;
	SELECT i FROM tst ORDER BY v <-> '[1,2,3]' LIMIT 1;
));
isnt($ret, 0, 'dimension mismatch fails');
like($stderr, qr/different vector dimensions 8 and 3/, 'dimension mismatch reports both dimensions');

done_testing();
