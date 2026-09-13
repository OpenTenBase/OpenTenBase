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
	SELECT setseed(0.20260911);
	INSERT INTO tst
	SELECT i, ARRAY[
		random(), random(), random(), random(),
		random(), random(), random(), random()
	]::vector
	FROM generate_series(1, 20000) i;
	CREATE INDEX idx ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 20);
	ANALYZE tst;
));

subtest 'cached directory preserves results' => sub {
	my $exact = $node->safe_psql('postgres', q(
		SET enable_indexscan = off;
		SELECT string_agg(i::text, ',' ORDER BY i)
		FROM (
			SELECT i FROM tst ORDER BY v <-> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]' LIMIT 100
		) topk;
	));

	my $first = $node->safe_psql('postgres', q(
		SET enable_seqscan = off;
		SET ivfflat.probes = 20;
		SELECT string_agg(i::text, ',' ORDER BY i)
		FROM (
			SELECT i FROM tst ORDER BY v <-> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]' LIMIT 100
		) topk;
	));

	my $second = $node->safe_psql('postgres', q(
		SET enable_seqscan = off;
		SET ivfflat.probes = 20;
		SELECT string_agg(i::text, ',' ORDER BY i)
		FROM (
			SELECT i FROM tst ORDER BY v <-> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]' LIMIT 100
		) topk;
	));

	is($first, $exact, 'first scan matches exact search');
	is($second, $exact, 'repeated scan matches exact search');
};

subtest 'cached directory survives inserts and reindex' => sub {
	$node->safe_psql('postgres', q(
		INSERT INTO tst
		SELECT i, ARRAY[
			random(), random(), random(), random(),
			random(), random(), random(), random()
		]::vector
		FROM generate_series(20001, 21000) i;
	));

	my $after_insert = $node->safe_psql('postgres', q(
		SET enable_seqscan = off;
		SET ivfflat.probes = 20;
		SELECT count(*) FROM (
			SELECT i FROM tst ORDER BY v <-> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]' LIMIT 100
		) topk;
	));
	is($after_insert, '100', 'cached scan returns all rows after insert');

	$node->safe_psql('postgres', 'REINDEX INDEX idx;');
	my $after_reindex = $node->safe_psql('postgres', q(
		SET enable_seqscan = off;
		SET ivfflat.probes = 20;
		SELECT count(*) FROM (
			SELECT i FROM tst ORDER BY v <-> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]' LIMIT 100
		) topk;
	));
	is($after_reindex, '100', 'scan returns all rows after reindex');
};

$node->stop;
done_testing();
