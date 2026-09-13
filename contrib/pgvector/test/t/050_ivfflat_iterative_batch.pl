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
	CREATE TABLE tst (i int4 PRIMARY KEY, v vector(3));
	SELECT setseed(0.20260911);
	INSERT INTO tst
	SELECT i, ARRAY[random(), random(), random()]::vector
	FROM generate_series(1, 100000) i;
	CREATE INDEX idx ON tst USING ivfflat (v vector_l2_ops) WITH (lists = 100);
	ANALYZE tst;
));

my $off = $node->safe_psql('postgres', q(
	SET enable_seqscan = off;
	SET ivfflat.probes = 1;
	SET ivfflat.iterative_scan = off;
	SELECT count(*) FROM (
		SELECT i FROM tst
		WHERE i % 1000 = 0
		ORDER BY v <-> (SELECT v FROM tst WHERE i = 1)
		LIMIT 10
	) filtered;
));

my $bounded = $node->safe_psql('postgres', q(
	SET enable_seqscan = off;
	SET ivfflat.probes = 1;
	SET ivfflat.iterative_scan = relaxed_order;
	SET ivfflat.max_probes = 1;
	SELECT count(*) FROM (
		SELECT i FROM tst
		WHERE i % 1000 = 0
		ORDER BY v <-> (SELECT v FROM tst WHERE i = 1)
		LIMIT 10
	) filtered;
));
is($bounded, $off, 'max_probes equal to probes preserves one-batch behavior');

my $expanded = $node->safe_psql('postgres', q(
	SET enable_seqscan = off;
	SET ivfflat.probes = 1;
	SET ivfflat.iterative_scan = relaxed_order;
	SET ivfflat.max_probes = 100;
	SELECT count(*) FROM (
		SELECT i FROM tst
		WHERE i % 1000 = 0
		ORDER BY v <-> (SELECT v FROM tst WHERE i = 1)
		LIMIT 10
	) filtered;
));
is($expanded, '10', 'expanded batches return the requested filtered rows');

my $rescans = $node->safe_psql('postgres', q(
	SET enable_seqscan = off;
	SET ivfflat.probes = 1;
	SET ivfflat.iterative_scan = relaxed_order;
	SET ivfflat.max_probes = 100;
	SELECT count(*)
	FROM (SELECT v FROM tst WHERE i IN (1, 2, 3)) queries
	CROSS JOIN LATERAL (
		SELECT i FROM tst
		WHERE i % 1000 = 0
		ORDER BY v <-> queries.v
		LIMIT 10
	) filtered;
));
is($rescans, '30', 'nested-loop rescans reset geometric batch growth');

$node->stop;
done_testing();
