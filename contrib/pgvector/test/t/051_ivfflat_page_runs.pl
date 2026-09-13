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
	SELECT setseed(0.20260912);
	INSERT INTO tst
	SELECT i, ARRAY[random(), random(), random()]::vector
	FROM generate_series(1, 10000) i;
	ANALYZE tst;
));

$node->safe_psql('postgres', q(
	CREATE TABLE small_t (v vector(3));
	INSERT INTO small_t VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]');
	CREATE INDEX small_idx ON small_t USING ivfflat (v vector_l2_ops)
		WITH (lists = 3);
));

my ($dimension_error, $dimension_stdout, $dimension_stderr) = $node->psql('postgres', q(
	SET enable_seqscan = off;
	SELECT * FROM small_t ORDER BY v <-> '[1,2]' LIMIT 1;
));
isnt($dimension_error, 0, 'dimension mismatch reports an error');
like($dimension_stderr, qr/different vector dimensions 3 and 2/,
	'dimension mismatch error is preserved');

my $small_scan = $node->safe_psql('postgres', q(
	SET enable_seqscan = off;
	SET ivfflat.probes = 1;
	SET ivfflat.iterative_scan = relaxed_order;
	SELECT count(*) FROM (SELECT * FROM small_t ORDER BY v <-> '[3,3,3]') scanned;
));
is($small_scan, '3', 'unbounded iterative scan works after an earlier error');

$node->safe_psql('postgres', 'DROP TABLE small_t;');

my @cases = (
	['vector_l2_ops', '<->'],
	['vector_ip_ops', '<#>'],
	['vector_cosine_ops', '<=>'],
);

sub split_results
{
	my ($results) = @_;
	my (@ids, @distances);

	for my $line (split("\n", $results))
	{
		my ($id, $distance) = split(/\|/, $line, 2);

		push(@ids, $id);
		push(@distances, $distance);
	}

	return (\@ids, \@distances);
}

sub distances_are_ordered
{
	my ($distances, $tolerance) = @_;

	for my $i (1 .. $#$distances)
	{
		return 0 if $distances->[$i - 1] > $distances->[$i] + $tolerance;
	}

	return 1;
}

for my $case (@cases)
{
	my ($opclass, $operator) = @$case;

	$node->safe_psql('postgres', qq(
		CREATE INDEX idx ON tst USING ivfflat (v $opclass) WITH (lists = 10);
	));

	my $page_runs = $node->safe_psql('postgres', qq(
		SET enable_seqscan = off;
		SET work_mem = '64MB';
		SET ivfflat.probes = 5;
		SET ivfflat.iterative_scan = off;
		SELECT i || '|' || (v $operator (SELECT v FROM tst WHERE i = 1)) FROM tst
		ORDER BY v $operator (SELECT v FROM tst WHERE i = 1)
		LIMIT 100;
	));

	my $fallback = $node->safe_psql('postgres', qq(
		SET enable_seqscan = off;
		SET work_mem = '64kB';
		SET ivfflat.probes = 5;
		SET ivfflat.iterative_scan = off;
		SELECT i || '|' || (v $operator (SELECT v FROM tst WHERE i = 1)) FROM tst
		ORDER BY v $operator (SELECT v FROM tst WHERE i = 1)
		LIMIT 100;
	));

	my ($page_run_ids, $page_run_distances) = split_results($page_runs);
	my ($fallback_ids, $fallback_distances) = split_results($fallback);

	is_deeply([sort { $a <=> $b } @$page_run_ids],
		[sort { $a <=> $b } @$fallback_ids],
		"$opclass page-run merge returns the fallback candidate set");
	ok(distances_are_ordered($page_run_distances, 1e-6),
		"$opclass page-run merge keeps distances ordered");
	ok(distances_are_ordered($fallback_distances, 1e-6),
		"$opclass tuplesort fallback keeps distances ordered");

	$node->safe_psql('postgres', 'DROP INDEX idx;');
}

$node->stop;
done_testing();
