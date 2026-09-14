use strict;
use warnings FATAL => 'all';
use File::Temp qw(tempdir);
use IPC::Open3;
use Symbol qw(gensym);
use Test::More;

my $port = $ENV{PGPORT} || 65432;
my $tmpdir = tempdir(CLEANUP => 1);
my $datadir = "$tmpdir/data";
my $sockdir = "$tmpdir";
my $node_started = 0;

sub run_cmd
{
	my (@cmd) = @_;
	my $ret = system(@cmd);
	return $ret == 0;
}

sub psql
{
	my ($sql) = @_;
	my $err = gensym;
	my $pid = open3(my $in, my $out, $err, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", $sockdir, "-p", $port, "-d", "postgres");

	print $in $sql;
	close $in;

	local $/;
	my $stdout = <$out> // "";
	my $stderr = <$err> // "";
	waitpid($pid, 0);

	return ($? >> 8, $stdout, $stderr);
}

sub safe_psql
{
	my ($sql) = @_;
	my ($ret, $stdout, $stderr) = psql($sql);

	die $stderr if $ret != 0;
	return $stdout;
}

END
{
	if ($node_started)
	{
		system("pg_ctl", "-D", $datadir, "-m", "immediate", "-w", "stop");
	}
}

ok(run_cmd("initdb", "-A", "trust", "-N", "-D", $datadir), "initdb");
ok(run_cmd("pg_ctl", "-D", $datadir, "-o", "-p $port -k $sockdir -F", "-w", "start"), "start postgres");
$node_started = 1;

safe_psql("CREATE EXTENSION vector;");

sub run_index_sql
{
	my ($sql) = @_;
	return psql("SET client_min_messages = debug1; $sql");
}

# Test A - Elkan fits
safe_psql(q(
	CREATE TABLE adaptive_elkan (id int, v vector(3));
	INSERT INTO adaptive_elkan
	SELECT i, ARRAY[random(), random(), random()]
	FROM generate_series(1, 1000) i;
));

my ($ret, $stdout, $stderr) = run_index_sql(q(
	SET maintenance_work_mem = '64MB';
	CREATE INDEX adaptive_elkan_idx ON adaptive_elkan USING ivfflat (v vector_l2_ops) WITH (lists = 10);
));
is($ret, 0, "Elkan fits build succeeds");
like($stderr, qr/using Elkan k-means.*Elkan fits maintenance_work_mem/, "Elkan selected when it fits");

# Test B - automatic Yinyang fallback
safe_psql(q(
	CREATE TABLE adaptive_fallback (id int, v vector(3));
	INSERT INTO adaptive_fallback
	SELECT i, ARRAY[random(), random(), random()]
	FROM generate_series(1, 10000) i;
));

($ret, $stdout, $stderr) = run_index_sql(q(
	SET maintenance_work_mem = '16MB';
	CREATE INDEX adaptive_fallback_idx ON adaptive_fallback USING ivfflat (v vector_l2_ops) WITH (lists = 1000);
));
is($ret, 0, "automatic Yinyang fallback build succeeds");
like($stderr, qr/using Yinyang k-means.*Elkan exceeds maintenance_work_mem and Yinyang fits/, "Yinyang fallback selected");

my $explain = safe_psql(q(
	SET enable_seqscan = off;
	EXPLAIN SELECT id FROM adaptive_fallback ORDER BY v <-> '[0.5,0.5,0.5]' LIMIT 5;
));
like($explain, qr/Index Scan using adaptive_fallback_idx/, "fallback index is query-usable");

# Test C - both fail
($ret, $stdout, $stderr) = run_index_sql(q(
	SET maintenance_work_mem = '4MB';
	CREATE INDEX adaptive_both_fail_idx ON adaptive_fallback USING ivfflat (v vector_l2_ops) WITH (lists = 3000);
));
isnt($ret, 0, "both-fail case errors");
like($stderr, qr/memory required is/, "both-fail reports required memory");
like($stderr, qr/maintenance_work_mem/, "both-fail mentions maintenance_work_mem");
like($stderr, qr/Elkan/, "both-fail detail mentions Elkan");
like($stderr, qr/Yinyang/, "both-fail detail mentions Yinyang");
like($stderr, qr/Increase maintenance_work_mem.*reduce lists/s, "both-fail hint suggests memory or lists");

# Test D - actual sample count rather than target samples
safe_psql(q(
	CREATE TABLE adaptive_small (id int, v vector(3));
	INSERT INTO adaptive_small
	SELECT i, ARRAY[random(), random(), random()]
	FROM generate_series(1, 100) i;
));

($ret, $stdout, $stderr) = run_index_sql(q(
	SET maintenance_work_mem = '8MB';
	CREATE INDEX adaptive_small_idx ON adaptive_small USING ivfflat (v vector_l2_ops) WITH (lists = 1000);
));
is($ret, 0, "small-table actual sample count build succeeds");
like($stderr, qr/using Elkan k-means.*Elkan fits maintenance_work_mem/, "selector uses actual samples for small table");

# Test E - cosine smoke
safe_psql(q(
	CREATE TABLE adaptive_cosine (id int, v vector(3));
	INSERT INTO adaptive_cosine
	SELECT i, ARRAY[random() + 0.1, random() + 0.1, random() + 0.1]
	FROM generate_series(1, 200) i;
));

($ret, $stdout, $stderr) = run_index_sql(q(
	SET maintenance_work_mem = '64MB';
	CREATE INDEX adaptive_cosine_idx ON adaptive_cosine USING ivfflat (v vector_cosine_ops) WITH (lists = 10);
));
is($ret, 0, "cosine adaptive build succeeds");
like($stderr, qr/using Elkan k-means/, "cosine path selects an algorithm");

my $cosine = safe_psql(q(
	SET enable_seqscan = off;
	EXPLAIN SELECT id FROM adaptive_cosine ORDER BY v <=> '[0.5,0.5,0.5]' LIMIT 5;
));
like($cosine, qr/Index Scan using adaptive_cosine_idx/, "cosine index is query-usable");

# Test F - high-dimension smoke
safe_psql(q(
	CREATE TABLE adaptive_highdim (id int, v vector(256));
	INSERT INTO adaptive_highdim
	SELECT i, array_agg(random() ORDER BY d)
	FROM generate_series(1, 120) i
	CROSS JOIN generate_series(1, 256) d
	GROUP BY i;
));

($ret, $stdout, $stderr) = run_index_sql(q(
	SET maintenance_work_mem = '64MB';
	CREATE INDEX adaptive_highdim_idx ON adaptive_highdim USING ivfflat (v vector_l2_ops) WITH (lists = 10);
));
is($ret, 0, "high-dimension adaptive build succeeds");
like($stderr, qr/using Elkan k-means/, "high-dimension path selects an algorithm");

done_testing();
