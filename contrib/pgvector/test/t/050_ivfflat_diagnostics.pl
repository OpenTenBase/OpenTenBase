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
	my $pid = open3(my $in, my $out, $err, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", $sockdir, "-p", $port, "-d", "postgres", "-At");

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

sub run_diag
{
	my ($index, $settings) = @_;
	$settings //= "";
	return safe_psql("$settings SELECT severity || '|' || issue || '|' || current_value || '|' || recommended_value || '|' || detail FROM ivfflat_index_diagnostics('$index'::regclass) ORDER BY issue;");
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

my ($ret, $stdout, $stderr);

# Invalid input: ordinary table
safe_psql(q(
	CREATE TABLE diag_plain (id int, v vector(3));
));
($ret, $stdout, $stderr) = psql(q(
	SELECT * FROM ivfflat_index_diagnostics('diag_plain'::regclass);
));
isnt($ret, 0, "table input is rejected");
like($stderr, qr/not an index/, "table rejection is clear");

# Invalid input: non-IVFFlat index
safe_psql(q(
	CREATE INDEX diag_plain_btree_idx ON diag_plain (id);
));
($ret, $stdout, $stderr) = psql(q(
	SELECT * FROM ivfflat_index_diagnostics('diag_plain_btree_idx'::regclass);
));
isnt($ret, 0, "non-IVFFlat index is rejected");
like($stderr, qr/not an IVFFlat index/, "non-IVFFlat rejection is clear");

# Empty table
safe_psql(q(
	CREATE TABLE diag_empty (id int, v vector(3));
	CREATE INDEX diag_empty_idx ON diag_empty USING ivfflat (v vector_l2_ops) WITH (lists = 10);
));
$stdout = run_diag("diag_empty_idx");
like($stdout, qr/empty_table/, "empty table diagnostic is present");
like($stdout, qr/representative data/, "empty table suggests representative data");

# Reasonable case, <= 1M recommendation
safe_psql(q(
	CREATE TABLE diag_reasonable (id int, v vector(3));
	INSERT INTO diag_reasonable
	SELECT i, ARRAY[random(), random(), random()]
	FROM generate_series(1, 10000) i;
	CREATE INDEX diag_reasonable_idx ON diag_reasonable USING ivfflat (v vector_l2_ops) WITH (lists = 10);
));
$stdout = run_diag("diag_reasonable_idx", "SET ivfflat.probes = 3;");
like($stdout, qr/parameter_summary/, "valid IVFFlat index returns diagnostics");
like($stdout, qr/recommended_lists_start=10/, "recommended lists uses rows / 1000 up to 1M rows");
unlike($stdout, qr/WARNING\|low_recall_risk/, "near-start probes do not trigger false low-recall warning");
unlike($stdout, qr/^WARNING\|/m, "reasonable case has no warning");

# > 1M recommendation without inserting 1M rows
safe_psql(q(
	UPDATE pg_class SET reltuples = 1000001 WHERE oid = 'diag_reasonable'::regclass;
));
$stdout = run_diag("diag_reasonable_idx", "SET ivfflat.probes = 3;");
like($stdout, qr/recommended_lists_start=1001/, "recommended lists uses sqrt(rows) over 1M rows");
like($stdout, qr/lists_low/, "low-lists diagnostic is present for far-below-starting-point lists");

# Automatic Yinyang fallback remains unaffected; high lists and low probes diagnostics
safe_psql(q(
	CREATE TABLE diag_high_lists (id int, v vector(3));
	INSERT INTO diag_high_lists
	SELECT i, ARRAY[random(), random(), random()]
	FROM generate_series(1, 10000) i;
));
($ret, $stdout, $stderr) = psql(q(
	SET client_min_messages = debug1;
	SET maintenance_work_mem = '16MB';
	CREATE INDEX diag_high_lists_idx ON diag_high_lists USING ivfflat (v vector_l2_ops) WITH (lists = 1000);
));
is($ret, 0, "high-lists IVFFlat index builds");
like($stderr, qr/using Yinyang k-means/, "adaptive Yinyang fallback remains active");

$stdout = run_diag("diag_high_lists_idx", "SET ivfflat.probes = 1;");
like($stdout, qr/lists_high/, "high-lists diagnostic is present");
like($stdout, qr/low_recall_risk/, "low probes diagnostic is present");
like($stdout, qr/validate recall against exact search/, "low recall wording asks for exact validation");
unlike($stdout, qr/Recall\@10|predicted recall|Recall =/, "diagnostics do not claim fake recall numbers");
like($stdout, qr/recommended_probes_start=32/, "recommended probes uses ceil sqrt(lists)");

# rows < lists
safe_psql(q(
	CREATE TABLE diag_tiny (id int, v vector(3));
	INSERT INTO diag_tiny
	SELECT i, ARRAY[random(), random(), random()]
	FROM generate_series(1, 10) i;
	CREATE INDEX diag_tiny_idx ON diag_tiny USING ivfflat (v vector_l2_ops) WITH (lists = 20);
));
$stdout = run_diag("diag_tiny_idx");
like($stdout, qr/too_little_data_for_lists/, "rows < lists diagnostic is present");

# High probes and probes = lists
$stdout = run_diag("diag_reasonable_idx", "SET ivfflat.probes = 8;");
like($stdout, qr/high_query_cost/, "high probes cost advisory is present");
$stdout = run_diag("diag_reasonable_idx", "SET ivfflat.probes = 10;");
like($stdout, qr/all_lists_searched/, "probes = lists diagnostic is present");
like($stdout, qr/planner may not use the IVFFlat index/, "all-lists diagnostic mentions planner behavior");

# Cosine smoke with non-zero vectors
safe_psql(q(
	CREATE TABLE diag_cosine (id int, v vector(3));
	INSERT INTO diag_cosine
	SELECT i, ARRAY[random() + 0.1, random() + 0.1, random() + 0.1]
	FROM generate_series(1, 200) i;
	CREATE INDEX diag_cosine_idx ON diag_cosine USING ivfflat (v vector_cosine_ops) WITH (lists = 10);
));
$stdout = run_diag("diag_cosine_idx");
like($stdout, qr/parameter_summary/, "cosine IVFFlat index returns diagnostics");

# High-dimension smoke
safe_psql(q(
	CREATE TABLE diag_highdim (id int, v vector(256));
	INSERT INTO diag_highdim
	SELECT i, array_agg(random() ORDER BY d)
	FROM generate_series(1, 120) i
	CROSS JOIN generate_series(1, 256) d
	GROUP BY i;
	CREATE INDEX diag_highdim_idx ON diag_highdim USING ivfflat (v vector_l2_ops) WITH (lists = 10);
));
$stdout = run_diag("diag_highdim_idx");
like($stdout, qr/dimensions=256/, "high-dimension diagnostic includes dimensions");

# Both algorithms memory fail remains an ERROR path
($ret, $stdout, $stderr) = psql(q(
	SET maintenance_work_mem = '4MB';
	CREATE INDEX diag_both_fail_idx ON diag_high_lists USING ivfflat (v vector_l2_ops) WITH (lists = 3000);
));
isnt($ret, 0, "both-fail memory case still errors");
like($stderr, qr/memory required is/, "both-fail memory error is preserved");

done_testing();
