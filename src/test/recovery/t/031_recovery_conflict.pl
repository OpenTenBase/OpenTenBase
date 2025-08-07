# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Test that connections to a hot standby are correctly canceled when a
# recovery conflict is detected Also, test that statistics in
# pg_stat_database_conflicts are populated correctly

use strict;
use warnings FATAL => 'all';
use PostgresNode;
use Test::More;


# Set up nodes
my $node_primary = get_new_node('master', 'datanode');
$node_primary->init(allows_streaming => 1,
                   extra => ['--master_gtm_nodename', 'no_gtm',
                             '--master_gtm_ip', '127.0.0.1',
                             '--master_gtm_port', '25001']);

$node_primary->append_conf(
	'postgresql.conf', qq[
log_temp_files = 0

# wait some to test the wait paths as well, but not long for obvious reasons
max_standby_streaming_delay = 1s

# Some of the recovery conflict logging code only gets exercised after
# deadlock_timeout. The test doesn't rely on that additional output, but it's
# nice to get some minimal coverage of that code.
#log_recovery_conflict_waits = on
deadlock_timeout = 10ms
allow_dml_on_datanode = on
is_centralized_mode = on
]);
$node_primary->start;

my $backup_name = 'my_backup';

$node_primary->backup($backup_name);
my $node_standby = get_new_node('standby', 'datanode');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);

$node_standby->start;

my $test_db = "test_db";

# use a new database, to trigger database recovery conflict
$node_primary->safe_psql('postgres', "CREATE DATABASE $test_db");

# test schema / data
my $table1 = "test_recovery_conflict_table1";
my $table2 = "test_recovery_conflict_table2";
$node_primary->safe_psql(
	$test_db, qq[
CREATE TABLE ${table1}(a int, b int);
INSERT INTO $table1 SELECT i % 3, 0 FROM generate_series(1,20) i;
CREATE TABLE ${table2}(a int, b int);
]);
$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));


# a longrunning psql that we can use to trigger conflicts
my $in  = '';
my $out = '';
my $timer = IPC::Run::timeout(180);
my $psql_standby =
  $node_standby->background_psql($test_db,\$in, \$out, $timer, on_error_stop => 0);
my $expected_conflicts = 0;


## RECOVERY CONFLICT 1: Buffer pin conflict
my $sect = "buffer pin conflict";
$expected_conflicts++;

# Aborted INSERT on primary that will be cleaned up by vacuum. Has to be old
# enough so that there's not a snapshot conflict before the buffer pin
# conflict.

$node_primary->safe_psql(
	$test_db,
	qq[
	BEGIN;
	INSERT INTO $table1 VALUES (1,0);
	ROLLBACK;
	-- ensure flush, rollback doesn't do so
	BEGIN; LOCK $table1; COMMIT;
	]);

$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));

my $cursor1 = "test_recovery_conflict_cursor";

# DECLARE and use a cursor on standby, causing buffer with the only block of
# the relation to be pinned on the standby
$in .= qq[
    BEGIN;
    DECLARE $cursor1 CURSOR FOR SELECT b FROM $table1;
    FETCH FORWARD FROM $cursor1;
];
$psql_standby->pump;

# FETCH FORWARD should have returned a 0 since all values of b in the table
# are 0
like($out, qr/^0$/m, "$sect: cursor with conflicting pin established");

# to check the log starting now for recovery conflict messages
my $log_location = -s $node_standby->logfile;

# VACUUM FREEZE on the primary
$node_primary->safe_psql($test_db, qq[VACUUM FREEZE $table1;]);

# Wait for catchup. Existing connection will be terminated before replay is
# finished, so waiting for catchup ensures that there is no race between
# encountering the recovery conflict which causes the disconnect and checking
# the logfile for the terminated connection.
$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));

sleep(1);
check_conflict_log("User was holding shared buffer pin for too long");
$psql_standby->finish;
check_conflict_stat("bufferpin");


## RECOVERY CONFLICT 2: Snapshot conflict
$sect = "snapshot conflict";
$expected_conflicts++;

$node_primary->safe_psql($test_db,
	qq[INSERT INTO $table1 SELECT i, 0 FROM generate_series(1,20) i]);
$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));

# DECLARE and FETCH from cursor on the standby
$in  = '';
$out = '';
$psql_standby =
  $node_standby->background_psql($test_db,\$in, \$out, $timer, on_error_stop => 0);
$in  = qq[
        BEGIN;
        DECLARE $cursor1 CURSOR FOR SELECT b FROM $table1;
        FETCH FORWARD FROM $cursor1;
        ];
$psql_standby->pump;
like($out, qr/^0$/m, "$sect: cursor with conflicting snapshot established");

# Do some HOT updates
$node_primary->safe_psql($test_db,
	qq[UPDATE $table1 SET a = a + 1 WHERE a > 2;]);

# VACUUM FREEZE, pruning those dead tuples
$node_primary->safe_psql($test_db, qq[VACUUM FREEZE $table1;]);

# Wait for attempted replay of PRUNE records
$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));

sleep(1);
check_conflict_log(
	"User query might have needed to see row versions that must be removed");
$psql_standby->finish;
check_conflict_stat("snapshot");


## RECOVERY CONFLICT 3: Lock conflict
$sect = "lock conflict";
$expected_conflicts++;

# acquire lock to conflict with
$in  = '';
$out = '';
$psql_standby =
  $node_standby->background_psql($test_db,\$in, \$out, $timer, on_error_stop => 0);
$in  = qq[
        BEGIN;
        LOCK TABLE $table1 IN ACCESS SHARE MODE;
        SELECT 1;
        ];
$psql_standby->pump;
like($out, qr/^1$/m, "$sect: conflicting lock acquired");

# DROP TABLE containing block which standby has in a pinned buffer
$node_primary->safe_psql($test_db, qq[DROP TABLE $table1;]);

$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));

sleep(1);
check_conflict_log("User was holding a relation lock for too long");
$psql_standby->finish;
check_conflict_stat("lock");


## RECOVERY CONFLICT 5: Deadlock
$sect = "startup deadlock";
$expected_conflicts++;

# Want to test recovery deadlock conflicts, not buffer pin conflicts. Without
# changing max_standby_streaming_delay it'd be timing dependent what we hit
# first
$node_standby->append_conf('postgresql.conf', 'max_standby_streaming_delay = 1s');
#$node_standby->restart();
$in  = '';
$out = '';
$psql_standby =
  $node_standby->background_psql($test_db,\$in, \$out, $timer, on_error_stop => 0);

# Generate a few dead rows, to later be cleaned up by vacuum. Then acquire a
# lock on another relation in a prepared xact, so it's held continuously by
# the startup process. The standby psql will block acquiring that lock while
# holding a pin that vacuum needs, triggering the deadlock.
$node_primary->safe_psql(
	$test_db,
	qq[
CREATE TABLE $table1(a int, b int);
INSERT INTO $table1 VALUES (1);
BEGIN;
INSERT INTO $table1(a) SELECT generate_series(1, 100) i;
ROLLBACK;
BEGIN;
LOCK TABLE $table2;
PREPARE TRANSACTION 'lock';
INSERT INTO $table1(a) VALUES (170);
SELECT txid_current();
]);

$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));

$in = qq[
     BEGIN;
     -- hold pin
     DECLARE $cursor1 CURSOR FOR SELECT a FROM $table1;
     FETCH FORWARD FROM $cursor1;
     -- wait for lock held by prepared transaction
         SELECT * FROM $table2;
     ];
     #my $until = qr/^1$/m;
     #pump $psql_standby until $$out =~ /$until/ || $timer->is_expired;
$psql_standby->pump;
like($out, qr/^1$/m, "$sect: conflicting lock acquired2");
ok(1,
	"$sect: cursor holding conflicting pin, also waiting for lock, established"
);

# just to make sure we're waiting for lock already
ok( $node_standby->poll_query_until(
		'postgres', qq[
SELECT 'waiting' FROM pg_locks WHERE locktype = 'relation' AND NOT granted;
], 'waiting'),
	"$sect: lock acquisition is waiting");

# VACUUM FREEZE will prune away rows, causing a buffer pin conflict, while
# standby psql is waiting on lock
$node_primary->safe_psql($test_db, qq[VACUUM FREEZE $table1;]);
$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));

sleep(1);
check_conflict_log("User transaction caused buffer deadlock with recovery.");
$psql_standby->finish;
check_conflict_stat("deadlock");

# clean up for next tests
$node_primary->safe_psql($test_db, qq[ROLLBACK PREPARED 'lock';]);
$node_standby->append_conf('postgresql.conf', 'max_standby_streaming_delay = 50ms');
#$node_standby->restart();
$in  = '';
$out = '';
$psql_standby =
   $node_standby->background_psql($test_db,\$in, \$out, $timer, on_error_stop => 0);


# Check that expected number of conflicts show in pg_stat_database. Needs to
# be tested before database is dropped, for obvious reasons.
is( $node_standby->safe_psql(
		$test_db,
		qq[SELECT conflicts FROM pg_stat_database WHERE datname='$test_db';]),
	$expected_conflicts,
	qq[$expected_conflicts recovery conflicts shown in pg_stat_database]);


## RECOVERY CONFLICT 6: Database conflict
$sect = "database conflict";

$node_primary->safe_psql('postgres', qq[DROP DATABASE $test_db;]);

$node_primary->wait_for_catchup($node_standby,'replay', $node_primary->lsn('insert'));

check_conflict_log("User was connected to a database that must be dropped");


# explicitly shut down psql instances gracefully - to avoid hangs or worse on
# windows
$psql_standby->finish;

$node_standby->stop();
$node_primary->stop();


done_testing();

sub check_conflict_log
{
	my $message = shift;
	my $old_log_location = $log_location;

	$log_location = $node_standby->wait_for_log(qr/$message/, $log_location);

	cmp_ok($log_location, '>', $old_log_location,
		"$sect: logfile contains terminated connection due to recovery conflict"
	);
}

sub check_conflict_stat
{
	my $conflict_type = shift;
	my $count = $node_standby->safe_psql($test_db,
		qq[SELECT confl_$conflict_type FROM pg_stat_database_conflicts WHERE datname='$test_db';]
	);

	is($count, 1, "$sect: stats show conflict on standby");
}
