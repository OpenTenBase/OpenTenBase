# test for archiving with hot standby
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;
use File::Copy;

# Initialize master node, doing archives
my $node_master = get_new_node('master', 'datanode');
# There is no gtm in centralized mode, so it doesn’t matter what 
# the master gtm IP and port are 
$node_master->init(
	has_archiving    => 1,
	allows_streaming => 1,
        extra => ['--master_gtm_nodename', 'no_gtm',
                  '--master_gtm_ip', '127.0.0.1',
                  '--master_gtm_port', '25001']);
$node_master->append_conf('postgresql.conf', "allow_dml_on_datanode = on");
$node_master->append_conf('postgresql.conf', "is_centralized_mode = on");
my $backup_name = 'my_backup';

# Start it
$node_master->start;

# Take backup for standby
$node_master->backup($backup_name);

# Initialize standby node from backup, fetching WAL from archives
my $node_standby = get_new_node('standby', 'datanode');
$node_standby->init_from_backup($node_master, $backup_name,
	has_restoring => 1);
$node_standby->append_conf('postgresql.conf',
	"wal_retrieve_retry_interval = '100ms'");
$node_standby->start;

# Create some content on master
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a");
my $current_lsn =
  $node_master->safe_psql('postgres', "SELECT pg_current_wal_lsn();");

# Force archiving of WAL file to make it present on master
$node_master->safe_psql('postgres', "SELECT pg_switch_wal()");

# Add some more content, it should not be present on standby
$node_master->safe_psql('postgres',
	"INSERT INTO tab_int VALUES (generate_series(1001,2000))");

# Wait until necessary replay has been done on standby
my $caughtup_query =
  "SELECT '$current_lsn'::pg_lsn <= pg_last_wal_replay_lsn()";
$node_standby->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for standby to catch up";

my $result =
  $node_standby->safe_psql('postgres', "SELECT count(*) FROM tab_int");
is($result, qq(1000), 'check content from archives');
