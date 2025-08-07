#
# Tests relating to PostgreSQL crash recovery and redo
#
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use Config;
if ($Config{osname} eq 'MSWin32')
{

   # some Windows Perls at least don't like IPC::Run's start/kill_kill regime.
	plan skip_all => "Test fails on Windows perl";
}
else
{
	plan tests => 3;
}

my $node = get_new_node('master', 'datanode');
# There is no gtm in centralized mode, so it doesn’t matter what
# the master gtm IP and port are
$node->init(allows_streaming => 1,
            extra => ['--master_gtm_nodename', 'no_gtm',
                      '--master_gtm_ip', '127.0.0.1',
                      '--master_gtm_port', '25001']);
$node->append_conf('postgresql.conf', "allow_dml_on_datanode = on");
$node->append_conf('postgresql.conf', "is_centralized_mode = on");
$node->start;

my ($stdin, $stdout, $stderr) = ('', '', '');

# Ensure that txid_status reports 'aborted' for xacts
# that were in-progress during crash. To do that, we need
# an xact to be in-progress when we crash and we need to know
# its xid.
my $tx = IPC::Run::start(
	[   'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres') ],
	'<',
	\$stdin,
	'>',
	\$stdout,
	'2>',
	\$stderr);
$stdin .= q[
BEGIN;
CREATE TABLE mine(x integer);
SELECT txid_current();
];
$tx->pump until $stdout =~ /[[:digit:]]+[\r\n]$/;

# Status should be in-progress
my $xid = $stdout;
chomp($xid);

is($node->safe_psql('postgres', qq[SELECT txid_status('$xid');]),
	'in progress', 'own xid is in-progres');

# Crash and restart the postmaster
$node->stop('immediate');
$node->start;

# Make sure we really got a new xid
cmp_ok($node->safe_psql('postgres', 'SELECT txid_current()'),
	'>', $xid, 'new xid after restart is greater');

# and make sure we show the in-progress xact as aborted
is($node->safe_psql('postgres', qq[SELECT txid_status('$xid');]),
	'aborted', 'xid is aborted after crash');

$tx->kill_kill;
