# Set of tests for authentication and pg_hba.conf. The following password
# methods are checked through this test:
# - Plain
# - MD5-encrypted
# - SCRAM-encrypted
# This test cannot run on Windows as Postgres cannot be set up with Unix
# sockets and needs to go through SSPI.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

# 如果正在运行的操作系统是 Windows，则跳过测试；否则计划执行 8 个测试用例
if ($windows_os)
{
	plan skip_all => "authentication tests cannot run on Windows";
}
else
{
	plan tests => 8;
}


# Delete pg_hba.conf from the given node, add a new entry to it
# and then execute a reload to refresh it.
# 重置pg_hba.conf配置文件，替换为指定的访问方法
sub reset_pg_hba
{
	my $node       = shift;
	my $hba_method = shift;

# 先删除当前数据目录下的pg_hba.conf文件
	unlink($node->data_dir . '/pg_hba.conf');

# 在pg_hba.conf文件末尾添加配置条目，格式为"local all all 访问方法"
	$node->append_conf('pg_hba.conf', "local all all $hba_method");

# 重新加载PostgreSQL配置，这样新的pg_hba.conf配置生效
	$node->reload;
}

# Test access for a single role, useful to wrap all tests into one.
# 测试不同角色在指定的访问方法下的身份验证结果
sub test_role
{
	my $node          = shift;
	my $role          = shift;
	my $method        = shift;
	my $expected_res  = shift;
	my $status_string = 'failed';

# 如果预期的结果为0，即成功，则修改$status_string为'success'
	$status_string = 'success' if ($expected_res eq 0);

 # 使用psql连接数据库，执行"SELECT 1"语句，extra_params参数用于指定连接用户
	my $res =
	  $node->psql('postgres', 'SELECT 1', extra_params => [ '-U', $role ]);# 使用psql连接数据库执行SELECT语句

	# 使用Test::More模块进行断言，判断实际结果$res是否与预期结果$expected_res相符
	is($res, $expected_res,
		"authentication $status_string for method $method, role $role"); # 使用Test::More模块进行断言
}

# Initialize master node
# 初始化一个名为 `master` 的 PostgreSQL 节点，并创建两个具有不同密码验证方式的用户角色
my $node = get_new_node('master'); # 创建一个名为master的PostgreSQL节点
$node->init;# 初始化该节点
$node->start; # 启动该节点

# Create 3 roles with different password methods for each one. The same
# password is used for all of them.
# 为scram_role创建具有scram-sha-256密码验证方式的角色.
$node->safe_psql('postgres',
"SET password_encryption='scram-sha-256'; CREATE ROLE scram_role LOGIN PASSWORD 'pass';"
);

# 为md5_role创建具有md5密码验证方式的角色
$node->safe_psql('postgres',
"SET password_encryption='md5'; CREATE ROLE md5_role LOGIN PASSWORD 'pass';");
 # 设置环境变量PGPASSWORD为pass
$ENV{"PGPASSWORD"} = 'pass';

# For "trust" method, all users should be able to connect.
# 使用reset_pg_hba函数测试不同的访问方法对不同的数据库角色进行正确的身份验证
reset_pg_hba($node, 'trust');
test_role($node, 'scram_role', 'trust', 0);
test_role($node, 'md5_role',   'trust', 0);

# For plain "password" method, all users should also be able to connect.
 # 针对password方法进行测试
reset_pg_hba($node, 'password');
test_role($node, 'scram_role', 'password', 0);
test_role($node, 'md5_role',   'password', 0);

# For "scram-sha-256" method, user "scram_role" should be able to connect.
 # 针对scram-sha-256方法进行测试
reset_pg_hba($node, 'scram-sha-256');
test_role($node, 'scram_role', 'scram-sha-256', 0);
test_role($node, 'md5_role',   'scram-sha-256', 2);

# For "md5" method, all users should be able to connect (SCRAM
# authentication will be performed for the user with a scram verifier.)
# 针对md5方法进行测试
reset_pg_hba($node, 'md5');
test_role($node, 'scram_role', 'md5', 0);
test_role($node, 'md5_role',   'md5', 0);