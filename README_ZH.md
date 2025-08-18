![logo](images/OpenTenBase_logo.svg)

___

# OpenTenBase 数据库管理系统

OpenTenBase 是基于 Postgres-XL 项目的先进企业级数据库管理系统。它支持SQL标准的扩展子集，包括事务、外键、用户定义类型和函数。此外，它还添加了并行计算、安全性、管理、审计和其他功能。

OpenTenBase具有许多类似于PostgreSQL的语言接口，其中的一些可以在下面的链接中找到：

	https://www.postgresql.org/download


## 概览

一个 OpenTenBase 集群由多个 `CoordinateNodes` 、`DataNodes` 和 `GTM` 节点组成。所有用户数据都存储在 `DataNode` 中，`CoordinateNode` 仅包含元数据，`GTM` 则用于全局事务管理。`CoordinateNodes` 和`DataNodes` 共享相同的视图。

用户总是会将请求发送到到 `CoordinateNodes`，`CoordinateNodes`将请求分段之后分发给 `DataNodes`执行 ，`CoordinateNodes`将收集到的请求汇总后返回给用户。

您可以在以下的链接获取 OpenTenBase 软件的最新版本：

	https://github.com/OpenTenBase/OpenTenBase

更多的信息则可以从我们的网站中获取到：

	https://www.opentenbase.org/

## 构建过程

### 系统要求

内存: 最小 4G RAM

操作系统: TencentOS 2, TencentOS 3, OpenCloudOS, CentOS 7, CentOS 8, Ubuntu等

### 安装依赖

TencentOS 2, TencentOS 3,  CentOS 7, CentOS 8使用如下命令安装必要依赖

` yum -y install gcc make readline-devel zlib-devel openssl-devel uuid-devel bison flex`

OpenCloudOS, Ubuntu使用如下命令安装必要依赖

` apt install -y gcc make libreadline-dev zlib1g-dev libssl-dev libossp-uuid-dev bison flex`

### 创建用户 'opentenbase'用于后续操作

```shell
mkdir /data
# add user opentenbase，并将/data/opentenbase设置为用户的家
useradd -d /data/opentenbase -s /bin/bash -m opentenbase
# set password
passwd opentenbase
```

### 编译

```shell
git clone https://github.com/OpenTenBase/OpenTenBase
#如果下载压缩包安装的话需要解压
unzip OpenTenBase-2.6.0-release_new.zip -d /data/opentenbase/OpenTenBase

#配置环境变量
export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install
#切换目录
cd ${SOURCECODE_PATH}
#清楚之前安装的软件
rm -rf ${INSTALL_PATH}/opentenbase_bin_v2.0
#为软件添加执行权限
chmod +x configure*
#在安装软件之前设置编译环境和配置选项
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v2.0 --enable-user-switch --with-openssl --with-ossp-uuid CFLAGS=-g
make clean
#编译与安装
make -sj
make install
#添加执行权限
chmod +x contrib/pgxc_ctl/make_signature
#切换目录，并在切换后的目录编译与安装
cd contrib
make -sj
make install
```

**注意: 如果您使用 Ubuntu 并且在"init all"的过程中出现了 `initgtm: command not found`错误, 你可以需要添加 `${INSTALL_PATH}/opentenbase_bin_v2.0/bin` 到 `/etc/environment`中**

## 安装

使用 PGXC\_CTL 工具 来搭建一个集群, 例如: 搭建一个具有1个 global transaction management(GTM) 节点, 1个 coordinator(COORDINATOR)节点以及2个 data nodes (DATANODE) 节点的集群。

![topology](images/topology.png)

### 准备工作

1. 安装 `pgxc` 并且把 `pgxc` 安装包的路径导入到系统环境变量中

   ```shell
   PG_HOME=${INSTALL_PATH}/opentenbase_bin_v2.0
   export PATH="$PATH:$PG_HOME/bin"
   export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PG_HOME/lib"
   export LC_ALL=C
   ```

2. 关掉 `SELinux` 和 `firewall` (可选的)

   ```
   vi /etc/selinux/config # set SELINUX=disabled
   # Disable firewalld
   systemctl disable firewalld
   systemctl stop firewalld
   ```

3. 实现集群节点所在机器之间的 `ssh` 无密码登录，然后进行部署和初始化将会通过 `ssh` 连接到每个节点的机器。一旦完成这一步，就无需输入密码

   ```
   ssh-keygen -t rsa
   ssh-copy-id -i ~/.ssh/id_rsa.pub destination-user@destination-server
   ```

### 集群启动步骤

1. 生成并填写配置文件 `pgxc_ctl.conf`，这个文件在 `/data/opentenbase/pgxc_ctl`下面。`pgxc_ctl` 工具可以生成配置文件的模板，需要在模板中填写集群节点的信息。启动 `pgxc_ctl` 工具后，将在当前用户的主目录中生成 `pgxc_ctl` 目录。在输入 "prepare config" 命令后，将在 `pgxc_ctl` 目录中生成可直接修改的配置文件模板。

  * 配置文件开头的 `pgxcInstallDir` 指的是 `pgxc` 安装包的存放位置, 数据库用户可以根据自己的需求进行设置

  ```
pgxcInstallDir=${INSTALL_PATH}/opentenbase_bin_v2.0
  ```

  * 对于GTM节点，您需要配置节点名称、IP端口、端口号和节点目录

  * Data节点与上述节点类似：需要配置IP地址、端口号、目录等（由于有两个数据节点，您需要配置与节点数量相同的信息）。

  * Coordination节点要配置IP、端口、目录等信息

  * Coordination节点和Data节点分别对应 `coordSlave` 和 `datanodeSlave`。如果不需要这些节点，则可以将它们配置为'n'；否则，需要根据配置文件的说明进行配置。

    此外，Coordination节点和Data节点需要配置两种类型的端口：`poolerPort` 和 `port`, `poolerPort` 用于节点之间的通信，`port` 用于用户登录节点。值得注意的是，`poolerPort` 和 `port` 必须配置不同，否则会发生冲突，导致集群无法启动。

    每个节点都需要有自己的目录，并且不能配置位相同的目录。

  这是单节点配置的 `pgxc_ctl.conf`文件

  ```
#!/bin/bash
# Single Node Config

IP_1=10.215.147.158
IP_2=

pgxcInstallDir=/data/opentenbase/install/opentenbase_bin_v2.0
pgxcOwner=opentenbase
defaultDatabase=postgres
pgxcUser=$pgxcOwner
tmpDir=/tmp
localTmpDir=$tmpDir
configBackup=n
configBackupHost=pgxc-linker
configBackupDir=$HOME/pgxc
configBackupFile=pgxc_ctl.bak


#---- GTM ----------
gtmName=gtm
gtmMasterServer=$IP_1
gtmMasterPort=50001
gtmMasterDir=/data/opentenbase/data/gtm
gtmExtraConfig=none
gtmMasterSpecificExtraConfig=none
gtmSlave=n
#gtmSlaveServer=$IP_2
#gtmSlavePort=50001
#gtmSlaveDir=/data/opentenbase/data/gtm
#gtmSlaveSpecificExtraConfig=none

#---- Coordinators -------
coordMasterDir=/data/opentenbase/data/coord
coordArchLogDir=/data/opentenbase/data/coord_archlog

coordNames=(cn001 )
coordPorts=(30004 )
poolerPorts=(31110 )
coordPgHbaEntries=(0.0.0.0/0)
coordMasterServers=($IP_1 )
coordMasterDirs=($coordMasterDir )
coordMaxWALsernder=1
coordMaxWALSenders=($coordMaxWALsernder )
coordSlave=n
coordSlaveSync=n
coordArchLogDirs=($coordArchLogDir )

coordExtraConfig=coordExtraConfig
cat > $coordExtraConfig <<EOF
#================================================
# Added to all the coordinator postgresql.conf
# Original: $coordExtraConfig

include_if_exists = '/data/opentenbase/global/global_opentenbase.conf'

wal_level = replica
wal_keep_segments = 256 
max_wal_senders = 4
archive_mode = on 
archive_timeout = 1800 
archive_command = 'echo 0' 
log_truncate_on_rotation = on 
log_filename = 'postgresql-%M.log' 
log_rotation_age = 4h 
log_rotation_size = 100MB
hot_standby = on 
wal_sender_timeout = 30min 
wal_receiver_timeout = 30min 
shared_buffers = 1024MB 
max_pool_size = 2000
log_statement = 'ddl'
log_destination = 'csvlog'
logging_collector = on
log_directory = 'pg_log'
listen_addresses = '*'
max_connections = 2000

EOF

coordSpecificExtraConfig=(none none)
coordExtraPgHba=coordExtraPgHba
cat > $coordExtraPgHba <<EOF

local   all             all                                     trust
host    all             all             0.0.0.0/0               trust
host    replication     all             0.0.0.0/0               trust
host    all             all             ::1/128                 trust
host    replication     all             ::1/128                 trust


EOF


coordSpecificExtraPgHba=(none none)
coordAdditionalSlaves=n	
cad1_Sync=n

#---- Datanodes ---------------------
dn1MstrDir=/data/opentenbase/data/dn001
#dn2MstrDir=/data/opentenbase/data/dn002
dn1SlvDir=/data/opentenbase/data/dn001
#dn2SlvDir=/data/opentenbase/data/dn002
dn1ALDir=/data/opentenbase/data/datanode_archlog
#dn2ALDir=/data/opentenbase/data/datanode_archlog

primaryDatanode=dn001
datanodeNames=(dn001 )
datanodePorts=(40004 )
datanodePoolerPorts=(41110 )
datanodePgHbaEntries=(0.0.0.0/0)
datanodeMasterServers=($IP_1 )
datanodeMasterDirs=($dn1MstrDir )
dnWALSndr=2
datanodeMaxWALSenders=($dnWALSndr )

datanodeSlave=n
#datanodeSlaveServers=($IP_2 $IP_1)
#datanodeSlavePorts=(50004 54004)
#datanodeSlavePoolerPorts=(51110 51110)
#datanodeSlaveSync=n
#datanodeSlaveDirs=($dn1SlvDir $dn2SlvDir)
datanodeArchLogDirs=($dn1ALDir/dn001 )

datanodeExtraConfig=datanodeExtraConfig
cat > $datanodeExtraConfig <<EOF
#================================================
# Added to all the coordinator postgresql.conf
# Original: $datanodeExtraConfig

include_if_exists = '/data/opentenbase/global/global_opentenbase.conf'
listen_addresses = '*' 
wal_level = replica 
wal_keep_segments = 256 
max_wal_senders = 4
archive_mode = on 
archive_timeout = 1800 
archive_command = 'echo 0' 
log_directory = 'pg_log' 
logging_collector = on 
log_truncate_on_rotation = on 
log_filename = 'postgresql-%M.log' 
log_rotation_age = 4h 
log_rotation_size = 100MB
hot_standby = on 
wal_sender_timeout = 30min 
wal_receiver_timeout = 30min 
shared_buffers = 1024MB 
max_connections = 4000 
max_pool_size = 4000
log_statement = 'ddl'
log_destination = 'csvlog'
wal_buffers = 1GB

EOF

datanodeSpecificExtraConfig=(none )
datanodeExtraPgHba=datanodeExtraPgHba
cat > $datanodeExtraPgHba <<EOF

local   all             all                                     trust
host    all             all             0.0.0.0/0               trust
host    replication     all             0.0.0.0/0               trust
host    all             all             ::1/128                 trust
host    replication     all             ::1/128                 trust


EOF


datanodeSpecificExtraPgHba=(none )

datanodeAdditionalSlaves=n
walArchive=n

  ```

  这是双节点配置的 `pgxc_ctl.conf`文件

  ```
#!/bin/bash
# Double Node Config

IP_1=10.215.147.158
IP_2=10.240.138.159

pgxcInstallDir=/data/opentenbase/install/opentenbase_bin_v2.0
pgxcOwner=opentenbase
defaultDatabase=postgres
pgxcUser=$pgxcOwner
tmpDir=/tmp
localTmpDir=$tmpDir
configBackup=n
configBackupHost=pgxc-linker
configBackupDir=$HOME/pgxc
configBackupFile=pgxc_ctl.bak


#---- GTM ----------
gtmName=gtm
gtmMasterServer=$IP_1
gtmMasterPort=50001
gtmMasterDir=/data/opentenbase/data/gtm
gtmExtraConfig=none
gtmMasterSpecificExtraConfig=none
gtmSlave=y
gtmSlaveServer=$IP_2
gtmSlavePort=50001
gtmSlaveDir=/data/opentenbase/data/gtm
gtmSlaveSpecificExtraConfig=none

#---- Coordinators -------
coordMasterDir=/data/opentenbase/data/coord
coordArchLogDir=/data/opentenbase/data/coord_archlog

coordNames=(cn001 cn002 )
coordPorts=(30004 30004 )
poolerPorts=(31110 31110 )
coordPgHbaEntries=(0.0.0.0/0)
coordMasterServers=($IP_1 $IP_2)
coordMasterDirs=($coordMasterDir $coordMasterDir)
coordMaxWALsernder=2
coordMaxWALSenders=($coordMaxWALsernder $coordMaxWALsernder )
coordSlave=n
coordSlaveSync=n
coordArchLogDirs=($coordArchLogDir $coordArchLogDir)

coordExtraConfig=coordExtraConfig
cat > $coordExtraConfig <<EOF
#================================================
# Added to all the coordinator postgresql.conf
# Original: $coordExtraConfig

include_if_exists = '/data/opentenbase/global/global_opentenbase.conf'

wal_level = replica
wal_keep_segments = 256 
max_wal_senders = 4
archive_mode = on 
archive_timeout = 1800 
archive_command = 'echo 0' 
log_truncate_on_rotation = on 
log_filename = 'postgresql-%M.log' 
log_rotation_age = 4h 
log_rotation_size = 100MB
hot_standby = on 
wal_sender_timeout = 30min 
wal_receiver_timeout = 30min 
shared_buffers = 1024MB 
max_pool_size = 2000
log_statement = 'ddl'
log_destination = 'csvlog'
logging_collector = on
log_directory = 'pg_log'
listen_addresses = '*'
max_connections = 2000

EOF

coordSpecificExtraConfig=(none none)
coordExtraPgHba=coordExtraPgHba
cat > $coordExtraPgHba <<EOF

local   all             all                                     trust
host    all             all             0.0.0.0/0               trust
host    replication     all             0.0.0.0/0               trust
host    all             all             ::1/128                 trust
host    replication     all             ::1/128                 trust


EOF


coordSpecificExtraPgHba=(none none)
coordAdditionalSlaves=n	
cad1_Sync=n

#---- Datanodes ---------------------
dn1MstrDir=/data/opentenbase/data/dn001
dn2MstrDir=/data/opentenbase/data/dn002
dn1SlvDir=/data/opentenbase/data/dn001
dn2SlvDir=/data/opentenbase/data/dn002
dn1ALDir=/data/opentenbase/data/datanode_archlog
dn2ALDir=/data/opentenbase/data/datanode_archlog

primaryDatanode=dn001
datanodeNames=(dn001 dn002)
datanodePorts=(40004 40004)
datanodePoolerPorts=(41110 41110)
datanodePgHbaEntries=(0.0.0.0/0)
datanodeMasterServers=($IP_1 $IP_2)
datanodeMasterDirs=($dn1MstrDir $dn2MstrDir)
dnWALSndr=4
datanodeMaxWALSenders=($dnWALSndr $dnWALSndr)

datanodeSlave=y
datanodeSlaveServers=($IP_2 $IP_1)
datanodeSlavePorts=(50004 54004)
datanodeSlavePoolerPorts=(51110 51110)
datanodeSlaveSync=n
datanodeSlaveDirs=($dn1SlvDir $dn2SlvDir)
datanodeArchLogDirs=($dn1ALDir/dn001 $dn2ALDir/dn002)

datanodeExtraConfig=datanodeExtraConfig
cat > $datanodeExtraConfig <<EOF
#================================================
# Added to all the coordinator postgresql.conf
# Original: $datanodeExtraConfig

include_if_exists = '/data/opentenbase/global/global_opentenbase.conf'
listen_addresses = '*' 
wal_level = replica 
wal_keep_segments = 256 
max_wal_senders = 4
archive_mode = on 
archive_timeout = 1800 
archive_command = 'echo 0' 
log_directory = 'pg_log' 
logging_collector = on 
log_truncate_on_rotation = on 
log_filename = 'postgresql-%M.log' 
log_rotation_age = 4h 
log_rotation_size = 100MB
hot_standby = on 
wal_sender_timeout = 30min 
wal_receiver_timeout = 30min 
shared_buffers = 1024MB 
max_connections = 4000 
max_pool_size = 4000
log_statement = 'ddl'
log_destination = 'csvlog'
wal_buffers = 1GB

EOF

datanodeSpecificExtraConfig=(none none)
datanodeExtraPgHba=datanodeExtraPgHba
cat > $datanodeExtraPgHba <<EOF

local   all             all                                     trust
host    all             all             0.0.0.0/0               trust
host    replication     all             0.0.0.0/0               trust
host    all             all             ::1/128                 trust
host    replication     all             ::1/128                 trust


EOF


datanodeSpecificExtraPgHba=(none none)

datanodeAdditionalSlaves=n
walArchive=n
  ```

2. 安装包的分发（deploy all）。在填写好配置文件后，运行 `pgxc_ctl` 工具，然后输入 "deploy all" 命令，将安装包分发到每个节点的IP机器上。
   ![topology](images/deploy.png)

3. 初始化集群的每个节点（init all）。在安装包分发完成后，在 `pgxc_ctl` 工具中输入 "init all" 命令，初始化配置文件 `pgxc_ctl.conf` 中的所有节点，并启动集群。到目前为止，集群已经完成启动。
   ![topology](images/init.png)

4. 最后可以输入命令monitor all查看节点启用状况

## 使用

OpenTenBase使用datanode group来增加节点的管理灵活度，要求有一个default group才能使用，因此需要预先创建；一般情况下，会将节点的所有datanode节点加入到default group里 另外一方面，OpenTenBase的数据分布为了增加灵活度，加了中间逻辑层来维护数据记录到物理节点的映射，我们叫sharding，所以需要预先创建sharding，命令如下：

```
#链接数据库
$ psql -h ${CoordinateNode_IP} -p ${CoordinateNode_PORT} -U ${pgxcOwner} -d postgres
#配置节点
postgres=# create default node group default_group  with (dn001,dn002);
CREATE NODE GROUP
postgres=# create sharding group to group default_group;
CREATE SHARDING GROUP
postgres=# create table foo(id bigint, str text) distribute by shard(id);
```

## 开始使用

创建一个表t1_update，其中f1：整数类型，不允许为空（not null），并被设置为主键；f2：长度为 20 的可变字符类型，不允许为空（not null），默认值为 'opentenbase'；f3：长度为 32 的可变字符类型。最后更新表 public.t1_update 中 f1 等于 1 的记录，将 f2 字段的值设置为 'opentenbase'

![img](file:///C:\Users\LENOVO\AppData\Local\Temp\ksohtml11436\wps1.jpg) 

向t1中插入一条数据

![img](file:///C:\Users\LENOVO\AppData\Local\Temp\ksohtml11436\wps2.jpg) 

删除表t1中f1等于2的数据

![img](file:///C:\Users\LENOVO\AppData\Local\Temp\ksohtml11436\wps3.jpg) 

创建t1_update表，并更新f2等于opentenbase数据，将f1修改为1

![img](file:///C:\Users\LENOVO\AppData\Local\Temp\ksohtml11436\wps4.jpg) 

查询表t1_update中 f1 等于 1 的记录

![img](file:///C:\Users\LENOVO\AppData\Local\Temp\ksohtml11436\wps5.jpg) 

## 引用  

```
https://docs.opentenbase.org/
https://docs.opentenbase.org/guide/01-quickstart/#_3
```

## 许可

OpenTenBase 使用 BSD 3-Clause 许可证，版权和许可信息可以在 [LICENSE.txt](LICENSE.txt) 中找到。

## 贡献者

感谢所有参与项目贡献的人: [CONTRIBUTORS](CONTRIBUTORS.md)

## 最新消息和活动

| 新闻                                                         |
| ------------------------------------------------------------ |
| [开放原子校源行走进苏南，加速开源人才培养和创新能力提升](https://mp.weixin.qq.com/s/SU5NYTcKQPyHqfiT4OXp8Q) |
| [OpenTenBase首亮相，腾讯云数据库开源取得重大突破](https://www.opentenbase.org/news/news-post-3/) |
| [开放原子校源行走进西部，加速开源人才培养](https://www.opentenbase.org/event/event-post-3/) |
| [开源数据库OpenTenBase获信通院“OSCAR尖峰开源项目优秀案例”奖](https://www.opentenbase.org/news/news-post-2/) |
| [开放原子开源基金会赴黑龙江科技大学走访交流](https://www.opentenbase.org/event/event-post-2/) |

## 博客和文章

| 博客和文章                                                  |
| ----------------------------------------------------------- |
| [快速入门](https://www.opentenbase.org/blog/01-quickstart/) |

## 过去的活动

[history_events](history_events.md)
