
<img src="images/OpenTenBase_logo.svg" width="60%" />

___
# OpenTenBase Database Management System
OpenTenBase is an advanced enterprise-level database management system based on prior work of Postgres-XL project. It supports an extended subset of the SQL standard, including transactions, foreign keys, user-defined types and functions. Additional, it adds parallel computing, security, management, audit and other functions.

OpenTenBase has many language interfaces similar to PostgreSQL, many of which are listed here:

	https://www.postgresql.org/download

## Overview
A OpenTenBase cluster consists of multiple CoordinateNodes, DataNodes, and GTM nodes. All user data resides in the DataNode, the CoordinateNode contains only metadata, the GTM for global transaction management. The CoordinateNodes and DataNodes share the same schema.

Users always connect to the CoordinateNodes, which divides up the query into fragments that are executed in the DataNodes, and collects the results.

The latest version of this software may be obtained at:

	https://github.com/OpenTenBase/OpenTenBase

For more information look at our website located at:

	https://www.opentenbase.org/

## Building
### System Requirements: 

Memory: 4G RAM minimum

OS: TencentOS 2, TencentOS 3, OpenCloudOS, CentOS 7, CentOS 8, Ubuntu

### Dependence

` yum -y install gcc make readline-devel zlib-devel openssl-devel uuid-devel bison flex cmake postgresql-devel libssh2-devel sshpass`

or

` apt install -y gcc make libreadline-dev zlib1g-dev libssl-dev libossp-uuid-dev bison flex cmake postgresql-devel libssh2-devel sshpass`

### Create User 'opentenbase'

```bash
# 1.make dir /data
mkdir -p /data

# 2. add user 
useradd -d /data/opentenbase -s /bin/bash -m opentenbase # add user opentenbase

# 3. set passwd
passwd opentenbase # set password

# 4. Add users to the wheel group
usermod -aG wheel opentenbase

# 5. Enable sudo permissions for the wheel group (via visudo), uncomment the line "% wheel", save and exit， # 取消注释 %wheel 行后保存
visudo 

```

### Building

```bash
su - opentenbase
cd /data/opentenbase/
git clone https://github.com/OpenTenBase/OpenTenBase

export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install/

cd ${SOURCECODE_PATH}
rm -rf ${INSTALL_PATH}/opentenbase_bin_v2.0
chmod +x configure*
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v2.0 --enable-user-switch --with-openssl --with-ossp-uuid CFLAGS=-g
make clean
make -sj
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib
make -sj
make install
```

**Notice: if you use Ubuntu and see *initgtm: command not found* while doing "init all", you may add *${INSTALL_PATH}/opentenbase_bin_v2.0/bin* to */etc/environment***

## Installation
Use PGXC\_CTL tool to build a cluster, for example: a cluster with a global transaction management node (GTM), a coordinator(COORDINATOR) and two data nodes (DATANODE).
<img src="images/topology.png" width="50%" />
### Preparation

* 1. Install pgxc and import the path of pgxc installation package into environment variable.

```shell
PG_HOME=${INSTALL_PATH}/opentenbase_bin_v2.0
export PATH="$PATH:$PG_HOME/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PG_HOME/lib"
export LC_ALL=C
```

* 2. Disable SELinux and firewall (optional)

```
vi /etc/selinux/config # set SELINUX=disabled
# Disable firewalld
systemctl disable firewalld
systemctl stop firewalld
```

* 3. Create the *. tar.gz package for initializing instances.

```
cd /data/opentenbase/install/
tar -zcf opentenbase-5.21.8-i.x86_64.tar.gz *
```

### Cluster startup steps

1. Generate and fill in configuration file opentenbase\_config.ini . pgxc\_ctl tool can generate a template for the configuration file. You need to fill in the cluster node information in the template. After the pgxc\_ctl tool is started, pgxc\_ctl directory will be generated in the current user's home directory. After entering " prepare config" command, the configuration file template that can be directly modified will be generated in pgxc\_ctl directory.

* Description of each field in opentenbase\_config.ini
```
| 配置分类        | 配置项           | 配置说明                                                                   |
|----------------|-----------------|--------------------------------------------------------------------------|
| instance       | name            | 实例名称,可用的字符：半角大小写字母、数字、下划线，例如: opentenbase_instance01         |
|                | type            | distributed代表分布式，需要gtm、协调节点和数据节点；centralized代表集中式        |
|                | package         | 软件包。全路径(建议)或opentenbase_ctl的相对路径                               |
| gtm            | master          | 主节点，只有一个IP                                                          |
|                | slave           | 备节点。如果需要n个备节点，这里配置n个IP，半角逗号分隔。                          |
| coordinators   | master          | 主节点IP，自动生成节点名称，每个IP上部署nodes-per-server个                     |
|                | slave           | 备节点IP，个数是master的整数倍。                                             |
|                |                 | 举例：如果1主1备，则IP个数和master一样多；如果1主2备，则IP个数是master的两倍。     |
|                | nodes-per-server| 可选，默认1。每个IP上部署的节点数。举例：master有3个IP，这里配置2，则实际会有6个节点 |
|                |                 | cn001-cn006共6个节点，每个服务器分布2个节点。                                  |
| datanodes      | master          | 主节点IP，自动生成节点名称，每个IP上部署nodes-per-server个                     |
|                | slave           | 备节点IP，个数是master的整数倍。                                             |
|                |                 | 举例：如果1主1备，则IP个数和master一样多；如果1主2备，则IP个数是master的两倍。     |
|                | nodes-per-server| 可选，默认1。每个IP上部署的节点数。举例：master有3个IP，这里配置2，则实际会有6个节点 |
|                |                 | dn001-dn006共6个节点，每个服务器分布2个节点。                                  |
| server         | ssh-user        | 远程执行命令的用户名，需提前创建好,为了配置管理更简单，要求所有服务器的账号一致        |
|                | ssh-password    | 远程执行命令的密码，需提前创建好,为了配置管理更简单，要求所有服务器的密码一致         |
|                | ssh-port        | ssh端口，为了配置管理更简单，要求所有服务器的一致                                | 
| log            | level           | opentenbase_ctl工具运行的日志打印级别(不是opentenbase节点的日志级别)            |

```

* Create a configuration file opentenbase\_config.ini for the instance
```
mkdir -p ./logs
touch opentenbase_config.ini
vim opentenbase_config.ini
```

* For example, if I have two servers 172.16.16.49 and 172.16.16.131, the typical configuration of a distributed instance distributed across the two servers is as follows. You can copy this configuration information and make modifications according to your deployment requirements.Don't forget to fill in the ssh password configuration.
```
# 实例配置
[instance]
name=opentenbase01
type=distributed
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# gtm节点
[gtm]
master=172.16.16.49
slave=172.16.16.50,172.16.16.131

# 协调节点
[coordinators]
master=172.16.16.49
slave= 172.16.16.131
nodes-per-server=1

# 数据节点
[datanodes]
master=172.16.16.49,172.16.16.131
slave=172.16.16.131,172.16.16.49
nodes-per-server=1

# 登录和部署账号
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```


* Similarly, the configuration of a typical centralized instance is as follows.Don't forget to fill in the ssh password configuration.
```
# 实例配置
[instance]
name=opentenbase02
type=centralized
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# 数据节点
[datanodes]
master=172.16.16.49
slave=172.16.16.131
nodes-per-server=1

# 登录和部署账号
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```

2. Execute command for instance installation.
* Execute installation command: ./opentenbase_ctl install  -c opentenbase_config.ini
```
[opentenbase@VM-16-49-tencentos opentenbase_ctl]# export LD_LIBRARY_PATH=/data/opentenbase/install/lib
[opentenbase@VM-16-49-tencentos opentenbase_ctl]# ./opentenbase_ctl install  -c opentenbase_config.ini

====== Start to Install Opentenbase test_cluster01  ====== 

step 1: Make *.tar.gz pkg ...
    Make opentenbase-5.21.8-i.x86_64.tar.gz successfully.

step 2: Transfer and extract pkg to servers ...
    Package_path: /data/opentenbase/opentenbase_ctl/opentenbase-5.21.8-i.x86_64.tar.gz
    Transfer and extract pkg to servers successfully.

step 3: Install gtm master node ...
    Install gtm0001(172.16.16.49) ...
    Install gtm0001(172.16.16.49) successfully
    Success to install  gtm master node. 

step 4: Install cn/dn master node ...
    Install cn0001(172.16.16.49) ...
    Install dn0001(172.16.16.49) ...
    Install dn0002(172.16.16.131) ...
    Install cn0001(172.16.16.49) successfully
    Install dn0001(172.16.16.49) successfully
    Install dn0002(172.16.16.131) successfully
    Success to install all cn/dn master nodes. 

step 5: Install slave nodes ...
    Install gtm0002(172.16.16.131) ...
    Install cn0001(172.16.16.131) ...
    Install dn0001(172.16.16.131) ...
    Install dn0002(172.16.16.49) ...
    Install gtm0002(172.16.16.131) successfully
    Install dn0002(172.16.16.49) successfully
    Install dn0001(172.16.16.131) successfully
    Install cn0001(172.16.16.131) successfully
    Success to install all slave nodes. 

step 6:Create node group ...
    Create node group successfully. 

====== Installation completed successfully  ====== 
```
* When you see the words' Installation completed successfully ', it means that the installation has been completed. Enjoy your opentenbase journey to the fullest.
* You can check the status of the instance
```
[opentenbase@VM-16-49-tencentos opentenbase_ctl]$ ./opentenbase_ctl status -c opentenbase_config.ini

------------- Instance status -----------  
Instance name: test_cluster01
Version: 5.21.8

-------------- Node status --------------  
Node gtm0001(172.16.16.49) is Running 
Node dn0001(172.16.16.49) is Running 
Node dn0002(172.16.16.49) is Running 
Node cn0001(172.16.16.49) is Running 
Node dn0002(172.16.16.131) is Running 
Node cn0001(172.16.16.131) is Running 
Node gtm0002(172.16.16.131) is Running 
Node dn0001(172.16.16.131) is Running 
[Result] Total: 8, Running: 8, Stopped: 0, Unknown: 0

------- Master CN Connection Info -------  
[1] cn0001(172.16.16.49)  
Environment variable: export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
PSQL connection: psql -h 172.16.16.49 -p 11000 -U opentenbase postgres 
```


## Usage
* Connect to CN Master node to execute SQL

```
export LD_LIBRARY_PATH=/home/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/home/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
$ psql -h ${CoordinateNode_IP} -p ${CoordinateNode_PORT} -U opentenbase -d postgres

postgres=# 

```

## References  

```
https://docs.opentenbase.org/
```

## Who are using OpenTenBase
Tencent


## License

The OpenTenBase is licensed under the BSD 3-Clause License. Copyright and license information can be found in the file [LICENSE.txt](LICENSE.txt)

## Contributors
Thanks for all contributors here: [CONTRIBUTORS](CONTRIBUTORS.md)

## News and Events

|Latest|
|------|
|[Special Review of Cloud Native Open Source Project Application Practice](https://www.opentenbase.org/en/event/event-post-1/)|

## Blogs and Articals
|Blogs and Articals|
|------------------|
|[Quick Start](https://www.opentenbase.org/en/blog/01-quickstart/)|

## History
[history_events](history_events.md)
