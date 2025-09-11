
<img src="images/OpenTenBase_logo.svg" width="60%" />

___
# OpenTenBase Database Management System

**Language**: [English](README.md) | [简体中文](README_ZH.md)

OpenTenBase is an advanced enterprise-level database management system based on prior work of Postgres-XL project. It supports an extended subset of the SQL standard, including transactions, foreign keys, user-defined types and functions. Additionally, it adds parallel computing, security, management, audit and other functions.

OpenTenBase has many language interfaces similar to PostgreSQL, many of which are listed here:

	https://www.postgresql.org/download

## Overview
An OpenTenBase cluster consists of multiple CoordinateNodes, DataNodes, and GTM nodes. All user data resides in the DataNodes, the CoordinateNode contains only metadata, the GTM is for global transaction management. The CoordinateNodes and DataNodes share the same schema.

Users always connect to the CoordinateNodes, which divide up the query into fragments that are executed in the DataNodes, and collect the results.

The latest version of this software may be obtained at:

	https://github.com/OpenTenBase/OpenTenBase

For more information look at our website located at:

	https://www.opentenbase.org/

## Building
### System Requirements: 

Memory: 8G RAM minimum

OS: TencentOS 2, TencentOS 3, OpenCloudOS 8.x, CentOS 7, CentOS 8, Ubuntu 18.04

### Dependencies

``` 
yum -y install git sudo gcc make readline-devel zlib-devel openssl-devel uuid-devel bison flex cmake postgresql-devel libssh2-devel sshpass  libcurl-devel libxml2-devel
```

or

```
apt install -y git sudo gcc make libreadline-dev zlib1g-dev libssl-dev libossp-uuid-dev bison flex cmake libssh2-1-dev sshpass libxml2-dev language-pack-zh-hans
```


### Create User 'opentenbase'

```bash
# 1. Make directory /data
mkdir -p /data

# 2. Add user 
useradd -d /data/opentenbase -s /bin/bash -m opentenbase # add user opentenbase

# 3. Set password
passwd opentenbase # set password

# 4. Add users to the wheel group
# For RedHat
usermod -aG wheel opentenbase
# For Debian
usermod -aG sudo opentenbase

# 5. Enable sudo permissions for the wheel group (via visudo)
visudo 
# Then uncomment the line "% wheel", save and exit
```

### Building

```bash
su - opentenbase
cd /data/opentenbase/
git clone https://github.com/OpenTenBase/OpenTenBase

export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install/

cd ${SOURCECODE_PATH}
rm -rf ${INSTALL_PATH}/opentenbase_bin_v5.0
chmod +x configure*
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS="-g"
make clean
make -sj
make install
chmod +x contrib/pgxc_ctl/make_signature
cd contrib
make -sj
make install
```

## Installation
Use OPENTENBASE\_CTL tool to build a cluster, for example: a cluster with a global transaction management node (GTM), a coordinator(COORDINATOR) and two data nodes (DATANODE).
<img src="images/topology.png" width="50%" />
### Preparation

#### 1. Install opentenbase and import the path of opentenbase installation package into environment variable.

```shell
PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0
export PATH="$PATH:$PG_HOME/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PG_HOME/lib"
export LC_ALL=C
```

#### 2. Disable SELinux and firewall (optional)

```
vi /etc/selinux/config 
set SELINUX=disabled

# Disable firewalld
sudo systemctl disable firewalld
sudo systemctl stop firewalld
```

#### 3. Create the *.tar.gz package for initializing instances.

```
cd ${PG_HOME}
tar -zcf ${INSTALL_PATH}/opentenbase-5.21.8-i.x86_64.tar.gz *
cd ${INSTALL_PATH}
```

### Cluster startup steps

#### Generate and fill in configuration file opentenbase\_config.ini .
opentenbase\_ctl tool can generate a template for the configuration file. You need to fill in the cluster node information in the template. After the opentenbase\_ctl tool is started, opentenbase\_ctl directory will be generated in the current user's home directory. After entering " prepare config" command, the configuration file template that can be directly modified will be generated in opentenbase\_ctl directory.

* Description of each field in opentenbase\_config.ini
```
| Configuration Category | Configuration Item | Description                                                                |
|------------------------|-------------------|----------------------------------------------------------------------------|
| instance               | name              | Instance name, available characters: letters, numbers, underscores, e.g.: opentenbase_instance01 |
|                        | type              | distributed represents distributed mode, requires gtm, coordinator and data nodes; centralized represents centralized mode |
|                        | package           | Software package. Full path (recommended) or relative path to opentenbase_ctl |
| gtm                    | master            | Master node, only one IP                                                   |
|                        | slave             | Slave nodes. If n slave nodes are needed, configure n IPs here, separated by commas |
| coordinators           | master            | Master node IPs, automatically generate node names, deploy nodes-per-server nodes on each IP |
|                        | slave             | Slave node IPs, the number is an integer multiple of master               |
|                        |                   | Example: If 1 master 1 slave, the number of IPs is the same as master; if 1 master 2 slaves, the number of IPs is twice that of master |
|                        | nodes-per-server  | Optional, default 1. Number of nodes deployed on each IP. Example: master has 3 IPs, configured as 2, then there will be 6 nodes |
|                        |                   | cn001-cn006 total 6 nodes, 2 nodes distributed on each server            |
| datanodes              | master            | Master node IPs, automatically generate node names, deploy nodes-per-server nodes on each IP |
|                        | slave             | Slave node IPs, the number is an integer multiple of master               |
|                        |                   | Example: If 1 master 1 slave, the number of IPs is the same as master; if 1 master 2 slaves, the number of IPs is twice that of master |
|                        | nodes-per-server  | Optional, default 1. Number of nodes deployed on each IP. Example: master has 3 IPs, configured as 2, then there will be 6 nodes |
|                        |                   | dn001-dn006 total 6 nodes, 2 nodes distributed on each server            |
| server                 | ssh-user          | Username for remote command execution, needs to be created in advance, all servers should have the same account for simpler configuration management |
|                        | ssh-password      | Password for remote command execution, needs to be created in advance, all servers should have the same password for simpler configuration management |
|                        | ssh-port          | SSH port, all servers should be consistent for simpler configuration management |
| log                    | level             | Log level for opentenbase_ctl tool execution (not the log level of opentenbase nodes) |

```

#### 1. Create a configuration file opentenbase\_config.ini for the instance
```
mkdir -p ./logs
touch opentenbase_config.ini
vim opentenbase_config.ini
```

* For example, if I have two servers 172.16.16.49 and 172.16.16.131, the typical configuration of a distributed instance distributed across the two servers is as follows. You can copy this configuration information and make modifications according to your deployment requirements. Don't forget to fill in the ssh password configuration.
```
# Instance configuration
[instance]
name=opentenbase01
type=distributed
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# GTM nodes
[gtm]
master=172.16.16.49
slave=172.16.16.50,172.16.16.131

# Coordinator nodes
[coordinators]
master=172.16.16.49
slave= 172.16.16.131
nodes-per-server=1

# Data nodes
[datanodes]
master=172.16.16.49,172.16.16.131
slave=172.16.16.131,172.16.16.49
nodes-per-server=1

# Login and deployment account
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=36000

# Log configuration
[log]
level=DEBUG
```


* Similarly, the configuration of a typical centralized instance is as follows. Don't forget to fill in the ssh password configuration.
```
# Instance configuration
[instance]
name=opentenbase02
type=centralized
package=/data/opentenbase/install/opentenbase-5.21.8-i.x86_64.tar.gz

# Data nodes
[datanodes]
master=172.16.16.49
slave=172.16.16.131
nodes-per-server=1

# Login and deployment account
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=36000

# Log configuration
[log]
level=DEBUG
```

#### 2. Execute command for instance installation.

```
opentenbase_ctl install  -c opentenbase_config.ini

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
* When you see the words 'Installation completed successfully', it means that the installation has been completed. Enjoy your opentenbase journey to the fullest.
* You can check the status of the instance
```
[opentenbase@VM-16-49-tencentos opentenbase_ctl]$ ./opentenbase_bin_v5.0/bin/opentenbase_ctl status -c opentenbase_config.ini

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

## Blogs and Articles
|Blogs and Articles|
|------------------|
|[Quick Start](https://www.opentenbase.org/en/blog/01-quickstart/)|

## History
[history_events](history_events.md)
