
# OpenTenBase Cluster Management Tool
opentenbase_ctl 在bin目录中，是为 Opentenbase 5 版本提供的极简的运维工具，支持对集群进行初始化、删除、节点启停、集群启停、查看状态，shell、sql等常用操作。

## 功能特性

- 支持通过配置文件完成集群的创建和删除
- 支持集群或节点的启动、停止、状态查看操作
- 支持shell、sql等常用操作
- 详细的日志记录
- 支持多种节点类型（CN、DN）

## 编译安装
流水线编译后，默认在bin目录下（opentenbase_ctl）。

### 依赖项

- C++17 或更高版本
- libssh2 开发库

### 编译步骤

1. 克隆代码仓库：
```bash
git clone <repository_url>
cd opentenbase_ctl
```

2. 创建构建目录：
```bash
mkdir build
cd build
```

3. 配置和编译：
```bash
cmake ..
make
```

4. 安装（可选）：
```bash
make install
```
流水线编译后，默认在bin目录下（opentenbase_ctl）。

## 使用方法

## 工具说明

`opentenbase_ctl`是为 Opentenbase 5 版本提供的极简的运维工具，支持对集群进行初始化、删除、节点启停、集群启停、查看状态，shell、sql等常用操作，还支持针对某一类节点操作，提升运维效率。
使用说明如下：
``` bash
[root@VM-16-49-tencentos opentenbase_ctl]# ./opentenbase_ctl -h
[opentenbase@VM-32-21-tencentos ~/install/opentenbase_bin_v5.0/bin]$ ./opentenbase_ctl -h
Opentenbase cluster management tool
Usage: opentenbase_ctl [OPTIONS] [SUBCOMMAND]

Options:
  -h,--help                   Print this help message and exit

Subcommands:
  install                     Install a new instance
  delete                      Delete an existing instance
  start                       Start a Instance
  stop                        Stop a instance
  status                      Show instance status
  scp                         scp files to cluster nodes
  shell                       Execute shell command
  sql                         Execute sql command
  guc                         Display or set GUC parameters
```

## 操作示例

### 1、准备工作
1）对安装的每个服务器上创建opentenbase用户,账号密码要求一致（如果之前已经操作，可跳过）
1.1) root账号下创建目录 /data
``` bash
  [root@VM-32-21-tencentos ~]# mkdir -p /data
```

1.2) root账号下创建用户opentenbase
``` bash
  [root@VM-32-21-tencentos ~]# useradd -d /data/opentenbase -s /bin/bash -m opentenbase
```

1.3) 修改opentenbase的密码（所有服务器的opentenbase密码要一致）
``` bash
  [root@VM-32-21-tencentos ~]# passwd opentenbase
```

1.4) 把opentenbase用户加入到 wheel group
根据操作系统，下面两个语句（二选一）执行一个。
（你也可通过 cat /etc/os-release 命令，查看ID_LIKE字段区分是RedHat还是Debian）
For RedHat（CentOS等）
``` bash
  [root@VM-32-21-tencentos ~]# usermod -aG wheel opentenbase
```
For Debian（Ubuntu/Kylin V10/统信UOS等）
``` bash
  [root@VM-32-21-tencentos ~]# usermod -aG sudo opentenbase
```

1.5) 启用wheel group 的 sudo 权限
执行 visudo 命令，然后搜索wheel，可以看到%wheel开头的两条配置，分别去掉最左边的#，保存退出。
``` bash
  [root@VM-32-21-tencentos ~]# visudo 
```
``` bash
%wheel  ALL=(ALL)       ALL
%wheel  ALL=(ALL)       NOPASSWD: ALL
```

2）服务器安装 sshpass，用于自动分发软件包
（which sshpass 可以判断是否安装了。很多操作系统默认已安装好，则跳过即可）。
只需要对执行机上安装sshpass就可以，其他的服务器可以不用安装。
yum install sshpass

3）准备三大件（一键部署工具、内核包、部署的配置文件）,都放入/data/opentenbase/ 目录中。
3.1）准备好一件部署工具
``` bash
  [root@VM-32-21-tencentos ~]# su - opentenbase
  Last login: Thu Nov  6 12:13:05 CST 2025 on pts/0
  [opentenbase@VM-32-21-tencentos ~]$ pwd
  /data/opentenbase
  [root@VM-32-21-tencentos ~]# wget https://opentenbase-1302252972.cos.ap-nanjing.myqcloud.com/opentenbase_ctl
  [root@VM-32-21-tencentos ~]# chmod +x opentenbase_ctl
  [root@VM-32-21-tencentos ~]# [opentenbase@VM-32-21-tencentos ~/temp]$ ls
  opentenbase_ctl
```

3.2）准备好内核包
方式1: 从归档地址下载 *.tar.gz 包。
Centos 7.* x86 的包：
``` bash
  [root@VM-32-21-tencentos ~]# wget  https://opentenbase-1302252972.cos.ap-nanjing.myqcloud.com/opentenbase-5.21.8-i.x86_64.tar.gz
```

方式2:源码编译后，可以通过 cd ${INSTALL_PATH} 到编译结果的目录，然后进行打包，然后cp到 /data/opentenbase 目录下。
过程和包结构信息如下：
``` bash
  [opentenbase@VM-32-21-tencentos ~]$ cd ${INSTALL_PATH}
  [opentenbase@VM-32-21-tencentos ~]$ ls
  bin  include  lib  share
  [opentenbase@VM-32-21-tencentos ~]$ tar zcf opentenbase-5.21.8-i.x86_64.tar.gz *
  [opentenbase@VM-32-21-tencentos ~]$ ls opentenbase-5.21.8-i.x86_64.tar.gz 
  bin  include  lib  share  opentenbase-5.21.8-i.x86_64.tar.gz
  [opentenbase@VM-32-21-tencentos ~]$ mv opentenbase-5.21.8-i.x86_64.tar.gz /data/opentenbase/
```

方式3:若你获取到rpm包，可用rpm2cpio opentenbase-5.21.8-i.x86_64.rpm | cpio -idmv 命令解压一下rpm包在当前目录，然后cd到对应目录打包成 *.tar.gz格式的包
``` bash
  [opentenbase@VM-32-21-tencentos ~]$ rpm2cpio opentenbase-5.21.8-i.x86_64.rpm | cpio -idmv
  [opentenbase@VM-32-21-tencentos ~]$ cd ./usr/local/install/opentenbase/
  [opentenb[opentenbase@VM-32-21-tencentos ~]$ ls
  bin  include  lib  share
  [opentenbase@VM-32-21-tencentos ~]$ tar -zcf opentenbase-5.21.8-i.x86_64.tar.gz *
  [opentenbase@VM-32-21-tencentos ~]$ ls
  bin  include  lib  opentenbase-5.21.8-i.x86_64.tar.gz  share
  [opentenbase@VM-32-21-tencentos ~]$ mv opentenbase-5.21.8-i.x86_64.tar.gz /data/opentenbase/
```

3.3）准备好部署的配置文件
在opentenbase用户下，cd 到 /data/opentenbase 目录下，新建2个配置文件`postgres.conf`和`config.ini`，
postgres.conf:用户自定义配置项，如果你想节点初始化后替换某个guc配置项，可以将该配置写在这个文件中。如果没有特别的自定义配置，直接使用工具自动配置的GUC，那么这个文件为空即可。
config.ini: 部署信息，包括gtm、coordinators、datanodes的端口。其中 server 中的 ssh-password 是opentenbase账号的密码。
然后复制下面的配置模板信息粘贴到 `config.ini`，并根据自己环境信息作调整，保存退出。
``` bash
[root@VM-32-21-tencentos ~]# su - opentenbase
Last login: Thu Nov  6 12:13:05 CST 2025 on pts/0
[opentenbase@VM-32-21-tencentos ~]$ pwd
/data/opentenbase
[opentenbase@VM-32-21-tencentos ~]$ touch postgres.conf
[opentenbase@VM-32-21-tencentos ~]$ touch config.ini
[opentenbase@VM-32-21-tencentos ~]$ vim config.ini 
```

配置文件模板：
``` bash
# 实例配置
[instance]
name=test_cluster01
type=distributed
package=/data/opentenbase/opentenbase-5.21.8-i.x86_64.tar.gz

# gtm节点
[gtm]
master=172.16.32.34
slave=172.16.32.23

# 协调节点
[coordinators]
master=172.16.32.23
slave= 172.16.32.34
nodes-per-server=1
conf=/data/opentenbase/postgres.conf

# 数据节点
[datanodes]
master=172.16.32.34,172.16.32.23
slave=172.16.32.23,172.16.32.34
nodes-per-server=1
conf=/data/opentenbase/postgres.conf

# 登录和部署账号
[server]
ssh-user=opentenbase
ssh-password=
ssh-port=22

# 日志配置
[log]
level=DEBUG
```

配置文件说明：

![](https://private-document-1305194262.cos.ap-guangzhou.tencentcos.cn/tRpcWrite/100041564813/21cec8f0936b11f089d25254007c27c5.png?q-sign-algorithm=sha1&q-ak=AKIDppsj4fD5tTxrr-lvjGvHB02hRd58_KxJcv3iGiV3Yp0UVckfGipZI2KHuyzZOoR4&q-sign-time=1762331071;1762331131&q-key-time=1762331071;1762331131&q-header-list=host&q-url-param-list=&q-signature=7d1a40cd7e0846c8d947f1ed267081107ca4b2b3&x-cos-security-token=NxgAlO6nzrDzPOUlmiyFSXfwKeyU4vLabf379586eeb5cf2184092451e30b43fc0dFYK295ftK0qSxeLLrUjwpL7SEfarlSF3A4upo0Mx_euDJiX8jBE1BF9INbMrmg3XOz4fKPlTCW71Pp9p9tAjrYg_oTCxqOTaX-bj1lYGVZfDaNccMVeXwCiiDI16H6lJZ4ER05500mnQhx86faJrBcUnvLdL4rarhHX1o82Tg93Ubpek1x84PPMlBEcyiG0oEDihoAbHGChoBw2dQs_JdwO2TWe8JSdfNs75kyMmL2Z-5pfz5eFTEiiqLlhkySj7mnUp3oWSz-3onWVEYjTxw0djB_rPpXskhMuN5ag3d1nAINj1t46INtV1LK5zILD6Ke7qqiSmh7WVkqwkKIuUUUg9_lHUcgIo-BiDsMskWTvwrkhYV3u0MMesrSwJtf)


### 2、实例初始化

指定配置文件 , 通过执行启动命令 `./opentenbase_ctl install -c config.ini `，工具会自动根据配置文件描述完成实例的初始化操作。

说明：执行install命令前，请再次确认一下前期准备的4个文件是否齐全（如果少了某个文件，请参考前面的步骤排查漏了哪步）：
``` bash
[opentenbase@VM-32-21-tencentos ~/install/opentenbase_bin_v5.0/bin/temp]$ ls -l
total 125160
-rwxr-xr-x 1 opentenbase opentenbase       569 Nov  6 14:23 config.ini
-rwxr-xr-x 1 opentenbase opentenbase 127332923 Nov  6 14:23 opentenbase-5.21.8-i.x86_64.tar.gz
-rwxr-xr-x 1 opentenbase opentenbase    815256 Nov  6 14:23 opentenbase_ctl
-rwxr-xr-x 1 opentenbase opentenbase         0 Nov  6 14:23 postgres.conf
```

操作示例：
``` bash
[opentenbase@VM-32-21-tencentos ~]# ./opentenbase_ctl install  -c config.ini 
====== Start to Install OpenTenBase test_cluster06  ====== 
step 1: Make *.tar.gz pkg ...
    Make opentenbase-5.0.x86_64.tar.gz successfully.
 
step 2: Transfer and extract pkg to servers ...
    Package_path: /data/opentenbase_ctl/opentenbase-5.0.x86_64.tar.gz
    Transfer and extract pkg to servers successfully.
 
step 3: Install gtm master node ...
    Install gtm001(172.16.16.131) ...
    Install gtm001(172.16.16.131) successfully
    Success to install  gtm master node. 
 
step 4: Install cn/dn master node ...
    Install cn001(172.16.16.49) ...
    Install dn001(172.16.16.131) ...
    Install cn001(172.16.16.49) successfully
    Install dn001(172.16.16.131) successfully
    Success to install all cn/dn master nodes. 
 
step 5: Install slave nodes ...
    Install cn001(172.16.16.131) ...
    Install dn001(172.16.16.49) ...
    Install gtm002(172.16.16.49) ...
    Install gtm002(172.16.16.49) successfully
    Install dn001(172.16.16.49) successfully
    Install cn001(172.16.16.131) successfully
    Success to install all slave nodes. 
 
step 6:Create node group ...
    Create node group successfully. 
 
 ====== Installation completed successfully  ====== 
```

如果看到整体的 `“Installation completed successfully”` 的字样，且没有报错，那么实例就初始化成功了。接下来你可以参考下面的章节查看实例或节点的状态，并根据需要进行其他日常运维操作。

> **说明：**
> 

> 一定要确保`config.ini`中的 SSH 账号的正确，否则会在执行命令时`ssh`超时或删除失败。
> 


### 3、实例状态查看

通过执行指定配置文件的状态查看命令 `./opentenbase_ctl status`，工具会输出每个节点的状态和 `master cn` 的连接信息。

1）Node status：显示了每个节点的状态。

Running 代表运行状态。

Stopped 代表节点已停止。

Unknown 代表节点是未知状态，可能是服务器故障或 SSH 的账号不对，无法获取到节点信息。

2）Master CN Connection Info 展示了主 CN 节点的信息，包括环境变量和 psql 的语句，便于用户便捷地复制并执行日常管理命令。

3）操作示例：
``` bash
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl status

 ------------- Instance status -----------  
Instance name: test_cluster01
Version: 5.21.8

 -------------- Node status --------------  
Node dn0001(172.16.32.23) is Running 
Node cn0001(172.16.32.23) is Running 
Node gtm0002(172.16.32.23) is Running 
Node gtm0001(172.16.32.34) is Running 
Node cn0001(172.16.32.34) is Running 
Node dn0001(172.16.32.34) is Running 
[Result] Total: 6, Running: 6, Stopped: 0, Unknown: 0

 ------- Master CN Connection Info -------  
[1] cn0001(172.16.32.23)  
Environment variable: export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
PSQL connection: psql -h 172.16.32.23 -p 11003 -U opentenbase postgres

[opentenbase@VM-32-21-tencentos ~]$ 
```



4）psql 登录并验证基本功能

我们可以复制 "Environment variable" 展示的环境变量（export 字样的内容，如下所示），声明节点的环境变量。

然后，我们复制 PSQL connection 的信息，进行登录。操作过程参考：
``` bash
[opentenbase@VM-32-21-tencentos ~]$ export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH}
[opentenbase@VM-32-21-tencentos ~]$ psql -h 172.16.32.23 -p 11003 -U opentenbase postgres
psql (PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: a469acaa5) 2025-06-17 20:36:55)
Type "help" for help.

postgres=# create table test(id int);
CREATE TABLE
postgres=# insert into test(id) values(1);
INSERT 0 1
postgres=# select * from test;
 id 
----
  1
(1 row)

postgres=#
```



### 4、节点状态查看

如果用户只需要查看某个节点的状态，可以指定操作的参数的方式进行节点查询。命令参考：

1）指定一组节点（1主多备）：

`./opentenbase_ctl status -n cn0001`

2）指定1个节点：

`./opentenbase_ctl status -n 172.16.32.23:11003`

3）指定某一类节点：
``` bash
./opentenbase_ctl status -n cn-slave
./opentenbase_ctl status -n cn-master
./opentenbase_ctl status -n dn-slave
./opentenbase_ctl status -n dn-master
```

操作示例：
``` bash
方式1:指定节点名称
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl status -n cn0001

 ------------- Instance status -----------  
Instance name: test_cluster01
Version: v5.21.8

 -------------- Node status --------------  
Node cn0001(172.16.32.23:11003) is Running 
Node cn0001(172.16.32.34:11003) is Running 
[Result] Total: 2, Running: 2, Stopped: 0, Unknown: 0

 ------- Master CN Connection Info -------  
[1] cn0001(172.16.32.23)  
Environment variable: export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
PSQL connection: psql -h 172.16.32.23 -p 11003 -U opentenbase postgres

[opentenbase@VM-32-23-tencentos opentenbase_ctl]$ 

# 方式2:指定ip:port
[opentenbase@VM-32-23-tencentos opentenbase_ctl]$ ./opentenbase_ctl status -n 172.16.32.23:11003

 ------------- Instance status -----------  
Instance name: test_cluster01
Version: v5.21.8

 -------------- Node status --------------  
Node cn0001(172.16.32.23:11003) is Running 
[Result] Total: 1, Running: 1, Stopped: 0, Unknown: 0

 ------- Master CN Connection Info -------  
[1] cn0001(172.16.32.23)  
Environment variable: export LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
PSQL connection: psql -h 172.16.32.23 -p 11003 -U opentenbase postgres
```



### 5、实例停止

指定配置文件,通过执行停止命令 .`/opentenbase_ctl stop`，工具会对配置文件中的每个节点并发进行停止。
``` bash
[opentenbase@VM-32-21-tencentos ~]# ./opentenbase_ctl stop
 
Start executing stop node... 
 
Stop node dn001(172.16.16.131) Success 
Stop node dn001(172.16.16.49) Success 
Stop node cn001(172.16.16.49) Success 
Stop node cn001(172.16.16.131) Success 
Stop node gtm002(172.16.16.49) Success 
Stop node gtm001(172.16.16.131) Success 
 
[Result] Total: 6, Success: 6, Failed: 0
```

> **说明：**
> 

> 一定要确保`config.ini`中的SSH账号的正确，否则会在停止时`ssh`超时或停止失败。
> 


### 6、停止某个节点

指定操作的参数的方式进行节点查询，命令参考：

1）指定一组节点（1主多备）：

`./opentenbase_ctl stop -n cn0001`

2）指定1个节点：

`./opentenbase_ctl stop -n 172.16.32.23:11003`

3）指定一类节点：
``` bash
./opentenbase_ctl stop -n cn-slave
./opentenbase_ctl stop -n cn-master
./opentenbase_ctl stop -n dn-slave
./opentenbase_ctl stop -n dn-master
```

操作示例：
``` bash
方式1:指定节点名称
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl stop -n cn0001

Start executing stop node... 

Stop node cn0001(172.16.32.23) Success 
Stop node cn0001(172.16.32.34) Success 

[Result] Total: 2, Success: 2, Failed: 0

# 方式2:指定 ip:port
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl stop -n 172.16.32.23:11003

Start executing stop node... 

Stop node cn0001(172.16.32.23) Success 

[Result] Total: 1, Success: 1, Failed: 0
```

###  7、实例启动

指定配置文件,通过执行启动命令 `./opentenbase_ctl start `，工具会对配置文件中的每个节点并发进行启动。



操作示例：
``` bash
[root@VM-16-49-tencentos opentenbase_ctl]# ./opentenbase_ctl start
 
Start executing start node... 
 
Start node gtm002(172.16.16.49) Success 
Start node gtm001(172.16.16.131) Success 
Start node dn001(172.16.16.49) Success 
Start node cn001(172.16.16.49) Success 
Start node cn001(172.16.16.131) Success 
Start node dn001(172.16.16.131) Success 
 
[Result] Total: 6, Success: 6, Failed: 0
```

> **说明：**
> 

> 一定要确保 `config.ini` 中的 SSH 账号的正确，否则会在启动时`ssh`超时或启动失败。
> 


### 8、启动某个节点

指定操作的参数的方式进行启动，命令参考：

1）指定一组节点（1主多备）：

`./opentenbase_ctl start -n cn0001`

2）指定1个节点：

`./opentenbase_ctl start -n 172.16.32.23:11003`

3）指定一类节点：
``` bash
./opentenbase_ctl start -n cn-slave
./opentenbase_ctl start -n cn-master
./opentenbase_ctl start -n dn-slave
./opentenbase_ctl start -n dn-master
```

操作示例：
``` bash
方式1:指定节点名称
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl start -n cn0001

Start executing start node... 

Start node cn0001(172.16.32.23) Success 
Start node cn0001(172.16.32.34) Success 

[Result] Total: 0, Success: 0, Failed: 0

# 方式2:指定ip:port
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl start -n 172.16.32.23:11003

Start executing start node... 

Start node cn0001(172.16.32.23) Success 

[Result] Total: 0, Success: 0, Failed: 0
```



### 9、批量执行shell命令

如果我们在运维过程中，需要查看实例相关节点的一些信息。可以直接带shell参数执行shell命令。

命令格式：`./opentenbase_ctl shell --cmd "ls -l /data"`

参考示例：
``` bash
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl shell --cmd "ls -l /data"
--------------------------------------------------------  
Instance name : test_cluster01
Version       : 5.21.8
Config file   : 
CMD           : ls -l /data
--------------------------------------------------------  

[ 172.16.32.23 ] Result: 
total 4
drwxr-xr-x 7 opentenbase opentenbase 4096 Sep 17 01:34 opentenbase

[ 172.16.32.34 ] Result: 
total 4
drwxr-xr-x 4 opentenbase opentenbase 4096 Sep 16 22:08 opentenbase

Total: 2,Success: 2
[opentenbase@VM-32-21-tencentos ~]$ 
```

### 10、执行scp命令

如果我们在运维过程中，需要往实例的相关节点上scp传包，可以直接带scp参数完成文件的分发。

命令格式：`./opentenbase_ctl scp --source-file testfile --dest-path /data/opentenbase/`

参考示例：如下图所示，我们先 touch 一个文件 testfile，然后执行`./opentenbase_ctl scp`命令进行包的分发，最后用`./opentenbase_ctl scp`命令查看一下包是否分发成功。
``` bash
[opentenbase@VM-32-21-tencentos ~]$ touch testfile
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl scp --source-file testfile --dest-path /data/opentenbase/

 ------------- Instance status -----------  
Instance name: test_cluster01
Version: 5.21.8
 172.16.32.23 successful 
 172.16.32.34 successful 
Total: 2,Success: 2
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl shell --cmd "ls /data/opentenbase/"
--------------------------------------------------------  
Instance name : test_cluster01
Version       : 5.21.8
Config file   : 
CMD           : ls /data/opentenbase/
--------------------------------------------------------  

[ 172.16.32.23 ] Result: 
a
b
c
install
OpenTenBase
opentenbase.tar.gz
pwd
run
testfile

[ 172.16.32.34 ] Result: 
a
b
c
install
run
testfile

Total: 2,Success: 2
[opentenbase@VM-32-21-tencentos ~]$ 
```



### 11、实例删除

指定配置文件,通过执行启动命令 `./opentenbase_ctl delete`，工具会对配置文件中的每个节点并发进行删除，主要是停止节点并清理节点的数据目录。
``` bash
[opentenbase@VM-32-21-tencentos ~]$ ./opentenbase_ctl delete
Delete node dn0001(172.16.32.23) Success 
Delete node cn0001(172.16.32.34) Success 
Delete node cn0001(172.16.32.23) Success 
Delete node dn0001(172.16.32.34) Success 
Delete node gtm0002(172.16.32.23) Success 
Delete node gtm0001(172.16.32.34) Success 
```

> **说明：**
> 

> 一定要确保`config.ini`中的SSH账号的正确，否则会在执行命令时`ssh`超时或删除失败。
> 


### 12、帮助和指引

查看工具支持的功能，可以执行命令 `./opentenbase_ctl -h `来看详细支持的功能。其中 `expand` 和 `shrink` 目前只有云数仓形态支持，其他产品形态暂时不支持。

#### 1)     查看支持的基本功能
``` bash
[root@VM-16-49-tencentos opentenbase_ctl]# ./opentenbase_ctl -h
OpenTenBase cluster management tool 
 
 
opentenbase_ctl [OPTIONS] [SUBCOMMAND]
 
 
OPTIONS:
  -h,     --help              Print this help message and exit 
          --version           Display program version information and exit 
 
SUBCOMMANDS:
  install                     Install a new OpenTenBase cluster 
  delete                      Delete an existing OpenTenBase cluster 
  start                       Start a OpenTenBase cluster 
  stop                        Stop a OpenTenBase cluster 
  status                      Show OpenTenBase cluster status 
  expand                      Expand a OpenTenBase cluster 
  shrink                      Shrink a OpenTenBase cluster 
```

#### 2）查看具体某个功能（比如 stop）的命令参数： 
``` bash
[opentenbase@VM-32-21-tencentos ~]# ./opentenbase_ctl stop -h
Stop a OpenTenBase cluster 
 
 
opentenbase_ctl stop [OPTIONS]
 
 
OPTIONS:
  -h,     --help              Print this help message and exit 
  -c,     --config TEXT       Path to configuration file 
          --instance-name TEXT 
                              Instance name 
          --package-path TEXT Package path 
          --node-name TEXT    Node name 
          --node-ip TEXT      Node IP 
          --ssh-user TEXT     SSH user 
          --ssh-password TEXT SSH password 
          --ssh-port TEXT     SSH port 
```

### 13、常见问题的解决方法

#### 问题1、我需要提前在每个机器上做哪些准备呢？

答：需要在每个机器上创建opentenbase用户名，并检查确认cpu、内存和磁盘信息满足基本测试需要。

#### 问题2、是否需要配置执行机和其他机器之间的互信？

答：不需要配置互信。工具是通过配置文件中的SSH账号进行远程命令执行。为了操作更高效，建议部署集群的各节点的ssh账号和端口保持一致。

#### 问题3、初始化实例后，如何通过psql工具连接到实例上？

答：参考如下步骤：

1）查看实例的状态：通过status命令（参考命令： ./opentenbase_ctl status） 命令可以获取到节点的状态。最下面会有显示Master CN的环境变量Environment variable的声明语句和PSQL的命令行。

2）切换到opentenbase命令，声明环境变量并执行psql语句，参考示例：
``` bash
[root@VM-16-131-tencentos data]# su - opentenbase
[opentenbase@VM-32-21-tencentos ~]$ LD_LIBRARY_PATH=/data/opentenbase/install/opentenbase/5.21.8/lib  && export PATH=/data/opentenbase/install/opentenbase/5.21.8/bin:${PATH} 
[opentenbase@VM-32-21-tencentos ~]$ psql -h 172.16.16.131 -p 11000 -U opentenbase postgres
psql (PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: a469acaa5) 2025-06-17 20:36:55)
Type "help" for help.
 
postgres=#
```

#### 问题4、如何查看节点的data目录？

答：1）执行 `./opentenbase_ctl status` 可以查看具体的节点信息。

2）SSH登录到节点上，执行 `ps -ef | egrep 'cn|dn|gtm' | grep -v grep` 可以查看到具体的节点的主进程，然后cd到具体节点目录下。

补充说明：节点的目录拼凑规则：`${opentenbase home路径}/run/instance/${实例名}/${节点名}/data`
``` bash
[opentenbase@VM-32-21-tencentos ~/temp]$ ps -ef | egrep 'cn|dn|gtm' | grep -v grep 
openten+  6482     1  0 Nov05 ?        00:00:37 gtm -D /data/opentenbase/run/instance/opentenbase01/gtm0001/data
openten+  7604     1  0 Nov05 ?        00:00:01 /data/opentenbase/install/opentenbase/5.21.8/bin/postgres --datanode -D /data/opentenbase/run/instance/opentenbase01/dn0001/data -i
openten+  7605     1  0 Nov05 ?        00:00:01 /data/opentenbase/install/opentenbase/5.21.8/bin/postgres --coordinator -D /data/opentenbase/run/instance/opentenbase01/cn0001/data -i
```

### 12、帮助和指引

查看工具支持的功能，可以执行命令 `./opentenbase_ctl -h `来看详细支持的功能。其中 `expand` 和 `shrink` 目前只有云数仓形态支持，其他产品形态暂时不支持。

#### 1)     查看支持的基本功能
``` java
[root@VM-16-49-tencentos opentenbase_ctl]# ./opentenbase_ctl -h
OpenTenBase cluster management tool 
 
 
opentenbase_ctl [OPTIONS] [SUBCOMMAND]
 
 
OPTIONS:
  -h,     --help              Print this help message and exit 
          --version           Display program version information and exit 
 
SUBCOMMANDS:
  install                     Install a new OpenTenBase cluster 
  delete                      Delete an existing OpenTenBase cluster 
  start                       Start a OpenTenBase cluster 
  stop                        Stop a OpenTenBase cluster 
  status                      Show OpenTenBase cluster status 
  expand                      Expand a OpenTenBase cluster 
  shrink                      Shrink a OpenTenBase cluster 
```

#### 2）查看具体某个功能（比如 stop）的命令参数： 
``` java
[root@VM-16-49-tencentos opentenbase_ctl]# ./opentenbase_ctl stop -h
Stop a OpenTenBase cluster 
 
 
opentenbase_ctl stop [OPTIONS]
 
 
OPTIONS:
  -h,     --help              Print this help message and exit 
  -c,     --config TEXT       Path to configuration file 
          --instance-name TEXT 
                              Instance name 
          --package-path TEXT Package path 
          --node-name TEXT    Node name 
          --node-ip TEXT      Node IP 
          --ssh-user TEXT     SSH user 
          --ssh-password TEXT SSH password 
          --ssh-port TEXT     SSH port 
```

### 13、常见问题的解决方法

#### 问题1、我需要提前在每个机器上做哪些准备呢？

答：需要在每个机器上创建opentenbase用户名，并检查确认cpu、内存和磁盘信息满足基本测试需要。

#### 问题2、是否需要配置执行机和其他机器之间的互信？

答：不需要配置互信。工具是通过配置文件中的SSH账号进行远程命令执行。为了操作更高效，建议部署集群的各节点的ssh账号和端口保持一致。

#### 问题3、初始化实例后，如何通过psql工具连接到实例上？

答：参考如下步骤：

1）查看实例的状态：通过status命令（参考命令： ./opentenbase_ctl status） 命令可以获取到节点的状态。最下面会有显示Master CN的环境变量Environment variable的声明语句和PSQL的命令行。

2）切换到opentenbase命令，声明环境变量并执行psql语句，参考示例：
``` java
[root@VM-16-131-tencentos data]# su opentenbase
[opentenbase@VM-16-131-tencentos data]$ export LD_LIBRARY_PATH=/home/opentenbase/install/opentenbase/5.0/lib  && export PATH=/home/opentenbase/install/opentenbase/5.0/bin:${PATH} 
[opentenbase@VM-16-131-tencentos data]$ psql -h 172.16.16.131 -p 11000 -U opentenbase postgres
psql (PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: a469acaa5) 2025-06-17 20:36:55)
Type "help" for help.
 
postgres=#
```

#### 问题4、如何查看节点的data目录？

答：1）执行 `./opentenbase_ctl status` 可以查看具体的节点信息。

2）SSH登录到节点上，执行 `ps -ef | egrep 'cn|dn|gtm' | grep -v grep` 可以查看到具体的节点的主进程，然后cd到具体节点目录下。

补充说明：节点的目录拼凑规则：`${opentenbase home路径}/run/instance/${实例名}/${节点名}/data`
``` java
[root@VM-16-49-tencentos opentenbase_ctl]# ps -ef | egrep 'cn|dn|gtm' | grep -v grep 
opentenbase    2198524       1  0 10:45 pts/0    00:03:20 gtm -D /home/opentenbase/run/instance/test_cluster06/gtm002/data
opentenbase    2198525       1  0 10:45 pts/0    00:00:00 /home/opentenbase/install/opentenbase/5.0/bin/postgres --datanode -D /home/opentenbase/run/instance/test_cluster06/dn001/data -i
opentenbase    2198530       1  0 10:45 pts/0    00:00:02 /home/opentenbase/install/opentenbase/5.0/bin/postgres --coordinator -D /home/opentenbase/run/instance/test_cluster06/cn001/data -i
```


### 节点命名规则
自动生成节点名称
- CN 节点：以 `cn` 开头，例如 `cn0001`, `cn0002` 等
- DN 节点：以 `dn` 开头，例如 `dn0001`, `dn0002` 等

工具会根据节点名称自动判断节点类型，无需额外指定。

## 日志

日志级别可以通过配置文件或环境变量设置：

- 配置文件：在 `[log]` 部分设置 `level` 参数
- 环境变量：设置 `OPENTENBASE_LOG_LEVEL` 环境变量

支持的日志级别：
- debug
- info
- warn
- error

## 注意事项

1. 确保所有节点的 SSH 访问权限正确配置
2. 确保安装包路径正确且可访问
3. 确保数据目录和安装目录有足够的权限
4. 建议在操作前备份重要数据

## 许可证

[许可证类型]

## 贡献指南

[贡献指南内容] 
