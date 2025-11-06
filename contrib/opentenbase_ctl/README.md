
# OpenTenBase Cluster Management Tool
OpenTenBase 集群管理工具，用于管理 OpenTenBase 集群的安装、删除、启动、停止、扩容和缩容等操作。

## 功能特性

- 支持通过配置文件或命令行参数管理集群
- 支持集群的安装、删除、启动、停止操作
- 自动端口分配和管理
- 详细的日志记录
- 支持多种节点类型（CN、DN）

## 编译安装
流水线编译后，默认在bin目录下（opentenbase_ctl）。

### 依赖项

- C++17 或更高版本
- CMake 3.10 或更高版本
- libssh2 开发库
- CLI11 库（已包含在项目中）

#### 1、安装cmake-3.26.5的过程：
```bash
说明：如果本地编译机cmake已经是3.*的版本，则跳过本步骤。
1) 下载源码，使用命令行下载：wget https://github.com/Kitware/CMake/releases/download/v3.26.5/cmake-3.26.5.tar.gz
2) 解压并进入目录
解压文件：tar -zxvf cmake-3.26.5.tar.gz
进入解压后的目录：cd cmake-3.26.5
3）编译和安装
执行引导脚本，指定安装目录（请耐心等3分钟）：./bootstrap --prefix=/usr/local/cmake
执行编译（请耐心等3分钟）：make
执行安装：make install
检查cmake版本：cmake --version
说明：如果cmake显示的版本还是老的，可以用bootstrap时指定的安装目录,配置到环境变量中：
vi ~/.bashrc
在文件末尾把cmake添加到PATH环境变量中，保存退出：export PATH=/usr/local/cmake/bin:$PATH
source ~/.bashrc
然后再次查看cmake版本。
```

#### 2、安装 CLI11 的过程（依赖cmake 3）
```bash
1) 克隆 CLI11 源码：
git clone https://github.com/CLIUtils/CLI11.git 
cd CLI11
2) 编译和安装
mkdir build
cd build
cmake ..
make
sudo make install
```

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
1. 准备一个内核包optentenbase-5.21.8-i.x86_64.tar.gz，包的打包过程和包结构信息如下：
```bash
opentenbase@VM-32-23-tencentos opentenbase]$ ls
bin  include  lib  share
[opentenbase@VM-32-23-tencentos opentenbase]$ tar zcf opentenbase-5.21.8-i.x86_64.tar.gz *
[opentenbase@VM-32-23-tencentos opentenbase]$ ls
bin  include  lib  share  opentenbase-5.21.8-i.x86_64.tar.gz
```
2. 生成一个配置文件，典型的配置文件：
建议你解压一个optentenbase-5.21.8-i.x86_64.tar.gz文件，cd到bin目录下touch一个配置文件config.ini,然后复制下面的配置信息粘贴到config.ini，并根据自己环境信息作调整，保存退出。

```ini
# 实例配置
[instance]
name=test_cluster01
type=distributed
package=/data/opentenbase/opentenbase_ctl/opentenbase-5.21.8-i.x86_64.tar.gz

# gtm节点
[gtm]
master=172.16.32.34
slave=172.16.32.23

# 协调节点
[coordinators]
master=172.16.32.23
slave= 172.16.32.34
nodes-per-server=1
conf=/data/opentenbase_ctl/postgres.conf

# 数据节点
[datanodes]
master=172.16.32.34
slave=172.16.32.23
nodes-per-server=2
conf=/data/opentenbase_ctl/postgres.conf

# 登录和部署账号
[server]
ssh-user=opentenbase
ssh-password=optentenbase123
ssh-port=36000

# 日志配置
[log]
level=DEBUG
```
3. 执行集群创建：
```bash
./opentenbase_ctl install -c config.ini
```
4、集群创建成功后iu，查看集群的状态：
```bash
./opentenbase_ctl status
```

### 配置文件

配置文件使用 INI 格式，各个字段说明如下：

```ini

# 实例配置
[instance]
name=optentenbase01                                   # 实例名称,可用的字符：半角大小写字母、数字、下划线，例如: optentenbase_instance01
type=distributed                               # distributed代表分布式，需要gtm、协调节点和数据节点；centralized代表集中式，此时会忽略gtm和协调节点的配置，只有1组数据节点
package=/data/opentenbase/install/opentenbase-3.16.9.301-i.x86_64.tar.gz  # 软件包。全路径或opentenbase_ctl的相对路径。

# gtm节点
[gtm]
master=172.16.16.49                             # 主节点，只有一个IP
slave=172.16.16.50,172.16.16.131                # 备节点，多个备节点时配置多个IP

# 协调节点
[coordinators]
master=172.16.16.49,172.16.16.131               # 主节点IP，自动生成节点名称，每个IP上部署nodes-per-server个节点
slave= 172.16.16.131,172.16.16.131              # 备节点IP，个数是master的整数倍。举例：如果1主1备，则IP个数和master一样多；如果1主2备，则IP个数是master的两倍。
nodes-per-server=2                              # 可选，默认1。每个IP上部署的节点数。举例：master有3个IP，这里配置2，则实际会有cn001-cn006共6个节点，每个服务器分布2个节点。
conf=/data/opentenbase_ctl/postgres.conf              # 可选。如果不配置，可以去掉这个选项，此时工具会使用默认配置；如果有配置，会用这些参数项逐个替换掉工具默认的配置项。

# 数据节点
[datanodes]
master=172.16.16.49,172.16.16.131,172.16.16.131 # 主节点IP，自动生成节点名称，每个IP上部署nodes-per-server个节点
slave=172.16.16.49,172.16.16.131,172.16.16.131  # 备节点IP，个数是master的整数倍。举例：如果1主1备，则IP个数和master一样多；如果1主2备，则IP个数是master的两倍。
nodes-per-server=2                              # 可选，默认1。每个IP上部署的节点数。举例：master有3个IP，这里配置2，则实际会有cn001-cn006共6个节点，每个服务器分布2个节点。
conf=/data/opentenbase_ctl/postgres.conf              # 可选。如果不配置，可去掉这个选项，此时工具会使用默认的系列配置；如果有配置，则会用这些参数项逐个替换掉工具默认的配置项。              

# 登录和部署账号
[server]
ssh-user=opentenbase                            # ssh登录、远程执行命令和节点部署的用户名，需提前创建好,为了配置管理更简单，要求所有服务器的账号一致
ssh-password=                                   # ssh登录、远程执行命令和节点部署的口令，为了配置管理更简单，要求所有服务器的账号和密码一致
ssh-port=36000                                  # ssh登录、远程执行命令的端口，为了配置管理更简单，要求所有服务器的账号和密码一致

# 日志配置
[log]
level=DEBUG
```

### 命令行参数

工具支持以下命令：
- `install`: 安装新的 opentenbase 集群
- `delete`: 删除现有的 opentenbase 集群
- `start`: 启动 opentenbase 集群或节点
- `stop`: 停止 opentenbase 集群或节点
- `status`: 查看 opentenbase 集群或节点状态

### 命令范例

1. 使用配置文件安装集群：
```bash
./opentenbase_ctl install -c config.ini
```
2. 启动集群：
```bash
./opentenbase_ctl start
```
3. 启动节点：
```bash
./opentenbase_ctl start -n cn0001
./opentenbase_ctl start -n 172.16.32.23:11003
./opentenbase_ctl start -n cn-master
./opentenbase_ctl start -n cn-slave
./opentenbase_ctl start -n dn-master
./opentenbase_ctl start -n dn-slave
4. 停止集群：
```bash
./opentenbase_ctl stop
```
5. 停止节点：
```bash
./opentenbase_ctl stop -n cn0001
./opentenbase_ctl stop -n 172.16.32.23:11003
./opentenbase_ctl stop -n cn-master
./opentenbase_ctl stop -n cn-slave
./opentenbase_ctl stop -n dn-master
./opentenbase_ctl stop -n dn-slave
```
6. 查看集群状态：
```bash
./opentenbase_ctl status
```
7. 查看节点状态：
```bash
./opentenbase_ctl status -n cn0001
./opentenbase_ctl status -n 172.16.32.23:11003
./opentenbase_ctl status -n cn-master
./opentenbase_ctl status -n cn-slave
./opentenbase_ctl status -n dn-master
./opentenbase_ctl status -n dn-slave
```
8. 删除集群：
```bash
./opentenbase_ctl delete
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
