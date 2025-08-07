/**
 * @file config.h
 * @brief 配置文件相关的结构和函数声明。
 * 这里的结构定义与配置文件完全对应，只是为了解析配置文件并获取到各个配置项的内容。
 * 在具体使用配置信息的模块中，还会定义实际的节点的信息，解析每个配置项的内容，判断字符或必填项的合法性，并按照规则生成实际的节点信息。
 */

#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @struct ConfigFileInstance
 * @brief instance 配置结构体
 */
struct ConfigFileInstance {
    std::string name;              // 实例名称,可用的字符：半角大小写字母、数字、下划线，例如: opentenbase_instance01
    std::string type;              // distributed代表分布式，需要gtm、协调节点和数据节点；centralized代表集中式，此时会忽略gtm和协调节点的配置，只有1组数据节点
    std::string package;           // 软件包。全路径或opentenbase_ctl的相对路径。
};

/**
 * @struct ConfigFileGtm
 * @brief gtm 配置结构体
 */
struct ConfigFileGtm {
    std::string master;            // 主节点，只有一个IP
    std::string slave;             // 备节点，多个备节点时配置多个IP
};

/**
 * @struct ConfigFileCoordinators
 * @brief coordinators 配置结构体
 */
struct ConfigFileCoordinators {
    std::string master;            // 主节点IP，自动生成节点名称，每个IP上部署nodes-per-server个节点
    std::string slave;             // 备节点IP，个数是master的整数倍。举例：如果1主1备，则IP个数和master一样多；如果1主2备，则IP个数是master的两倍。
    std::string nodes_per_server;  // 可选，默认1。每个IP上部署的节点数。举例：master有3个IP，这里配置2，则实际会有cn001-cn006共6个节点，每个服务器分布2个节点。
    std::string conf;              // 可选。如果不配置，可去掉这个选项，此时工具会使用默认的系列配置；如果有配置，则会用这些参数项逐个替换掉工具默认的配置项。 
};

/**
 * @struct ConfigFileDatanodes
 * @brief datanodes 配置结构体
 */
struct ConfigFileDatanodes {
    std::string master;             // 主节点IP，自动生成节点名称，每个IP上部署nodes-per-server个节点
    std::string slave;              // 备节点IP，个数是master的整数倍。举例：如果1主1备，则IP个数和master一样多；如果1主2备，则IP个数是master的两倍。
    std::string nodes_per_server;   // 可选，默认1。每个IP上部署的节点数。举例：master有3个IP，这里配置2，则实际会有cn001-cn006共6个节点，每个服务器分布2个节点。
    std::string conf;               // 可选。如果不配置，可去掉这个选项，此时工具会使用默认的系列配置；如果有配置，则会用这些参数项逐个替换掉工具默认的配置项。 
};

/**
 * @struct ConfigFileServer
 * @brief server 配置结构体
 */
struct ConfigFileServer {
    std::string ssh_user;           // ssh登录、远程执行命令和节点部署的用户名，需提前创建好,为了配置管理更简单，要求所有服务器的账号一致
    std::string ssh_password;       // ssh登录、远程执行命令和节点部署的口令，为了配置管理更简单，要求所有服务器的账号和密码一致
    std::string ssh_port;           // ssh登录、远程执行命令的端口，为了配置管理更简单，要求所有服务器的账号和密码一致
};

/**
 * @struct ConfigFileLog
 * @brief log 配置结构体
 */
struct ConfigFileLog {
    std::string level;              // 日志级别，可选值：DEBUG, INFO, WARNING, ERROR, CRITICAL
};

/**
 * @struct ConfigFile
 * @brief config.ini 配置文件结构体
 */
struct ConfigFile {
    ConfigFileInstance instance;    // instance 配置
    ConfigFileGtm gtm;              // gtm 配置
    ConfigFileCoordinators coordinators;   // coordinators 配置
    ConfigFileDatanodes datanodes;  // datanodes 配置
    ConfigFileServer server;        // server 配置
    ConfigFileLog log;              // log 配置
};

/**
 * 去掉空格
 * @param str 待处理字符串的地址
 * @return void
 */
void trim_whitespace(std::string &str);

/**
 * 解析节点列表
 * @param str 节点str
 * @return 节点名称列表
 */
std::vector<std::string> parse_node_list(const char* str);

/**
 * 释放配置结构体占用的内存
 * @param config 要释放的配置结构体指针
 * @return void
 */

/**
 * 根据名称获得master节点的类型
 * @param node_name 节点名称
 * @return std::string 节点类型
 */
std::string infer_node_type(const std::string& node_name);

/**
 * 根据名称获得slave节点的类型
 * @param node_name 节点名称
 * @return std::string 节点类型
 */
std::string infer_slave_node_type(const std::string& node_name);

/**
 * 解析配置文件
 * @param filename 配置文件路径
 * @param cfg_file 配置结构体指针，用于存储解析结果
 * @return 0: 解析成功; 非0: 解析失败
 */
int parse_config_file(const std::string& filename, ConfigFile& cfg_file);

#ifdef __cplusplus
}
#endif

#endif // CONFIG_H