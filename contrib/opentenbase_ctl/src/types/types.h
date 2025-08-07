#ifndef TYPES_H
#define TYPES_H

#include <string>
#include <vector>
#include "config/config.h"

// 定义一个命名空间来组织常量
namespace Constants {
    
    // 节点类型相关的常量
    inline constexpr auto NODE_TYPE_GTM_MASTER = "gtm_master";
    inline constexpr auto NODE_TYPE_GTM_SLAVE = "gtm_slave";
    inline constexpr auto NODE_TYPE_CN_MASTER = "cn_master";
    inline constexpr auto NODE_TYPE_CN_SLAVE = "cn_slave";
    inline constexpr auto NODE_TYPE_DN_MASTER = "dn_master";
    inline constexpr auto NODE_TYPE_DN_SLAVE = "dn_slave";
    
    // 实例类型相关的常量
    inline constexpr auto INSTANCE_TYPE_CENTRALIZED = "centralized";
    inline constexpr auto INSTANCE_TYPE_DISTRIBUTED = "distributed";

    // 操作命令
    inline constexpr auto COMMAND_TYPE_INSTALL = "install";
    inline constexpr auto COMMAND_TYPE_DELETE = "delete";
    inline constexpr auto COMMAND_TYPE_START = "start";
    inline constexpr auto COMMAND_TYPE_STOP = "stop";
    inline constexpr auto COMMAND_TYPE_STATUS = "status";

    // 默认平面cluster
    //inline constexpr auto MAIN_CLUSTER_NAME = "opentenbase_cluster";
    inline constexpr auto MAIN_CLUSTER_NAME = "opentenbase_cluster";
}

// 节点信息结构体
struct NodeInfo {
    std::string name;          // 节点名称
    std::string ip;            // 节点 IP 地址
    std::string install_path;  // 安装路径
    std::string data_path;     // 数据路径
    std::string type;          // 节点类型,取值范围参考Constants中的节点类型的定义
    int port;                  // 节点端口（自动分配）
    int pooler_port;           // 节点连接池端口（自动分配，与node port相邻）
    int forward_port;          // 转发节点端口（自动分配，与node port相邻）
    std::string  gtm_name;     // gtm节点名称
    std::string  gtm_ip;       // gtm节点IP
    int gtm_port;              // gtm节点端口
};

// meta 配置结构体
struct MetaConfig {
    std::string etcd_server;  // etcd 服务器地址
    std::string meta_server;  // meta 服务器地址
    std::string txn_server;   // txn 服务器地址
    std::string cluster_oid;  // 虚拟集群 OID
    int meta_id;              // meta 节点 ID
    int shard_num;
};

// instance 配置结构体
struct InstanceConfig {
    std::string instance_name;  // 实例名称
    std::string package_path;   // 安装包路径
    std::string package_name;   // 安装包文件名
    std::string version;        // 版本
    std::string instance_type;  // 实例类型 0:存算一体 1:云数仓 
};

// server 配置结构体
struct ServerConfig {
    std::string ssh_user;     // SSH 用户名
    std::string ssh_password; // SSH 密码
    int ssh_port;             // SSH 端口
};

// 日志配置结构体
struct LogConfig {
    std::string level;  // 日志级别
};

// 总配置结构体
struct OpentenbaseConfig {
    MetaConfig meta;              // meta 配置
    InstanceConfig instance;      // instance 配置
    std::vector<NodeInfo> nodes;  // 节点配置
    ServerConfig server;          // server 配置
    LogConfig log;                // 日志配置
};


// 打印节点信息的辅助函数
void printNodeInfo(const NodeInfo& node);

// 打印 MetaConfig 的辅助函数
void printMetaConfig(const MetaConfig& meta);

// 打印 InstanceConfig 的辅助函数
void printInstanceConfig(const InstanceConfig& instance);

// 打印 ServerConfig 的辅助函数
void printServerConfig(const ServerConfig& server);

// 打印 LogConfig 的辅助函数
void printLogConfig(const LogConfig& log);

// 主打印函数，用于打印整个 OpentenbaseConfig 对象
void printOpentenbaseConfig(const OpentenbaseConfig& config);

// 生成操作的opentenbase-config对象的内容
int build_opentenbase_config(const ConfigFile& cfg_file, OpentenbaseConfig& config);

/**
 * 获取节点的名称。
 *
 * 根据输入的节点名称前缀进行处理：
 * - 如果名称以 "cn" 或 "dn" 开头，直接返回原名称。
 *   例如："cn001", "cn0001", "dn001", "dn00001"
 * - 如果名称以 "gtm" 开头，对数字部分进行递增，并保持相同的位数。
 *   例如：
 *     "gtm001"  → "gtm002"
 *     "gtm009"  → "gtm010"
 *     "gtm0001" → "gtm0002"
 *     "gtm00001"→ "gtm00002"
 * - 如果名称不以 "cn"、"dn" 或 "gtm" 开头，返回空字符串。
 *
 * @param node_name 原始的节点名称。
 * @return 处理后的节点名称，若不符合前缀要求则返回空字符串。
 */
std::string get_node_name(const std::string& node_name);

#endif // TYPES_H 
