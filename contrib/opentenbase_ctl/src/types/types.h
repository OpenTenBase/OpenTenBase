#ifndef TYPES_H
#define TYPES_H

#include <string>
#include <vector>
#include "../config/config.h"
#include "../command/command.h"

// 定义一个命名空间来组织常量
namespace Constants {
    
    // APP basic info
    inline constexpr auto APP_NAME = "opentenbase_ctl";
    inline constexpr auto APP_DESC = "OpenTenBase cluster management tool";
    inline constexpr auto APP_VERSION = "v5.21.9.7";

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
    inline constexpr auto COMMAND_TYPE_SCP = "scp";
    inline constexpr auto COMMAND_TYPE_SHELL = "shell";
    inline constexpr auto COMMAND_TYPE_SQL = "sql";

    // 默认平面cluster,{"optentenbase_cluster","opentenbase_cluster"}
    inline constexpr auto MAIN_CLUSTER_NAME = "optentenbase_cluster";

    // 环境变量
    inline constexpr auto ENV_CLUSTER_CONFIG_FILE = "CLUSTER_CONFIG_FILE";

    // initdb
    inline constexpr auto DEFAULT_USER_OF_INITDB = "opentenbase";
    inline constexpr auto DEFAULT_DB = "postgres";
    inline constexpr auto DEFAULT_INSTALL_DIR = "/usr/local/install/opentenbase";
    inline constexpr auto DEFAULT_PKG_TMP_DIR = "opentenbase";
    
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
    bool is_op_node = true;    // 是否是本次操作指定的节点，install时默认全部都是true，其他操作要看命令的--node是否指定本节点
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
    MetaConfig meta;                 // meta 配置
    std::string config_file;         // 配置文件完整路径
    InstanceConfig instance;         // instance 配置
    std::vector<NodeInfo> nodes;     // 全量节点配置
    std::map<std::string, std::string> cn_guc_cfg; // 用户指定的cn的guc配置
    std::map<std::string, std::string> dn_guc_cfg; // 用户指定的dn的guc配置
    ServerConfig server;             // server 配置
    LogConfig log;                   // 日志配置
    ScpConfig scpfile;               // scp 命令的配置
    ShellConfig shell;               // shell 命令的配置
    SQLConfig sql;                   // sql 命令的配置
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
int build_opentenbase_config(CommandLineArgs& args, const ConfigFile& cfg_file, OpentenbaseConfig& config);

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

int build_scp_config(CommandLineArgs& args, OpentenbaseConfig& config);
int build_shell_config(CommandLineArgs& args, OpentenbaseConfig& config);

// 根据配置文件的内容，生成sql的结构体的信息
int build_sql_config(CommandLineArgs& args, OpentenbaseConfig& config);

// 命令行处理函数
bool parse_command_line(int argc, char** argv, CommandLineArgs& args, OpentenbaseConfig& config);

int build_config_from_args(const CommandLineArgs& args, OpentenbaseConfig& config);
int init_node_directories(OpentenbaseConfig& config);
int fill_node_with_gtm_info(OpentenbaseConfig& config);

int build_config_from_args(CommandLineArgs& args, OpentenbaseConfig& config);

// set Environment Variable In Bashrc
int set_config_file_for_args(const CommandLineArgs& args);
int process_op_nodes(CommandLineArgs& args, OpentenbaseConfig& config);


/**
 * @brief 为配置中的每个节点填充端口号。
 *
 * 此函数遍历给定的 `OpentenbaseConfig` 配置中的所有节点，
 * 并为每个节点的 `port` 字段分配或更新端口号。
 *
 * @param[in,out] config 指向 `OpentenbaseConfig` 结构体的指针，包含要更新的节点信息。
 *                       必须保证 `config` 及其 `nodes` 成员有效且不为 `nullptr`。
 *
 * @return 返回整数值表示操作状态。
 *         - `0`：成功为所有节点填充端口号。
 *         - 非零值：在填充过程中遇到错误。
 *
 * @note 确保在调用此函数之前，`config` 及其 `nodes` 成员已被正确初始化。
 * @warning 如果函数返回非零值，建议检查日志以获取详细的错误信息。
 */
int fill_ports_for_nodes(OpentenbaseConfig* config);

int set_config_file_for_args(CommandLineArgs& args);

/**
 * @brief 获取指定节点的端口号。
 *
 * 此函数用于检索给定节点的端口号。它从节点信息结构体中提取端口号信息。
 *
 * @param node 指向包含节点信息的 NodeInfo 结构体的指针。
 *             必须确保 node 不为 nullptr，否则行为未定义。
 * @return 返回节点的端口号作为 std::string。如果节点信息无效或端口号未设置，可能返回空字符串。
 */
std::string get_node_port(NodeInfo* node, OpentenbaseConfig* install);

#endif // TYPES_H 
