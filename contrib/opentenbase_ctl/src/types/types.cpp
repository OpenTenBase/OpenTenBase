#include "types.h"
#include "../log/log.h"
#include "../utils/utils.h"
#include "../config/config.h"
#include <iostream>
#include <string>
#include <iomanip>
#include <sstream>
#include <regex>

// 假设 LOG_INFO_FMT 函数已经在某个头文件中定义
// #include "logging.h" // 请根据实际情况包含相应的头文件

// 打印节点信息的辅助函数
void printNodeInfo(const NodeInfo& node) {
    LOG_INFO_FMT("Node name: %s", node.name.c_str());
    LOG_INFO_FMT("Node IP     : %s", node.ip.c_str());
    LOG_INFO_FMT("Install Path: %s", node.install_path.c_str());
    LOG_INFO_FMT("Data Dir    : %s", node.data_path.c_str());
    LOG_INFO_FMT("Node type   : %s", node.type.c_str());
    LOG_INFO_FMT("Node port   : %zu", node.port);
    LOG_INFO_FMT("Pooler Port : %zu", node.pooler_port);
    LOG_INFO_FMT("GTM name    : %s", node.gtm_name.c_str());
    LOG_INFO_FMT("GTM   IP    : %s", node.gtm_ip.c_str());
    LOG_INFO_FMT("GTM port    : %zu", node.gtm_port);
    LOG_INFO_FMT("-----------------------------");
}

// 打印 MetaConfig 的辅助函数
void printMetaConfig(const MetaConfig& meta) {
    LOG_INFO_FMT("etcd Address: %s", meta.etcd_server.c_str());
    LOG_INFO_FMT("meta Address: %s", meta.meta_server.c_str());
    LOG_INFO_FMT("txn Address : %s", meta.txn_server.c_str());
    LOG_INFO_FMT("Instance OID: %s", meta.cluster_oid.c_str());
    LOG_INFO_FMT("meta node ID: %d", meta.meta_id);
    LOG_INFO_FMT("Shard number: %d", meta.shard_num);
    LOG_INFO_FMT("-----------------------------");
}

// 打印 InstanceConfig 的辅助函数
void printInstanceConfig(const InstanceConfig& instance) {
    LOG_INFO_FMT("Instance name: %s", instance.instance_name.c_str());
    LOG_INFO_FMT("Package path : %s", instance.package_path.c_str());
    LOG_INFO_FMT("Package name : %s", instance.package_name.c_str());
    LOG_INFO_FMT("Version      : %s", instance.version.c_str());
    LOG_INFO_FMT("instance type: %s", instance.instance_type.c_str());
    LOG_INFO_FMT("-----------------------------");
}

// 打印 ServerConfig 的辅助函数
void printServerConfig(const ServerConfig& server) {
    LOG_INFO_FMT("SSH user name: %s", server.ssh_user.c_str());
    LOG_INFO_FMT("SSH password : %s", server.ssh_password.c_str());
    LOG_INFO_FMT("SSH port     : %d", server.ssh_port);
    LOG_INFO_FMT("-----------------------------");
}

// 打印 LogConfig 的辅助函数
void printLogConfig(const LogConfig& log) {
    LOG_INFO_FMT("Log level: %s", log.level.c_str());
    LOG_INFO_FMT("-----------------------------");
}

// 主打印函数，用于打印整个 OpentenbaseConfig 对象
void printOpentenbaseConfig(const OpentenbaseConfig& config) {
    LOG_INFO_FMT("=== Meta Config ===");
    printMetaConfig(config.meta);

    LOG_INFO_FMT("=== Instance Config ===");
    printInstanceConfig(config.instance);

    LOG_INFO_FMT("=== Node Config ===");
    for (size_t i = 0; i < config.nodes.size(); ++i) {
        LOG_INFO_FMT("Node %zu:", i + 1);
        printNodeInfo(config.nodes[i]);
    }

    LOG_INFO_FMT("=== Server Config ===");
    printServerConfig(config.server);

    LOG_INFO_FMT("=== Log Config ===");
    printLogConfig(config.log);

    LOG_INFO_FMT("=== OpentenbaseConfig Print End ===");
}

// 函数声明：从完整路径中提取软件包名称
std::string get_package_name(const std::string& full_path_pkg) {
    // 查找最后一个斜杠（兼容Windows和Linux路径）
    size_t last_slash = full_path_pkg.find_last_of("/\\"); 

    // 如果没有找到斜杠，返回整个路径作为包名
    if (last_slash == std::string::npos) {
        return full_path_pkg;
    }

    // 返回斜杠之后的部分作为包名
    return full_path_pkg.substr(last_slash + 1);
}

// 根据配置文件的内容，生成instance结构体的信息
int
build_instance_config(const ConfigFile& cfg_file, OpentenbaseConfig& config) {
    LOG_DEBUG_FMT("Build instance config begin.");

    InstanceConfig instance;  

    // name
    std::string name = cfg_file.instance.name;
    instance.instance_name = name;

    // type
    std::string type = cfg_file.instance.type;
    if (Constants::INSTANCE_TYPE_CENTRALIZED != type && Constants::INSTANCE_TYPE_DISTRIBUTED != type)
    {
        LOG_ERROR_FMT("The instance type(%s) in the configuration file is illegal. The optional values are centralized or distributed.", type.c_str());
        return -1;
    }
    instance.instance_type = type;
    
    // package
    std::string full_path_pkg = cfg_file.instance.package;
    instance.package_path = full_path_pkg;
    std::string pkg_name = get_package_name(full_path_pkg);
    if (pkg_name == "")
    {
        LOG_ERROR_FMT("The instance package in the configuration file is not configured correctly.");
        return -1;
    }
    instance.package_name = pkg_name;
    instance.version = extract_version_from_package_name(instance.package_name);

    // 生成instance结构体的信息
    config.instance = instance;

    LOG_DEBUG_FMT("Build instance config begin.");
    return 0;
}


// 函数声明：判断字符串是否为合法的IP地址（IPv4或IPv6）
bool is_valid_ip_address(const std::string& ip) {
    // IPv4的正则表达式模式
    std::regex ipv4_pattern(
        R"(^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$)"
    );

    // IPv6的正则表达式模式
    std::regex ipv6_pattern(
        R"(^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|"
        R"(([0-9a-fA-F]{1,4}:){1,7}:)|"
        R"(([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4})|"
        R"(([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2})|"
        R"(([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3})|"
        R"(([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4})|"
        R"(([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5})|"
        R"(:((:[0-9a-fA-F]{1,4}){1,6})|:))$)"
    );

    // 使用正则表达式进行匹配
    return std::regex_match(ip, ipv4_pattern) || std::regex_match(ip, ipv6_pattern);
}

// 拆分节点列表
// 使用 std::stringstream 分割字符串
std::vector<std::string> parse_ip_list(const std::string& str) {
    LOG_DEBUG_FMT("Parsing node list: %s", str.c_str());
    std::vector<std::string> items;
    std::stringstream ss(str);
    std::string item;
    
    while (std::getline(ss, item, ',')) {
        trim_whitespace(item);
        items.push_back(item);
        LOG_DEBUG_FMT("Parsed node item: %s", item.c_str());
    }
    
    return items;
}

// 使用 std::stoi 封装的转换函数
int get_nodes_per_servers(const std::string& str) {
    try {
        return std::stoi(str);
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_FMT("nodes-per-server in Config file is invalid.");
        return -1;
    } catch (const std::out_of_range& e) {
        LOG_ERROR_FMT("nodes-per-server in Config file is to big.");
        return -1;
    }
    
    return 1; 
}

// 使用 std::stoi 封装的转换函数
int get_ssh_port(const std::string& str) {
    try {
        return std::stoi(str);
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_FMT("ssh-port in Config file is invalid.");
        return -1;
    } catch (const std::out_of_range& e) {
        LOG_ERROR_FMT("ssh-port in Config file is to big.");
        return -1;
    }
    
    return 1; 
}

// 根据配置文件的内容，生成gtm结构体的信息
int
build_gtm_node_config(const ConfigFile& cfg_file, OpentenbaseConfig& config) {
    LOG_DEBUG_FMT("Build gtm config begin.");

    if (is_Centralized_instance(config.instance.instance_type))
    {
        LOG_DEBUG_FMT("Centralized instance has no gtm,return.");
        return 0;
    }
    
    // gtm master node
    NodeInfo node;
    node.type = Constants::NODE_TYPE_GTM_MASTER;
    node.name = "gtm0001";
    std::string master_ip = cfg_file.gtm.master;
    if (!is_valid_ip_address(master_ip))
    {
        LOG_ERROR_FMT("GTM master node IP is illegal or has more than one IP.");
        return -1;
    }
    node.ip = master_ip;
    config.nodes.push_back(node);

    // gtm slave node
    std::vector<std::string> gtm_slave_ips = parse_ip_list(cfg_file.gtm.slave);
    std::string current_name = node.name;
    for (const auto& ip : gtm_slave_ips) {
         NodeInfo node;
        node.type = Constants::NODE_TYPE_GTM_SLAVE;
        node.name = get_node_name(current_name);
        node.ip = ip;
        current_name = node.name;

        config.nodes.push_back(node);
    }

    LOG_DEBUG_FMT("Build gtm config end.");
    return 0;
}

// 函数声明：将字符串转换为大写
std::string toUpperCase(const std::string& input) {
    std::string output = input; // 创建输入字符串的副本，以免修改原字符串
    std::transform(output.begin(), output.end(), output.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return output;
}

std::string get_prefix_by_node_type(const std::string type) {
    if (type == Constants::NODE_TYPE_CN_MASTER || type == Constants::NODE_TYPE_CN_SLAVE)
    {
        return "cn";
    } else if (type == Constants::NODE_TYPE_DN_MASTER || type == Constants::NODE_TYPE_DN_SLAVE)
    {
        return "dn";
    } else if (type == Constants::NODE_TYPE_GTM_MASTER || type == Constants::NODE_TYPE_GTM_SLAVE)
    {
        return "gtm";
    }
    
    return "";
}

// 函数声明：生成节点列表
int generate_nodes(const std::vector<std::string>& ips, int nodes_per_server, int rounds_num, const std::string type, OpentenbaseConfig& config) {

    LOG_DEBUG_FMT("Generate_nodes begin, ips.size=%d, nodes_per_server=%d, rounds_num=%d ", ips.size(), rounds_num);

    if (rounds_num <= 0)
    {
        return -1;
    }
    
    const std::string prefix = get_prefix_by_node_type(type);
    const int max_number = 4096;
    const int number_digits = 4;
    int current_number = 0;

    for (size_t i = 0; i < ips.size(); ++i) {
        for (int j = 1; j <= nodes_per_server; ++j) {

            // 计算当前节点的编号
            current_number++;
            if (current_number > rounds_num)
            {
                current_number = 1;
            }
            //current_number = (i * nodes_per_server + j) % rounds_num;
            LOG_DEBUG_FMT("Generate_nodes begin, current_number=%d, i=%d, nodes_per_server=%d, j=%d, rounds_num=%d ", current_number, i, nodes_per_server, j, rounds_num);

            // 检查是否超过最大节点数
            if (current_number > max_number) {
                LOG_ERROR_FMT("The total num is more than 4096.");
                continue;
            }

            NodeInfo node;
            node.ip = ips[i];
            node.type = type;

            // 生成节点名称，使用 std::ostringstream 保证编号为四位数
            std::ostringstream name_stream;
            name_stream << prefix << std::setw(number_digits) << std::setfill('0') << current_number;
            node.name = name_stream.str();

            config.nodes.emplace_back(node);
        }
    }

    return 0;
}

// 根据配置文件的内容，生成cn的node结构体的信息
int
build_cn_node_config(const ConfigFile& cfg_file, OpentenbaseConfig& config) {
    LOG_DEBUG_FMT("Build gtm config begin.");

    if (is_Centralized_instance(config.instance.instance_type))
    {
        LOG_DEBUG_FMT("Centralized instance has no cn,return.");
        return 0;
    }

    // 获得 nodes_per_server
    int nodes_per_server = get_nodes_per_servers(cfg_file.coordinators.nodes_per_server);
    if (nodes_per_server == -1)
    {
        LOG_ERROR_FMT("nodes-per-server in Config file is invalied.");
        return -1;
    }
    if (nodes_per_server > 5)
    {
        LOG_ERROR_FMT("The maximum value for nodes-per-server in the configuration file is 5. Please correct your configuration.");
        return -1;
    }
    
    // 获得IP列表
    std::vector<std::string> master_ips = parse_ip_list(cfg_file.coordinators.master);
    std::vector<std::string> slave_ips = parse_ip_list(cfg_file.coordinators.slave);
    if (slave_ips.size() % master_ips.size() != 0) {
        LOG_ERROR_FMT("The number of IPs contained in master should be exactly several times that of the number of IPs in slave. Please correct your configuration..");
        return -1;
    }

    // generate CN master nodes
    int round_num = master_ips.size() * nodes_per_server;
    if (generate_nodes(master_ips, nodes_per_server, round_num, Constants::NODE_TYPE_CN_MASTER, config) == -1) {
        LOG_ERROR_FMT("Generate CN master nodes failed.");
        return -1;
    }

    // generate CN slave nodes
    if (generate_nodes(slave_ips, nodes_per_server, round_num, Constants::NODE_TYPE_CN_SLAVE, config) == -1) {
        LOG_ERROR_FMT("Generate CN master nodes failed.");
        return -1;
    }

    LOG_DEBUG_FMT("Build gtm config end.");
    return 0;
}


// 根据配置文件的内容，生成cn的node结构体的信息
int
build_dn_node_config(const ConfigFile& cfg_file, OpentenbaseConfig& config) {
    LOG_DEBUG_FMT("Build gtm config begin.");

    // 获得 nodes_per_server
    int nodes_per_server = get_nodes_per_servers(cfg_file.datanodes.nodes_per_server);
    if (nodes_per_server == -1)
    {
        LOG_ERROR_FMT("nodes-per-server in Config file is invalied.");
        return -1;
    }
    if (nodes_per_server > 5)
    {
        LOG_ERROR_FMT("The maximum value for nodes-per-server in the configuration file is 5. Please correct your configuration.");
        return -1;
    }
    
    // 获得IP列表
    std::vector<std::string> master_ips = parse_ip_list(cfg_file.datanodes.master);
    std::vector<std::string> slave_ips = parse_ip_list(cfg_file.datanodes.slave);
    if (slave_ips.size() % master_ips.size() != 0) {
        LOG_ERROR_FMT("The number of IPs contained in master should be exactly several times that of the number of IPs in slave. Please correct your configuration..");
        return -1;
    }

    // 集中式只有一个dn节点
    if (is_Centralized_instance(config.instance.instance_type) && (nodes_per_server > 1 || master_ips.size() > 1 || slave_ips.size() > 1))
    {
        if (nodes_per_server > 1 || master_ips.size() > 1 || slave_ips.size() > 1)
        {
            LOG_ERROR_FMT("Centralized instance should has only one data node, please check your config file.");
            return -1;
        }
    }

    // generate CN master nodes
    int round_num = master_ips.size() * nodes_per_server;
    if (generate_nodes(master_ips, nodes_per_server, round_num, Constants::NODE_TYPE_DN_MASTER, config) == -1) {
        LOG_ERROR_FMT("Generate CN master nodes failed.");
        return -1;
    }

    // generate CN slave nodes
    if (generate_nodes(slave_ips, nodes_per_server, round_num, Constants::NODE_TYPE_DN_SLAVE, config) == -1) {
        LOG_ERROR_FMT("Generate CN master nodes failed.");
        return -1;
    }

    LOG_DEBUG_FMT("Build gtm config end.");
    return 0;
}

// 根据配置文件的内容，生成server的结构体的信息
int
build_server_config(const ConfigFile& cfg_file, OpentenbaseConfig& config) {
    LOG_DEBUG_FMT("Build server config begin.");

    ServerConfig server; 
    server.ssh_user = cfg_file.server.ssh_user;
    server.ssh_password = cfg_file.server.ssh_password;

    int ssh_port = get_ssh_port(cfg_file.server.ssh_port);
    if (ssh_port == -1)
    {
        LOG_ERROR_FMT("The ssh-port in Config file is invalid.");
        return -1;
    }
    server.ssh_port = ssh_port;

    // 生成instance结构体的信息
    config.server = server;

    LOG_DEBUG_FMT("Build server config end.");
    return 0;
}

// 根据配置文件的内容，生成server的结构体的信息
int
build_log_config(const ConfigFile& cfg_file, OpentenbaseConfig& config) {
    LOG_DEBUG_FMT("Build log config begin.");

    // 
    std::string level = toUpperCase(cfg_file.log.level);
    if (level == LOG_LEVEL_DEBUG || level ==LOG_LEVEL_INFO || level ==LOG_LEVEL_ERROR ) {
        set_log_level(level.c_str());
    } else 
    {
        set_log_level(LOG_LEVEL_INFO);
    }

    LOG_DEBUG_FMT("Build log config end.");
    return 0;
}


// 根据配置文件的内容，生成业务操作的结构体
int
build_opentenbase_config(const ConfigFile& cfg_file, OpentenbaseConfig& config) {
    LOG_INFO_FMT("Build opentenbase config begin.");

    // 校验和构建instance信息
    if (build_instance_config(cfg_file,config) != 0)
    {
        LOG_ERROR_FMT("There are some errors in the instance configurations in the file.");
        return -1;
    }
    
    // 校验和构建gtm节点信息
    if (build_gtm_node_config(cfg_file,config) != 0)
    {
        LOG_ERROR_FMT("There are some errors in the gtm configurations in the file.");
        return -1;
    }
    
    // 校验和构建cn节点信息
    if (build_cn_node_config(cfg_file,config) != 0)
    {
        LOG_ERROR_FMT("There are some errors in the coordinators configurations in the file.");
        return -1;
    }
    
    // 校验和构建dn节点信息
    if (build_dn_node_config(cfg_file,config) != 0)
    {
        LOG_ERROR_FMT("There are some errors in the datanodes configurations in the file.");
        return -1;
    }

    // 校验和构建server信息
    if (build_server_config(cfg_file,config) != 0)
    {
        LOG_ERROR_FMT("There are some errors in the server configurations in the file.");
        return -1;
    }

    // 校验和构建log信息
    if (build_log_config(cfg_file,config) != 0)
    {
        LOG_ERROR_FMT("There are some errors in the log configurations in the file.");
        return -1;
    }

    LOG_INFO_FMT("Build opentenbase config end.");
    return 0;
}

std::string get_node_name(const std::string& node_name) {
    if (node_name.empty()) {
        return "";
    }

    // 检查前缀
    std::string prefix;
    size_t prefix_len = 0;
    
    if (node_name.length() >= 2 && (node_name.substr(0, 2) == "cn" || node_name.substr(0, 2) == "dn")) {
        prefix = node_name.substr(0, 2);
        prefix_len = 2;
    } 
    else if (node_name.length() >= 3 && node_name.substr(0, 3) == "gtm") {
        prefix = node_name.substr(0, 3);
        prefix_len = 3;
    } 
    else {
        return "";
    }

    // 检查剩余部分是否全是数字
    std::string num_str = node_name.substr(prefix_len);
    if (num_str.empty()) {
        return "";
    }

    for (char c : num_str) {
        if (!isdigit(c)) {
            return "";
        }
    }

    // 处理cn/dn前缀的情况
    if (prefix == "cn" || prefix == "dn") {
        return node_name;
    }
    // 处理gtm前缀的情况
    else if (prefix == "gtm") {
        try {
            // 计算原始数字的位数
            size_t num_digits = num_str.length();
            
            // 数字加1
            int num = std::stoi(num_str);
            num += 1;
            
            // 格式化新数字，保持相同位数
            std::string new_num_str = std::to_string(num);
            if (new_num_str.length() > num_digits) {
                // 如果数字溢出（比如gtm999->1000但只有3位），理论上应该返回错误
                // 但根据需求描述，似乎允许位数增加？这里保持与需求描述一致
                // 需求示例中gtm999->1000是允许的（位数从3变为4）
                // 如果需要严格保持位数，可以取消下面这行的注释
                // return "";
            } 
            else {
                // 前面补零保持位数
                new_num_str = std::string(num_digits - new_num_str.length(), '0') + new_num_str;
            }
            
            return prefix + new_num_str;
        } 
        catch (const std::out_of_range&) {
            // 处理数字溢出（比如gtm99999999999999999999）
            return "";
        }
        catch (...) {
            // 其他异常
            return "";
        }
    }

    return ""; // 理论上不会执行到这里
}
