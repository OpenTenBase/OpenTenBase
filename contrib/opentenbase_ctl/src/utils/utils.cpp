#include "utils.h"
#include "../log/log.h"
#include "../ssh/remote_ssh.h"
#include "../types/types.h"
#include <cstdio>
#include <cstring>
#include <memory>
#include <array>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <poll.h>
#include <regex>

// Extract version from package name
std::string extract_version_from_package_name(const std::string& package_name) {
    // 改进后的正则表达式，确保捕获所有数字段
    // 解释：
    // ^[^-]+-       : 匹配包名开头，至少一个非 '-' 字符，后跟 '-'
    // ([0-9]+(?:\.[0-9]+)+) : 捕获组，匹配版本号，如 3.16.9.301
    // .*$           : 匹配版本号后剩余的部分
    std::regex versionPattern(R"(^[^-]+-([0-9]+(?:\.[0-9]+)+).*?)");

    std::smatch match;
    if (std::regex_search(package_name, match, versionPattern)) {
        // match[1] 包含第一个捕获组，即完整的版本号
        return match[1].str();
    } else {
        // 如果没有匹配到，返回空字符串
        return "";
    }
}

// Check if port is available
int 
check_port_available(const char *ip, int port, const char *username, const char *password, int ssh_port) {
    std::string cmd = "export PATH=/usr/local/bin:/usr/bin:/usr/sbin:$PATH && ss -tln | grep -q ':" + std::to_string(port) + "'";
    std::string result;
    
    int ret = remote_ssh_exec(ip, ssh_port, username, password, cmd, result);
    
    if (ret == 0) {
        // Command executed successfully, check return value
        if (result.empty()) {
            // No port found, means port is available
            return 0;
        } else {
            // Port found, means port is occupied
            return 1;
        }
    } else {
        // Command execution failed
        LOG_ERROR_FMT("Failed to check port %d on %s: %s", port, ip, result.c_str());
        return -1;
    }
}


// Get available port pair
int 
get_available_port_pair(const std::string& ip, int start_port, int& node_port, int& pooler_port, int& forward_port,
                          const std::string& username, const std::string& password, int ssh_port) {
    const int MAX_PORT = 65535;
    const int RETRY_LIMIT = 100;  // Maximum retry times
    int current_port = start_port;
    int retry_count = 0;

    while (retry_count < RETRY_LIMIT && current_port < MAX_PORT - 1) {
        // Check if both adjacent ports are available
        if (check_port_available(ip.c_str(), current_port, username.c_str(), password.c_str(), ssh_port) == 0 &&
            check_port_available(ip.c_str(), current_port + 1, username.c_str(), password.c_str(), ssh_port) == 0 &&
            check_port_available(ip.c_str(), current_port + 2, username.c_str(), password.c_str(), ssh_port) == 0) {
            node_port = current_port;
            pooler_port = current_port + 1;
            forward_port = current_port + 2;
            LOG_INFO_FMT("Found available port pair for %s: node_port=%d, pooler_port=%d",
                        ip.c_str(), node_port, pooler_port);
            return 0;
        }
        current_port += 3;  // Skip the two ports already checked
        retry_count++;
    }

    LOG_ERROR_FMT("Failed to find available port pair for %s after %d attempts",
                 ip.c_str(), retry_count);
    return -1;
}

// Assign ports for nodes
int 
assign_ports_for_nodes(std::vector<NodeInfo>& nodes, const std::string& username, const std::string& password, int ssh_port) {
    const int START_PORT = 11000;
    std::map<std::string, int> ip_next_port;  // Record the next check port for each IP

    for (auto& node : nodes) {
        int node_port, pooler_port,forward_port;
        
        // Get the starting check port for this IP
        auto it = ip_next_port.find(node.ip);
        int start_port = (it != ip_next_port.end()) ? it->second : START_PORT;

        // Get available port pair
        if (get_available_port_pair(node.ip, start_port, node_port, pooler_port, forward_port, username, password, ssh_port) != 0) {
            LOG_ERROR_FMT("Failed to assign ports for node %s", node.name.c_str());
            return -1;
        }

        // Update node information
        node.port = node_port;
        node.pooler_port = pooler_port;
        node.forward_port = forward_port;

        // Update the next starting check port for this IP
        ip_next_port[node.ip] = forward_port + 1;
    }

    return 0;
}

bool is_rpm_package(const std::string& package_name) {
    if (package_name.length() < 4) {
        return false; // 文件名太短，不可能是 .rpm
    }

    // 检查后缀是否是 ".rpm"（不区分大小写）
    const std::string rpm_suffix = ".rpm";
    return std::equal(
        rpm_suffix.rbegin(), rpm_suffix.rend(),
        package_name.rbegin(),
        [](char a, char b) { return std::tolower(a) == std::tolower(b); }
    );
}

// Node type is gtm master node
bool is_master_gtm(std::string node_type) {

    return node_type == Constants::NODE_TYPE_GTM_MASTER;
}

// Node type is gtm master node
bool is_slave_gtm(std::string node_type) {

    return node_type == Constants::NODE_TYPE_GTM_SLAVE;
}

// Node type is gtm master
bool is_gtm_node(std::string node_type) {

    return node_type == Constants::NODE_TYPE_GTM_MASTER 
    || node_type == Constants::NODE_TYPE_GTM_SLAVE;
}

// Node type is cn master
bool is_master_cn(std::string node_type) {

    return node_type == Constants::NODE_TYPE_CN_MASTER;
}

// Node type is cn slave
bool is_slave_cn(std::string node_type) {

    return node_type == Constants::NODE_TYPE_CN_SLAVE;
}

// Node type is cn
bool is_cn_node(std::string node_type) {

    return node_type == Constants::NODE_TYPE_CN_MASTER 
    || node_type == Constants::NODE_TYPE_CN_SLAVE;
}

// Node type is dn master
bool is_master_dn(std::string node_type) {

    return node_type == Constants::NODE_TYPE_DN_MASTER;
}

// Node type is dn slave
bool is_slave_dn(std::string node_type) {

    return node_type == Constants::NODE_TYPE_DN_SLAVE;
}

// Node type is dn
bool is_dn_node(std::string node_type) {

    return node_type == Constants::NODE_TYPE_DN_MASTER 
    || node_type == Constants::NODE_TYPE_DN_SLAVE;
}

// Node type is master
bool is_master_node(std::string node_type) {

    return node_type == Constants::NODE_TYPE_CN_MASTER
    || node_type == Constants::NODE_TYPE_DN_MASTER
    || node_type == Constants::NODE_TYPE_GTM_MASTER;
}

/**
 * 是否是融合版本（5.21.*的版本）
 * 说明：与融合版本相对的是较期的 2.15.*、5.05或5.06 的版本
 * @param version 实例版本
 * @return true 是; false: 否
 */
bool is_fusion_version(std::string version) {
    // 检查版本号长度是否至少为4
    if (version.length() < 4) {
        return false;
    }

    // 提取前四个字符并比较
    std::string prefix = version.substr(0, 4);
    return prefix == "5.21" || prefix == "3.16" ;
}

/**
 * 判断实例是否是集中式
 * 说明：集中式实例只有一组dn节点，没有gtm和cn节点。
 * 集中式节点的guc中多了 allow_dml_on_datanode等几个配置项。
 * @param instance_type 实例类型
 * @return true 是; false: 否
 */
bool is_Centralized_instance(std::string instance_type) {
    return instance_type == Constants::INSTANCE_TYPE_CENTRALIZED;
}