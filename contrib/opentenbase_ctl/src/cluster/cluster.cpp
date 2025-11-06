#include <unistd.h>
#include "../config/config.h"
#include "../node/node.h"
#include "../log/log.h"
#include "../ssh/remote_ssh.h"
#include "../utils/utils.h"
#include "../types/types.h"
#include "cluster.h"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <cstdarg>
// #include <indicators/progress_bar.hpp>
// #include <indicators/termcolor.hpp>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <unordered_set>
#include <cctype> 

//using namespace indicators;

int
create_virtual_cluster(OpentenbaseConfig *config) {
    LOG_INFO_FMT("Creating virtual cluster for instance %s", 
            config->instance.instance_name.c_str());
    std::string command;
    std::string result;
    int ret;

    // Build command string
    command = "meta-cli instance create"
              " -c " + config->instance.instance_name +
              " -m " + config->meta.meta_server +
              " -i " + std::to_string(config->meta.meta_id) +
              " -t " + config->meta.txn_server;

    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    // Execute locally, result assigned to cluster_oid

    ret = local_exec(command, result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to create instance on local: %s", command.c_str());
        return -1;
    }
    LOG_INFO_FMT("Command output: %s", result.c_str());

    result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
    result.erase(std::remove(result.begin(), result.end(), '\r'), result.end());

    config->meta.cluster_oid = result;
    if (config->meta.cluster_oid.empty() || config->meta.cluster_oid == "") {
        LOG_ERROR_FMT("Failed to create instance on local: %s", command.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully created instance on local: %s, cluster_oid: %s", 
            command.c_str(), config->meta.cluster_oid.c_str());
    return 0;
}

int
delete_virtual_cluster(OpentenbaseConfig *config) {
    LOG_INFO_FMT("Deleting virtual cluster for instance %s", 
            config->instance.instance_name.c_str());

    // 云数仓才需要删除元数据
    if (config->instance.instance_type != "1")
    {
        LOG_DEBUG_FMT("Node need to delete meta for this type of instance.");
        return 0;
    }
    
    std::string command;
    std::string result;
    int ret;

    // Build command string
    command = "meta-cli instance delete"
              " -m " + config->meta.meta_server +
              " -i " + std::to_string(config->meta.meta_id) +
              " -t " + config->meta.txn_server +
              " -o " + config->meta.cluster_oid;

    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    // Execute locally

    ret = local_exec(command, result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to create instance on local: %s", command.c_str());
        return -1;
    }
    LOG_INFO_FMT("Command output: %s", result.c_str());

    result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
    result.erase(std::remove(result.begin(), result.end(), '\r'), result.end());

    LOG_INFO_FMT("Successfully deleted instance on local: %s, cluster_oid: %s", 
            command.c_str(), config->meta.cluster_oid.c_str());
    return 0;
}

// Get meta and txn connection info from etcd
int get_connections(OpentenbaseConfig *config) {
    return 0;
}

int
create_pgxc_node(NodeInfo *node, OpentenbaseConfig *config) {
    LOG_INFO_FMT("Creating pgxc node for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    int ret;

    // Build base command string
    command = "meta-cli node create"
              " -m " + config->meta.meta_server +
              " -t " + config->meta.txn_server +
              " -i " + std::to_string(config->meta.meta_id) +
              " -o " + config->meta.cluster_oid +
              " --node_host " + node->ip +
              " --node_name " + node->name +
              " --pg_port " + std::to_string(node->port) +
              " --node_type " + (node->type == Constants::NODE_TYPE_CN_MASTER ? "C" : "D");

    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    ret = local_exec(command, result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to create instance on local: %s", command.c_str());
        return -1;
    }
    LOG_INFO_FMT("Command output: %s", result.c_str());

    return 0;
}

std::string get_node_type(const std::string& type) {
    static const std::unordered_map<std::string_view, std::string> type_map = {
        {Constants::NODE_TYPE_CN_MASTER, "coordinator"},
        {Constants::NODE_TYPE_CN_SLAVE, "coordinator"},
        {Constants::NODE_TYPE_DN_MASTER, "datanode"},
        {Constants::NODE_TYPE_DN_SLAVE, "datanode"},
        {Constants::NODE_TYPE_GTM_MASTER, "gtm"},
        {Constants::NODE_TYPE_GTM_SLAVE, "gtm"}
    };

    auto it = type_map.find(type);
    if (it != type_map.end()) {
        return it->second;
    }
    // 处理未匹配的情况
    return "";
}

// build create pgxz_node cmd
//  /home/opentenbase/install/opentenbase/3.16.9.3/bin/postgres --datanode -D /home/opentenbase/run/instance/test_cluster05/dn001/data -i
std::string build_create_pgxz_node_cmd(NodeInfo *node, OpentenbaseConfig* config) {

    std::string node_type = "";
    std::string create_sql = "";


    // Create node for each node
    for (size_t i = 0; i < config->nodes.size(); ++i) {

        if (config->nodes[i].type == Constants::NODE_TYPE_GTM_MASTER 
            || config->nodes[i].type == Constants::NODE_TYPE_GTM_SLAVE 
            || config->nodes[i].type == Constants::NODE_TYPE_CN_SLAVE 
            || config->nodes[i].type == Constants::NODE_TYPE_DN_SLAVE)
        {
            continue;
        }

        node_type = get_node_type(config->nodes[i].type);

        if (config->nodes[i].name == node->name 
            && config->nodes[i].ip == node->ip 
            && config->nodes[i].port == node->port
            && !is_Centralized_instance(config->instance.instance_type))
        {
            // 本节点默认会生成一个IP为 localhost/端口为5432 的记录，需要修改一下节点路由的IP/Port
            // ALTER NODE cn001 with(HOST='172.16.16.49',PORT=11000);
            create_sql += "ALTER NODE " + config->nodes[i].name + " WITH (HOST='"+ config->nodes[i].ip +"', PORT=" + std::to_string(config->nodes[i].port) +");";
        } else {
            // CREATE NODE cn001 WITH (TYPE='coordinator', HOST='172.16.16.49', PORT=11000);
            create_sql += "CREATE NODE " + config->nodes[i].name + " WITH (TYPE='"+ node_type +"', HOST='"+ config->nodes[i].ip +"', PORT=" + std::to_string(config->nodes[i].port) +");";
        }
    }

    LOG_INFO_FMT("Create node command: %s", create_sql.c_str());
    return create_sql;
}

// build create pgxz_node cmd
// CREATE DEFAULT node group group01 with(dn001);
std::string build_create_node_group_cmd(OpentenbaseConfig* config) {

    std::string create_sql = "";
    std::string node_name_list = "";

    // Create node for each node
    for (size_t i = 0; i < config->nodes.size(); ++i) {

        if (config->nodes[i].type != Constants::NODE_TYPE_DN_MASTER)
        {
            continue;
        }

        node_name_list += config->nodes[i].name + ",";
    }

    //删除最后一个逗号
    node_name_list.pop_back();
	create_sql = "CREATE DEFAULT node group default_group with (" + node_name_list + ");";

    LOG_INFO_FMT("Create node command: %s", create_sql.c_str());
    return create_sql;
}

int
create_pgxc_node_for_mapp(NodeInfo *node, OpentenbaseConfig *config) {

    LOG_INFO_FMT("Creating pgxc node for %s (%s)", node->name.c_str(), node->ip.c_str());

    // cn主和dn主才需要执行，否则直接return
    if (node->type != Constants::NODE_TYPE_CN_MASTER 
        && node->type != Constants::NODE_TYPE_DN_MASTER)
    {
        LOG_INFO_FMT("Creat pgxc step jump for %s (%s)", node->name.c_str(), node->ip.c_str());
        return 0;
    }

    // 生成create和reload的sql语句
    std::string create_table_sql = escape_sql(build_create_pgxz_node_cmd(node, config));
    std::string reload_sql = "SELECT pgxc_pool_reload(); ";

    // 构建psql命令 
    std::string binDir =  node->install_path;
    std::string conn_str = "psql -h " + node->ip + " -p " + std::to_string(node->port) + " -d " + std::string(Constants::DEFAULT_DB)  + " -U " + std::string(Constants::DEFAULT_USER_OF_INITDB);

    // 如果明确指定是本地执行，或者IP是本地IP，则本地执行
    std::string result;
    bool should_execute_local = is_local_ip(node->ip);
    if (should_execute_local) {
        // Local execution
        std::string psql_cmd =  conn_str + "  --command \\\"" + create_table_sql + reload_sql + "\\\" ";
        std::string command = buid_ld_library_path_str(binDir)+ " && " + psql_cmd;
        LOG_INFO_FMT("Executing local command: %s", command.c_str());
        int ret = local_exec_as_user(config->server.ssh_user, command, result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to excute(%s) on node %s (%s): %s",
                    command.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }

    } else {
        // Remote execution
        std::string psql_cmd =  conn_str + "  --command \"" + create_table_sql + reload_sql + "\" ";
        std::string command = buid_ld_library_path_str(binDir)+ " && " + psql_cmd;
        LOG_INFO_FMT("Executing remote command: %s", command.c_str());
        if (remote_ssh_exec(node->ip, config->server.ssh_port, config->server.ssh_user, config->server.ssh_password, command, result) != 0) {
            LOG_ERROR_FMT("Failed to excute(%s) on node %s (%s): %s",
                    command.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
    }
    
    LOG_INFO_FMT("Create pgxc node for %s (%s) Successfully", node->name.c_str(), node->ip.c_str());
    return 0;
}

int
create_default_node_group(NodeInfo *node, OpentenbaseConfig *config) {

    LOG_INFO_FMT("Creating node group for %s (%s)", node->name.c_str(), node->ip.c_str());
    if (node->type != Constants::NODE_TYPE_CN_MASTER && !is_Centralized_instance(config->instance.instance_type))
    {
        LOG_INFO_FMT("Creat pgxc step jump for %s (%s)", node->name.c_str(), node->ip.c_str());
        return 0;
    }

    // 生成sql语句
    std::string create_node_sql = build_create_node_group_cmd(config);
    std::string create_sharding_group_sql = "CREATE sharding group to group default_group;";
    std::string clean_sharding_sql = "clean sharding;";

    std::string conn_str = "psql -h " + node->ip + " -p " + std::to_string(node->port) + " -d " + std::string(Constants::DEFAULT_DB)  + " -U " + std::string(Constants::DEFAULT_USER_OF_INITDB);
    std::string binDir =  node->install_path;


    // 如果明确指定是本地执行，或者IP是本地IP，则本地执行
    std::string result;
    bool should_execute_local = is_local_ip(node->ip);
    if (should_execute_local) {
        // Local execution
        std::string psql_cmd =  conn_str + "  --command \\\"" +  escape_sql(create_node_sql) +escape_sql(create_sharding_group_sql) + escape_sql(clean_sharding_sql) + "\\\" ";
        std::string command = buid_ld_library_path_str(binDir)+ " && " + psql_cmd;
        LOG_INFO_FMT("Executing local command: %s", command.c_str());
        int ret = local_exec_as_user(config->server.ssh_user, command, result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to excute(%s) on node %s (%s): %s",
                    command.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }

    } else {
        // Remote execution
        std::string psql_cmd =  conn_str + "  --command \"" +  escape_sql(create_node_sql) +escape_sql(create_sharding_group_sql) + escape_sql(clean_sharding_sql) + "\" ";
        std::string command = buid_ld_library_path_str(binDir)+ " && " + psql_cmd;
        LOG_INFO_FMT("Executing remote command: %s", command.c_str());
        if (remote_ssh_exec(node->ip, config->server.ssh_port, config->server.ssh_user, config->server.ssh_password, command, result) != 0) {
            LOG_ERROR_FMT("Failed to excute(%s) on node %s (%s): %s",
                    command.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
    }

    LOG_INFO_FMT("Create node group for %s (%s) Successfully", node->name.c_str(), node->ip.c_str());
    return 0;
}

// install rpm pkg
int install_rpm_pkg(OpentenbaseConfig *config) {

    std::string command;
    std::string result;
    int ret;

    // Check Package file exists or not
    if (access(config->instance.package_path.c_str(), F_OK) == -1) {
        LOG_ERROR_FMT("Package file not found: %s", config->instance.package_path.c_str());
        return -1;
    }
    LOG_INFO_FMT("Package file exists: %s", config->instance.package_path.c_str());

    // install rpm pkg
    std::string baseName = getFileBaseName(config->instance.package_name);
    command = std::string("rm -rf ") + Constants::DEFAULT_INSTALL_DIR + 
              " && rpm -e " + baseName + " 2>/dev/null || true " +
              " && sudo rpm -i --force " + config->instance.package_path;
    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    ret = local_exec(command, result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to create instance on local: %s", command.c_str());
        return -1;
    }
    LOG_INFO_FMT("Command output: %s", result.c_str());

    return 0;
}

//make tar.gz package
int make_tar_gz_pkg(OpentenbaseConfig *config) {

    std::string command;
    std::string result;
    int ret;

    std::string baseName = getFileBaseName(config->instance.package_name);

    // Build base command string
    command = std::string("rm -rf ./") + Constants::DEFAULT_PKG_TMP_DIR + 
              " && mkdir " + Constants::DEFAULT_PKG_TMP_DIR +
              " && cp -af " + Constants::DEFAULT_INSTALL_DIR + "/* " + Constants::DEFAULT_PKG_TMP_DIR +
              " && cd " + + Constants::DEFAULT_PKG_TMP_DIR + 
              " && tar -zcf " + baseName + ".tar.gz * " + 
              " && mv *.tar.gz ..";

    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    ret = local_exec(command, result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to create *.tar.gz on local: %s", command.c_str());
        return -1;
    }
    LOG_INFO_FMT("Command output: %s", result.c_str());

    // update file name to transfer
    config->instance.package_path = "./" + baseName + ".tar.gz";
    config->instance.package_name = baseName + ".tar.gz";


    return 0;
}

// 函数声明：获取文件的基本名称（去掉扩展名）
std::string getFileBaseName(const std::string& filename) {
    // 查找最后一个 '.' 的位置
    size_t dotPos = filename.find_last_of('.');

    // 如果找到了 '.'，则截取从开始到 '.' 之前的子字符串
    if (dotPos != std::string::npos) {
        return filename.substr(0, dotPos);
    }

    // 如果没有找到 '.'，则返回原始文件名
    return filename;
}

int pre_process_pkg(OpentenbaseConfig *config) {

    if (access(config->instance.package_path.c_str(), F_OK) == -1) {
        LOG_ERROR_FMT("Package file not found: %s", config->instance.package_path.c_str());
        return -1;
    }
    LOG_INFO_FMT("Package file exists: %s", config->instance.package_path.c_str());

    if (!is_rpm_package(config->instance.package_name))
    {
        return 0;
    }
    
    if (install_rpm_pkg(config) != 0) {
        LOG_ERROR_FMT("Failed to install rpm pkg: %s", config->instance.package_path.c_str());
        return -1;
    }

    if (make_tar_gz_pkg(config) != 0) {
        LOG_ERROR_FMT("Failed to prepare *.tar.gz pkg");
        return -1;
    }
    LOG_INFO_FMT("Sucess to make tar.gz Package for %s", config->instance.package_path.c_str());

    return 0;
}

int pre_install_command(OpentenbaseConfig *config) {
    if (access(config->instance.package_path.c_str(), F_OK) == -1) {
        LOG_ERROR_FMT("Package file not found: %s", config->instance.package_path.c_str());
        return -1;
    }
    LOG_INFO_FMT("Package file exists: %s", config->instance.package_path.c_str());

    // Get unique IP node list
    std::vector<std::thread> threads;
    std::map<std::string, NodeInfo*> unique_ip_nodes;
    for (auto& node : config->nodes) {
        unique_ip_nodes[node.ip] = &node;
    }
    LOG_INFO_FMT("Found %zu unique IP nodes", unique_ip_nodes.size());

    // Create directories for each node
    for (auto& node : config->nodes) {
        // Step 1: Create node directories
        LOG_INFO_FMT("Step %d/%d: Creating directories for node %s", 
            1, 5, node.name.c_str());
        if (create_node_directories(&node, config) != 0) {
            LOG_ERROR_FMT("Failed to create node directories");
            return -1;
        }
    }

    // Step 2: Transfer and extract package (only for unique IPs)
    for (const auto& ip_node : unique_ip_nodes) {
        LOG_INFO_FMT("Step %d/%d: Transferring and extracting package for node %s", 
            2, 5, ip_node.second->name.c_str());
        
        // Check if remote directory exists
        std::string check_cmd = "test -d " + ip_node.second->install_path;
        std::string check_result;
        if (remote_ssh_exec(ip_node.second->ip, config->server.ssh_port, 
            config->server.ssh_user, config->server.ssh_password, check_cmd, check_result) != 0) {
            LOG_ERROR_FMT("Remote directory does not exist: %s", ip_node.second->install_path.c_str());
            return -1;
        }

        if (transfer_and_extract_package(ip_node.second, config) != 0) {
            LOG_ERROR_FMT("Failed to transfer and extract package to node %s (%s)", 
                ip_node.second->name.c_str(), ip_node.second->ip.c_str());
            return -1;
        }
    }
    
    return 0;
}

int pre_install_command_concurrency(OpentenbaseConfig *config) {
    if (access(config->instance.package_path.c_str(), F_OK) == -1) {
        LOG_ERROR_FMT("Package file not found: %s", config->instance.package_path.c_str());
        return -1;
    }
    LOG_INFO_FMT("Package file exists: %s", config->instance.package_path.c_str());

    std::vector<std::thread> threads;
    std::vector<int> results(config->nodes.size(), 0);
    // Create directories for each node
    for (size_t i = 0; i < config->nodes.size(); ++i) {
        LOG_INFO_FMT("Step %d/%d: Creating directories for node %s", 
            1, 5, config->nodes[i].name.c_str());

        threads.emplace_back([&, i]() {
            results[i] = create_node_directories(&config->nodes[i], config);
            if (results[i] == 0)
            {
                std::cout << "Create directory for node " << config->nodes[i].name << "(" << config->nodes[i].ip << ") Success " << std::endl;
            } else {
                std::cout << "Create directory for node " << config->nodes[i].name << "(" << config->nodes[i].ip << ") Failed " << std::endl;
                LOG_ERROR_FMT("Failed to create node(%s:%s) directories",  config->nodes[i].name.c_str(), config->nodes[i].ip.c_str());
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (int result : results) {
        if (result != 0) {
            LOG_ERROR_FMT("Failed to create directory for nodes");
            return -1;
        }
    }


    // Step 2: Transfer and extract package (only for unique IPs)
    std::map<std::string, NodeInfo*> unique_ip_nodes;
    std::vector<int> result(unique_ip_nodes.size(), 0);
    int failedCount = 0;
    for (auto& node : config->nodes) {
        unique_ip_nodes[node.ip] = &node;
    }
    LOG_INFO_FMT("Found %zu unique IP nodes", unique_ip_nodes.size());
    for (const auto& ip_node : unique_ip_nodes) {
        LOG_INFO_FMT("Step %d/%d: Transferring and extracting package for %s", 
            2, 5, ip_node.second->ip.c_str());
        
        threads.emplace_back([&, ip_node]() {

            // Check if remote directory exists
            // std::string check_cmd = "test -d " + ip_node.second->install_path;
            // std::string check_result;
            // int ret = 0;
            // ret = execute_command(ip_node.second->ip, install->server.ssh_port, 
            //     install->server.ssh_user, install->server.ssh_password, check_cmd, check_result);
            // if (ret != 0) {
            //     LOG_ERROR_FMT("Remote directory does not exist: %s", ip_node.second->install_path.c_str());
            //     failedCount++;
            //     return;
            // }

            if (transfer_and_extract_package(ip_node.second, config) != 0) {
                LOG_ERROR_FMT("Failed to transfer and extract package to node %s (%s)", 
                    ip_node.second->name.c_str(), ip_node.second->ip.c_str());

                std::cout << "Transfer and extract package to server %s" << ip_node.second->ip.c_str() << "Failed " << std::endl;
                failedCount++;
                return;
            } else {
                std::cout << "Transfer and extract package to server %s" << ip_node.second->ip.c_str() << "Success" << std::endl;
            }
        
        });

    }

    for (auto& thread : threads) {
        thread.join();
    }
    if (failedCount > 0)
    {
        LOG_ERROR_FMT("Failed to Transfer and extract package for nodes");
        return -1;
    }

    std::cout << "Success to transfer and extract package to " << std::to_string(unique_ip_nodes.size()) << " servers " << std::endl;
    return 0;
}

// Install single node
int install_node(NodeInfo* node, OpentenbaseConfig* install) {

    std::string result;
    std::string command = "cd " + node->install_path + " && tar xzf data.tar.gz -C " + node->data_path + " && chmod 0700 " + node->data_path;
    LOG_INFO_FMT("Executing command: %s", command.c_str());
    if (remote_ssh_exec(node->ip, install->server.ssh_port, install->server.ssh_user, 
        install->server.ssh_password, command, result) != 0) {
        LOG_ERROR_FMT("Failed to extract data directory for node %s (%s)", node->name.c_str(), node->ip.c_str());
    }
    LOG_INFO_FMT("Successfully extracted data directory for node %s (%s)", node->name.c_str(), node->ip.c_str());
    
    // Step 3: Configure PostgreSQL
    LOG_INFO_FMT("Step %d/%d: Configuring PostgreSQL for node %s", 
        3, 5, node->name.c_str());
    if (configure_postgresql_for_cdw(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure PostgreSQL");
        return -1;
    }

    // Step 4: Create pgxc node
    LOG_INFO_FMT("Step %d/%d: Creating pgxc node for node %s", 
        4, 5, node->name.c_str());
    if (create_pgxc_node(node, install) != 0) {
        LOG_ERROR_FMT("Failed to create pgxc node");
        return -1;
    }

    // Step 5: Start node
    LOG_INFO_FMT("Step %d/%d: Starting node %s", 
        5, 5, node->name.c_str());
    if (start_node(node, install) != 0) {
        LOG_ERROR_FMT("Failed to start node");
        return -1;
    }

    return 0;
}

// Install single node
int init_and_start_slave_gtm(NodeInfo* node, OpentenbaseConfig* install) {

    //  Step 1: initDB
    LOG_INFO_FMT("Step %d/%d: InitDB for node %s", 1, 6, node->name.c_str());
    if (init_master_node(node, install) != 0) {
        LOG_ERROR_FMT("Failed to init slave node %s in %s", node->name.c_str(), node->ip.c_str());
        return -1;
    }
    
    // Step 2: Configure PostgreSQL
    LOG_INFO_FMT("Step %d/%d: Configuring PostgreSQL for node %s", 2, 6, node->name.c_str());
    if (configure_postgresql_for_mpp(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure PostgreSQL");
        return -1;
    }

    // Step 3: Start node
    LOG_INFO_FMT("Step %d/%d: Starting node %s", 3, 6, node->name.c_str());
    if (start_node(node, install) != 0) {
        LOG_ERROR_FMT("Failed to start node");
        return -1;
    }

    LOG_INFO_FMT("Install node %s successfully", node->name.c_str());
    return 0;
}

// Install single node
int init_and_start_master_node(NodeInfo* node, OpentenbaseConfig* install) {

    //  Step 1: initDB
    LOG_INFO_FMT("Step %d/%d: InitDB for node %s", 1, 6, node->name.c_str());
    if (init_master_node(node, install) != 0) {
        LOG_ERROR_FMT("Failed to init master node %s in %s", node->name.c_str(), node->ip.c_str());
        return -1;
    }
    
    // Step 2: Configure PostgreSQL
    LOG_INFO_FMT("Step %d/%d: Configuring PostgreSQL for node %s", 2, 6, node->name.c_str());
    if (configure_postgresql_for_mpp(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure PostgreSQL");
        return -1;
    }

    // Step 3: Start node
    LOG_INFO_FMT("Step %d/%d: Starting node %s", 3, 6, node->name.c_str());
    if (start_node(node, install) != 0) {
        LOG_ERROR_FMT("Failed to start node");
        return -1;
    }

    if (is_master_gtm(node->type))
    {
        return 0;
    }
    if (!is_master_node(node->type))
    {
        LOG_ERROR_FMT("Node type is not master or gtm master");
        return -1;
    }

    //Step 4: Create pgxc node
    LOG_INFO_FMT("Step %d/%d: Creating pgxc node for node %s",  4, 6, node->name.c_str());
    if (create_pgxc_node_for_mapp(node, install) != 0) {
        LOG_ERROR_FMT("Failed to create pgxc node");
        return -1;
    }

    // Step 5: Stop node
    LOG_INFO_FMT("Step %d/%d: Stopping node %s", 5, 6, node->name.c_str());
    if (stop_node(node, install) != 0) {
        LOG_ERROR_FMT("Failed to start node");
        return -1;
    }

    // Step 6: Start node
    LOG_INFO_FMT("Step %d/%d: Starting node %s", 6, 6, node->name.c_str());
    if (start_node(node, install) != 0) {
        LOG_ERROR_FMT("Failed to start node");
        return -1;
    }

    LOG_INFO_FMT("Install node %s successfully", node->name.c_str());
    return 0;
}

// redo slave node
int redo_and_start_slave_node(NodeInfo* node, OpentenbaseConfig* config) {

    //  Step 1: initDB
    LOG_INFO_FMT("Step %d/%d: InitDB for slave  node %s", 1, 6, node->name.c_str());
    if (redo_slave_node(node, config) != 0) {
        LOG_ERROR_FMT("Failed to init slave node %s in %s", node->name.c_str(), node->ip.c_str());
        return -1;
    }
    
    // Step 2: Configure PostgreSQL
    LOG_INFO_FMT("Step %d/%d: Configuring PostgreSQL for slave node %s", 2, 6, node->name.c_str());
    if (configure_postgresql_for_mpp(node, config) != 0) {
        LOG_ERROR_FMT("Failed to configure PostgreSQL");
        return -1;
    }

    // Step 3: Start node
    LOG_INFO_FMT("Step %d/%d: Starting node %s", 3, 6, node->name.c_str());
    if (start_node(node, config) != 0) {
        LOG_ERROR_FMT("Failed to start node");
        return -1;
    }

    LOG_INFO_FMT("Install node %s successfully", node->name.c_str());
    return 0;
}


// initdb node
int init_node(NodeInfo* node, OpentenbaseConfig* install) {

    if (is_master_node(node->type))
    {
        if (init_and_start_master_node(node, install) != 0) {
            LOG_ERROR_FMT("Failed to init node %s in %s", node->name.c_str(), node->ip.c_str());
            return -1;
        }

    } else if (is_slave_gtm(node->type)) {

        if (init_and_start_slave_gtm(node, install) != 0) {
            LOG_ERROR_FMT("Failed to init slave gtm node %s in %s", node->name, node->ip);
            return -1;
        }
        
    } else if (is_slave_cn(node->type) || is_slave_dn(node->type))
    {
        if (redo_and_start_slave_node(node, install) != 0) {
            LOG_ERROR_FMT("Failed to init slave node %s in %s", node->name, node->ip);
            return -1;
        }
        
    } else {
        //
    }

    LOG_INFO_FMT("Successfully initdb for node %s (%s)", node->name.c_str(), node->ip.c_str());
    
    return 0;
}

// initdb for master node
int init_master_node(NodeInfo* node, OpentenbaseConfig* install) {

    //Step 1:  initdb 
    std::string result;
    std::string command = build_initdb_cmd(node, install);

    LOG_INFO_FMT("Executing command: %s", command.c_str());
    if (execute_command(node->ip, install->server.ssh_port, install->server.ssh_user, 
        install->server.ssh_password, command, result) != 0) {
        LOG_ERROR_FMT("Failed to initdb for data directory for node %s (%s)", node->name.c_str(), node->ip.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully initdb for node %s (%s)", node->name.c_str(), node->ip.c_str());
    
    return 0;
}


// initdb for master node
// 1、找到主节点IP和端口
// 2、拼接重做的命令
// 3、执行重做的命令
int redo_slave_node(NodeInfo* node, OpentenbaseConfig* install) {

    //Step 1:  find master node
    NodeInfo master_node;
    for (size_t i = 0; i < install->nodes.size(); ++i) {

        if ((install->nodes[i].name == node->name && is_master_node(install->nodes[i].type)))
        {
            master_node.ip = install->nodes[i].ip;
            master_node.port = install->nodes[i].port;
            break;
        }
    }

    std::string home_dir = "";
    if (get_home_dir(node->ip, install->server.ssh_port, install->server.ssh_user, 
        install->server.ssh_password, home_dir) == -1) {
        LOG_ERROR_FMT("Get home dir for server %s failed", node->ip.c_str());
        return -1;
    }

    //Step 2:  get redo cmd
    std::string dataDir = node->data_path;
    std::string binDir =  node->install_path;
    std::string command = "";
	command = "rm -rf " + dataDir + "; mkdir -p " + dataDir + "; chmod 0700 " + dataDir + 
        " && " + buid_ld_library_path_str(binDir) + 
        " && pg_basebackup -p " + std::to_string(master_node.port) + " -h " + master_node.ip + " -U " + Constants::DEFAULT_USER_OF_INITDB + " -D " + dataDir + " --wal-method=stream -v &>/dev/null";

    //Step 3:  Execute command
    LOG_INFO_FMT("Executing command: %s", command.c_str());

    std::string result;
    if (execute_command(node->ip, install->server.ssh_port, install->server.ssh_user, 
        install->server.ssh_password, command, result) != 0) {
        LOG_ERROR_FMT("Failed to redo with pg_basebackup for data directory for node %s (%s)", node->name.c_str(), node->ip.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully redo slave node %s (%s)", node->name.c_str(), node->ip.c_str());
    
    return 0;
}


// initdb for master node
//  /home/opentenbase/install/opentenbase/3.16.9.3/bin/postgres --datanode -D /home/opentenbase/run/instance/test_cluster05/dn001/data -i
std::string build_initdb_cmd(NodeInfo* node, OpentenbaseConfig* install) {

    std::string command = "";
    std::string dataDir = node->data_path;
    std::string binDir =  node->install_path;
    std::string encodeType = "UTF8";
    std::string locale = get_db_locale_by_ctype(encodeType);
    std::string home_dir = "";
    if (get_home_dir("localhost", install->server.ssh_port, install->server.ssh_user, 
        install->server.ssh_password, home_dir) == -1) {
        LOG_ERROR_FMT("Get home dir for server %s failed", node->ip.c_str());
        return "";
    }
    //std::string log_path = dataDir + "/initdb_" + node->name + "_" + node->ip + ".log";
    std::string  node_type = node->type;
    if (node_type == Constants::NODE_TYPE_GTM_MASTER || node_type == Constants::NODE_TYPE_GTM_SLAVE)
    {
        // /home/opentenbase/run/instance/test_cluster05/gtm001/data
        command = "rm -rf " + dataDir + "/* " +
            " && " + buid_ld_library_path_str(binDir) + 
            " && PGXC_CTL_SILENT=1 " + 
            " && " + binDir + "/bin/initgtm -Z gtm -D " + dataDir + " >/tmp/initgtm_" + node->name + "_" + node->ip + ".log 2>&1";
    } 
    else if (node_type == Constants::NODE_TYPE_CN_MASTER || node_type == Constants::NODE_TYPE_CN_SLAVE)
    {
        // /home/opentenbase/run/instance/test_cluster05/cn001/data
        command = "rm -rf " + dataDir + "/* " +
            " && " + buid_ld_library_path_str(binDir) + 
            " && PGXC_CTL_SILENT=1 " + 
            " && " + binDir + "/bin/initdb -U " + Constants::DEFAULT_USER_OF_INITDB + " -E utf8 --locale=" + locale + " --nodename " + node->name + " --nodetype coordinator -D " + dataDir 
            + " --master_gtm_nodename " + node->gtm_name + " --master_gtm_ip " + node->gtm_ip + " --master_gtm_port " + std::to_string(node->gtm_port) + " >/tmp/initdb_" + node->name + "_" + node->ip + ".log 2>&1";

    } 
    else if (node_type == Constants::NODE_TYPE_DN_MASTER || node_type == Constants::NODE_TYPE_DN_SLAVE)
    {
        // /home/opentenbase/run/instance/test_cluster05/dn001/data
        command = "rm -rf " + dataDir + "/* " +
            " && " + buid_ld_library_path_str(binDir) + 
            " && PGXC_CTL_SILENT=1 " + 
            " && " + binDir + "/bin/initdb -U " + Constants::DEFAULT_USER_OF_INITDB + " -E utf8 --locale=" + locale + " --nodename " + node->name + " --nodetype datanode -D " + dataDir 
            + " --master_gtm_nodename " + node->gtm_name + " --master_gtm_ip " + node->gtm_ip + " --master_gtm_port " + std::to_string(node->gtm_port) + " >/tmp/initdb_" + node->name + "_" + node->ip + ".log 2>&1";
    }

    LOG_INFO_FMT("InitDB command: %s", command.c_str());
    return command;
}


std::string get_db_locale_by_ctype(const std::string& ctype) {
    std::string locale = "C";  // 默认值
    
    if (ctype == "EUC_CN") {
        locale = "zh_CN";
    } else if (ctype == "LATIN1") {
        locale = "en_US";
    } else if (ctype == "SQL_ASCII") {
        locale = "C";
    } else if (ctype == "UTF8") {
        locale = "zh_CN.utf8";
    } else if (ctype == "GB18030") {
        locale = "zh_CN.gb18030";
    }
    // 其他情况保持默认值"C"
    
    return locale;
}

int install_command(OpentenbaseConfig *install) {

    int ret = 0;
    if (install->instance.instance_type == Constants::INSTANCE_TYPE_DISTRIBUTED) {
        // 分布式
        ret = install_distributed_instance(install);

    } else if (install->instance.instance_type == Constants::INSTANCE_TYPE_CENTRALIZED) {
        // 集中式
        ret = install_centralized_instance(install);

    } else {
        // do nothing
    }

    return ret;
}

// 新定义的函数：并行安装所有节点
int install_nodes_parallel(OpentenbaseConfig* install,const std::vector<std::string>& types) {
    
    LOG_INFO_FMT("Instal nodes parallel begin.");
    std::vector<std::thread> threads;
    std::vector<int> results(install->nodes.size(), 0);
    std::string alignedSpaces = "    ";

    // Create progress bar and thread for each node
    for (size_t i = 0; i < install->nodes.size(); ++i) {

        // 检查当前节点的 type 是否在 types 列表中
        LOG_DEBUG_FMT("Befor find node type %s for node %s .", install->nodes[i].type.c_str(), install->nodes[i].name.c_str());
        if (std::find(types.begin(), types.end(), install->nodes[i].type) == types.end()) {
            LOG_INFO_FMT("Can not find node type %s for node %s, continue to check next node info", install->nodes[i].type.c_str(), install->nodes[i].name.c_str());
            continue;
        }
        LOG_DEBUG_FMT("after find node type %s for node %s , now install this node.", install->nodes[i].type.c_str(), install->nodes[i].name.c_str());

        std::string prefix_text = "Node: " + install->nodes[i].name + "_" + 
                                std::to_string(install->nodes[i].port) + "_" + 
                                install->nodes[i].ip;

        int node_idx = i;
        threads.emplace_back([&, node_idx, prefix_text]() {

            LOG_DEBUG_FMT("emplace_back i=%zu ", node_idx); // 使用 %zu 格式化 size_t

            std::cout  << alignedSpaces << "Install " << install->nodes[node_idx].name.c_str() << "(" << install->nodes[node_idx].ip.c_str() << ") ..." << std::endl;
            int install_result = init_node(&install->nodes[node_idx], install);
            LOG_DEBUG_FMT("install node %s result is %d .", 
                        install->nodes[node_idx].name.c_str(), install_result);
            results[node_idx] = install_result;
            if (results[node_idx] == 0)
            {
                std::cout << alignedSpaces << "Install " << install->nodes[node_idx].name.c_str() << "(" << install->nodes[node_idx].ip.c_str() << ") successfully" << std::endl;
            } else {
                std::cout << alignedSpaces << "Install " << install->nodes[node_idx].name.c_str() << "(" << install->nodes[node_idx].ip.c_str() << ") failed" << std::endl;
            }
            
        });

        LOG_DEBUG_FMT("Success to new a thread to install node %s(%s) .", 
                    install->nodes[i].name.c_str(), install->nodes[i].ip.c_str());
    }

    // Wait for all threads to complete
    LOG_INFO_FMT("Wait for %zu node to complete installation.", install->nodes.size());
    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // Check if any node installation failed
    for (int result : results) {
        if (result != 0) {
            LOG_ERROR_FMT("Installation failed for one or more nodes");
            return -1;
        }
    }

    LOG_INFO_FMT("Instal nodes parallel end.");
    return 0;
}

// 新定义的函数：并行安装所有节点
int create_nodes_group(OpentenbaseConfig* install) {
    
    LOG_INFO_FMT("create default nodes group begin.");

    for (size_t i = 0; i < install->nodes.size(); ++i) {

        // Create node_group on master cn or Centralized dn
        if ((is_Centralized_instance(install->instance.instance_type) && install->nodes[i].type == Constants::NODE_TYPE_DN_MASTER)
        || install->nodes[i].type == Constants::NODE_TYPE_CN_MASTER)
        {
            if (create_default_node_group(&install->nodes[i], install) != 0) {
                LOG_ERROR_FMT("Failed to create default_node_group for %s(%s)", install->nodes[i].name.c_str(), install->nodes[i].ip.c_str());
                return -1;
            }
            break;

        } else{

            continue;
        }
    }

    LOG_INFO_FMT("create default nodes group end.");
    return 0;
}

// 存算一体架构的install_command
// 这里会直接把进展步骤输出到页面
int install_distributed_instance(OpentenbaseConfig *install) {
    LOG_INFO_FMT("Starting installation process");
    std::cout << "\n ====== Start to Install Instance " << install->instance.instance_name <<"  ====== " << std::endl;
    int stepNum = 1;
    std::string alignedSpaces = "    ";

    // make *.tar.gz pkg
    std::cout << "\nstep " << std::to_string(stepNum++) << ": Make *.tar.gz pkg ..." << std::endl; 
    if (pre_process_pkg(install) != 0) {
        LOG_ERROR_FMT("Failed to install rpm package and then make *.tar.gz package");
        return -1;
    }
    std::cout << alignedSpaces << "Make " << install->instance.package_name << " successfully." << std::endl; 

    std::cout << "\nstep " << std::to_string(stepNum++) << ": Transfer and extract pkg to servers ..." << std::endl; 
    std::cout << alignedSpaces << "Package_path: " << install->instance.package_path  << std::endl;
    if (pre_install_command(install) != 0) {
        LOG_ERROR_FMT("Failed to pre-install command");
        return -1;
    }
    std::cout << alignedSpaces << "Transfer and extract pkg to servers successfully." << std::endl; 

    // gtm 主节点
    std::cout << "\nstep " << std::to_string(stepNum++) << ": Install gtm master node ..." << std::endl; 
    std::vector<std::string> install_types = {Constants::NODE_TYPE_GTM_MASTER};
    if (install_nodes_parallel(install, install_types) != 0) {
        LOG_ERROR_FMT("Failed to install gtm master node");
        std::cout << "\n ====== Failed to install gtm master nodes  ====== \n" << std::endl;
        return -1;
    }
    LOG_INFO_FMT("Success to install gtm master node.");
    std::cout << alignedSpaces << "Success to install  gtm master node. " << std::endl;

    // cn/dn 主节点
    std::cout << "\nstep " << std::to_string(stepNum++) << ": Install cn/dn master node ..." << std::endl; 
    install_types = {Constants::NODE_TYPE_CN_MASTER, Constants::NODE_TYPE_DN_MASTER};
    if (install_nodes_parallel(install, install_types) != 0) {
        LOG_ERROR_FMT("Failed to install cn/dn master node");
        std::cout << "\n ====== Failed to install all cn/dn master nodes  ====== \n" << std::endl;
        return -1;
    }
    LOG_INFO_FMT("Success to install cn/dn master nodes.");
    std::cout << alignedSpaces << "Success to install all cn/dn master nodes. " << std::endl;

    // cn/dn/gtm 备节点
    std::cout << "\nstep " << std::to_string(stepNum++) << ": Install slave nodes ..." << std::endl; 
    install_types = {Constants::NODE_TYPE_CN_SLAVE, Constants::NODE_TYPE_DN_SLAVE, Constants::NODE_TYPE_GTM_SLAVE};
    if (install_nodes_parallel(install, install_types) != 0) {
        LOG_ERROR_FMT("Failed to install all slave node");
        std::cout << "\n ====== Failed to install all slave nodes  ====== \n" << std::endl;
        return -1;
    }
    LOG_INFO_FMT("Success to install slave nodes.");
    std::cout << alignedSpaces << "Success to install all slave nodes. " << std::endl;

    // 创建节点组
    std::cout << "\nstep " << std::to_string(stepNum++) << ":Create node group ..." << std::endl; 
    if (create_nodes_group(install) != 0) {
        LOG_ERROR_FMT("Failed to iCreate node group");
        std::cout << "\n ====== Failed to Create node group  ====== \n" << std::endl;
        return -1;
    }
    LOG_INFO_FMT("Success to install cn/dn node.");
    std::cout << alignedSpaces << "Create node group successfully. " << std::endl;


    std::cout << "\n ====== Installation completed successfully  ====== \n" << std::endl;
    LOG_INFO_FMT("Installation completed successfully");
    return 0;
}

// 存算一体架构的install_command
// 这里会直接把进展步骤输出到页面
int install_centralized_instance(OpentenbaseConfig *install) {
    LOG_INFO_FMT("Starting installation process");
    std::cout << "\n ====== Start to Install  instance " << install->instance.instance_name <<"  ====== " << std::endl;
    int stepNum = 1;
    std::string alignedSpaces = "    ";


    // make *.tar.gz pkg
    std::cout << "\nstep " << std::to_string(stepNum++) << ": Make *.tar.gz pkg ..." << std::endl; 
    if (pre_process_pkg(install) != 0) {
        LOG_ERROR_FMT("Failed to install rpm package and then make *.tar.gz package");
        return -1;
    }
    std::cout << alignedSpaces << "Make " << install->instance.package_name << " successfully." << std::endl; 

    std::cout << "\nstep " << std::to_string(stepNum++) << ": Transfer and extract pkg to servers ..." << std::endl; 
    std::cout << alignedSpaces << "Package_path: " << install->instance.package_path  << std::endl;
    if (pre_install_command(install) != 0) {
        LOG_ERROR_FMT("Failed to pre-install command");
        return -1;
    }
    std::cout << alignedSpaces << "Transfer and extract pkg to servers successfully." << std::endl; 

    // dn 主节点
    std::cout << "\nstep " << std::to_string(stepNum++) << ": Install cn/dn master node ..." << std::endl; 
    std::vector<std::string> install_types = {Constants::NODE_TYPE_DN_MASTER};
    if (install_nodes_parallel(install, install_types) != 0) {
        LOG_ERROR_FMT("Failed to install cn/dn master node");
        std::cout << "\n ====== Failed to install all cn/dn master nodes  ====== \n" << std::endl;
        return -1;
    }
    LOG_INFO_FMT("Success to install cn/dn master nodes.");
    std::cout << alignedSpaces << "Success to install all cn/dn master nodes. " << std::endl;

    // dn 备节点
    std::cout << "\nstep " << std::to_string(stepNum++) << ": Install slave nodes ..." << std::endl; 
    install_types = {Constants::NODE_TYPE_DN_SLAVE};
    if (install_nodes_parallel(install, install_types) != 0) {
        LOG_ERROR_FMT("Failed to install all slave node");
        std::cout << "\n ====== Failed to install all slave nodes  ====== \n" << std::endl;
        return -1;
    }
    LOG_INFO_FMT("Success to install slave nodes.");
    std::cout << alignedSpaces << "Success to install all slave nodes. " << std::endl;

    // 创建节点组
    std::cout << "\nstep " << std::to_string(stepNum++) << ":Create node group ..." << std::endl; 
    if (create_nodes_group(install) != 0) {
        LOG_ERROR_FMT("Failed to iCreate node group");
        std::cout << "\n ====== Failed to Create node group  ====== \n" << std::endl;
        return -1;
    }
    LOG_INFO_FMT("Success to install cn/dn node.");
    std::cout << alignedSpaces << "Create node group successfully. " << std::endl;


    std::cout << "\n ====== Installation completed successfully  ====== \n" << std::endl;
    LOG_INFO_FMT("Installation completed successfully");
    return 0;
}

// Delete single node
int delete_node(NodeInfo* node, OpentenbaseConfig* config) {
    // Step 1: Stop node
    LOG_INFO_FMT("Step %d/%d: Stopping node %s", 
        1, 2, node->name.c_str());
    int ret = stop_node(node, config);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to stop node %s", node->name.c_str());
        return -1;
    }

    // Step 2: Delete node directories
    LOG_INFO_FMT("Step %d/%d: Deleting directories for node %s", 
        2, 2, node->name.c_str());
    ret = delete_node_directories(node, config);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to delete directories for node %s", node->name.c_str());
        return -1;
    }

    return 0;
}

int delete_command(OpentenbaseConfig *config) {
    LOG_INFO_FMT("Starting deletion process");

    if (delete_virtual_cluster(config) != 0) {
        LOG_ERROR_FMT("Failed to delete virtual cluster");
        return -1;
    }

    std::vector<std::thread> threads;
    std::vector<int> results(config->nodes.size(), 0);

    for (size_t i = 0; i < config->nodes.size(); ++i) {

        threads.emplace_back([&, i]() {
            results[i] = delete_node(&config->nodes[i], config);
            if (results[i] == 0)
            {
                std::cout << "Delete node " << config->nodes[i].name << "(" << config->nodes[i].ip << ") Success " << std::endl;
            } else {
                std::cout << "Delete node " << config->nodes[i].name << "(" << config->nodes[i].ip << ") Failed " << std::endl;
            }
            
        });
    }

    // Update total progress bar
    while (true) {
        int total_progress = 30;  // Base progress
        bool all_completed = true;
        
        for (size_t i = 0; i < config->nodes.size(); ++i) {
            if (results[i] == 0) {
                total_progress += 70 / config->nodes.size();
            } else {
                all_completed = false;
            }
        }
        
        if (all_completed) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (int result : results) {
        if (result != 0) {
            LOG_ERROR_FMT("Deletion failed for one or more nodes");
            return -1;
        }
    }

    LOG_INFO_FMT("Deletion completed successfully");
    return 0;
}


// Start single node
int start_single_node(NodeInfo* node, OpentenbaseConfig* config) {
    LOG_INFO_FMT("Step %d/%d: Starting node %s", 
        1, 1, node->name.c_str());
    int ret = start_node(node, config);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to start node %s", node->name.c_str());
        return -1;
    }
    return 0;
}

int start_command(OpentenbaseConfig *config) {
    LOG_INFO_FMT("Starting start process");


    // 筛选出所有 is_op_node 为 true 的节点
    std::vector<NodeInfo> op_nodes = filterNodesByOpStatus(*config, true);
    std::vector<std::thread> threads;
    std::vector<int> results(op_nodes.size(), 0);
    
    int failedCount = 0;
    int successCount = 0;
    int op_node_count = op_nodes.size();

    // Create thread for each node
    std::cout << "\nStart executing start node... \n" << std::endl;
    for (size_t i = 0; i < op_nodes.size(); ++i) {

        threads.emplace_back([&, i]() {
            results[i] = start_single_node(&op_nodes[i], config);
            if (results[i] == 0)
            {
                std::cout << "Start node " << op_nodes[i].name << "(" << op_nodes[i].ip << ") Success " << std::endl;
            } else {
                std::cout << "Start node " << op_nodes[i].name << "(" << op_nodes[i].ip << ") Failed " << std::endl;
            }
            
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Check if any node start failed
    for (int result : results) {
        if (result != 0) {
            LOG_ERROR_FMT("Start failed for one or more nodes");
            failedCount++;
        }
    }
    successCount = op_node_count - failedCount;
    
    std::cout << "\n[Result] Total: " << op_node_count
            << ", Success: " << successCount 
            << ", Failed: " << failedCount << "\n" << std::endl;


    LOG_INFO_FMT("[Result] Total: %zu, Success: %zu, Failed: %zu", 
             op_node_count, successCount, failedCount);
    return 0;
}

// Stop single node
int stop_single_node(NodeInfo* node, OpentenbaseConfig* config) {
    LOG_INFO_FMT("Step %d/%d: Stopping node %s", 
        1, 1, node->name.c_str());
    // check if node is running
    if (is_node_running(node, config) != 0) {
        LOG_INFO_FMT("Node %s is not running", node->name.c_str());
        return 0;
    }
    
    if (stop_node(node, config) != 0) {
        LOG_ERROR_FMT("Failed to stop node %s", node->name.c_str());
        return -1;
    }
    return 0;
}

int stop_command(OpentenbaseConfig *config) {
    LOG_INFO_FMT("Starting stop process");

    // 筛选出所有 is_op_node 为 true 的节点
    std::vector<NodeInfo> op_nodes = filterNodesByOpStatus(*config, true);
    std::vector<std::thread> threads;
    std::vector<int> results(op_nodes.size(), 0);
    
    int failedCount = 0;
    int successCount = 0;

    // Create thread for each node

    std::cout << "\nStart executing stop node... \n" << std::endl;
    for (size_t i = 0; i < op_nodes.size(); ++i) {
        threads.emplace_back([&, i]() {
            results[i] = stop_single_node(&op_nodes[i], config);
            if (results[i] == 0)
            {
                std::cout << "Stop node " << op_nodes[i].name << "(" << op_nodes[i].ip << ") Success " << std::endl;
            } else {
                std::cout << "Stop node " << op_nodes[i].name << "(" << op_nodes[i].ip << ") Failed " << std::endl;
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Check if any node stop failed
    for (int result : results) {
        if (result != 0) {
            LOG_ERROR_FMT("Stop failed for one or more nodes");
            failedCount++;
        }
    }
    successCount = op_nodes.size() - failedCount;
    
    std::cout << "\n[Result] Total: " << op_nodes.size()
            << ", Success: " << successCount 
            << ", Failed: " << failedCount << "\n" << std::endl;


    LOG_INFO_FMT("[Result] Total: %zu, Success: %zu, Failed: %zu", 
             op_nodes.size(), successCount, failedCount);
    return 0;
}

// status command
int status_command(OpentenbaseConfig *config) {

    // 筛选出所有 is_op_node 为 true 的节点
    std::vector<NodeInfo> opNodes = filterNodesByOpStatus(*config, true);
    InstanceConfig instance = config->instance;

    std::vector<std::thread> threads;
    std::vector<int> results(opNodes.size(), 0);
    int runningCount = 0;
    int stoppedCount = 0;
    int unkownCount = 0;

    // instance
    std::cout << "\n ------------- Instance status -----------  " << std::endl;
    std::cout << "Instance name: " << instance.instance_name.c_str() << std::endl;
    std::cout << "Version: v" << instance.version.c_str() << std::endl;

    // node status
    std::cout << "\n -------------- Node status --------------  " << std::endl;
    for (size_t i = 0; i < opNodes.size(); ++i) {
        threads.emplace_back([&, i]() {

            std::string port = get_node_port(&opNodes[i], config);
            
            results[i] = is_node_running(&opNodes[i], config);
            switch (results[i]) {
                case 0:
                    std::cout << "Node " << opNodes[i].name.c_str() << "(" << opNodes[i].ip.c_str() << ":" << port << ") is Running " << std::endl;
                    LOG_INFO_FMT("node(%s:%s) is running", opNodes[i].name.c_str(), opNodes[i].ip.c_str());
                    runningCount++;
                    break;
                case 1:
                    std::cout << "Node " << opNodes[i].name.c_str() << "(" << opNodes[i].ip.c_str() << ":" << port << ") is Stopped " << std::endl;
                    LOG_INFO_FMT("node(%s:%s) is Stopped", opNodes[i].name.c_str(), opNodes[i].ip.c_str());
                    stoppedCount++;
                    break;
                case 2:
                    std::cout << "Node " << opNodes[i].name.c_str() << "(" << opNodes[i].ip.c_str() << ":" << port << ") is Unknown " << std::endl;
                    LOG_INFO_FMT("node(%s:%s) is Unknown", opNodes[i].name.c_str(), opNodes[i].ip.c_str());
                    unkownCount++;
                    break;
                default:
                    std::cout << "Node " << opNodes[i].name.c_str() << "(" << opNodes[i].ip.c_str() << ":" << port << ") is Unknown " << std::endl;
                    LOG_INFO_FMT("node(%s:%s) is Unknown", opNodes[i].name.c_str(), opNodes[i].ip.c_str());
                    unkownCount++;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    std::cout << "[Result] Total: " << opNodes.size()
            << ", Running: " << runningCount 
            << ", Stopped: " << stoppedCount
            << ", Unknown: " << unkownCount << "\n" << std::endl;
    LOG_INFO_FMT("[Result] Total: %zu, Running: %zu, Stopped: %zu, Unknown: %zu", 
             opNodes.size(), runningCount , stoppedCount, unkownCount);


    // connection info
    int masterCnCount = 0;
    for (size_t i = 0; i < opNodes.size(); ++i) {

        if (is_Centralized_instance(instance.instance_type) && is_master_dn(opNodes[i].type))
        {
            std::string port = get_node_port(&opNodes[i], config);
            std::cout << " ------- Master DN Connection Info -------  " << std::endl;

            std::cout << "Environment variable: " << buid_ld_library_path_str(opNodes[i].install_path) << std::endl;
            std::cout << "PSQL connection: psql -h " << opNodes[i].ip << " -p " << port << " -U " + std::string(Constants::DEFAULT_USER_OF_INITDB) << " " << std::string(Constants::DEFAULT_DB) << "\n" << std::endl;
            break;

        } else if (is_master_cn(opNodes[i].type))
        {
            masterCnCount++;
            // 如果查询到有master cn的情况下才打印节点信息
            if (masterCnCount == 1)
            {
                std::cout << " ------- Master CN Connection Info -------  " << std::endl;
            }
            std::cout << "[" << masterCnCount << "] " << opNodes[i].name << "(" << opNodes[i].ip << ")  " << std::endl;
            std::cout << "Environment variable: " << buid_ld_library_path_str(opNodes[i].install_path) << std::endl;

            std::string port = get_node_port(&opNodes[i], config);
            std::cout << "PSQL connection: psql -h " << opNodes[i].ip << " -p " << port << " -U " << std::string(Constants::DEFAULT_USER_OF_INITDB) << " " << std::string(Constants::DEFAULT_DB) << "\n" << std::endl;
        }
    }

    return 0;
}


// scp command
int scp_command(OpentenbaseConfig *config) {


    // 筛选出所有 is_op_node 为 true 的节点
    std::vector<NodeInfo> op_nodes = filterNodesByOpStatus(*config, true);
    InstanceConfig instance = config->instance;

    std::vector<std::thread> threads;
    std::vector<int> results(op_nodes.size(), 0);
    int ret = 0;

    // instance
    std::cout << "\n ------------- Instance status -----------  " << std::endl;
    std::cout << "Instance name: " << instance.instance_name.c_str() << std::endl;
    std::cout << "Version: " << instance.version.c_str() << std::endl;

    // 获得不重复的IP列表
    std::vector<std::string> server_list = getServerList(op_nodes);
    if (server_list.size() == 0)
    {
        LOG_ERROR_FMT("Server IP list size is zero, please specify the configuration file through - c ");
        return -1;
    }
    
    // 批量拷贝包
    ret = transfer_package_concurrency(config, config->scpfile.source_file, config->scpfile.dest_path, server_list);
    if (ret != 0)
    {
        LOG_ERROR_FMT("Failed to scp file");
        return -1;
    }
    
    return 0;
}

// shell command
int shell_command(OpentenbaseConfig *config_info) {

    std::vector<std::thread> threads;
    std::vector<int> results(config_info->nodes.size(), 0);
    int ret = 0;

    // 页面输出信息
    std::cout << "--------------------------------------------------------  " << '\n';
    std::cout << "Instance name : " << config_info->instance.instance_name.c_str() << '\n';
    std::cout << "Version       : " << config_info->instance.version.c_str() << '\n';
    std::cout << "Config file   : " << config_info->config_file.c_str() << '\n';
    std::cout << "CMD           : " << config_info->shell.shell_cmd << '\n';
    std::cout << "--------------------------------------------------------  " << '\n' << '\n';

    // 获得不重复的IP列表
    std::vector<std::string> server_list = getServerList(config_info->nodes);
    if (server_list.size() == 0)
    {
        LOG_ERROR_FMT("Server IP list size is zero, please specify the configuration file through - c ");
        return -1;
    }
    
    // 批量拷贝包
    ret = excute_cmd_concurrency(config_info, server_list);
    if (ret != 0)
    {
        LOG_ERROR_FMT("Failed to scp file");
        return -1;
    }
    
    return 0;
}

// sql command
int sql_command(OpentenbaseConfig *config_info) {

    std::vector<std::thread> threads;
    std::vector<int> results(config_info->nodes.size(), 0);
    int ret = 0;

    // 页面展示的基本信息
    std::cout << "--------------------------------------------------------  " << '\n';
    std::cout << "Instance name : " << config_info->instance.instance_name.c_str() << '\n';
    std::cout << "Version       : " << config_info->instance.version.c_str() << '\n';
    std::cout << "Config file   : " << config_info->config_file.c_str() << '\n';
    std::cout << "SQL           : " << config_info->sql.sql.c_str() << '\n';
    std::cout << "--------------------------------------------------------  " << '\n' << '\n';
    
    // 并行执行sql
    ret = excute_sql_concurrency(config_info, config_info->nodes);
    if (ret != 0)
    {
        LOG_ERROR_FMT("Failed to scp file");
        return -1;
    }
    
    return 0;
}

// instance status command
int get_instance_status(OpentenbaseConfig *config) {

    std::vector<std::thread> threads;
    std::vector<int> results(config->nodes.size(), 0);
    int runningCount = 0;
    int stoppedCount = 0;
    int unkownCount = 0;

    // node status
    std::cout << "\n -------------- Node status --------------  " << std::endl;
    for (size_t i = 0; i < config->nodes.size(); ++i) {
        threads.emplace_back([&, i]() {
            
            results[i] = is_node_running(&config->nodes[i], config);
            switch (results[i]) {
                case 0:
                    std::cout << "Node " << config->nodes[i].name << "(" << config->nodes[i].ip << ") is Running " << std::endl;
                    LOG_INFO_FMT("node(%s:%s) is running", config->nodes[i].name, config->nodes[i].ip);
                    runningCount++;
                    break;
                case 1:
                    std::cout << "Node " << config->nodes[i].name << "(" << config->nodes[i].ip << ") is Stopped " << std::endl;
                    LOG_INFO_FMT("node(%s:%s) is Stopped", config->nodes[i].name, config->nodes[i].ip);
                    stoppedCount++;
                    break;
                case 2:
                    std::cout << "Node " << config->nodes[i].name << "(" << config->nodes[i].ip << ") is Unknown " << std::endl;
                    LOG_INFO_FMT("node(%s:%s) is Unknown", config->nodes[i].name, config->nodes[i].ip);
                    unkownCount++;
                    break;
                default:
                    std::cout << "Node " << config->nodes[i].name << "(" << config->nodes[i].ip << ") is Unknown " << std::endl;
                    LOG_INFO_FMT("node(%s:%s) is Unknown", config->nodes[i].name, config->nodes[i].ip);
                    unkownCount++;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    std::cout << "\n[Result] Total: " << config->nodes.size()
            << ", Running: " << runningCount 
            << ", Stopped: " << stoppedCount
            << ", Unknown: " << unkownCount << "\n" << std::endl;
    LOG_INFO_FMT("[Result] Total: %zu, Running: %zu, Stopped: %zu, Unknown: %zu", 
             config->nodes.size(), runningCount , stoppedCount, unkownCount);


    // connection info
    int masterCnCount = 0;
    std::cout << " ------- Master CN Connection Info -------  " << std::endl;
    for (size_t i = 0; i < config->nodes.size(); ++i) {
        if (is_master_cn(config->nodes[i].type))
        {
            masterCnCount++;

            std::cout << "[" << masterCnCount << "] " << config->nodes[i].name << "(" << config->nodes[i].ip << ")  " << std::endl;
            std::cout << "Environment variable: " << buid_ld_library_path_str(config->nodes[i].install_path) << std::endl;

            std::string port = get_node_port(&config->nodes[i], config);
            std::cout << "PSQL connection: psql -h " << config->nodes[i].ip << " -p " << port << " -U " << std::string(Constants::DEFAULT_USER_OF_INITDB) << " " << std::string(Constants::DEFAULT_DB) << "\n" << std::endl;
        }
    }

    return 0;
}


// node status command
int get_node_status(NodeInfo *node, OpentenbaseConfig *config) {

    int result;

    std::cout << "\n -------------- Node status --------------  " << std::endl;
    result = is_node_running(node, config);
    switch (result) {
        case 0:
            std::cout << "Node " << node->name << "(" << node->ip << ") is Running " << std::endl;
            LOG_INFO_FMT("node(%s:%s) is running", node->name, node->ip);
            break;
        case 1:
            std::cout << "Node " << node->name << "(" << node->ip << ") is Stopped " << std::endl;
            LOG_INFO_FMT("node(%s:%s) is Stopped", node->name, node->ip);
            break;
        case 2:
            std::cout << "Node " << node->name << "(" << node->ip << ") is Unknown " << std::endl;
            LOG_INFO_FMT("node(%s:%s) is Unknown", node->name, node->ip);
            break;
        default:
            std::cout << "Node " << node->name << "(" << node->ip << ") is Unknown " << std::endl;
            LOG_INFO_FMT("node(%s:%s) is Unknown", node->name, node->ip);
    }

    // connection info
    std::cout << " ------- Node Connection Info -------  " << std::endl;
    std::cout << "Environment variable: " << buid_ld_library_path_str(node->install_path) << std::endl;

    std::string port = get_node_port(node, config);
    std::cout << "PSQL connection: psql -h " << node->ip << " -p " << port << " -U " << std::string(Constants::DEFAULT_USER_OF_INITDB) << " " << std::string(Constants::DEFAULT_DB) << "\n" << std::endl;

    return 0;
}

// Expand command uses the same concurrent logic as config command
int expand_command(OpentenbaseConfig *config) {
    LOG_INFO_FMT("Starting expand process");

    if (get_connections(config) != 0) {
        LOG_ERROR_FMT("Failed to get meta connection info");
        return -1;
    }

    if (pre_install_command(config) != 0) {
        LOG_ERROR_FMT("Failed to pre-install command");
        return -1;
    }

    // Create thread array
    std::vector<std::thread> threads;
    std::vector<int> results(config->nodes.size(), 0);

    // Create thread for each node
    for (size_t i = 0; i < config->nodes.size(); ++i) {
        threads.emplace_back([&, i]() {
            results[i] = install_node(&config->nodes[i], config);
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Check if any node installation failed
    for (int result : results) {
        if (result != 0) {
            LOG_ERROR_FMT("Installation failed for one or more nodes");
            return -1;
        }
    }

    LOG_INFO_FMT("Installation completed successfully");
    return 0;
}

// Shrink command uses the same concurrent logic as delete command
int shrink_command(OpentenbaseConfig *config) {
    // Create thread array
    std::vector<std::thread> threads;
    std::vector<int> results(config->nodes.size(), 0);

    for (size_t i = 0; i < config->nodes.size(); ++i) {
        threads.emplace_back([&, i]() {
            results[i] = delete_node(&config->nodes[i], config);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (int result : results) {
        if (result != 0) {
            LOG_ERROR_FMT("Deletion failed for one or more nodes");
            return -1;
        }
    }

    LOG_INFO_FMT("Cluster shrinking completed successfully");
    return 0;
}

// Check if node is running
int is_node_running(NodeInfo* node, OpentenbaseConfig* config) {
    std::string command = "ps -ef | grep " + node->data_path + " | grep -v grep";
    std::string result;
    int ret = 0;
    ret = execute_command(node->ip, config->server.ssh_port, 
        config->server.ssh_user, config->server.ssh_password, command, result);
    if (ret != 0)
    {
        // Unkown
        LOG_ERROR_FMT("Command execution to retrieve (%s:%s)status failed", node->name.c_str(), node->ip.c_str());
        return 2;
    }
    
    if (result.empty())
    {
        // Stopped
        return 1;
    } else {
        // Running
        return 0;
    } 
}

/**
 * @brief 根据节点信息，获得不重复的服务器IP列表
 * 
 * @param nodes 节点列表
 * @return 服务器的IP列表
 */
std::vector<std::string> getServerList(const std::vector<NodeInfo>& nodes) {
    std::unordered_set<std::string> ips;
    for (const auto& node : nodes) {
        if (!node.ip.empty()) {
            ips.insert(node.ip);
        }
    }
    return {ips.begin(), ips.end()}; // 转为 vector
}
