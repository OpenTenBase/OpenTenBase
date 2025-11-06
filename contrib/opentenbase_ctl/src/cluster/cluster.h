#ifndef CLUSTER_H
#define CLUSTER_H

#include "../config/config.h"
#include "../types/types.h"
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>

// Check if node is running
int is_node_running(NodeInfo* node, OpentenbaseConfig* install);

// Get meta and txn connection info from etcd
int get_connections(OpentenbaseConfig *install);

// Create virtual cluster
int create_virtual_cluster(OpentenbaseConfig *install);

// Delete virtual cluster
int delete_virtual_cluster(OpentenbaseConfig *install);

// Create pgxc node
int create_pgxc_node(NodeInfo *node, OpentenbaseConfig *install);

// Get node type for pg_ctl
std::string get_node_type(const std::string& type);

// Pre-install command
int pre_install_command(OpentenbaseConfig *install);

int pre_install_command_concurrency(OpentenbaseConfig *install);

// Install single node
int install_node(NodeInfo* node, OpentenbaseConfig* install);

// Install slave gtm
int init_and_start_slave_gtm(NodeInfo* node, OpentenbaseConfig* install);

// Install master node
int init_and_start_master_node(NodeInfo* node, OpentenbaseConfig* install);

// Install slave node
int redo_and_start_slave_node(NodeInfo* node, OpentenbaseConfig* install);

// init master node
int init_master_node(NodeInfo* node, OpentenbaseConfig* install);

// rego slave node
int redo_slave_node(NodeInfo* node, OpentenbaseConfig* install);

// build initdb cmd
std::string build_initdb_cmd(NodeInfo* node, OpentenbaseConfig* install);

// get db locale by ctype
std::string get_db_locale_by_ctype(const std::string& ctype);

// Install command
int install_command(OpentenbaseConfig *install);

// Install mpp cluster
int install_mpp(OpentenbaseConfig *install);

// Install distributed cluster
int install_distributed_instance(OpentenbaseConfig *install);

// Installcentralized cluster
int install_centralized_instance(OpentenbaseConfig *install);

// Delete single node
int delete_node(NodeInfo* node, OpentenbaseConfig* install);

// Delete command
int delete_command(OpentenbaseConfig *install);

// Start single node
int start_single_node(NodeInfo* node, OpentenbaseConfig* install);

// Start command
int start_command(OpentenbaseConfig *install);

// Stop single node
int stop_single_node(NodeInfo* node, OpentenbaseConfig* install);

// Stop command
int stop_command(OpentenbaseConfig *install);

// Status command
int status_command(OpentenbaseConfig *install);

// Status command
int scp_command(OpentenbaseConfig *install);

// Status command
int shell_command(OpentenbaseConfig *install);

// Expand command
int expand_command(OpentenbaseConfig *install);

// Shrink command
int shrink_command(OpentenbaseConfig *install);

// 获取文件的基本名称（去掉扩展名）
std::string getFileBaseName(const std::string& filename);

// 根据节点信息获得所有的IP列表
std::vector<std::string> getServerList(const std::vector<NodeInfo>& nodes);

// 批量传输软件包
int transfer_package_concurrency(OpentenbaseConfig *install,
    const std::string& source_file,
    const std::string& dest_path,
    const std::vector<std::string>& server_list);

// excute sql
int sql_command(OpentenbaseConfig *config_info);

#endif // CLUSTER_H 