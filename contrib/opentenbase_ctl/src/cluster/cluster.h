#ifndef CLUSTER_H
#define CLUSTER_H

#include "../config/config.h"
#include "../types/types.h"
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>

#define OSS_INSTALL_DIR "/usr/local/install/opentenbase"
#define OSS_PKG_TMP_DIR "opentenbase"

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

int init_master_node(NodeInfo* node, OpentenbaseConfig* install);
int redo_slave_node(NodeInfo* node, OpentenbaseConfig* install);

std::string build_initdb_cmd(NodeInfo* node, OpentenbaseConfig* install);

std::string GetDBLocaleByCType(const std::string& ctype);

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

// Expand command
int expand_command(OpentenbaseConfig *install);

// Shrink command
int shrink_command(OpentenbaseConfig *install);

// 获取文件的基本名称（去掉扩展名）
std::string getFileBaseName(const std::string& filename);

#endif // CLUSTER_H 