#include "node.h"
#include "../../src/ssh/remote_ssh.h"
#include "../log/log.h"
#include "../utils/utils.h"
#include <unistd.h>
#include <thread>
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <pqxx/pqxx>

// Create node directories
int 
create_node_directories(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Creating directories for node %s (%s)", node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    int ret;

    // Create install directory
    command = "mkdir -p " + node->install_path;
    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    ret = execute_command(node->ip, 
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        command,
        result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to create install directory on node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully created install directory on node %s (%s)", 
            node->name.c_str(), node->ip.c_str());

    // Create data directory
    command = "mkdir -p " + node->data_path;
    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    ret = execute_command(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        command,
        result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to create data directory on node %s (%s): %s",
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully created data directory on node %s (%s)",
            node->name.c_str(), node->ip.c_str());

    return 0;
}

// Delete node directories
int 
delete_node_directories(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Deleting directories for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    int ret;
    // Delete install directory
    command = "rm -rf " + node->data_path;
    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    ret = execute_command(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        command,
        result);
    if (ret!= 0) {
        LOG_ERROR_FMT("Failed to delete directories on node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully deleted directories on node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    return 0; 
}

// Configure PostgreSQL
int 
configure_postgresql_for_cdw(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Configuring PostgreSQL for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    std::string base_command;
    std::string include_if_exists;
    std::string etcd_conn;
    int ret;

    // Build base command string
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";

    // Set pg_controldata
    command = base_command + "pg_controldata"
              " -S " + std::to_string(install->meta.shard_num) +
              " -C " + install->meta.cluster_oid +
              " -M " + std::to_string(install->meta.meta_id) +
              " -D " + node->data_path;

    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    ret = remote_ssh_exec(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        command,
        result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to execute pg_controldata on node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully executed pg_controldata on node %s (%s)", 
            node->name.c_str(), node->ip.c_str());

    include_if_exists = "\\'"+node->data_path+"/postgresql.conf.user\\'";
    etcd_conn = "\\'"+install->meta.etcd_server+"\\'";
    // Configure postgresql.conf
    std::vector<std::pair<std::string, std::string>> config_items = {
        {"pgxc_node_name", node->name},
        {"port", std::to_string(node->port)},
        {"pooler_port", std::to_string(node->pooler_port)},
        {"instance_name", install->instance.instance_name},
        {"include_if_exists", include_if_exists},
        {"etcd_server_conn", etcd_conn}
    };

    for (const auto& item : config_items) {
        command = base_command + "confmod"
                  " -a mod"
                  " -d " + node->data_path +
                  " -f postgresql.conf"
                  " -g " + item.first +
                  " -v " + item.second;

        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = remote_ssh_exec(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to configure %s on node %s (%s): %s", 
                    item.first.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully configured %s on node %s (%s)", 
                item.first.c_str(), node->name.c_str(), node->ip.c_str());
    }

    // Set pg_hba.conf
    if (configure_pg_hba(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure pg_hba.conf");
        return -1;
    }
    return 0;
}


// Configure PostgreSQL
int 
configure_postgresql_for_mpp(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Configuring PostgreSQL for node %s (%s)", node->name.c_str(), node->ip.c_str());

    if (is_master_cn(node->type)) {
        if (configure_cn_node(node, install) != 0) {
            LOG_ERROR_FMT("Failed to configure node %s (%s)", node->name.c_str(), node->ip.c_str());
            return -1;
        }

    } else if (is_master_dn(node->type)) {
        if (configure_dn_node(node, install) != 0) {
            LOG_ERROR_FMT("Failed to configure node %s (%s)", node->name.c_str(), node->ip.c_str());
            return -1;
        }

    } else if (is_master_gtm(node->type)) {
        if (configure_gtm_node(node, install) != 0) {
            LOG_ERROR_FMT("Failed to configure node %s (%s)", node->name.c_str(), node->ip.c_str());
            return -1;
        }

    } else if (is_slave_cn(node->type)) {
        if (configure_cn_slave_node(node, install) != 0) {
            LOG_ERROR_FMT("Failed to configure node %s (%s)", node->name.c_str(), node->ip.c_str());
            return -1;
        }

    } else if (is_slave_dn(node->type)) {
        if (configure_dn_slave_node(node, install) != 0) {
            LOG_ERROR_FMT("Failed to configure node %s (%s)", node->name.c_str(), node->ip.c_str());
            return -1;
        }

    } else if (is_slave_gtm(node->type)) {
        if (configure_gtm_slave_node(node, install) != 0) {
            LOG_ERROR_FMT("Failed to configure node %s (%s)", node->name.c_str(), node->ip.c_str());
            return -1;
        }

    } else {
        // do nothing
        LOG_ERROR_FMT("Failed to configure node %s (%s), node type is invalid.", node->name.c_str(), node->ip.c_str());
    }

    LOG_INFO_FMT("Configuring PostgreSQL for node %s (%s)", node->name.c_str(), node->ip.c_str());
    return 0;
}


// Configure  cn master node
int configure_cn_node(NodeInfo *node, OpentenbaseConfig *install) {
    
    LOG_INFO_FMT("Configuring PostgreSQL for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    std::string base_command;
    std::string include_if_exists;
    std::string etcd_conn;
    int ret;

    // Build base command string
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";

    include_if_exists = "\\'"+node->data_path+"/postgresql.conf.user\\'";

    // Configure postgresql.conf
    std::vector<std::pair<std::string, std::string>> config_items = {
        {"listen_addresses", "\\'*\\'"},
        {"port", std::to_string(node->port)},
        {"pooler_port", std::to_string(node->pooler_port)},
        {"forward_port", std::to_string(node->forward_port)},
        {"shared_preload_libraries", "\\'pg_stat_statements,pageinspect,pg_errcode_stat,pg_squeeze,pg_stat_log,pg_stat_error\\'"},
        {"pg_stat_statements.max", "1000"},
        {"pg_stat_statements.track", "all"},
        {"pg_stat_log.max", "1024"},
        {"pg_stat_log.track", "all"},
        {"pg_stat_log.track_utility", "true"},
        {"log_min_duration_statement", "10000"},
        {"include_if_exists", include_if_exists}
    };

    for (const auto& item : config_items) {
        command = base_command + "confmod"
                  " -a mod"
                  " -d " + node->data_path +
                  " -f postgresql.conf"
                  " -g " + item.first +
                  " -v " + item.second;

        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to configure %s on node %s (%s): %s", 
                    item.first.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully configured %s on node %s (%s)", 
                item.first.c_str(), node->name.c_str(), node->ip.c_str());
    }

    // Configure postgresql.conf.user
    std::vector<std::pair<std::string, std::string>> config_user_items = {
        {"fn_shared_buffers", "100MB"},
        {"shared_buffers", "512MB"},
        {"max_wal_size", "16MB"},
        {"max_connections", "1000"},
        {"max_pool_size", "1024"},
        {"wal_keep_segments", "1024"},
        {"tcp_keepalives_idle", "60"},
        {"tcp_keepalives_interval", "10"},
        {"tcp_keepalives_count", "10"},
        {"wal_level", "logical"},
        {"max_standby_archive_delay", "1800"},
        {"max_standby_streaming_delay", "1800"},
        {"archive_mode", "always"},
        {"archive_timeout", "1800"},
        {"archive_command", "ulimit -c 0 && echo 0"},
        {"bgwriter_lru_maxpages", "500"},
        {"bgwriter_lru_multiplier", "2.0"},
        {"bgwriter_delay", "10ms"},
        {"checkpoint_timeout", "10min"},
        {"checkpoint_completion_target", "0.93"},
        {"checkpoint_warning", "30s"},
        {"log_destination", "csvlog"},
        {"log_truncate_on_rotation", "on"},
        {"logging_collector", "on"},
        {"log_rotation_age", "120"},
        {"log_rotation_size", "1024MB"},
        {"track_activity_query_size", "4096"},
        {"log_min_duration_statement", "10000"},
        {"log_checkpoints", "on"},
        {"log_connections", "on"},
        {"log_disconnections", "on"},
        {"log_duration", "off"},
        {"log_line_prefix", "%m %a %r %d %u %p %x"},
        {"log_filename", "postgresql-%A-%H.log"},
        {"log_timezone", "PRC"},
        {"log_statement", "ddl"},
        {"log_directory", "pg_log"},
        {"hot_standby", "on"},
        {"synchronous_commit", "1"}, 
        {"synchronous_standby_names", ""},
        {"track_counts", "on"},
        {"autovacuum", "on"},
        {"autovacuum_max_workers", "2"},
        {"autovacuum_vacuum_cost_delay", "20ms"},
        {"autovacuum_vacuum_scale_factor", "0.0002"},
        {"autovacuum_analyze_scale_factor", "0.0001"},
        {"vacuum_cost_delay", "0"},
        {"vacuum_cost_limit", "10000"},
        {"log_lock_waits", "on"},
        {"deadlock_timeout", "1s"},
        {"datestyle", "iso, ymd"},
        {"timezone", "PRC"},
        {"max_replication_slots", "64"},
        {"max_logical_replication_workers", "64"},
        {"max_sync_workers_per_subscription", "4"},
        {"max_parallel_workers", "1024"},
        {"base_backup_limit", "1000"},
        {"vacuum_delta", "2000"},
        {"max_files_per_process", "4096"},
        {"enable_null_string", "off"},
        {"max_stack_depth", "2MB"},
        {"replication_level", "0"},
        {"wal_sender_timeout", "30min"},
        {"wal_receiver_timeout", "30min"},
        {"pooler_scale_factor", "64"},
        {"lock_timeout", "30s"}
    };

    // 生成配置文件内容
    std::ostringstream config_stream;
    for (const auto& item : config_user_items) {
        if (item.first == "log_filename" || 
            item.first == "archive_command" || 
            item.first == "log_line_prefix" ||
            item.first == "synchronous_standby_names" ||
            item.first == "datestyle") {
            config_stream << item.first << " = '" << item.second << "'\n";
        } else {
            config_stream << item.first << " = " << item.second << "\n";
        }
    }
    std::string config_content = config_stream.str();

    // 将配置内容写入 postgresql.conf.user 文件
    std::string config_file_path = "./postgresql.conf.user";
    std::ofstream config_file(config_file_path);
    if (!config_file.is_open()) {
        LOG_ERROR_FMT("Failed to open %s for writing", config_file_path.c_str());
        return -1;
    }
    config_file << config_content;
    config_file.close();

    // Transfer package
    ret = remote_scp_file(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        "./postgresql.conf.user",
        node->data_path + "/postgresql.conf.user");
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to transfer package to node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully transferred package to node %s (%s)", 
            node->name.c_str(), node->ip.c_str());


    // Set pg_hba.conf
    if (configure_pg_hba(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure pg_hba.conf");
        return -1;
    }
    return 0;
}

// Configure cn slave node
int configure_cn_slave_node(NodeInfo *node, OpentenbaseConfig *install) {
    
    LOG_INFO_FMT("Configuring PostgreSQL for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    std::string base_command;
    int ret;
    
    // find master node
    NodeInfo master_node;
    for (size_t i = 0; i < install->nodes.size(); ++i) {
        if ((is_slave_gtm(node->type) && is_master_gtm(install->nodes[i].type)) 
        || (install->nodes[i].name == node->name && is_master_node(install->nodes[i].type)))
        {
            master_node.ip = install->nodes[i].ip;
            master_node.port = install->nodes[i].port;
        }
    }

    // Configure postgresql.conf.user
    std::string primary_conninfo = "host = " + master_node.ip + " port = " + std::to_string(master_node.port) + " user = opentenbase application_name = " + master_node.ip + ":" + std::to_string(master_node.port);
    std::vector<std::pair<std::string, std::string>> recovery_conf_items = {
        {"recovery_target_timeline", "latest"},
        {"standby_mode", "on"},
        {"primary_conninfo", primary_conninfo}
    };

    // 生成配置文件内容
    std::ostringstream config_stream;
    for (const auto& item : recovery_conf_items) {
        if (item.first == "primary_conninfo") {
            config_stream << item.first << " = '" << item.second << "'\n";
        } else {
            config_stream << item.first << " = " << item.second << "\n";
        }
    }
    std::string config_content = config_stream.str();

    // 将配置内容写入 recovery.conf 文件
    std::string config_file_path = "./recovery.conf";
    std::ofstream config_file(config_file_path);
    if (!config_file.is_open()) {
        LOG_ERROR_FMT("Failed to open %s for writing", config_file_path.c_str());
        return -1;
    }
    config_file << config_content;
    config_file.close();

    // Transfer package
    ret = remote_scp_file(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        "./recovery.conf",
        node->data_path + "/recovery.conf");
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to transfer package to node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully transferred package to node %s (%s)", 
            node->name.c_str(), node->ip.c_str());


    // Configure postgresql.conf
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";
    std::vector<std::pair<std::string, std::string>> config_items = {
        {"listen_addresses", "\\'*\\'"},
        {"port", std::to_string(node->port)},
        {"pooler_port", std::to_string(node->pooler_port)},
        {"forward_port", std::to_string(node->forward_port)}
    };
    for (const auto& item : config_items) {
        command = base_command + "confmod"
                  " -a mod"
                  " -d " + node->data_path +
                  " -f postgresql.conf"
                  " -g " + item.first +
                  " -v " + item.second;

        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to configure %s on node %s (%s): %s", 
                    item.first.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully configured %s on node %s (%s)", 
                item.first.c_str(), node->name.c_str(), node->ip.c_str());
    }

    // Configure postgresql.conf.user
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";
    std::vector<std::pair<std::string, std::string>> config_user_items = {
        {"synchronous_commit", "1"}, 
        {"pgxc_main_cluster_name", Constants::MAIN_CLUSTER_NAME}, 
        {"pgxc_cluster_name", Constants::MAIN_CLUSTER_NAME}
    };

    for (const auto& item : config_user_items) {
        command = base_command + "confmod"
                  " -a mod"
                  " -d " + node->data_path +
                  " -f postgresql.conf"
                  " -g " + item.first +
                  " -v " + item.second;

        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to configure %s on node %s (%s): %s", 
                    item.first.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully configured %s on node %s (%s)", 
                item.first.c_str(), node->name.c_str(), node->ip.c_str());
    }

    return 0;
}

// 新增的函数：设置集中式 GUC
void set_centralized_guc(std::vector<std::pair<std::string, std::string>>& config_user_items, const std::string& version) {
    if (is_fusion_version(version)) {
        config_user_items.emplace_back("allow_dml_on_datanode", "on");
        config_user_items.emplace_back("is_centralized_mode", "on");
    } else {
        config_user_items.emplace_back("set_global_snapshot", "off");
        config_user_items.emplace_back("allow_dml_on_datanode", "on");
        config_user_items.emplace_back("use_local_sequence", "on");
    }
}

// Configure PostgreSQL
int configure_dn_node(NodeInfo *node, OpentenbaseConfig *install) {
    
    LOG_INFO_FMT("Configuring PostgreSQL for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    std::string base_command;
    std::string include_if_exists;
    std::string etcd_conn;
    int ret;

    // Build base command string
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";

    include_if_exists = "\\'"+node->data_path+"/postgresql.conf.user\\'";

    // Configure postgresql.conf
    std::vector<std::pair<std::string, std::string>> config_items = {
        {"listen_addresses", "\\'*\\'"},
        {"port", std::to_string(node->port)},
        {"pooler_port", std::to_string(node->pooler_port)},
        {"forward_port", std::to_string(node->forward_port)},
        {"shared_preload_libraries", "\\'pageinspect,pg_stat_error\\'"},
        {"pg_stat_statements.max", "1000"},
        {"pg_stat_statements.track", "all"},
        {"pg_stat_log.max", "1024"},
        {"pg_stat_log.track", "all"},
        {"pg_stat_log.track_utility", "true"},
        {"log_min_duration_statement", "10000"},
        {"include_if_exists", include_if_exists}
    };

    for (const auto& item : config_items) {
        command = base_command + "confmod"
                  " -a mod"
                  " -d " + node->data_path +
                  " -f postgresql.conf"
                  " -g " + item.first +
                  " -v " + item.second;

        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to configure %s on node %s (%s): %s", 
                    item.first.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully configured %s on node %s (%s)", 
                item.first.c_str(), node->name.c_str(), node->ip.c_str());
    }

    // Configure postgresql.conf.user
    std::vector<std::pair<std::string, std::string>> config_user_items = {
        {"fn_shared_buffers", "100MB"},
        {"shared_buffers", "512MB"},
        {"max_wal_size", "16MB"},
        {"max_connections", "1000"},
        {"max_pool_size", "1024"},
        {"wal_keep_segments", "1024"},
        {"tcp_keepalives_idle", "60"},
        {"tcp_keepalives_interval", "10"},
        {"tcp_keepalives_count", "10"},
        {"wal_level", "logical"},
        {"max_standby_archive_delay", "1800"},
        {"max_standby_streaming_delay", "1800"},
        {"archive_mode", "always"},
        {"archive_timeout", "1800"},
        {"archive_command", "ulimit -c 0 && echo 0"},
        {"bgwriter_lru_maxpages", "500"},
        {"bgwriter_lru_multiplier", "2.0"},
        {"bgwriter_delay", "10ms"},
        {"checkpoint_timeout", "10min"},
        {"checkpoint_completion_target", "0.93"},
        {"checkpoint_warning", "30s"},
        {"log_destination", "csvlog"},
        {"log_truncate_on_rotation", "on"},
        {"logging_collector", "on"},
        {"log_rotation_age", "120"},
        {"log_rotation_size", "1024MB"},
        {"track_activity_query_size", "4096"},
        {"log_min_duration_statement", "10000"},
        {"log_checkpoints", "on"},
        {"log_connections", "on"},
        {"log_disconnections", "on"},
        {"log_duration", "off"},
        {"log_line_prefix", "%m %a %r %d %u %p %x"},
        {"log_filename", "postgresql-%A-%H.log"},
        {"log_timezone", "PRC"},
        {"log_statement", "ddl"},
        {"log_directory", "pg_log"},
        {"hot_standby", "on"},
        {"synchronous_commit", "1"}, 
        {"synchronous_standby_names", ""},
        {"track_counts", "on"},
        {"autovacuum", "on"},
        {"autovacuum_max_workers", "2"},
        {"autovacuum_vacuum_cost_delay", "20ms"},
        {"autovacuum_vacuum_scale_factor", "0.0002"},
        {"autovacuum_analyze_scale_factor", "0.0001"},
        {"vacuum_cost_delay", "0"},
        {"vacuum_cost_limit", "10000"},
        {"log_lock_waits", "on"},
        {"deadlock_timeout", "1s"},
        {"datestyle", "iso, ymd"},
        {"timezone", "PRC"},
        {"max_replication_slots", "64"},
        {"max_logical_replication_workers", "64"},
        {"max_sync_workers_per_subscription", "4"},
        {"max_parallel_workers", "1024"},
        {"base_backup_limit", "1000"},
        {"vacuum_delta", "2000"},
        {"max_files_per_process", "4096"},
        {"enable_null_string", "off"},
        {"max_stack_depth", "2MB"},
        {"replication_level", "0"},
        {"wal_sender_timeout", "30min"},
        {"wal_receiver_timeout", "30min"},
        {"pooler_scale_factor", "64"},
        {"lock_timeout", "30s"}
    };

    // 设置集中式 GUC
    if (is_Centralized_instance(install->instance.instance_type)) {
        set_centralized_guc(config_user_items, install->instance.version);
    }

    // 生成配置文件内容
    std::ostringstream config_stream;
    for (const auto& item : config_user_items) {
        if (item.first == "log_filename" || 
            item.first == "archive_command" || 
            item.first == "log_line_prefix" ||
            item.first == "synchronous_standby_names" ||
            item.first == "datestyle") {
            config_stream << item.first << " = '" << item.second << "'\n";
        } else {
            config_stream << item.first << " = " << item.second << "\n";
        }
    }
    std::string config_content = config_stream.str();

    // 将配置内容写入 postgresql.conf.user 文件
    std::string config_file_path = "./postgresql.conf.user";
    std::ofstream config_file(config_file_path);
    if (!config_file.is_open()) {
        LOG_ERROR_FMT("Failed to open %s for writing", config_file_path.c_str());
        return -1;
    }
    config_file << config_content;
    config_file.close();

    // Transfer package
    ret = remote_scp_file(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        "./postgresql.conf.user",
        node->data_path + "/postgresql.conf.user");
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to transfer package to node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully transferred package to node %s (%s)", 
            node->name.c_str(), node->ip.c_str());


    // Set pg_hba.conf
    if (configure_pg_hba(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure pg_hba.conf");
        return -1;
    }
    return 0;
}

// Configure dn slave node
int configure_dn_slave_node(NodeInfo *node, OpentenbaseConfig *install) {
    
    LOG_INFO_FMT("Configuring PostgreSQL for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    std::string base_command;
    int ret;
    
    // find master node
    NodeInfo master_node;
    for (size_t i = 0; i < install->nodes.size(); ++i) {
        if ((is_slave_gtm(node->type) && is_master_gtm(install->nodes[i].type)) 
        || (install->nodes[i].name == node->name && is_master_node(install->nodes[i].type)))
        {
            master_node.ip = install->nodes[i].ip;
            master_node.port = install->nodes[i].port;
        }
    }

    // Configure postgresql.conf.user
    std::string primary_conninfo = "host = " + master_node.ip + " port = " + std::to_string(master_node.port) + " user = opentenbase application_name = " + master_node.ip + ":" + std::to_string(master_node.port);
    std::vector<std::pair<std::string, std::string>> recovery_conf_items = {
        {"recovery_target_timeline", "latest"},
        {"standby_mode", "on"},
        {"primary_conninfo", primary_conninfo}
    };

    // 生成配置文件内容
    std::ostringstream config_stream;
    for (const auto& item : recovery_conf_items) {
        if (item.first == "primary_conninfo") {
            config_stream << item.first << " = '" << item.second << "'\n";
        } else {
            config_stream << item.first << " = " << item.second << "\n";
        }
    }
    std::string config_content = config_stream.str();

    // 将配置内容写入 recovery.conf 文件
    std::string config_file_path = "./recovery.conf";
    std::ofstream config_file(config_file_path);
    if (!config_file.is_open()) {
        LOG_ERROR_FMT("Failed to open %s for writing", config_file_path.c_str());
        return -1;
    }
    config_file << config_content;
    config_file.close();

    // Transfer package
    ret = remote_scp_file(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        "./recovery.conf",
        node->data_path + "/recovery.conf");
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to transfer package to node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully transferred package to node %s (%s)", 
            node->name.c_str(), node->ip.c_str());


    // Configure postgresql.conf
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";
    std::vector<std::pair<std::string, std::string>> config_items = {
        {"listen_addresses", "\\'*\\'"},
        {"port", std::to_string(node->port)},
        {"pooler_port", std::to_string(node->pooler_port)},
        {"forward_port", std::to_string(node->forward_port)}
    };
    for (const auto& item : config_items) {
        command = base_command + "confmod"
                  " -a mod"
                  " -d " + node->data_path +
                  " -f postgresql.conf"
                  " -g " + item.first +
                  " -v " + item.second;

        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to configure %s on node %s (%s): %s", 
                    item.first.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully configured %s on node %s (%s)", 
                item.first.c_str(), node->name.c_str(), node->ip.c_str());
    }

    // Configure postgresql.conf.user
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";
    std::vector<std::pair<std::string, std::string>> config_user_items = {
        {"synchronous_commit", "1"}, 
        {"pgxc_main_cluster_name", Constants::MAIN_CLUSTER_NAME},
        {"pgxc_cluster_name", Constants::MAIN_CLUSTER_NAME}
    };

    for (const auto& item : config_user_items) {
        command = base_command + "confmod"
                  " -a mod"
                  " -d " + node->data_path +
                  " -f postgresql.conf"
                  " -g " + item.first +
                  " -v " + item.second;

        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to configure %s on node %s (%s): %s", 
                    item.first.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully configured %s on node %s (%s)", 
                item.first.c_str(), node->name.c_str(), node->ip.c_str());
    }

    return 0;
}

// 配置 GTM 节点的函数
int configure_gtm_node(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Configuring GTM for node %s (%s)", 
                 node->name.c_str(), node->ip.c_str());
    std::string result;
    int ret;
 
    // Step 1: 创建 gtm.conf 文件（如果不存在）
    std::string touch_command = "mkdir -p " + node->data_path + " && touch " + node->data_path + "/gtm.conf";
    LOG_DEBUG_FMT("Executing command to touch gtm.conf: %s", touch_command.c_str());
    ret = execute_command(node->ip,
                          install->server.ssh_port,
                          install->server.ssh_user,
                          install->server.ssh_password,
                          touch_command,
                          result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to touch file gtm.conf on node %s (%s): %s", 
                      node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully touched file gtm.conf on node %s (%s)", 
                 node->name.c_str(), node->ip.c_str());
 
    // 定义配置项
    std::vector<std::pair<std::string, std::string>> config_items = {
        {"listen_addresses", "'*'"},
        {"port", std::to_string(node->port)},
        {"nodename", "'" + node->name + "'"},
        {"max_wal_sender", "5"},
        {"checkpoint_interval", "5"},
        {"startup", "'ACT'"}
    };
 
    // Step 2: 使用heredoc方式写入配置文件，避免引号问题
    std::ostringstream config_stream;
    config_stream << "cat > " << node->data_path << "/gtm.conf << 'EOF'\n";
    for(const auto& item : config_items){
        config_stream << item.first << " = " << item.second << "\n";
    }
    config_stream << "EOF\n";
    std::string config_command = config_stream.str();
 
    LOG_DEBUG_FMT("Writing configurations to gtm.conf: %s", config_command.c_str());
    ret = execute_command(node->ip,
                          install->server.ssh_port,
                          install->server.ssh_user,
                          install->server.ssh_password,
                          config_command,
                          result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to write configurations to gtm.conf on node %s (%s): %s", 
                      node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully wrote configurations to gtm.conf on node %s (%s)", 
                 node->name.c_str(), node->ip.c_str());
 
    // Step 3: 设置 pg_hba.conf
    if (configure_pg_hba(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure pg_hba.conf");
        return -1;
    }
 
    return 0;
}

// 配置 GTM 备节点的函数
int configure_gtm_slave_node(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Configuring GTM for node %s (%s)", 
                 node->name.c_str(), node->ip.c_str());
    std::string result;
    std::string base_command;
    int ret;

    // Step 1: 创建 gtm.conf 文件（如果不存在）
    std::string touch_command = base_command + "touch " + node->data_path + "/gtm.conf";
    LOG_DEBUG_FMT("Executing command to touch gtm.conf: %s", touch_command.c_str());
    ret = execute_command(node->ip,
                          install->server.ssh_port,
                          install->server.ssh_user,
                          install->server.ssh_password,
                          touch_command,
                          result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to touch file gtm.conf on node %s (%s): %s", 
                      node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully touched file gtm.conf on node %s (%s)", 
                 node->name.c_str(), node->ip.c_str());

    // 构建基础命令字符串
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + 
                   node->install_path + "/bin/";

    // 定义配置项
    NodeInfo master_node;
    for (size_t i = 0; i < install->nodes.size(); ++i) {
        if ((is_slave_gtm(node->type) && is_master_gtm(install->nodes[i].type)) 
        || (install->nodes[i].name == node->name && is_master_node(install->nodes[i].type)))
        {
            master_node.ip = install->nodes[i].ip;
            master_node.port = install->nodes[i].port;
        }
    }
    std::vector<std::pair<std::string, std::string>> config_items = {
        {"listen_addresses", "\\'*\\'"},
        {"port", std::to_string(node->port)},
        {"nodename", "\\'" + node->name + "\\'"},
        {"max_wal_sender", "5"},
        {"checkpoint_interval", "5"},
        {"startup", "\\'STANDBY\\'"},
        {"active_host", "\\'" + master_node.ip + "\\'"},
        {"active_port", std::to_string(master_node.port)},
        {"application_name", "\\'" + master_node.ip + ":" + std::to_string(master_node.port) + "\\'"}
    };

    // Step 2: 构建 sed 命令以追加配置项
    std::ostringstream sed_commands;
    for(size_t i = 0; i < config_items.size(); ++i){
        std::string key = config_items[i].first;
        std::string value = config_items[i].second;

        // 转义单引号
        std::string escaped_value;
        for(char c : value){
            if(c == '\'') escaped_value += "\\'";
            else escaped_value += c;
        }

        // 添加每个配置项的追加命令
        sed_commands << "a\\" << std::endl
                     << " " << key << " = " << escaped_value;

        // 如果不是最后一个配置项，添加换行符以分隔命令
        if(i != config_items.size() -1){
            sed_commands << "\\";
        }
    }

    // 移除最后一个分号（如果有）
    std::string sed_command = sed_commands.str();
    if(!sed_command.empty() && sed_command.back() == '\\'){
        sed_command.pop_back();
    }

    // 构建完整的远程 sed 命令
    std::string full_sed_command = base_command + "sed -i \"" + sed_command + "\" " + node->data_path + "/gtm.conf";

    LOG_DEBUG_FMT("Executing command to append configurations to gtm.conf: %s", full_sed_command.c_str());
    ret = execute_command(node->ip,
                          install->server.ssh_port,
                          install->server.ssh_user,
                          install->server.ssh_password,
                          full_sed_command,
                          result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to append configurations to gtm.conf on node %s (%s): %s", 
                      node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully appended configurations to gtm.conf on node %s (%s)", 
                 node->name.c_str(), node->ip.c_str());

    // Step 3: 设置 pg_hba.conf
    if (configure_pg_hba(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure pg_hba.conf");
        return -1;
    }

    return 0;
}

// Configure gtm node
int configure_gtm_slave_node1(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Configuring PostgreSQL for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    std::string base_command;
    std::string etcd_conn;
    int ret;


    // find master node
    NodeInfo master_node;
    for (size_t i = 0; i < install->nodes.size(); ++i) {
        if ((is_slave_gtm(node->type) && is_master_gtm(install->nodes[i].type)) 
        || (install->nodes[i].name == node->name && is_master_node(install->nodes[i].type)))
        {
            master_node.ip = install->nodes[i].ip;
            master_node.port = install->nodes[i].port;
        }
    }

    // Build base command string
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";


    //etcd_conn = "\\'"+install->meta.etcd_server+"\\'";
    // Configure gtm.conf
    std::vector<std::pair<std::string, std::string>> config_items = {
        {"listen_addresses", "\\'*\\'"},
        {"port", std::to_string(node->port)},
        {"nodename", "\\'" + node->name + "\\'"},
        {"max_wal_sender", "5"},
        {"checkpoint_interval", "5"},
        {"startup", "\\'STANDBY\\'"},
        {"active_host", "\\'" + master_node.ip + "\\'"},
        {"active_port", std::to_string(master_node.port)},
        {"application_name", "\\'" + master_node.ip + ":" + std::to_string(master_node.port) + "\\'"}
    };

    for (const auto& item : config_items) {
        command = base_command + "confmod"
                  " -a mod"
                  " -d " + node->data_path +
                  " -f gtm.conf"
                  " -g " + item.first +
                  " -v " + item.second;

        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to configure %s on node %s (%s): %s", 
                    item.first.c_str(), node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully configured %s on node %s (%s)", 
                item.first.c_str(), node->name.c_str(), node->ip.c_str());
    }

    // Set pg_hba.conf
    if (configure_pg_hba(node, install) != 0) {
        LOG_ERROR_FMT("Failed to configure pg_hba.conf");
        return -1;
    }
    return 0;
}

// Set pg_hba.conf
int configure_pg_hba(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Configuring pg_hba.conf for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    int ret;

    // Build base command string
    command = "echo -e '";
    
    // Add access rules for each node
    for (const auto& node_info : install->nodes) {
        command += "host    replication    all   " + node_info.ip + "/32    trust\n";
        command += "host    all            all   " + node_info.ip + "/32    trust\n";
    }
    
    // Add specific rules based on node type
    if (is_dn_node(node->type)) {  // datanode
        //command += "host    all    opentenbase    all    reject\n";
        command += "host    all    opentenbase    all    trust\n";
    } else if (is_cn_node(node->type)) {  // coordinator
        // All other IPs must input password to connect CN
        // command += "host    all    all    0.0.0.0/0    md5\n";
        // command += "host    all    all    ::0/0        md5\n";
        command += "host    all    all    0.0.0.0/0    trust\n";
        command += "host    all    all    ::0/0        trust\n";
    }
    
    command += "' >> " + node->data_path + "/pg_hba.conf";

    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    ret = execute_command(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        command,
        result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to configure pg_hba.conf on node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully configured pg_hba.conf on node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    return 0;
}

// Start node
int 
start_node(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Starting node %s (%s)", node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    std::string base_command;
    int ret;
    // Build base command string
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib  && export PATH=" + node->install_path + "/bin:${PATH} && " + node->install_path + "/bin/";

    if (is_cn_node(node->type)) {
        command = base_command + "pg_ctl start -Z coordinator -D " + node->data_path + " -o -i -w -t 30 > "+ node->data_path + "/pg_ctl_start.log 2>&1";
    } else if (is_dn_node(node->type)) {
        command = base_command + "pg_ctl start -Z datanode -D " + node->data_path + " -o -i -w -t 30 > "+ node->data_path + "/pg_ctl_start.log 2>&1";
    } else if (is_gtm_node(node->type)) {
        command = base_command + "gtm_ctl start -Z gtm -D " + node->data_path + " > "+ node->data_path + "/gtm_ctl_start.log 2>&1";
    } else {
        LOG_ERROR_FMT("Invalid node type,failed to start node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }

    LOG_DEBUG_FMT("Executing command: %s", command.c_str());
    // Execute start command
    ret = execute_command(node->ip,
        install->server.ssh_port,
        install->server.ssh_user,
        install->server.ssh_password,
        command,
        result);
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to start node %s (%s): %s", 
                node->name.c_str(), node->ip.c_str(), result.c_str());
        return -1;
    }
    LOG_INFO_FMT("Successfully started node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    return 0;
}

// Stop node (retry 3 times if failed)
int 
stop_node(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Stopping node %s (%s)", node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    std::string base_command;
    int ret;
    const int MAX_RETRIES = 3;
    const int RETRY_DELAY_MS = 1000;  // 1 second delay between retries

    // Build base command string
    base_command = "export LD_LIBRARY_PATH=" + node->install_path + "/lib && " + node->install_path + "/bin/";

    // Build stop command string
    if (is_gtm_node(node->type))
    {
        command = base_command + "gtm_ctl stop -Z gtm -D " + node->data_path + " -w -t 30 > "+ node->data_path + "/gtm_ctl_stop.log 2>&1";
    } else {
        command = base_command + "pg_ctl stop -m fast -D " + node->data_path + " -w -t 30 > "+ node->data_path + "/pg_ctl_stop.log 2>&1";
    }
    
    // Try up to MAX_RETRIES times
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        LOG_DEBUG_FMT("Executing stop command (attempt %d/%d): %s", 
            attempt, MAX_RETRIES, command.c_str());
        
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);

        if (ret == 0) {
            LOG_INFO_FMT("Successfully stopped node %s (%s) on attempt %d", 
                node->name.c_str(), node->ip.c_str(), attempt);
            return 0;
        }

        LOG_WARN_FMT("Failed to stop node %s (%s) on attempt %d: %s", 
            node->name.c_str(), node->ip.c_str(), attempt, result.c_str());

        if (attempt < MAX_RETRIES) {
            LOG_INFO_FMT("Retrying in %d ms...", RETRY_DELAY_MS);
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_DELAY_MS));
        }
    }

    LOG_ERROR_FMT("Failed to stop node %s (%s) after %d attempts", 
        node->name.c_str(), node->ip.c_str(), MAX_RETRIES);
    return -1;
}

// Transfer and extract package
int 
transfer_and_extract_package(NodeInfo *node, OpentenbaseConfig *install) {
    LOG_INFO_FMT("Transferring and extracting package for node %s (%s)", 
            node->name.c_str(), node->ip.c_str());
    std::string command;
    std::string result;
    int ret;

    try {
        // Transfer package
        ret = excute_cp_file(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            install->instance.package_path,
            node->install_path + "/" + install->instance.package_name);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to transfer package to node %s (%s): %s", 
                    node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
        LOG_INFO_FMT("Successfully transferred package to node %s (%s)", 
                node->name.c_str(), node->ip.c_str());

        // Extract package
        command = "cd " + node->install_path + " && tar xzf " + install->instance.package_name;
        LOG_DEBUG_FMT("Executing command: %s", command.c_str());
        ret = execute_command(node->ip,
            install->server.ssh_port,
            install->server.ssh_user,
            install->server.ssh_password,
            command,
            result);
        if (ret != 0) {
            LOG_ERROR_FMT("Failed to extract package on node %s (%s): %s", 
                    node->name.c_str(), node->ip.c_str(), result.c_str());
            return -1;
        }
    }
    catch (const std::system_error& e) {
        std::cerr << "捕获到 system_error: " << e.what() << '\n';
        std::cerr << "错误码: " << e.code() << '\n';
        std::cerr << "错误码值: " << e.code().value() << '\n';
        std::cerr << "错误类别: " << e.code().category().name() << '\n';
    }
    
    LOG_INFO_FMT("Successfully extracted package on node %s (%s)", 
            node->name.c_str(), node->ip.c_str());

    return 0;
} 


// 创建数据库连接
pqxx::connection* createConnection(const std::string& conninfo) {
    try {
        pqxx::connection* conn = new pqxx::connection(conninfo);
        if (conn->is_open()) {
            LOG_DEBUG_FMT("Successfully connected to database: %s ",conn->dbname());
            return conn;
        } else {
            LOG_ERROR_FMT("Unable to connect to database: %s ",conn->dbname());
            throw std::runtime_error("Database connection failed");
        }
    } catch (const std::exception& e) {
        LOG_ERROR_FMT("Database connection error: %s ",e.what());
        throw;
    }
}

// 销毁数据库连接
void destroyConnection(pqxx::connection* conn) {
    if (conn && conn->is_open()) {
        conn->close();
    }
    delete conn;
    LOG_DEBUG_FMT("The database connection has been closed. ");
}

// 执行查询SQL并返回结果
pqxx::result executeQuery(pqxx::connection& conn, const std::string& sql) {
    try {
        pqxx::nontransaction n(conn);
        pqxx::result r = n.exec(sql);
        LOG_DEBUG_FMT("Query execution successful, return %zu records ",r.size());
        return r;
    } catch (const std::exception& e) {
        LOG_ERROR_FMT("Query execution error: %s ",e.what());
        throw;
    }
}

// 执行DDL语句（创建表、修改表等）
void executeDDL(pqxx::connection& conn, const std::string& ddl) {
    try {
        pqxx::work w(conn);
        w.exec(ddl);
        w.commit();
        LOG_DEBUG_FMT("DDL execution successful.");
    } catch (const std::exception& e) {
        LOG_ERROR_FMT("DDL execution error: %s ",e.what());
        throw;
    }
}
