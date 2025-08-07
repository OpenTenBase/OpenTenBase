#ifndef COMMAND_H
#define COMMAND_H

#include <string>
#include <vector>
#include <CLI/CLI.hpp>
#include "../config/config.h"
#include "../types/types.h"

// Initialize node directories
int init_node_directories(OpentenbaseConfig* config);

// Command line arguments structure
struct CommandLineArgs {
    std::string command;
    std::string config_file;      // Configuration file path
    std::string etcd_server;      // Etcd server address
    std::string meta_id;          // Meta service ID
    std::string instance_name;    // Instance name
    std::string package_path;     // Package path
    std::string cluster_oid;      // Cluster OID
    std::string shard_num;        // Number of shards
    std::string node_name;        // Node names (comma-separated)
    std::string node_ip;          // Node IPs (comma-separated)
    std::string role;             // Node role (master/slave)
    std::string ssh_user;         // SSH username
    std::string ssh_password;     // SSH password
    std::string ssh_port;         // SSH port
};

// 命令行处理函数
bool parse_command_line(int argc, char** argv, CommandLineArgs& args, OpentenbaseConfig& config);

// 初始化命令函数
void init_install_command(CLI::App& app, CommandLineArgs& args);
void init_delete_command(CLI::App& app, CommandLineArgs& args);
void init_start_command(CLI::App& app, CommandLineArgs& args);
void init_stop_command(CLI::App& app, CommandLineArgs& args);
void init_status_command(CLI::App& app, CommandLineArgs& args);
void init_expand_command(CLI::App& app, CommandLineArgs& args);
void init_shrink_command(CLI::App& app, CommandLineArgs& args);

#endif // COMMAND_H 