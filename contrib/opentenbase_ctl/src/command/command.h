#ifndef COMMAND_H
#define COMMAND_H

#include <string>
#include <vector>
#include "../CLI/CLI.hpp"
#include "../config/config.h"
//#include "../types/types.h"

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
    std::string shell_cmd;        // shell cmd
    std::string source_file;      // source file
    std::string dest_path;        // dest path
    std::string op_node;          // Operated nodes
    std::string sql;              // sql
    std::string database;         // The database name that psql will connect to
    std::string user;             // Username for connecting to the database
};

// 初始化命令函数
void init_install_command(CLI::App& app, CommandLineArgs& args);
void init_delete_command(CLI::App& app, CommandLineArgs& args);
void init_start_command(CLI::App& app, CommandLineArgs& args);
void init_stop_command(CLI::App& app, CommandLineArgs& args);
void init_status_command(CLI::App& app, CommandLineArgs& args);
void init_scp_command(CLI::App& app, CommandLineArgs& args);
void init_shell_command(CLI::App& app, CommandLineArgs& args);
void init_sql_command(CLI::App& app, CommandLineArgs& args);
void init_expand_command(CLI::App& app, CommandLineArgs& args);
void init_shrink_command(CLI::App& app, CommandLineArgs& args);

std::vector<std::string> parse_comma_separated_list(const char* str);
std::string getEnvironmentVariableFromBashrc(const std::string& var_name);
int setEnvironmentVariableInBashrc(const std::string& var_name, const std::string& var_value);



#endif // COMMAND_H 