#include "command.h"
#include "../log/log.h"
#include "../config/config.h"
#include "../utils/utils.h"
#include "../ssh/remote_ssh.h"
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <string>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <array>
#include <regex>

// 解析逗号分隔的字符串列表
std::vector<std::string> parse_comma_separated_list(const char* str) {
    std::vector<std::string> items;
    if (!str || str[0] == '\0') {
        return items;
    }

    std::string input(str);
    std::stringstream ss(input);
    std::string item;

    while (std::getline(ss, item, ',')) {
        // 去除前后空格
        size_t start = item.find_first_not_of(" \t\n\r");
        size_t end = item.find_last_not_of(" \t\n\r");
        if (start != std::string::npos && end != std::string::npos) {
            items.push_back(item.substr(start, end - start + 1));
        }
    }

    return items;
}

// 初始化安装命令
void init_install_command(CLI::App& app, CommandLineArgs& args) {
    auto install_cmd = app.add_subcommand("install", "Install a new instance");
    install_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
}

// 初始化删除命令
void init_delete_command(CLI::App& app, CommandLineArgs& args) {
    auto delete_cmd = app.add_subcommand("delete", "Delete an existing instance");
    delete_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
}

// 初始化启动命令
void init_start_command(CLI::App& app, CommandLineArgs& args) {
    auto start_cmd = app.add_subcommand("start", "Start a Instance");

    // 方式1:配置文件
    start_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
    start_cmd->add_option("-n,--node", args.op_node, 
                           "Node to start. "
                           "A certain type of node, Or a certain node.such as cn-master/cn-slave/dn-master/dn-slave. "
                           "The available options are: cn-master, cn-slave, dn-master, dn-slave, cn0001, ip:port");
}

// 初始化停止命令
// 配置文件传参方式 ./opentenbase_ctl start -c config/config.ini
void init_stop_command(CLI::App& app, CommandLineArgs& args) {
    auto stop_cmd = app.add_subcommand("stop", "Stop a instance");

    // 方式1:配置文件
    stop_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
    stop_cmd->add_option("-n,--node", args.op_node, 
                           "Node to stop. "
                           "A certain type of node, Or a certain node.such as cn-master/cn-slave/dn-master/dn-slave. "
                           "The available options are: cn-master, cn-slave, dn-master, dn-slave, cn0001, ip:port");
}

// 查看状态命令
void init_status_command(CLI::App& app, CommandLineArgs& args) {
    auto status_cmd = app.add_subcommand("status", "Show instance status");
    status_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
    status_cmd->add_option("-n,--node", args.op_node, 
                           "Node to  show status. "
                           "A certain type of node, Or a certain node.such as cn-master/cn-slave/dn-master/dn-slave. "
                           "The available options are: cn-master, cn-slave, dn-master, dn-slave, cn0001, ip:port");
}

// 分发包命令
void init_scp_command(CLI::App& app, CommandLineArgs& args) {
    auto status_cmd = app.add_subcommand("scp", "scp files to cluster nodes");
    status_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
    status_cmd->add_option("-n,--node", args.op_node, 
                           "Node to scp file. "
                           "A certain type of node, Or a certain node.such as cn-master/cn-slave/dn-master/dn-slave. "
                           "The available options are: cn-master, cn-slave, dn-master, dn-slave, cn0001, ip:port");

    // 方式2:命令参数
    status_cmd->add_option("--source-file", args.source_file, "Local file path");
    status_cmd->add_option("--dest-path", args.dest_path, "Path to remote server");
}

// 执行shell命令
void init_shell_command(CLI::App& app, CommandLineArgs& args) {
    auto status_cmd = app.add_subcommand("shell", "Execute shell command");
    status_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
    status_cmd->add_option("-n,--node", args.op_node, 
                           "Node for executing cmd. "
                           "A certain type of node, Or a certain node.such as cn-master/cn-slave/dn-master/dn-slave. "
                           "The available options are: cn-master, cn-slave, dn-master, dn-slave, cn0001, ip:port");
    status_cmd->add_option("--cmd", args.shell_cmd, "Shell command");
}

// 执行shell命令
void init_sql_command(CLI::App& app, CommandLineArgs& args) {
    auto status_cmd = app.add_subcommand("sql", "Execute sql command");
    status_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
    status_cmd->add_option("-n,--node", args.op_node, 
                           "Node for executing SQL. "
                           "A certain type of node, Or a certain node.such as cn-master/cn-slave/dn-master/dn-slave. "
                           "The available options are: cn-master, cn-slave, dn-master, dn-slave, cn0001, ip:port");

    // 方式2:命令参数
    status_cmd->add_option("--sql", args.shell_cmd, "sql");
    status_cmd->add_option("-u,--user", args.user, "Username for connecting to the database");
    status_cmd->add_option("-d,--db", args.database, "The database name for executing SQL");
}

// 初始化扩容命令
void init_expand_command(CLI::App& app, CommandLineArgs& args) {
    auto expand_cmd = app.add_subcommand("expand", "Expand a Instance");
    expand_cmd->add_option("--etcd-server", args.etcd_server, "Etcd server");
    expand_cmd->add_option("--meta-id", args.meta_id, "Meta ID");
    expand_cmd->add_option("--cluster-oid", args.cluster_oid, "Cluster OID");
    expand_cmd->add_option("--instance-name", args.instance_name, "Instance name");
    expand_cmd->add_option("--package-path", args.package_path, "Package path");
    expand_cmd->add_option("--node-name", args.node_name, "Node name");
    expand_cmd->add_option("--node-ip", args.node_ip, "Node IP");
    expand_cmd->add_option("--ssh-user", args.ssh_user, "SSH user");
    expand_cmd->add_option("--ssh-password", args.ssh_password, "SSH password");
    expand_cmd->add_option("--ssh-port", args.ssh_port, "SSH port");
}

// 初始化缩容命令
void init_shrink_command(CLI::App& app, CommandLineArgs& args) {
    auto shrink_cmd = app.add_subcommand("shrink", "Shrink a Instance");
    shrink_cmd->add_option("--instance-name", args.instance_name, "Instance name");
    shrink_cmd->add_option("--package-path", args.package_path, "Package path");
    shrink_cmd->add_option("--node-name", args.node_name, "Node name");
    shrink_cmd->add_option("--node-ip", args.node_ip, "Node IP");
    shrink_cmd->add_option("--ssh-user", args.ssh_user, "SSH user");
    shrink_cmd->add_option("--ssh-password", args.ssh_password, "SSH password");
    shrink_cmd->add_option("--ssh-port", args.ssh_port, "SSH port");
}

int setEnvironmentVariableInBashrc(const std::string& var_name, const std::string& var_value) {
    // 构建 sed 删除命令，删除以 var_name= 开头的行
    std::string sed_command = "sed -i '/^" + var_name + "=.*$/d' ~/.bashrc";

    // 执行 sed 删除命令
    int ret = system(sed_command.c_str());
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to execute sed delete command, return code: %d", ret);
        return 1;
    }

    // 构建 echo 追加命令，将新的环境变量追加到 ~/.bashrc
    std::string echo_command = "echo '" + var_name + "=" + var_value + "' >> ~/.bashrc";

    // 执行 echo 追加命令
    ret = system(echo_command.c_str());
    if (ret != 0) {
        LOG_ERROR_FMT("Execution of echo append command failed, return code: %d", ret);
        return 1;
    }

    LOG_INFO_FMT("The environment variable %s has been successfully written to~/. bashrc", var_name.c_str());
    return 0;
}

std::string getEnvironmentVariableFromBashrc(const std::string& var_name) {
    std::string command = "grep '^" + var_name + "=.*$' ~/.bashrc | awk -F= '{print $2}'";

    std::array<char, 128> buffer;
    std::string result;
    std::shared_ptr<FILE> pipe(popen(command.c_str(), "r"), pclose);
    if (!pipe) {
        LOG_ERROR_FMT("Failed to popen() for ~/.bashrc");
        return "";
    }

    // 读取命令输出
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        // 去除可能的换行符
        std::string line(buffer.data());
        line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());
        result = line;
    }

    return result;
}

int setEnvironmentVariableInBashrc_old(const std::string& var_name, const std::string& var_value) {
    // 构建 sed 命令以替换或添加环境变量
    std::string command = "sed -i '/^" + var_name + "=.*$/c\\" + var_name + "=" + var_value + "\\\" ~/.bashrc || echo \"" + var_name + "=" + var_value + "\" >> ~/.bashrc";

    // 执行命令
    int ret = system(command.c_str());
    if (ret != 0) {
        LOG_ERROR_FMT("Failed to set Environment variable %s into ~/.bashrc", var_name.c_str());
        return 1;
    }

    LOG_INFO_FMT("Environment variable %s successfully written into ~/.bashrc", var_name.c_str());
    return 0;
}
