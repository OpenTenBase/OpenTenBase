#include "command.h"
#include "../log/log.h"
#include "../config/config.h"
#include "../utils/utils.h"
#include "../ssh/remote_ssh.h"
#include "../types/types.h"
#include <iostream>
#include <sstream>

// 解析逗号分隔的字符串列表
static std::vector<std::string> parse_comma_separated_list(const char* str) {
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
    auto install_cmd = app.add_subcommand("install", "Install a new Opentenbase cluster");
    install_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
}

// 初始化删除命令
void init_delete_command(CLI::App& app, CommandLineArgs& args) {
    auto delete_cmd = app.add_subcommand("delete", "Delete an existing Opentenbase cluster");
    delete_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");
}

// 初始化启动命令
void init_start_command(CLI::App& app, CommandLineArgs& args) {
    auto start_cmd = app.add_subcommand("start", "Start a Opentenbase cluster");

    // 方式1:配置文件
    start_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");

    // 方式2:命令参数
    start_cmd->add_option("--instance-name", args.instance_name, "Instance name");
    start_cmd->add_option("--package-path", args.package_path, "Package path");
    start_cmd->add_option("--node-name", args.node_name, "Node name");
    start_cmd->add_option("--node-ip", args.node_ip, "Node IP");
    start_cmd->add_option("--ssh-user", args.ssh_user, "SSH user");
    start_cmd->add_option("--ssh-password", args.ssh_password, "SSH password");
    start_cmd->add_option("--ssh-port", args.ssh_port, "SSH port");
}

// 初始化停止命令
// 配置文件传参方式 ./opentenbase_ctl start -c config/config.ini
void init_stop_command(CLI::App& app, CommandLineArgs& args) {
    auto stop_cmd = app.add_subcommand("stop", "Stop a Opentenbase cluster");

    // 方式1:配置文件
    stop_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");

    // 方式2:命令参数
    stop_cmd->add_option("--instance-name", args.instance_name, "Instance name");
    stop_cmd->add_option("--package-path", args.package_path, "Package path");
    stop_cmd->add_option("--node-name", args.node_name, "Node name");
    stop_cmd->add_option("--node-ip", args.node_ip, "Node IP");
    stop_cmd->add_option("--ssh-user", args.ssh_user, "SSH user");
    stop_cmd->add_option("--ssh-password", args.ssh_password, "SSH password");
    stop_cmd->add_option("--ssh-port", args.ssh_port, "SSH port");
}

// 查看状态命令
void init_status_command(CLI::App& app, CommandLineArgs& args) {
    auto status_cmd = app.add_subcommand("status", "Show Opentenbase cluster status");
    status_cmd->add_option("-c,--config", args.config_file, "Path to configuration file");

    // 方式2:命令参数
    status_cmd->add_option("--instance-name", args.instance_name, "Instance name");
    status_cmd->add_option("--package-path", args.package_path, "Package path");
    status_cmd->add_option("--node-name", args.node_name, "Node name");
    status_cmd->add_option("--node-ip", args.node_ip, "Node IP");
    status_cmd->add_option("--ssh-user", args.ssh_user, "SSH user");
    status_cmd->add_option("--ssh-password", args.ssh_password, "SSH password");
    status_cmd->add_option("--ssh-port", args.ssh_port, "SSH port");
}

// 初始化扩容命令
void init_expand_command(CLI::App& app, CommandLineArgs& args) {
    auto expand_cmd = app.add_subcommand("expand", "Expand a Opentenbase cluster");
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
    auto shrink_cmd = app.add_subcommand("shrink", "Shrink a Opentenbase cluster");
    shrink_cmd->add_option("--instance-name", args.instance_name, "Instance name");
    shrink_cmd->add_option("--package-path", args.package_path, "Package path");
    shrink_cmd->add_option("--node-name", args.node_name, "Node name");
    shrink_cmd->add_option("--node-ip", args.node_ip, "Node IP");
    shrink_cmd->add_option("--ssh-user", args.ssh_user, "SSH user");
    shrink_cmd->add_option("--ssh-password", args.ssh_password, "SSH password");
    shrink_cmd->add_option("--ssh-port", args.ssh_port, "SSH port");
}

// Initialize node directories
int
init_node_directories(OpentenbaseConfig& config) {
    LOG_INFO_FMT("Initializing node directories");
    std::string home_dir;
    int ret;

    // Initialize directories for each node
    for (auto& node : config.nodes) {
        // Generate default paths if not specified
        if (node.install_path.empty() || node.data_path.empty()) {
            ret = get_home_dir(node.ip, config.server.ssh_port, 
                config.server.ssh_user, config.server.ssh_password, home_dir);
            if (ret != 0) {    
                LOG_ERROR_FMT("Failed to get home directory on node %s (%s)", 
                        node.name.c_str(), node.ip.c_str());
                return -1;
            }

            // Set default install path if not specified
            if (node.install_path.empty()) {
                node.install_path = home_dir + "/install/opentenbase/" + config.instance.version;
            }

            // Set default data path if not specified
            if (node.data_path.empty()) {
                node.data_path = home_dir + "/run/instance/" + config.instance.instance_name + 
                    "/" + node.name + "/data";
            }

            LOG_DEBUG_FMT("Node %s: install_path=%s, data_path=%s", 
                    node.name.c_str(), node.install_path.c_str(), node.data_path.c_str());
        }
    }

    LOG_INFO_FMT("Successfully initialized node directories");
    return 0;
}

// Initialize node directories
int
fill_node_with_gtm_info(OpentenbaseConfig& config) {
    LOG_INFO_FMT("fill node with gtminfo");

    // find gtm node
    NodeInfo gtm_node;
    for (auto& node : config.nodes) {
        // Generate default paths if not specified
        if (is_master_gtm(node.type)) {
            gtm_node.name = node.name;
            gtm_node.ip = node.ip;
            gtm_node.port = node.port;
            break;
        }
    }
    if (gtm_node.name.empty()) {
        LOG_INFO_FMT("GTM node not found in config.");
        return -1;
    }
    LOG_INFO_FMT("Gtm node info: name(%s) IP(%s) Port(%d) ", gtm_node.name.c_str(),gtm_node.ip.c_str(),gtm_node.port);

    // Initialize gtm info for each node
    for (auto& node : config.nodes) {
            node.gtm_name = gtm_node.name;
            node.gtm_ip = gtm_node.ip;
            node.gtm_port = gtm_node.port;
    }

    LOG_INFO_FMT("Successfully fill node with gtminfo");
    return 0;
}

// Build configuration from command line arguments
int
build_config_from_args(const CommandLineArgs& args, OpentenbaseConfig& config) {
    LOG_INFO_FMT("Building configuration from command line arguments");

    // Set meta configuration
    config.meta.etcd_server = args.etcd_server;
    config.meta.meta_id = args.meta_id.empty() ? 0 : std::stoi(args.meta_id);
    config.meta.shard_num = args.shard_num.empty() ? 16 : std::stoi(args.shard_num);
    config.meta.cluster_oid = args.cluster_oid.empty() ? "" : args.cluster_oid;

    // Set instance configuration
    config.instance.instance_name = args.instance_name;
    config.instance.package_path = args.package_path;
    
    // Extract package name and version
    // if (args.command == "install" || args.command == "expand" || args.command == "shrink") {
    // }
    size_t last_slash_pos = args.package_path.find_last_of('/');
    config.instance.package_name = (last_slash_pos != std::string::npos) ? 
        args.package_path.substr(last_slash_pos + 1) : args.package_path;
    config.instance.version = extract_version_from_package_name(config.instance.package_name);
    if (config.instance.version.empty()) {
        LOG_ERROR_FMT("Failed to extract version from package name: %s", 
                config.instance.package_name.c_str());
        return -1;
    }
    
    LOG_DEBUG_FMT("Set package-path: %s, package-name: %s, version: %s", 
            args.package_path.c_str(), config.instance.package_name.c_str(), 
            config.instance.version.c_str());

    // Parse node information
    auto node_names = parse_comma_separated_list(args.node_name.c_str());
    auto node_ips = parse_comma_separated_list(args.node_ip.c_str());
    
    // Validate node configuration
    size_t node_count = node_names.size();
    if (node_count == 0 || node_ips.size() != node_count) {
        LOG_ERROR_FMT("Invalid node configuration: inconsistent list lengths");
        return -1;
    }

    // Build node information
    config.nodes.clear();
    for (size_t i = 0; i < node_count; ++i) {
        NodeInfo node;
        node.name = node_names[i];
        node.type = infer_node_type(node.name);
        if (node.type == "") {
            LOG_ERROR_FMT("Invalid node name format: %s, should start with 'cn' or 'dn'", 
                    node.name.c_str());
            return -1;
        }
        node.ip = node_ips[i];
        node.data_path = "";
        node.install_path = "";
        config.nodes.push_back(node);
    }

    // Set server configuration
    config.server.ssh_user = args.ssh_user;
    config.server.ssh_password = args.ssh_password;
    config.server.ssh_port = std::stoi(args.ssh_port);


    LOG_INFO_FMT("Successfully built configuration from command line arguments");
    return 0;
}

// 解析命令行参数
bool parse_command_line(int argc, char** argv, CommandLineArgs& args, OpentenbaseConfig& config) {
    CLI::App app{"Opentenbase Cluster Management Tool"};

    // 设置程序基本信息
    app.name("opentenbase_ctl");
    app.description("Opentenbase cluster management tool");
    //app.set_version_flag("--version", "1.0.0");

    // 初始化所有子命令
    init_install_command(app, args);
    init_delete_command(app, args);
    init_start_command(app, args);
    init_stop_command(app, args);
    init_status_command(app, args);
    init_expand_command(app, args);
    init_shrink_command(app, args);

    try {
        app.parse(argc, argv);

        // 如果没有子命令，显示帮助信息
        if (app.get_subcommands().empty()) {
            std::cout << app.help() << std::endl;
            return false;
        }

        // 确定当前命令
        for (const auto& subcmd : app.get_subcommands()) {
            if (subcmd->parsed()) {
                args.command = subcmd->get_name();
                LOG_INFO_FMT("Command: %s", args.command.c_str());
                break;
            }
        }

        // 如果使用了-c 参数则获取并解析配置文件

        if (!args.config_file.empty()) {
            
            // 读取配置文件内容
            ConfigFile cfg_file;
            if (parse_config_file(args.config_file, cfg_file) != 0) {
                std::cout << "Failed to parse configuration file: " << args.config_file << std::endl;
                LOG_ERROR_FMT("Failed to parse configuration file: %s", args.config_file.c_str());
                return false;
            }

            // 根据配置文件的内容生成 node节点等信息
            if (build_opentenbase_config(cfg_file, config) != 0) {
                std::cout << "Failed to parse configuration file: " << args.config_file << std::endl;
                LOG_ERROR_FMT("Failed to parse configuration file: %s", args.config_file.c_str());
                return false;
            }
        } else {
            // Build configuration from command line arguments
            if (build_config_from_args(args, config) != 0) {
                LOG_ERROR_FMT("Failed to build configuration from command line arguments");
                return false;
            }
        }

        // Initialize node directories
        if (init_node_directories(config) != 0) {
            LOG_ERROR_FMT("Failed to initialize node directories");
            return -1;
        }

        // Assign ports for nodes
        if (args.command == "install" || args.command == "expand") {
            if (assign_ports_for_nodes(config.nodes, config.server.ssh_user, config.server.ssh_password, config.server.ssh_port) != 0) {
                LOG_ERROR_FMT("Failed to assign ports for nodes");
                return -1;
            }
        }

        // fill with gtm info
        if (fill_node_with_gtm_info(config) != 0) {
            LOG_INFO_FMT("Failed to fill node with gtm info, Perhaps no GTM information has been entered");
        }

        return true;
    } catch (const CLI::ParseError &e) {
        return app.exit(e);
    } catch (const std::exception &e) {
        LOG_ERROR_FMT("Error parsing command line arguments: %s", e.what());
        return false;
    }
} 