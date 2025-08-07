#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <string>
#include <fstream>
#include <vector>
#include <regex>
#include <map>
#include <string>
#include <cctype>
#include <stdexcept>
#include "config.h"
#include "../log/log.h"
#include "../utils/utils.h"
#include "../types/types.h"

void trim_whitespace(std::string &str) {
    size_t start = str.find_first_not_of(" \t\n\r");
    size_t end = str.find_last_not_of(" \t\n\r");
    if (start == std::string::npos || end == std::string::npos) {
        str = "";
    } else {
        str = str.substr(start, end - start + 1);
    }
}

std::vector<std::string> parse_node_list(const char* str) {
    LOG_DEBUG_FMT("Parsing node list: %s", str);
    std::vector<std::string> items;
    char* str_copy = strdup(str);
    char* saveptr = NULL;
    char* token = strtok_r(str_copy, ",", &saveptr);
    
    while (token != NULL) {
        std::string token_str(token);
        trim_whitespace(token_str);
        items.push_back(token_str);
        LOG_DEBUG_FMT("Parsed node item: %s", token_str.c_str());
        token = strtok_r(NULL, ",", &saveptr);
    }
    
    free(str_copy);
    return items;
}


// Infer node type based on node name
std::string infer_node_type(const std::string& node_name) {
    if (node_name.length() >= 2) {
        if (node_name.substr(0, 2) == "cn") {
            return Constants::NODE_TYPE_CN_MASTER;
        } else if (node_name.substr(0, 2) == "dn") {
            return Constants::NODE_TYPE_DN_MASTER;
        }else if (node_name.length() >= 3 && node_name.substr(0, 3) == "gtm") {
            return Constants::NODE_TYPE_GTM_MASTER;
        }
    }
    return "";  // Unknown type
}

// Infer node type based on node name
std::string infer_slave_node_type(const std::string& node_name) {
    if (node_name.length() >= 2) {
        if (node_name.substr(0, 2) == "cn") {
            return Constants::NODE_TYPE_CN_SLAVE;
        } else if (node_name.substr(0, 2) == "dn") {
            return Constants::NODE_TYPE_DN_SLAVE;
        } else if (node_name.length() >= 3 && node_name.substr(0, 3) == "gtm") {
            return Constants::NODE_TYPE_GTM_SLAVE;
        }
    }
    
    return "";  // Unknown type
}

// 读取并解析配置文件
int
parse_config_file(const std::string& filename, ConfigFile& cfg_file) {
    LOG_INFO_FMT("Opening config file: %s", filename.c_str());
    std::ifstream file(filename);
    if (!file.is_open()) {
        LOG_ERROR_FMT("Failed to open config file: %s", filename.c_str());
        return -1;
    }

    std::string line;
    std::string section;

    // 遍历读取每行
    while (std::getline(file, line)) {
        trim_whitespace(line);
        if (line.empty() || line[0] == ';' || line[0] == '#') {
            continue;
        }

        if (line[0] == '[' && line[line.size()-1] == ']') {
            section = line.substr(1, line.size() - 2);
            LOG_DEBUG_FMT("Parsing section: [%s]", section.c_str());
            continue;
        }

        size_t equal_pos = line.find('=');
        if (equal_pos == std::string::npos) continue;

        std::string key = line.substr(0, equal_pos);
        std::string value = line.substr(equal_pos + 1);
        trim_whitespace(key);
        trim_whitespace(value);

        LOG_DEBUG_FMT("Parsing config item: %s = %s", key.c_str(), value.c_str());

        if (section == "instance") {
            if (key == "name") {
                cfg_file.instance.name  = value.c_str();
            } else if (key == "package") {
                cfg_file.instance.package  = value.c_str();
            } else if (key == "type") {
                cfg_file.instance.type = value.c_str();
            }

        } else if (section == "gtm") {
            if (key == "master") {
                cfg_file.gtm.master = value.c_str();
            } else if (key == "slave") {
                cfg_file.gtm.slave  = value.c_str();
            } 

        } else if (section == "coordinators") {

            if (key == "master") {
                cfg_file.coordinators.master = value.c_str();
            } else if (key == "slave") {
                cfg_file.coordinators.slave = value.c_str();
            } else if (key == "nodes-per-server") {
                cfg_file.coordinators.nodes_per_server = value.c_str();
            } else if (key == "conf") {
                cfg_file.coordinators.conf = value.c_str();
            }

        } else if (section == "datanodes") {

            if (key == "master") {
                cfg_file.datanodes.master = value.c_str();
            } else if (key == "slave") {
                cfg_file.datanodes.slave = value.c_str();
            } else if (key == "nodes-per-server") {
                cfg_file.datanodes.nodes_per_server = value.c_str();
            } else if (key == "conf") {
                cfg_file.datanodes.conf = value.c_str();
            }

        } else if (section == "server") {

            if (key == "ssh-user") {
                cfg_file.server.ssh_user = value.c_str();
            } else if (key == "ssh-password") {
                cfg_file.server.ssh_password = value.c_str();
            } else if (key == "ssh-port") {
                cfg_file.server.ssh_port = value.c_str();
            }

        } else if (section == "log") {
            if (key == "level") {
                cfg_file.log.level = value.c_str();
            }
        }
    }

    file.close();
    LOG_INFO_FMT("Configuration file parsed successfully");
    return 0;
}
