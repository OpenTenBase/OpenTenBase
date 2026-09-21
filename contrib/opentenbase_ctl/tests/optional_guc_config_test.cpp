/*
 * Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
 * Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.
 */

#include "types/types.h"

#include <fstream>
#include <iostream>
#include <map>
#include <string>

int build_cn_node_config(const ConfigFile&, OpentenbaseConfig&);
int build_dn_node_config(const ConfigFile&, OpentenbaseConfig&);

int main() {
    std::ofstream override_file("override.conf");
    override_file << "shared_buffers = 128MB\n";
    override_file.close();
    if (!override_file) {
        std::cerr << "Failed to write GUC fixture\n";
        return 1;
    }

    const char* sections[] = {"coordinators", "datanodes", "datanodes"};
    const char* instance_types[] = {"distributed", "distributed", "centralized"};
    const char* settings[] = {"", "conf=\n", "conf=override.conf\n", "conf=missing.conf\n"};
    int failures = 0;
    int total = 0;
    for (size_t kind = 0; kind < 3; ++kind) {
        for (size_t setting = 0; setting < 4; ++setting) {
            ++total;
            std::ofstream file("config.ini");
            file << '[' << sections[kind] << "]\nmaster=127.0.0.1\n" << settings[setting];
            file.close();
            if (!file) {
                std::cerr << "Failed to write config fixture\n";
                return 1;
            }

            ConfigFile parsed;
            if (parse_config_file("config.ini", parsed) != 0) {
                std::cerr << "Failed to parse config fixture\n";
                return 1;
            }
            OpentenbaseConfig config{};
            config.instance.instance_type = instance_types[kind];
            auto& guc = kind == 0 ? config.cn_guc_cfg : config.dn_guc_cfg;
            guc["stale"] = "value";
            const int result = kind == 0 ? build_cn_node_config(parsed, config)
                                         : build_dn_node_config(parsed, config);
            const std::map<std::string, std::string> expected = setting == 2
                ? std::map<std::string, std::string>{{"shared_buffers", "128MB"}}
                : std::map<std::string, std::string>{};
            if ((setting == 3 && result == 0) ||
                (setting != 3 && (result != 0 || guc != expected || config.nodes.size() != 1))) {
                std::cerr << "FAIL " << instance_types[kind] << ' ' << sections[kind]
                          << " setting=" << setting << " status=" << result << '\n';
                ++failures;
            }
        }
    }

    ++total;
    ConfigFile unused;
    unused.coordinators.conf = "missing.conf";
    OpentenbaseConfig centralized{};
    centralized.instance.instance_type = "centralized";
    if (build_cn_node_config(unused, centralized) != 0 || !centralized.nodes.empty()) {
        std::cerr << "FAIL centralized instance must ignore coordinator configuration\n";
        ++failures;
    }
    if (failures) {
        std::cerr << failures << " of " << total << " optional GUC config tests failed\n";
        return 1;
    }
    std::cout << "All " << total << " optional GUC config tests passed\n";
    return 0;
}
