/*
 * Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
 * Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.
 */

#include "types/types.h"

#include <exception>
#include <iostream>
#include <string>

int build_server_config(const ConfigFile&, OpentenbaseConfig&);

int main() {
    const struct {
        const char* input;
        int expected;
    } cases[] = {
        {"", -1}, {"1", 1}, {"22", 22}, {"65535", 65535}, {"00022", 22},
        {"0", -1}, {"-1", -1}, {"-2", -1}, {"65536", -1},
        {"junk", -1}, {"22junk", -1}, {"22.5", -1}, {"1e2", -1},
        {"+22", -1}, {" 22", -1}, {"22 ", -1},
        {"999999999999999999999999999999999999", -1},
    };

    int failures = 0;
    int total = 0;
    for (bool from_args : {false, true}) {
        for (const auto& test : cases) {
            ++total;
            OpentenbaseConfig config{};
            config.server.ssh_port = 2222;
            int result = 0;
            try {
                if (from_args) {
                    CommandLineArgs args;
                    args.package_path = "opentenbase-3.16.9.tar.gz";
                    args.node_name = "dn0001";
                    args.node_ip = "127.0.0.1";
                    args.ssh_port = test.input;
                    result = build_config_from_args(args, config);
                } else {
                    ConfigFile parsed;
                    parsed.server.ssh_port = test.input;
                    result = build_server_config(parsed, config);
                }
            } catch (const std::exception& error) {
                std::cerr << "FAIL " << (from_args ? "arguments" : "file")
                          << " port=[" << test.input << "] threw " << error.what() << '\n';
                ++failures;
                continue;
            }
            if ((test.expected < 0 && (result == 0 || config.server.ssh_port != 2222)) ||
                (test.expected > 0 && (result != 0 || config.server.ssh_port != test.expected))) {
                std::cerr << "FAIL " << (from_args ? "arguments" : "file")
                          << " port=[" << test.input << "] status=" << result
                          << " stored=" << config.server.ssh_port << '\n';
                ++failures;
            }
        }
    }
    if (failures) {
        std::cerr << failures << " of " << total << " SSH port config tests failed\n";
        return 1;
    }
    std::cout << "All " << total << " SSH port config tests passed\n";
    return 0;
}
