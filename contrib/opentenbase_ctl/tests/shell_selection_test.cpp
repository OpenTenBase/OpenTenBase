/*
 * Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
 * Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.
 */

#include "cluster/cluster.h"
#include "ssh/remote_ssh.h"

#include <algorithm>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

static std::mutex targets_mutex;
static std::vector<std::string> targets;

// Replace only the SSH execution boundary; never send commands to real hosts.
int execute_command(const std::string& ip, int, const std::string&,
                    const std::string&, const std::string&, std::string&) {
    std::lock_guard<std::mutex> lock(targets_mutex);
    targets.push_back(ip);
    return 0;
}

static NodeInfo node(const std::string& ip, bool selected) {
    NodeInfo result{};
    result.ip = ip;
    result.is_op_node = selected;
    return result;
}

int main() {
    const struct {
        const char* name;
        std::vector<NodeInfo> nodes;
        std::vector<std::string> expected;
    } cases[] = {
        {"one selected host", {node("192.0.2.1", true), node("192.0.2.2", false)},
         {"192.0.2.1"}},
        {"selected nodes share host", {node("192.0.2.1", true), node("192.0.2.1", true),
                                       node("192.0.2.2", false)}, {"192.0.2.1"}},
        {"selected and excluded nodes share host", {node("192.0.2.1", false),
             node("192.0.2.1", true), node("192.0.2.2", true)}, {"192.0.2.1", "192.0.2.2"}},
        {"all selected", {node("192.0.2.1", true), node("192.0.2.2", true)},
         {"192.0.2.1", "192.0.2.2"}},
        {"none selected", {node("192.0.2.1", false), node("192.0.2.2", false)}, {}},
        {"empty host ignored", {node("", true), node("192.0.2.2", false)}, {}},
    };

    int failures = 0;
    for (const auto& test : cases) {
        OpentenbaseConfig config{};
        config.nodes = test.nodes;
        config.shell.shell_cmd = "printf selected";
        targets.clear();
        const int status = shell_command(&config);
        std::sort(targets.begin(), targets.end());
        const bool expected_status = test.expected.empty() ? status != 0 : status == 0;
        if (targets != test.expected || !expected_status) {
            std::cerr << "FAIL " << test.name << ": dispatched to " << targets.size()
                      << " hosts, expected " << test.expected.size() << '\n';
            ++failures;
        }
    }
    if (failures != 0) return 1;
    std::cout << "All 6 shell selection tests passed\n";
    return 0;
}
