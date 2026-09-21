/*
 * Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
 * Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.
 */

#include "command/command.h"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>

static int failures = 0;
static int total = 0;

static void check(const std::string& name, const std::string& contents,
                  const std::string& expected) {
    ++total;
    std::ofstream file(".bashrc");
    file << contents;
    file.close();
    if (!file) {
        std::cerr << "Failed to write isolated bashrc fixture\n";
        ++failures;
        return;
    }
    const auto actual = getEnvironmentVariableFromBashrc("CLUSTER_CONFIG_FILE");
    if (actual != expected) {
        std::cerr << "FAIL " << name << ": expected " << expected.size()
                  << " bytes, got " << actual.size() << " bytes\n";
        ++failures;
    }
}

int main() {
    const std::string prefix = "CLUSTER_CONFIG_FILE=";
    for (size_t length : {size_t(22), size_t(126), size_t(127), size_t(128),
                          size_t(186), size_t(253), size_t(254), size_t(255), size_t(508)}) {
        const std::string path = "/tmp/" + std::string(length - 16, 'x') + "/config.ini";
        check("path length " + std::to_string(length), prefix + path + '\n', path);
    }
    const std::string long_path = "/tmp/" + std::string(170, 'x') + "/config.ini";
    check("last assignment wins", prefix + "/old.ini\n" + prefix + long_path + '\n', long_path);
    check("short final assignment", prefix + long_path + '\n' + prefix + "/last.ini\n", "/last.ini");
    check("empty final assignment", prefix + long_path + '\n' + prefix + '\n', "");
    check("empty assignment", prefix + '\n', "");
    check("no assignment", "# no saved path\nUNRELATED=value\n", "");
    check("no trailing newline", prefix + long_path, long_path);
    check("ignore unrelated and commented assignments",
          "# " + prefix + "/comment.ini\n" + prefix + long_path + "\nUNRELATED=value\n", long_path);

    ++total;
    if (std::remove(".bashrc") != 0 || !getEnvironmentVariableFromBashrc("CLUSTER_CONFIG_FILE").empty()) {
        std::cerr << "FAIL absent bashrc must not yield a saved path\n";
        ++failures;
    }
    if (failures) {
        std::cerr << failures << " of " << total << " saved config path tests failed\n";
        return 1;
    }
    std::cout << "All " << total << " saved config path tests passed\n";
    return 0;
}
