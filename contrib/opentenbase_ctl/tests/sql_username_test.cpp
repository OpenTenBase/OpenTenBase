/*
 * Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
 * Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.
 */

#include "utils/utils.h"
#include "ssh/remote_ssh.h"

#include <cstdio>
#include <iostream>
#include <string>
#include <unistd.h>

// Production currently disables local detection. Override only that
// predicate to exercise both builders and the real local execution path.
bool is_local_ip(const std::string& ip) {
    return ip == "127.0.0.1";
}

int main(int argc, char* argv[]) {
    if (argc != 2) return 1;
    const std::string directory = argv[1];
    const std::string usernames[] = {
        "opentenbase", "reporter", "reporting user", "O'Reilly",
        "$(touch injected)", "user; touch injected", "user\"$HOME`touch injected`",
        "literal $HOME", "back\\slash'user"
    };
    int failures = 0;
    for (bool local : {false, true}) {
        const std::string ip = local ? "127.0.0.1" : "192.0.2.1";
        for (const auto& username : usernames) {
            std::remove("injected");
            const std::string command = build_sql_cmd_for_psql(
                directory, ip, 15432, username, "analytics",
                " --no-align --tuples-only ", "SELECT current_user");
            std::string result;
            int status;
            if (local) {
                status = execute_sql_by_psql(ip, 22, "testuser", "unused", command, result);
            } else {
                FILE* pipe = popen(command.c_str(), "r");
                if (pipe == nullptr) return 1;
                char buffer[512];
                while (fgets(buffer, sizeof(buffer), pipe) != nullptr) result += buffer;
                status = pclose(pipe);
            }
            const std::string expected = "-h\n" + ip + "\n-p\n15432\n-d\nanalytics\n-U\n" +
                username + "\n--no-align\n--tuples-only\n--command\nSELECT current_user\n";
            if (status != 0 || result != expected || access("injected", F_OK) == 0) {
                std::cerr << "FAIL " << (local ? "local" : "remote") << " SQL username ["
                          << username << "]: got [" << result << "]\n";
                ++failures;
            }
        }
    }
    if (failures != 0) return 1;
    std::cout << "All 18 SQL username tests passed\n";
    return 0;
}
