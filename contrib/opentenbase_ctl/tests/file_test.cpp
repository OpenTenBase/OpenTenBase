/*
 * Copyright (C) 2026 OpenTenBase Authors. All rights reserved.
 * Licensed under the BSD 3-Clause License. See the root COPYRIGHT file.
 */

#include "file.h"

#include <fstream>
#include <iostream>
#include <map>
#include <string>

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: file_test temporary-config-path\n";
        return 1;
    }

    const std::string filename = argv[1];
    const struct {
        const char* name;
        const char* input;
        const char* key;
        const char* expected;
    } cases[] = {
        {"quoted hash", "log_line_prefix = '%m #%p '",
         "log_line_prefix", "'%m #%p '"},
        {"quoted hash and outside comment", "log_line_prefix = '%m #%p ' # comment",
         "log_line_prefix", "'%m #%p '"},
        {"adjacent outside comment", "log_line_prefix = '#'# comment",
         "log_line_prefix", "'#'"},
        {"escaped quote", R"(log_line_prefix = 'it\'s # literal' # comment)",
         "log_line_prefix", R"('it\'s # literal')"},
        {"escaped backslash before hash", R"(log_line_prefix = 'path\\#tag' # comment)",
         "log_line_prefix", R"('path\\#tag')"},
        {"escaped backslash before closing quote", R"(log_line_prefix = 'path\\' # comment)",
         "log_line_prefix", R"('path\\')"},
        {"escaped hash", R"(log_line_prefix = '\#' # comment)",
         "log_line_prefix", R"('\#')"},
        {"doubled quote", "log_line_prefix = 'it''s # literal' # comment",
         "log_line_prefix", "'it''s # literal'"},
        {"empty quoted value", "log_line_prefix = '' # comment",
         "log_line_prefix", "''"},
        {"ordinary value and whitespace", "\n\tshared_buffers = 128MB \t\r\n",
         "shared_buffers", "128MB"},
        {"unquoted comment", "shared_buffers = 128MB # comment",
         "shared_buffers", "128MB"},
        {"duplicate key", "shared_buffers = 64MB\nshared_buffers = 128MB # comment",
         "shared_buffers", "128MB"},
    };

    int failures = 0;
    for (const auto& test : cases) {
        std::ofstream file(filename);
        file << test.input << '\n';
        file.close();
        if (!file) {
            std::cerr << "Failed to write test configuration\n";
            return 1;
        }

        std::map<std::string, std::string> config;
        bool parsed = parseConfigFile(filename, config);
        auto value = config.find(test.key);
        if (!parsed || config.size() != 1 || value == config.end() ||
            value->second != test.expected) {
            std::cerr << "FAIL " << test.name << ": expected [" << test.expected
                      << "], got [" << (value == config.end() ? "<missing>" : value->second)
                      << "]\n";
            ++failures;
        }
    }

    std::map<std::string, std::string> config{{"existing", "value"}};
    const auto original = config;
    if (parseConfigFile(filename + ".missing", config) || config != original) {
        std::cerr << "FAIL missing file: expected false and unchanged map\n";
        ++failures;
    }

    const size_t total = sizeof(cases) / sizeof(cases[0]) + 1;
    if (failures != 0) {
        std::cerr << failures << " of " << total << " file tests failed\n";
        return 1;
    }
    std::cout << "All " << total << " file tests passed\n";
    return 0;
}
