/*
 * Copyright (c) 2026 OpenTenBase Authors.
 * SPDX-License-Identifier: BSD-3-Clause
 * See the top-level LICENSE.txt file.
 */
#include "file.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

namespace {
namespace fs = std::filesystem;
using ConfigMap = std::map<std::string, std::string>;

struct Case {
    const char* name;
    const char* input;
    ConfigMap expected;
};

std::string readBytes(const fs::path& path) {
    std::ifstream stream(path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("cannot read fixture: " + path.string());
    }
    std::string bytes((std::istreambuf_iterator<char>(stream)),
                      std::istreambuf_iterator<char>());
    if (stream.bad()) {
        throw std::runtime_error("fixture read failed: " + path.string());
    }
    return bytes;
}

void writeBytes(const fs::path& path, const std::string& bytes) {
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    if (!stream) {
        throw std::runtime_error("cannot create fixture: " + path.string());
    }
    stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    stream.close();
    if (!stream) {
        throw std::runtime_error("fixture write failed: " + path.string());
    }
}

void dumpMap(const char* label, const ConfigMap& values) {
    std::cerr << label << " entries=" << values.size() << '\n';
    for (const auto& entry : values) {
        std::cerr << "  [" << entry.first << "] = [" << entry.second << "]\n";
    }
}
} // namespace

int main(int argc, char** argv) {
    if (argc != 3 || std::string(argv[1]) != "--work-dir") {
        std::cerr << "usage: parse_config_file_test --work-dir NEW_DIRECTORY\n";
        return 2;
    }

    const fs::path work(argv[2]);
    std::error_code error;
    if (!fs::create_directory(work, error)) {
        std::cerr << "refusing existing or unavailable work directory: "
                  << work.string() << '\n';
        return 2;
    }

    // Literal on-disk inputs and literal expected maps; no parser-derived oracle.
    const std::vector<Case> cases = {
        {"quoted_hash_and_comment",
         R"CFG(log_line_prefix = '[%m] # %p' # trailing comment
)CFG",
         {{"log_line_prefix", "'[%m] # %p'"}}},
        {"quoted_hash_without_comment",
         R"CFG(log_line_prefix='#%m#%p'
)CFG",
         {{"log_line_prefix", "'#%m#%p'"}}},
        {"doubled_quote_before_hash",
         R"CFG(log_line_prefix = 'can''t # %p' # outside
)CFG",
         {{"log_line_prefix", R"VAL('can''t # %p')VAL"}}},
        {"escaped_quote_before_hash",
         R"CFG(log_line_prefix = 'can\'t # %p' # outside
)CFG",
         {{"log_line_prefix", R"VAL('can\'t # %p')VAL"}}},
        {"escaped_backslash_before_hash",
         R"CFG(log_line_prefix = 'C:\\logs # %p' # outside
)CFG",
         {{"log_line_prefix", R"VAL('C:\\logs # %p')VAL"}}},
        {"doubled_quotes_around_hash",
         R"CFG(log_line_prefix = '''#''' # outside
)CFG",
         {{"log_line_prefix", R"VAL('''#''')VAL"}}},
        {"quoted_hash_and_equals",
         R"CFG(log_line_prefix = '# key=value # %p' # outside
)CFG",
         {{"log_line_prefix", "'# key=value # %p'"}}},
        {"last_assignment_with_quoted_hash",
         R"CFG(log_line_prefix = 'old'
log_line_prefix = 'new # %p' # outside
)CFG",
         {{"log_line_prefix", "'new # %p'"}}},
        {"numeric_trailing_comment",
         R"CFG(max_connections = 100 # 200
)CFG",
         {{"max_connections", "100"}}},
        {"plain_value",
         R"CFG(log_timezone = PRC
)CFG",
         {{"log_timezone", "PRC"}}},
        {"plain_trailing_comment",
         R"CFG(log_directory = pg_log # outside
)CFG",
         {{"log_directory", "pg_log"}}},
        {"empty_quoted_value",
         R"CFG(log_line_prefix = '' # ignored ' quote in comment
)CFG",
         {{"log_line_prefix", "''"}}},
        {"quoted_value_without_hash",
         R"CFG(log_line_prefix = '[%m] %p' # outside
)CFG",
         {{"log_line_prefix", "'[%m] %p'"}}},
        {"escaped_backslash_at_quote_boundary",
         R"CFG(log_line_prefix = 'tail\\' # outside
)CFG",
         {{"log_line_prefix", R"VAL('tail\\')VAL"}}},
        {"last_plain_assignment",
         R"CFG(log_line_prefix = 'old'
log_line_prefix = off # outside
)CFG",
         {{"log_line_prefix", "off"}}},
        {"whitespace_and_ignored_lines",
         "\n \t\n# ordinary comment\nno assignment here\n"
         " \tmax_connections \t= \t100 \t# outside\n",
         {{"max_connections", "100"}}}
    };

    std::size_t passed = 0;
    std::size_t failed = 0;
    std::size_t calls = 0;
    try {
        for (const auto& test : cases) {
            const fs::path fixture = work / (std::string(test.name) + ".conf");
            const std::string input(test.input);
            writeBytes(fixture, input);
            if (readBytes(fixture) != input) {
                throw std::runtime_error("fixture setup mismatch: " + fixture.string());
            }
            ConfigMap actual;
            const bool parsed = parseConfigFile(fixture.string(), actual);
            ++calls;
            if (readBytes(fixture) != input) {
                throw std::runtime_error("parser changed fixture: " + fixture.string());
            }
            if (parsed && actual == test.expected) {
                ++passed;
                std::cout << "PASS " << test.name << '\n';
            } else {
                ++failed;
                std::cout << "FAIL " << test.name << '\n';
                std::cerr << test.name << ": parse_return=" << parsed << '\n';
                dumpMap("expected", test.expected);
                dumpMap("actual", actual);
            }
        }
    } catch (const std::exception& exception) {
        std::cerr << "fixture error: " << exception.what() << '\n';
        return 2;
    }
    std::cout << "SUMMARY cases=" << cases.size() << " passed=" << passed
              << " failed=" << failed << " parse_calls=" << calls << '\n';
    if (!std::cout) {
        return 2;
    }
    return failed == 0 ? 0 : 1;
}
