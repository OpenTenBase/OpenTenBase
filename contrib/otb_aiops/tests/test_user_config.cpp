/*
 * Build from contrib/otb_aiops.
 *
 * c++ -std=c++17 -Wall -Wextra -Isrc tests/test_user_config.cpp \
 *     src/config/user_config.cpp -o /tmp/test_user_config
 */

#include "config/user_config.h"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace {

void test_user_config_roundtrip(const std::string& path) {
    otb::UserConfig config{};
    config.api_key = "test-api-key-123456";
    std::string error;
    assert(otb::save_user_config(path, config, error));

    struct stat file_status {};
    assert(stat(path.c_str(), &file_status) == 0);
    assert((file_status.st_mode & 07777) == 0600);

    otb::UserConfig loaded{};
    assert(otb::load_user_config(path, loaded));
    assert(loaded.api_key == config.api_key);
}

void test_missing_user_config(const std::string& path) {
    otb::UserConfig config{};
    assert(!otb::load_user_config(path, config));
}

void test_mask_secret() {
    assert(otb::mask_secret("").empty());

    const std::string short_mask = otb::mask_secret("abc");
    assert(!short_mask.empty());
    assert(short_mask.find_first_of("abc") == std::string::npos);

    const std::string long_secret = "abcdefghijklmnop";
    const std::string long_mask = otb::mask_secret(long_secret);
    assert(long_mask == "abcd****mnop");
    assert(long_mask.find(long_secret) == std::string::npos);
}

}  // namespace

int main() {
    char temp_dir[] = "/tmp/otb_user_config_test.XXXXXX";
    assert(mkdtemp(temp_dir) != nullptr);
    const std::string path = std::string(temp_dir) + "/config";

    test_missing_user_config(path);
    test_user_config_roundtrip(path);
    assert(unlink(path.c_str()) == 0);
    assert(rmdir(temp_dir) == 0);

    test_mask_secret();
    std::cout << "All user config tests passed\n";
    return 0;
}
