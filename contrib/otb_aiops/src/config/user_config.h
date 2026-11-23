#ifndef OTB_AIOPS_USER_CONFIG_H
#define OTB_AIOPS_USER_CONFIG_H

#include <iosfwd>
#include <string>

namespace otb {

struct UserConfig {
    std::string host, port, user, dbname, provider, api_url, api_key, model;
};

std::string default_config_path();
bool discover_local_instance(UserConfig& out);
bool load_user_config(const std::string& path, UserConfig& out);
bool save_user_config(const std::string& path, const UserConfig& cfg, std::string& err);
std::string mask_secret(const std::string& s);
int run_config_wizard(const std::string& path);
void print_user_config(const std::string& path, std::ostream& out);

}  // namespace otb

#endif  // OTB_AIOPS_USER_CONFIG_H
