#include "user_config.h"

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <system_error>
#include <utility>

#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>
#include <vector>

namespace otb {
namespace {

struct ConfigField {
    const char* section;
    const char* key;
    std::string UserConfig::*value;
};

constexpr ConfigField kFields[] = {
    {"db", "host", &UserConfig::host},
    {"db", "port", &UserConfig::port},
    {"db", "user", &UserConfig::user},
    {"db", "dbname", &UserConfig::dbname},
    {"ai", "provider", &UserConfig::provider},
    {"ai", "api_url", &UserConfig::api_url},
    {"ai", "api_key", &UserConfig::api_key},
    {"ai", "model", &UserConfig::model},
};

struct Provider {
    const char* label;
    const char* id;
    const char* url;
    std::array<const char*, 2> models;
};

constexpr std::array<Provider, 4> kProviders{{
    {"DeepSeek", "deepseek", "https://api.deepseek.com/v1/chat/completions",
     {"deepseek-chat", "deepseek-reasoner"}},
    {"OpenAI", "openai", "https://api.openai.com/v1/chat/completions",
     {"gpt-4o-mini", "gpt-4o"}},
    {"Qwen DashScope", "qwen", "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
     {"qwen-plus", "qwen-max"}},
    {"Ollama local", "ollama", "http://127.0.0.1:11434/v1/chat/completions",
     {"qwen2.5", "llama3.1"}},
}};

std::string trim(const std::string& s) {
    constexpr char kWhitespace[] = " \t\r\n";
    const auto first = s.find_first_not_of(kWhitespace);
    if (first == std::string::npos) {
        return {};
    }
    return s.substr(first, s.find_last_not_of(kWhitespace) - first + 1);
}

void write_config(std::ostream& out, const UserConfig& cfg, bool mask_key) {
    std::string section;
    for (const auto& field : kFields) {
        if (section != field.section) {
            if (!section.empty()) {
                out << '\n';
            }
            section = field.section;
            out << '[' << section << "]\n";
        }
        const auto& value = cfg.*field.value;
        out << field.key << '='
            << (mask_key && field.value == &UserConfig::api_key ? mask_secret(value) : value)
            << '\n';
    }
}

int read_choice(int max) {
    std::string input;
    while (std::cout << "Choice [1]: " && std::getline(std::cin, input)) {
        input = trim(input);
        if (input.empty()) {
            return 1;
        }
        if (input.size() == 1 && input[0] >= '1' && input[0] - '0' <= max) {
            return input[0] - '0';
        }
        std::cout << "Enter a number from 1 to " << max << ".\n";
    }
    return 0;
}

bool prompt(const char* label, std::string& value, bool required = false) {
    std::string input;
    for (;;) {
        std::cout << label;
        if (!value.empty()) {
            std::cout << " [" << value << ']';
        }
        std::cout << ": ";
        if (!std::getline(std::cin, input)) {
            return false;
        }
        input = trim(input);
        if (!input.empty()) {
            value = std::move(input);
        }
        if (!required || !value.empty()) {
            return true;
        }
        std::cout << "Enter a value.\n";
    }
}

}  // namespace

std::string default_config_path() {
    const char* xdg = std::getenv("XDG_CONFIG_HOME");
    if (xdg && *xdg) {
        return (std::filesystem::path(xdg) / "otb_aiops/config.ini").string();
    }
    const char* home = std::getenv("HOME");
    return (std::filesystem::path(home ? home : "") / ".config/otb_aiops/config.ini").string();
}

bool load_user_config(const std::string& path, UserConfig& out) {
    std::ifstream file(path);
    if (!file) {
        return false;
    }

    UserConfig cfg;
    std::string section;
    std::string line;
    while (std::getline(file, line)) {
        line = trim(line);
        if (line.empty() || line.front() == '#' || line.front() == ';') {
            continue;
        }
        if (line.front() == '[' && line.back() == ']') {
            section = trim(line.substr(1, line.size() - 2));
            continue;
        }
        const auto eq = line.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const auto key = trim(line.substr(0, eq));
        for (const auto& field : kFields) {
            if (section == field.section && key == field.key) {
                cfg.*field.value = trim(line.substr(eq + 1));
                break;
            }
        }
    }
    if (file.bad() || !file.eof()) {
        return false;
    }
    out = std::move(cfg);
    return true;
}

bool save_user_config(const std::string& path, const UserConfig& cfg, std::string& err) {
    err.clear();
    for (const auto& field : kFields) {
        if ((cfg.*field.value).find_first_of("\r\n") != std::string::npos) {
            err = std::string("Line break in config field ") + field.key;
            return false;
        }
    }

    const auto parent = std::filesystem::path(path).parent_path();
    std::error_code ec;
    if (!parent.empty()) {
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            err = "Could not create config directory: " + ec.message();
            return false;
        }
    }

    std::ostringstream ini;
    write_config(ini, cfg, false);
    const auto contents = ini.str();

    // Set permissions before writing the key and replace the destination only after closing.
    std::string temp_path = path + ".XXXXXX";
    const int fd = mkstemp(temp_path.data());
    if (fd == -1) {
        err = "Could not create temporary config: " + std::generic_category().message(errno);
        return false;
    }

    const char* failure = nullptr;
    int error = 0;
    if (fchmod(fd, S_IRUSR | S_IWUSR) == -1) {
        failure = "Could not set config permissions";
        error = errno;
    } else {
        std::size_t offset = 0;
        while (offset < contents.size()) {
            const auto written = write(fd, contents.data() + offset, contents.size() - offset);
            if (written < 0 && errno == EINTR) {
                continue;
            }
            if (written <= 0) {
                failure = "Could not write config";
                error = written == 0 ? EIO : errno;
                break;
            }
            offset += static_cast<std::size_t>(written);
        }
    }
    if (close(fd) == -1 && !failure) {
        failure = "Could not close config";
        error = errno;
    }
    if (!failure && std::rename(temp_path.c_str(), path.c_str()) == -1) {
        failure = "Could not replace config";
        error = errno;
    }
    if (failure) {
        std::remove(temp_path.c_str());
        err = std::string(failure) + ": " + std::generic_category().message(error);
        return false;
    }
    return true;
}

std::string mask_secret(const std::string& s) {
    if (s.empty()) {
        return {};
    }
    if (s.size() <= 10) {
        return "****";
    }
    return s.substr(0, 4) + "****" + s.substr(s.size() - 4);
}

namespace {

bool prompt_secret(const std::string& label, std::string& value) {
    std::cout << label << ": " << std::flush;
    struct EchoGuard {
        bool active = false;
        termios old_term{};
        EchoGuard() {
            if (isatty(STDIN_FILENO) && tcgetattr(STDIN_FILENO, &old_term) == 0) {
                termios no_echo = old_term;
                no_echo.c_lflag &= ~static_cast<tcflag_t>(ECHO);
                if (tcsetattr(STDIN_FILENO, TCSANOW, &no_echo) == 0) {
                    active = true;
                }
            }
        }
        ~EchoGuard() {
            if (active) {
                tcsetattr(STDIN_FILENO, TCSANOW, &old_term);
                std::cout << '\n';
            }
        }
    } guard;

    std::string line;
    if (!std::getline(std::cin, line)) {
        return false;
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    value = line;
    return true;
}

bool read_postmaster_pid(const std::filesystem::path& data_dir, std::string& host_out, std::string& port_out) {
    std::ifstream in(data_dir / "postmaster.pid");
    if (!in) {
        return false;
    }
    std::string line;
    std::vector<std::string> lines;
    while (lines.size() < 5 && std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(line);
    }
    if (lines.size() < 5 || lines[3].empty() || lines[4].empty()) {
        return false;
    }
    port_out = lines[3];
    host_out = lines[4];
    return true;
}

}  // namespace

bool discover_local_instance(UserConfig& out) {
    std::vector<std::filesystem::path> candidates;
    if (const char* pgdata = std::getenv("PGDATA"); pgdata && *pgdata) {
        candidates.emplace_back(pgdata);
    }
    if (const char* home = std::getenv("HOME"); home && *home) {
        candidates.push_back(std::filesystem::path(home) / ".local" / "opentenbase" / "data");
    }

    // Scan /proc for running postgres processes with -D <data_dir>.
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator("/proc", ec)) {
        if (ec) break;
        const auto name = entry.path().filename().string();
        if (name.empty() || !std::isdigit(static_cast<unsigned char>(name[0]))) {
            continue;
        }
        std::ifstream cmd(entry.path() / "cmdline", std::ios::binary);
        if (!cmd) continue;
        std::string raw((std::istreambuf_iterator<char>(cmd)), std::istreambuf_iterator<char>());
        if (raw.find("postgres") == std::string::npos) continue;

        std::vector<std::string> args;
        std::size_t start = 0;
        while (start < raw.size()) {
            std::size_t nul = raw.find('\0', start);
            if (nul == std::string::npos) nul = raw.size();
            if (nul > start) args.push_back(raw.substr(start, nul - start));
            start = nul + 1;
        }
        for (std::size_t i = 0; i + 1 < args.size(); ++i) {
            if (args[i] == "-D") {
                candidates.emplace_back(args[i + 1]);
                break;
            }
        }
    }

    for (const auto& dir : candidates) {
        std::string host, port;
        if (read_postmaster_pid(dir, host, port)) {
            out.host = host;
            out.port = port;
            const char* sys_user = std::getenv("USER");
            out.user = (sys_user && *sys_user) ? std::string(sys_user) : "opentenbase";
            out.dbname = "postgres";
            return true;
        }
    }
    return false;
}

int run_config_wizard(const std::string& path) {
    UserConfig cfg;
    if (!discover_local_instance(cfg)) {
        std::cerr << "Error: no running local OpenTenBase instance detected. "
                  << "Start OpenTenBase first, or pass -h/-p/-U/-d directly.\n";
        return 1;
    }
    std::cout << "Using local OpenTenBase instance: "
              << cfg.host << ":" << cfg.port
              << " (user=" << cfg.user << ", db=" << cfg.dbname << ")\n";

    std::cout << "AI provider\n";
    for (std::size_t i = 0; i < kProviders.size(); ++i) {
        std::cout << "  " << i + 1 << ") " << kProviders[i].label << '\n';
    }
    std::cout << "  " << kProviders.size() + 1 << ") Custom\n";
    const int choice = read_choice(static_cast<int>(kProviders.size()) + 1);
    if (choice == 0) {
        return 1;
    }
    if (choice <= static_cast<int>(kProviders.size())) {
        const auto& provider = kProviders[choice - 1];
        cfg.provider = provider.id;
        cfg.api_url = provider.url;
        std::cout << "Model\n"
                  << "  1) " << provider.models[0] << '\n'
                  << "  2) " << provider.models[1] << '\n';
        const int model = read_choice(static_cast<int>(provider.models.size()));
        if (model == 0) {
            return 1;
        }
        cfg.model = provider.models[model - 1];
    } else {
        cfg.provider = "custom";
        if (!prompt("API URL", cfg.api_url, true) || !prompt("Model", cfg.model, true)) {
            return 1;
        }
    }

    if (!prompt_secret("API key (hidden, leave blank if unused)", cfg.api_key)) {
        return 1;
    }

    std::string err;
    if (!save_user_config(path, cfg, err)) {
        std::cerr << "Error: " << err << '\n';
        return 1;
    }
    std::cout << "Saved config to " << path << '\n';
    return 0;
}

void print_user_config(const std::string& path, std::ostream& out) {
    UserConfig cfg;
    if (!load_user_config(path, cfg)) {
        out << "Could not load config from " << path << '\n';
        return;
    }
    write_config(out, cfg, true);
}

}  // namespace otb
