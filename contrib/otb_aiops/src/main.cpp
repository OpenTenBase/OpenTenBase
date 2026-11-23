#include "ai/llm_client.h"
#include "collector/collector.h"
#include "collector/storage_collector.h"
#include "config/user_config.h"
#include "db/pg_client.h"
#include "formatter/formatter.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

const char* kVersion = "0.1.0";

void print_usage(std::ostream& out) {
    out << "otb_aiops - OpenTenBase CLI diagnostic and AIOps tool (v" << kVersion << ")\n\n"
        << "Usage:\n"
        << "  otb_aiops [OPTIONS] <COMMAND> [SUBCOMMAND]\n\n"
        << "Commands:\n"
        << "  config [show]          Interactive setup wizard (or show current saved config)\n"
        << "  health                 Run cluster health checks (nodes, groups, sessions, replication)\n"
        << "  inspect locks          Check blocked sessions and long-running transactions\n"
        << "  inspect 2pc            Scan dangling prepared (2PC) transactions\n"
        << "  inspect slow-sql       Show top active / slow queries\n"
        << "  inspect guc            Inspect key GUC configuration parameters\n"
        << "  inspect storage        Inspect largest relations and table bloat / dead tuples\n"
        << "  diagnose               Collect all diagnostics and run LLM root-cause analysis\n"
        << "  ask <QUESTION>         Ask a natural-language ops question with live cluster snapshot\n\n"
        << "Connection Options (fallback to PGHOST/PGPORT/PGUSER/PGDATABASE/PGPASSWORD):\n"
        << "  -h, --host HOST        Database host (default: 127.0.0.1)\n"
        << "  -p, --port PORT        Database port (default: 5432)\n"
        << "  -U, --user USER        Database user (default: opentenbase)\n"
        << "  -d, --dbname DB        Database name (default: postgres)\n"
        << "      --timeout MS       Statement timeout in ms (default: 5000)\n\n"
        << "Output & AI Options:\n"
        << "      --json             Output structured JSON\n"
        << "      --top N            Limit rows for slow-sql / storage (default: 10)\n"
        << "      --api-url URL      OpenAI-compatible chat completions endpoint (env: OTB_AI_BASE_URL)\n"
        << "      --api-key KEY      API key for LLM (env: OTB_AI_API_KEY)\n"
        << "      --model MODEL      Model name (env: OTB_AI_MODEL, default: gpt-4o-mini)\n"
        << "      --help             Show this help message\n"
        << "      --version          Show version\n";
}

}  // namespace

int main(int argc, char** argv) {
    // Precedence: CLI flags > Environment variables > User config file > Auto-discovered local instance > Defaults
    otb::UserConfig saved_cfg;
    const std::string cfg_path = otb::default_config_path();
    const bool has_saved = otb::load_user_config(cfg_path, saved_cfg);

    otb::UserConfig local_cfg;
    const bool has_local = (!has_saved) && otb::discover_local_instance(local_cfg);
    const otb::UserConfig& fallback_cfg = has_saved ? saved_cfg : local_cfg;

    otb::ConnConfig db_cfg = otb::ConnConfig::from_env();
    if (!std::getenv("PGHOST") && !fallback_cfg.host.empty()) db_cfg.host = fallback_cfg.host;
    if (!std::getenv("PGPORT") && !fallback_cfg.port.empty()) db_cfg.port = fallback_cfg.port;
    if (!std::getenv("PGUSER") && !fallback_cfg.user.empty()) db_cfg.user = fallback_cfg.user;
    if (!std::getenv("PGDATABASE") && !fallback_cfg.dbname.empty()) db_cfg.dbname = fallback_cfg.dbname;
    (void)has_local;

    otb::LlmConfig llm_cfg = otb::LlmConfig::from_env();
    if (!std::getenv("OTB_AI_BASE_URL") && !saved_cfg.api_url.empty()) llm_cfg.api_url = saved_cfg.api_url;
    if (!std::getenv("OTB_AI_API_KEY") && !std::getenv("OPENAI_API_KEY") && !saved_cfg.api_key.empty()) {
        llm_cfg.api_key = saved_cfg.api_key;
    }
    if (!std::getenv("OTB_AI_MODEL") && !saved_cfg.model.empty()) llm_cfg.model = saved_cfg.model;
    bool json_output = false;
    int top_n = 10;
    std::vector<std::string> positional;

    auto require_arg = [&](int& i, const std::string& flag) -> std::string {
        if (i + 1 >= argc) {
            std::cerr << "Error: missing value for " << flag << "\n";
            std::exit(2);
        }
        return argv[++i];
    };

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage(std::cout);
            return 0;
        } else if (arg == "--version") {
            std::cout << "otb_aiops " << kVersion << "\n";
            return 0;
        } else if (arg == "-h" || arg == "--host") {
            db_cfg.host = require_arg(i, arg);
        } else if (arg == "-p" || arg == "--port") {
            db_cfg.port = require_arg(i, arg);
        } else if (arg == "-U" || arg == "--user") {
            db_cfg.user = require_arg(i, arg);
        } else if (arg == "-d" || arg == "--dbname") {
            db_cfg.dbname = require_arg(i, arg);
        } else if (arg == "--timeout") {
            db_cfg.statement_timeout_ms = std::stoi(require_arg(i, arg));
        } else if (arg == "--json") {
            json_output = true;
        } else if (arg == "--top") {
            top_n = std::stoi(require_arg(i, arg));
        } else if (arg == "--api-url") {
            llm_cfg.api_url = require_arg(i, arg);
        } else if (arg == "--api-key") {
            llm_cfg.api_key = require_arg(i, arg);
        } else if (arg == "--model") {
            llm_cfg.model = require_arg(i, arg);
        } else if (!arg.empty() && arg[0] == '-') {
            std::cerr << "Error: unknown option '" << arg << "'\n";
            print_usage(std::cerr);
            return 2;
        } else {
            positional.push_back(arg);
        }
    }

    if (positional.empty()) {
        print_usage(std::cout);
        return 0;
    }

    const std::string& cmd = positional[0];
    if (cmd == "config") {
        if (positional.size() >= 2 && positional[1] == "show") {
            otb::print_user_config(cfg_path, std::cout);
            return 0;
        }
        return otb::run_config_wizard(cfg_path);
    }
    if (cmd == "inspect") {
        if (positional.size() < 2) {
            std::cerr << "Error: 'inspect' requires a subcommand: locks | 2pc | slow-sql | guc | storage\n";
            return 2;
        }
        const std::string& sub = positional[1];
        if (sub != "locks" && sub != "2pc" && sub != "slow-sql" && sub != "guc" && sub != "storage") {
            std::cerr << "Error: unknown inspect target '" << sub << "'\n";
            return 2;
        }
    } else if (cmd == "ask") {
        if (positional.size() < 2) {
            std::cerr << "Error: 'ask' requires a question string\n";
            return 2;
        }
    } else if (cmd != "health" && cmd != "diagnose") {
        std::cerr << "Error: unknown command '" << cmd << "'\n";
        print_usage(std::cerr);
        return 2;
    }

    otb::PgClient db;
    std::string conn_err;
    if (!db.connect(db_cfg, conn_err)) {
        std::cerr << "Connection failed (" << db_cfg.host << ":" << db_cfg.port << "/" << db_cfg.dbname << "): "
                  << conn_err << "\n";
        return 1;
    }

    otb::DiagnosticReport report;
    if (cmd == "health") {
        report = otb::collect_health(db);
    } else if (cmd == "inspect") {
        const std::string& sub = positional[1];
        if (sub == "locks") {
            report = otb::collect_locks(db);
        } else if (sub == "2pc") {
            report = otb::collect_2pc(db);
        } else if (sub == "slow-sql") {
            report = otb::collect_slow_sql(db, top_n);
        } else if (sub == "guc") {
            report = otb::collect_guc(db);
        } else if (sub == "storage") {
            report = otb::collect_storage(db, top_n);
        } else {
            std::cerr << "Error: unknown inspect target '" << sub << "'\n";
            return 2;
        }
    } else if (cmd == "diagnose" || cmd == "ask") {
        if (!json_output) {
            std::cerr << "Collecting cluster diagnostics and querying " << llm_cfg.model << "...\n";
        }
        report = otb::collect_all(db, top_n);
        std::string snapshot_json = otb::report_to_json(report);
        otb::LlmResponse ai_resp;
        if (cmd == "diagnose") {
            ai_resp = otb::LlmClient::diagnose(llm_cfg, snapshot_json);
        } else {
            std::string question;
            for (size_t i = 1; i < positional.size(); ++i) {
                if (i > 1) question.push_back(' ');
                question += positional[i];
            }
            ai_resp = otb::LlmClient::ask(llm_cfg, question, snapshot_json);
        }
        if (!ai_resp.ok) {
            std::cerr << "AI request failed: " << ai_resp.error << "\n";
            return 1;
        }
        if (json_output) {
            std::cout << "{\n"
                      << "  \"model\": \"" << otb::json_escape(llm_cfg.model) << "\",\n"
                      << "  \"analysis\": \"" << otb::json_escape(ai_resp.content) << "\"\n"
                      << "}\n";
        } else {
            std::cout << "=== OpenTenBase AIOps " << (cmd == "diagnose" ? "Diagnosis" : "Answer")
                      << " (model: " << llm_cfg.model << ") ===\n\n"
                      << ai_resp.content << "\n";
        }
        return 0;
    } else {
        std::cerr << "Error: unknown command '" << cmd << "'\n";
        print_usage(std::cerr);
        return 2;
    }

    if (json_output) {
        std::cout << otb::report_to_json(report);
    } else {
        otb::print_report_human(report, std::cout);
    }
    return 0;
}
