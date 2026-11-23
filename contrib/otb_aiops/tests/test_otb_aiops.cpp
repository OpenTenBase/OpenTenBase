/*
 * Build from contrib/otb_aiops with a GCC-compatible compiler and ELF linker.
 *
 * c++ -std=c++17 -Wall -Wextra -ffunction-sections -fdata-sections -Isrc \
 *     $(pkg-config --cflags libpq libcurl) tests/test_otb_aiops.cpp \
 *     src/db/pg_client.cpp src/ai/llm_client.cpp -Wl,--gc-sections -o /tmp/test_otb_aiops
 *
 * The client headers are needed to compile. The linker discards unused I/O code.
 */

#include "ai/llm_client.h"
#include "formatter/formatter.h"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

namespace {

void test_json_escape() {
    assert(otb::json_escape("").empty());
    assert(otb::json_escape("plain text") == "plain text");
    assert(otb::json_escape("\"\\\n\t\r\b\f") == R"(\"\\\n\t\r\b\f)");
    assert(otb::json_escape(std::string("a\0\x01\x1f", 4)) == R"(a\u0000\u0001\u001f)");
}

void test_report_to_json() {
    const otb::DiagnosticReport report{
        "check\"all",
        {
            {"Nodes", "ok", "", {true, "", {"name", "state"}, {{"node1", "up"}, {"node2", "ready\nnow"}}}},
            {"Activity", "warn", "No samples", {true, "", {"count"}, {}}},
            {"Locks", "error", "", {false, "permission denied", {}, {}}},
        },
    };

    assert(otb::report_to_json(report) == R"({
  "command": "check\"all",
  "sections": [
    {
      "title": "Nodes",
      "status": "ok",
      "note": "",
      "query_ok": true,
      "rows": [
        {"name": "node1", "state": "up"},
        {"name": "node2", "state": "ready\nnow"}
      ]
    },
    {
      "title": "Activity",
      "status": "warn",
      "note": "No samples",
      "query_ok": true,
      "rows": [
      ]
    },
    {
      "title": "Locks",
      "status": "error",
      "note": "",
      "query_ok": false,
      "error": "permission denied",
      "rows": [
      ]
    }
  ]
}
)");
}

otb::QueryResult sample_table() {
    return {true, "", {std::string(49, 'h'), "state"},
            {{std::string(60, 'x'), "up\nnow\r\tok"}, {std::string(48, 'y'), "ready"}}};
}

std::string expected_table() {
    return "  " + std::string(45, 'h') + "... | state     \n"
         + "  " + std::string(48, '-') + "-+-" + std::string(10, '-') + "\n"
         + "  " + std::string(45, 'x') + "... | up now  ok\n"
         + "  " + std::string(48, 'y') + " | ready     \n";
}

void test_print_table() {
    std::ostringstream out;
    otb::print_table(sample_table(), out);
    otb::print_table({true, "", {"state"}, {}}, out);
    otb::print_table({true, "", {}, {}}, out);
    otb::print_table({false, "permission denied", {}, {}}, out);

    assert(out.str() == expected_table()
        + "  (0 rows)\n"
          "  (no columns)\n"
          "  [Query Error] permission denied\n");
}

void test_print_report_human() {
    const otb::DiagnosticReport report{
        "health",
        {
            {"Nodes", "ok", "", sample_table()},
            {"Activity", "warn", "No samples", {true, "", {"count"}, {}}},
            {"Locks", "error", "", {false, "permission denied", {}, {}}},
        },
    };
    std::ostringstream out;
    otb::print_report_human(report, out);
    assert(out.str() == "=== OpenTenBase AIOps Report: health ===\n\n[ok] Nodes\n"
        + expected_table()
        + "\n[warn] Activity -- No samples\n"
          "  (0 rows)\n"
          "\n[error] Locks\n"
          "  [Query Error] permission denied\n\n");
}

void set_env(const char* name, const char* value) {
    const int result = value ? setenv(name, value, 1) : unsetenv(name);
    assert(result == 0);
}

void test_conn_config_from_env() {
    for (const char* name : {"PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD"}) {
        set_env(name, nullptr);
    }
    const auto defaults = otb::ConnConfig::from_env();
    assert(defaults.host == "127.0.0.1");
    assert(defaults.port == "5432");
    assert(defaults.user == "opentenbase");
    assert(defaults.dbname == "postgres");
    assert(defaults.password.empty());
    assert(defaults.connect_timeout_sec == 5);
    assert(defaults.statement_timeout_ms == 5000);

    set_env("PGHOST", "db.example");
    set_env("PGPORT", "6543");
    set_env("PGUSER", "ops");
    set_env("PGDATABASE", "metrics");
    set_env("PGPASSWORD", "test-password");
    const auto custom = otb::ConnConfig::from_env();
    assert(custom.host == "db.example");
    assert(custom.port == "6543");
    assert(custom.user == "ops");
    assert(custom.dbname == "metrics");
    assert(custom.password == "test-password");

    set_env("PGHOST", "");
    assert(otb::ConnConfig::from_env().host == defaults.host);
}

void test_llm_config_from_env() {
    for (const char* name : {"OTB_AI_BASE_URL", "OTB_AI_API_KEY", "OPENAI_API_KEY", "OTB_AI_MODEL"}) {
        set_env(name, nullptr);
    }
    const auto defaults = otb::LlmConfig::from_env();
    assert(defaults.api_url == "https://api.openai.com/v1/chat/completions");
    assert(defaults.api_key.empty());
    assert(defaults.model == "gpt-4o-mini");
    assert(defaults.timeout_sec == 60);

    set_env("OPENAI_API_KEY", "fallback-key");
    assert(otb::LlmConfig::from_env().api_key == "fallback-key");

    set_env("OTB_AI_API_KEY", "preferred-key");
    set_env("OTB_AI_BASE_URL", "https://api.example/v1/chat/completions");
    set_env("OTB_AI_MODEL", "test-model");
    const auto custom = otb::LlmConfig::from_env();
    assert(custom.api_key == "preferred-key");
    assert(custom.api_url == "https://api.example/v1/chat/completions");
    assert(custom.model == "test-model");

    set_env("OTB_AI_API_KEY", "");
    assert(otb::LlmConfig::from_env().api_key == "fallback-key");
}

}  // namespace

int main() {
    test_json_escape();
    test_report_to_json();
    test_print_table();
    test_print_report_human();
    test_conn_config_from_env();
    test_llm_config_from_env();
    std::cout << "All otb_aiops tests passed\n";
    return 0;
}
