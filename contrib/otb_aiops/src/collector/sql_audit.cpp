#include "collector/sql_audit.h"

#include <algorithm>
#include <regex>
#include <utility>

namespace otb {

namespace {

std::vector<std::string> sql_tokens(const std::string& sql) {
    static const std::regex token(
        R"SQL(--[^\r\n]*|/\*|[eE]'(?:''|\\[\s\S]|[^'\\])*'|'(?:''|[^'])*'|"(?:""|[^"])*"|\$(?:[a-zA-Z_][a-zA-Z_0-9]*)?\$|[a-zA-Z_\x80-\xff][a-zA-Z_0-9$\x80-\xff]*|[^\s])SQL");
    std::vector<std::string> tokens;
    auto cursor = sql.cbegin();
    std::smatch match;
    while (std::regex_search(cursor, sql.cend(), match, token)) {
        cursor = match[0].second;
        std::string word = match.str();
        if (word.compare(0, 2, "--") == 0) continue;
        if (word == "/*") {
            int depth = 1;
            while (cursor != sql.cend() && depth > 0) {
                if (sql.cend() - cursor >= 2 && *cursor == '/' && cursor[1] == '*') {
                    ++depth;
                    cursor += 2;
                } else if (sql.cend() - cursor >= 2 && *cursor == '*' && cursor[1] == '/') {
                    --depth;
                    cursor += 2;
                } else {
                    ++cursor;
                }
            }
            continue;
        }

        if (word.size() > 1 && word.front() == '$') {
            auto end = std::search(cursor, sql.cend(), word.begin(), word.end());
            cursor = end == sql.cend() ? end : end + word.size();
            word = "<literal>";
        } else if (word.front() == '\'' || word.front() == '"' ||
                   (word.size() > 1 && word[1] == '\'' &&
                    (word.front() == 'e' || word.front() == 'E'))) {
            word = "<quoted>";
        } else {
            for (char& ch : word) {
                if (ch >= 'a' && ch <= 'z') ch = ch - 'a' + 'A';
            }
        }
        tokens.push_back(std::move(word));
    }
    return tokens;
}

}  // namespace

std::vector<AuditFinding> static_audit_sql(const std::string& sql) {
    const auto tokens = sql_tokens(sql);
    std::vector<AuditFinding> findings;
    for (size_t i = 0; i < tokens.size(); ++i) {
        const auto& word = tokens[i];
        const std::string next = i + 1 < tokens.size() ? tokens[i + 1] : "";
        const bool locking_clause = i > 0 && tokens[i - 1] == "FOR";
        const bool no_key_lock = i >= 3 && tokens[i - 3] == "FOR" &&
                                 tokens[i - 2] == "NO" && tokens[i - 1] == "KEY";
        if ((word == "DELETE" && next == "FROM") ||
            (word == "UPDATE" && next != "SET" && !locking_clause && !no_key_lock)) {
            int depth = 0;
            bool has_where = false;
            for (size_t j = i + 1; j < tokens.size(); ++j) {
                const auto& part = tokens[j];
                if (part == ";" || (part == ")" && depth == 0)) break;
                if (part == "WHERE" && depth == 0) {
                    has_where = true;
                    break;
                }
                if (part == "(") ++depth;
                if (part == ")") --depth;
            }
            if (!has_where) {
                findings.push_back({"CRITICAL", word + "_WITHOUT_WHERE",
                                    word + " has no WHERE clause"});
            }
        } else if (word == "DROP" && next == "TABLE") {
            findings.push_back({"CRITICAL", "DROP_TABLE", "DROP TABLE removes a table"});
        } else if (word == "TRUNCATE") {
            findings.push_back({"CRITICAL", "TRUNCATE", "TRUNCATE removes all rows"});
        } else if (word == "SELECT" && next == "*") {
            findings.push_back({"WARN", "SELECT_STAR", "SELECT * retrieves all columns"});
        } else if (word == "ALTER" && next == "TABLE") {
            findings.push_back({"WARN", "ALTER_TABLE", "ALTER TABLE changes a table definition"});
        }
    }
    return findings;
}

DiagnosticReport audit_sql(PgClient& db, const std::string& sql) {
    DiagnosticReport report;
    report.command = "audit";

    DiagnosticBlock audit;
    audit.title = "Static SQL Audit";
    audit.table.ok = true;
    audit.table.columns = {"severity", "rule", "detail"};
    for (const auto& finding : static_audit_sql(sql)) {
        audit.table.rows.push_back({finding.severity, finding.rule, finding.detail});
        if (finding.severity == "CRITICAL") {
            audit.status = "error";
        } else if (audit.status == "ok") {
            audit.status = "warn";
        }
    }
    report.blocks.push_back(std::move(audit));

    DiagnosticBlock plan;
    plan.title = "Execution Plan (EXPLAIN)";
    const auto tokens = sql_tokens(sql);
    const auto terminator = std::find(tokens.begin(), tokens.end(), ";");
    const auto semicolon = sql.find(';');
    // Backslash handling in ordinary string literals depends on the server session.
    const bool ambiguous_terminator = sql.find('\\') != std::string::npos &&
        semicolon != std::string::npos &&
        sql.find_first_not_of(" \t\n\r\f\v", semicolon + 1) != std::string::npos;
    // PgClient uses PQexec, which accepts multiple commands in one call.
    // Also exclude inputs that can supply options to the EXPLAIN prefix.
    if (tokens.empty() || tokens.front() == "(" || tokens.front() == "ANALYZE" ||
        tokens.front() == "ANALYSE" || tokens.front() == "VERBOSE" ||
        sql.find('\0') != std::string::npos ||
        ambiguous_terminator ||
        (terminator != tokens.end() && terminator + 1 != tokens.end())) {
        plan.table.error = "EXPLAIN requires a single statement without EXPLAIN options";
    } else {
        plan.table = db.query("EXPLAIN " + sql);
    }
    if (!plan.table.ok) {
        plan.status = "error";
        plan.note = "query failed";
    } else {
        for (const auto& row : plan.table.rows) {
            if (!row.empty() && row.front().find("Seq Scan") != std::string::npos) {
                plan.status = "warn";
                plan.note = "full table scan detected";
                break;
            }
        }
    }
    report.blocks.push_back(std::move(plan));

    return report;
}

}  // namespace otb
