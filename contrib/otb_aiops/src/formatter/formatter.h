#ifndef OTB_AIOPS_FORMATTER_H
#define OTB_AIOPS_FORMATTER_H

#include "db/pg_client.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace otb {

struct DiagnosticBlock {
    std::string title;
    std::string status = "ok";  // ok | warn | error
    std::string note;
    QueryResult table;
};

struct DiagnosticReport {
    std::string command;
    std::vector<DiagnosticBlock> blocks;
};

inline std::string json_escape(const std::string& s) {
    std::ostringstream oss;
    for (unsigned char c : s) {
        switch (c) {
            case '\"': oss << "\\\""; break;
            case '\\': oss << "\\\\"; break;
            case '\b': oss << "\\b"; break;
            case '\f': oss << "\\f"; break;
            case '\n': oss << "\\n"; break;
            case '\r': oss << "\\r"; break;
            case '\t': oss << "\\t"; break;
            default:
                if (c < 0x20) {
                    oss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(c) << std::dec;
                } else {
                    oss << static_cast<char>(c);
                }
        }
    }
    return oss.str();
}

inline std::string report_to_json(const DiagnosticReport& report) {
    std::ostringstream oss;
    oss << "{\n";
    oss << "  \"command\": \"" << json_escape(report.command) << "\",\n";
    oss << "  \"sections\": [\n";
    for (size_t i = 0; i < report.blocks.size(); ++i) {
        const auto& b = report.blocks[i];
        oss << "    {\n";
        oss << "      \"title\": \"" << json_escape(b.title) << "\",\n";
        oss << "      \"status\": \"" << json_escape(b.status) << "\",\n";
        oss << "      \"note\": \"" << json_escape(b.note) << "\",\n";
        oss << "      \"query_ok\": " << (b.table.ok ? "true" : "false") << ",\n";
        if (!b.table.ok) {
            oss << "      \"error\": \"" << json_escape(b.table.error) << "\",\n";
        }
        oss << "      \"rows\": [\n";
        for (size_t r = 0; r < b.table.rows.size(); ++r) {
            oss << "        {";
            for (size_t c = 0; c < b.table.columns.size() && c < b.table.rows[r].size(); ++c) {
                if (c > 0) oss << ", ";
                oss << "\"" << json_escape(b.table.columns[c]) << "\": \""
                    << json_escape(b.table.rows[r][c]) << "\"";
            }
            oss << "}" << (r + 1 < b.table.rows.size() ? "," : "") << "\n";
        }
        oss << "      ]\n";
        oss << "    }" << (i + 1 < report.blocks.size() ? "," : "") << "\n";
    }
    oss << "  ]\n";
    oss << "}\n";
    return oss.str();
}

inline void print_table(const QueryResult& qr, std::ostream& out) {
    if (!qr.ok) {
        out << "  [Query Error] " << qr.error << "\n";
        return;
    }
    if (qr.columns.empty()) {
        out << "  (no columns)\n";
        return;
    }
    if (qr.rows.empty()) {
        out << "  (0 rows)\n";
        return;
    }

    const size_t max_col_width = 48;
    std::vector<size_t> widths(qr.columns.size(), 0);
    for (size_t c = 0; c < qr.columns.size(); ++c) {
        widths[c] = std::min(qr.columns[c].size(), max_col_width);
    }
    for (const auto& row : qr.rows) {
        for (size_t c = 0; c < row.size() && c < widths.size(); ++c) {
            widths[c] = std::min(std::max(widths[c], row[c].size()), max_col_width);
        }
    }

    auto trunc = [&](const std::string& s, size_t w) -> std::string {
        std::string clean;
        clean.reserve(s.size());
        for (char ch : s) {
            clean.push_back((ch == '\n' || ch == '\r' || ch == '\t') ? ' ' : ch);
        }
        if (clean.size() <= w) return clean;
        if (w <= 3) return clean.substr(0, w);
        return clean.substr(0, w - 3) + "...";
    };

    out << "  ";
    for (size_t c = 0; c < qr.columns.size(); ++c) {
        if (c > 0) out << " | ";
        out << std::left << std::setw(static_cast<int>(widths[c])) << trunc(qr.columns[c], widths[c]);
    }
    out << "\n  ";
    for (size_t c = 0; c < qr.columns.size(); ++c) {
        if (c > 0) out << "-+-";
        out << std::string(widths[c], '-');
    }
    out << "\n";

    for (const auto& row : qr.rows) {
        out << "  ";
        for (size_t c = 0; c < qr.columns.size(); ++c) {
            if (c > 0) out << " | ";
            std::string val = (c < row.size()) ? row[c] : "";
            out << std::left << std::setw(static_cast<int>(widths[c])) << trunc(val, widths[c]);
        }
        out << "\n";
    }
}

inline void print_report_human(const DiagnosticReport& report, std::ostream& out) {
    out << "=== OpenTenBase AIOps Report: " << report.command << " ===\n";
    for (const auto& b : report.blocks) {
        out << "\n[" << b.status << "] " << b.title;
        if (!b.note.empty()) {
            out << " -- " << b.note;
        }
        out << "\n";
        print_table(b.table, out);
    }
    out << "\n";
}

}  // namespace otb

#endif  // OTB_AIOPS_FORMATTER_H
