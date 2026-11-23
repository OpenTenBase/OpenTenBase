#include "collector/storage_collector.h"

#include <sstream>

namespace otb {

namespace {

DiagnosticBlock make_block(const std::string& title, QueryResult qr,
                           const std::string& ok_note,
                           bool warn_if_nonempty = false) {
    DiagnosticBlock b;
    b.title = title;
    b.table = std::move(qr);
    if (!b.table.ok) {
        b.status = "error";
        b.note = "query failed";
    } else if (warn_if_nonempty && !b.table.rows.empty()) {
        b.status = "warn";
        b.note = std::to_string(b.table.rows.size()) + " issue(s) found";
    } else {
        b.status = "ok";
        b.note = ok_note;
    }
    return b;
}

}  // namespace

DiagnosticReport collect_storage(PgClient& db, int top_n) {
    DiagnosticReport rep;
    rep.command = "inspect storage";

    if (top_n <= 0) top_n = 10;
    std::ostringstream sql;
    sql << "SELECT schemaname, relname, pg_total_relation_size(relid) AS total_bytes "
        << "FROM pg_stat_user_tables "
        << "ORDER BY total_bytes DESC NULLS LAST, schemaname, relname LIMIT " << top_n << ";";

    rep.blocks.push_back(make_block(
        "Top Largest Relations",
        db.query(sql.str()),
        "relation sizes"));

    rep.blocks.push_back(make_block(
        "Table Bloat / Dead Tuples",
        db.query(
            "SELECT schemaname, relname, n_live_tup, n_dead_tup, "
            "       round(dead_ratio, 2) AS dead_ratio, last_vacuum, last_autovacuum "
            "FROM ("
            "  SELECT schemaname, relname, n_live_tup, n_dead_tup, last_vacuum, last_autovacuum, "
            "         100.0 * n_dead_tup / NULLIF(n_live_tup::numeric + n_dead_tup, 0) AS dead_ratio "
            "  FROM pg_stat_user_tables "
            "  WHERE n_dead_tup > 10000"
            ") AS tables "
            "WHERE tables.dead_ratio > 20 "
            "ORDER BY tables.dead_ratio DESC, n_dead_tup DESC;"),
        "no tables above dead tuple thresholds",
        true));

    return rep;
}

}  // namespace otb
