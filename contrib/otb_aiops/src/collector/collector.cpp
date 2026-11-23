#include "collector/collector.h"
#include "collector/storage_collector.h"

#include <sstream>

namespace otb {

namespace {

DiagnosticBlock make_block(const std::string& title, QueryResult qr,
                           const std::string& ok_note,
                           bool warn_if_nonempty = false,
                           bool warn_if_empty = false) {
    DiagnosticBlock b;
    b.title = title;
    b.table = std::move(qr);
    if (!b.table.ok) {
        b.status = "error";
        b.note = "query failed";
    } else if (warn_if_nonempty && !b.table.rows.empty()) {
        b.status = "warn";
        b.note = std::to_string(b.table.rows.size()) + " issue(s) found";
    } else if (warn_if_empty && b.table.rows.empty()) {
        b.status = "warn";
        b.note = "no entries found";
    } else {
        b.status = "ok";
        b.note = ok_note;
    }
    return b;
}

}  // namespace

DiagnosticReport collect_health(PgClient& db) {
    DiagnosticReport rep;
    rep.command = "health";

    rep.blocks.push_back(make_block(
        "Server Version & Uptime",
        db.query("SELECT version(), pg_postmaster_start_time() AS start_time, "
                 "now() - pg_postmaster_start_time() AS uptime;"),
        "connected"));

    rep.blocks.push_back(make_block(
        "Cluster Nodes (pgxc_node)",
        db.query("SELECT node_name, node_type, node_host, node_port, nodeis_primary, nodeis_preferred "
                 "FROM pgxc_node ORDER BY node_type, node_name;"),
        "registered nodes",
        false,
        true));

    rep.blocks.push_back(make_block(
        "Node Groups (pgxc_group)",
        db.query("SELECT group_name, default_group, group_members FROM pgxc_group ORDER BY group_name;"),
        "node groups configured",
        false,
        true));

    rep.blocks.push_back(make_block(
        "Connection Summary",
        db.query("SELECT coalesce(state, 'background') AS state, count(*) AS cnt "
                 "FROM pg_stat_activity GROUP BY 1 ORDER BY 2 DESC;"),
        "active sessions"));

    rep.blocks.push_back(make_block(
        "Replication Status (pg_stat_replication)",
        db.query("SELECT pid, usename, application_name, client_addr, state, sync_state "
                 "FROM pg_stat_replication ORDER BY pid;"),
        "replication overview"));

    return rep;
}

DiagnosticReport collect_locks(PgClient& db) {
    DiagnosticReport rep;
    rep.command = "inspect locks";

    rep.blocks.push_back(make_block(
        "Blocked Sessions",
        db.query(
            "SELECT a.pid AS blocked_pid, a.usename AS blocked_user, "
            "       pg_blocking_pids(a.pid) AS blocking_pids, "
            "       now() - a.query_start AS wait_duration, "
            "       left(a.query, 120) AS blocked_query "
            "FROM pg_stat_activity a "
            "WHERE cardinality(pg_blocking_pids(a.pid)) > 0 "
            "ORDER BY wait_duration DESC LIMIT 20;"),
        "no blocked sessions",
        true,
        false));

    rep.blocks.push_back(make_block(
        "Long-Running Transactions (> 30s)",
        db.query(
            "SELECT pid, usename, state, now() - xact_start AS xact_age, "
            "       left(query, 120) AS query "
            "FROM pg_stat_activity "
            "WHERE xact_start IS NOT NULL "
            "  AND now() - xact_start > interval '30 seconds' "
            "  AND pid <> pg_backend_pid() "
            "ORDER BY xact_age DESC LIMIT 20;"),
        "no long transactions",
        true,
        false));

    return rep;
}

DiagnosticReport collect_2pc(PgClient& db) {
    DiagnosticReport rep;
    rep.command = "inspect 2pc";

    rep.blocks.push_back(make_block(
        "Prepared Transactions (pg_prepared_xacts)",
        db.query(
            "SELECT transaction, gid, prepared, now() - prepared AS age, owner, database "
            "FROM pg_prepared_xacts "
            "ORDER BY prepared ASC LIMIT 50;"),
        "no dangling 2PC transactions",
        true,
        false));

    return rep;
}

DiagnosticReport collect_slow_sql(PgClient& db, int top_n) {
    DiagnosticReport rep;
    rep.command = "inspect slow-sql";

    if (top_n <= 0) top_n = 10;
    std::ostringstream sql;
    sql << "SELECT pid, usename, datname, state, "
        << "       now() - query_start AS duration, "
        << "       left(query, 160) AS query "
        << "FROM pg_stat_activity "
        << "WHERE state <> 'idle' AND pid <> pg_backend_pid() "
        << "  AND query_start IS NOT NULL "
        << "ORDER BY duration DESC LIMIT " << top_n << ";";

    rep.blocks.push_back(make_block(
        "Top Active / Slow Queries",
        db.query(sql.str()),
        "active queries snapshot"));

    return rep;
}

DiagnosticReport collect_guc(PgClient& db) {
    DiagnosticReport rep;
    rep.command = "inspect guc";

    rep.blocks.push_back(make_block(
        "Key GUC Parameters",
        db.query(
            "SELECT name, setting, unit, source "
            "FROM pg_settings "
            "WHERE name IN ("
            "  'max_connections', 'shared_buffers', 'work_mem', 'maintenance_work_mem', "
            "  'max_prepared_transactions', 'wal_level', 'synchronous_commit', "
            "  'statement_timeout', 'idle_in_transaction_session_timeout', "
            "  'log_min_duration_statement'"
            ") ORDER BY name;"),
        "key parameters"));

    return rep;
}

DiagnosticReport collect_all(PgClient& db, int top_n) {
    DiagnosticReport all;
    all.command = "diagnose";

    auto append = [&](DiagnosticReport sub) {
        for (auto& b : sub.blocks) {
            all.blocks.push_back(std::move(b));
        }
    };

    append(collect_health(db));
    append(collect_locks(db));
    append(collect_2pc(db));
    append(collect_slow_sql(db, top_n));
    append(collect_guc(db));
    append(collect_storage(db, top_n));
    return all;
}

}  // namespace otb
