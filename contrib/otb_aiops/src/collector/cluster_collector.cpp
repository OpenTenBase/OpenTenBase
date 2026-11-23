#include "collector/cluster_collector.h"
#include "collector/collector.h"
#include "collector/storage_collector.h"

#include <charconv>
#include <utility>

namespace otb {

std::vector<ClusterNode> parse_pgxc_nodes(const QueryResult& qr) {
    std::vector<ClusterNode> nodes;
    if (!qr.ok) return nodes;

    for (const auto& row : qr.rows) {
        if (row.size() != 4 || (row[1] != "C" && row[1] != "D")) continue;
        if (row[0].empty() || row[2].empty()) continue;

        int port = 0;
        const char* end = row[3].data() + row[3].size();
        const auto parsed = std::from_chars(row[3].data(), end, port);
        if (parsed.ec != std::errc{} || parsed.ptr != end || port < 1 || port > 65535) continue;

        nodes.push_back({row[0], row[1], row[2], row[3]});
    }
    return nodes;
}

DiagnosticReport collect_cluster(PgClient& entry_db, const ConnConfig& base_cfg,
                                 const std::string& target, int top_n) {
    DiagnosticReport rep;
    rep.command = "cluster " + target;

    auto collect = [&](PgClient& db) {
        if (target == "health") return collect_health(db);
        if (target == "locks") return collect_locks(db);
        if (target == "2pc") return collect_2pc(db);
        if (target == "slow-sql") return collect_slow_sql(db, top_n);
        if (target == "guc") return collect_guc(db);
        if (target == "storage") return collect_storage(db, top_n);
        if (target == "all") return collect_all(db, top_n);

        DiagnosticReport sub;
        DiagnosticBlock b;
        b.title = "Collector";
        b.status = "error";
        b.table.error = "unknown collector: " + target;
        sub.blocks.push_back(std::move(b));
        return sub;
    };

    const auto discovery = entry_db.query(
        "SELECT node_name, node_type, node_host, node_port "
        "FROM pgxc_node ORDER BY node_type, node_name;");
    const auto nodes = parse_pgxc_nodes(discovery);
    size_t connected = 0;

    for (const auto& node : nodes) {
        const std::string prefix = "[" + node.name + " (" + node.type + ")] ";
        ConnConfig cfg = base_cfg;
        cfg.host = node.host;
        cfg.port = node.port;
        PgClient db;
        std::string error;
        if (!db.connect(cfg, error)) {
            DiagnosticBlock b;
            b.title = prefix + "Connection";
            b.status = "error";
            b.note = "connection failed";
            b.table.error = std::move(error);
            rep.blocks.push_back(std::move(b));
            continue;
        }

        ++connected;
        auto sub = collect(db);
        for (auto& b : sub.blocks) {
            b.title = prefix + b.title;
            rep.blocks.push_back(std::move(b));
        }
    }

    const auto failed = nodes.size() - connected;
    DiagnosticBlock summary;
    summary.title = "Cluster Fan-Out Summary";
    summary.table.ok = true;
    summary.table.columns = {"total", "connected", "failed"};
    summary.table.rows = {{std::to_string(nodes.size()), std::to_string(connected),
                           std::to_string(failed)}};
    if (!discovery.ok) {
        summary.status = "error";
        summary.note = "pgxc_node discovery failed: " + discovery.error;
    } else if (nodes.empty()) {
        summary.status = "warn";
        summary.note = "no coordinator or datanode entries found";
    } else if (failed != 0) {
        summary.status = connected == 0 ? "error" : "warn";
        summary.note = "node connection failures";
    }
    rep.blocks.insert(rep.blocks.begin(), std::move(summary));
    return rep;
}

}  // namespace otb
