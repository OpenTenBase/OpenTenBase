#ifndef OTB_AIOPS_CLUSTER_COLLECTOR_H
#define OTB_AIOPS_CLUSTER_COLLECTOR_H

#include "db/pg_client.h"
#include "formatter/formatter.h"

#include <string>
#include <vector>

namespace otb {

struct ClusterNode {
    std::string name, type, host, port;
};

std::vector<ClusterNode> parse_pgxc_nodes(const QueryResult& qr);
DiagnosticReport collect_cluster(PgClient& entry_db, const ConnConfig& base_cfg,
                                 const std::string& target, int top_n);

}  // namespace otb

#endif  // OTB_AIOPS_CLUSTER_COLLECTOR_H
