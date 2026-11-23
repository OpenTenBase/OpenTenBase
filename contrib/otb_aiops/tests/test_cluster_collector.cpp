/*
 * Build from contrib/otb_aiops with the cluster collector available.
 *
 * c++ -std=c++17 -Wall -Wextra -ffunction-sections -fdata-sections -Isrc \
 *     $(pkg-config --cflags libpq) tests/test_cluster_collector.cpp \
 *     src/collector/cluster_collector.cpp -Wl,--gc-sections -o /tmp/test_cluster_collector
 */

#include "collector/cluster_collector.h"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#include <iostream>

namespace {

void test_parse_pgxc_nodes_filters_node_types_and_preserves_fields() {
    const otb::QueryResult result{
        true, "", {"node_name", "node_type", "node_host", "node_port"},
        {
            {"coord1", "C", "coord.example", "5432"},
            {"gtm1", "G", "gtm.example", "6666"},
            {"data1", "D", "data.example", "15432"},
        },
    };

    const auto nodes = otb::parse_pgxc_nodes(result);
    assert(nodes.size() == 2);
    assert(nodes[0].name == "coord1");
    assert(nodes[0].type == "C");
    assert(nodes[0].host == "coord.example");
    assert(nodes[0].port == "5432");
    assert(nodes[1].name == "data1");
    assert(nodes[1].type == "D");
    assert(nodes[1].host == "data.example");
    assert(nodes[1].port == "15432");
}

void test_parse_pgxc_nodes_failed_query() {
    const otb::QueryResult result{
        false, "permission denied", {"node_name", "node_type", "node_host", "node_port"},
        {{"coord1", "C", "coord.example", "5432"}},
    };

    assert(otb::parse_pgxc_nodes(result).empty());
}

void test_parse_pgxc_nodes_skips_incomplete_rows() {
    const otb::QueryResult result{
        true, "", {"node_name", "node_type", "node_host", "node_port"},
        {
            {"missing_port", "C", "coord.example"},
            {"empty_host", "D", "", "15432"},
            {"empty_port", "C", "coord.example", ""},
            {"data1", "D", "data.example", "15432"},
        },
    };

    const auto nodes = otb::parse_pgxc_nodes(result);
    assert(nodes.size() == 1);
    assert(nodes[0].name == "data1");
}

}  // namespace

int main() {
    test_parse_pgxc_nodes_filters_node_types_and_preserves_fields();
    test_parse_pgxc_nodes_failed_query();
    test_parse_pgxc_nodes_skips_incomplete_rows();
    std::cout << "All cluster collector tests passed\n";
    return 0;
}
