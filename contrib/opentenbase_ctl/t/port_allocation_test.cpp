#include "utils/utils.h"
#include "log/log.h"

#include <cstring>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <new>
#include <string>
#include <vector>

namespace {

std::deque<int> probe_results;
int probe_call_count = 0;
std::vector<int> probed_ports;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << message << std::endl;
        std::exit(1);
    }
}

void set_probe_results(std::deque<int> results) {
    probe_results.swap(results);
    probe_call_count = 0;
    probed_ports.clear();
}

void expect_probed_ports(const std::vector<int>& expected_ports) {
    expect(probed_ports == expected_ports, "unexpected port probe sequence");
}

void expect_probe_error(const std::deque<int>& results,
                        const std::vector<int>& expected_ports) {
    int node_port = -777;
    int pooler_port = -777;
    int forward_port = -777;

    set_probe_results(results);
    expect(get_available_port_pair("127.0.0.1", 11000, node_port, pooler_port, forward_port,
                                   "user", "password", 22) == -1,
           "probe error should fail allocation");
    expect(probe_call_count == static_cast<int>(expected_ports.size()),
           "probe error should stop immediately");
    expect_probed_ports(expected_ports);
    expect(node_port == -777 && pooler_port == -777 && forward_port == -777,
           "probe error should not change output ports");
}

void expect_zero_initialized_port_fields() {
    alignas(NodeInfo) unsigned char storage[sizeof(NodeInfo)];
    alignas(int) unsigned char zero_storage[sizeof(int)];
    std::memset(storage, 0xA5, sizeof(storage));
    NodeInfo* node = ::new (storage) NodeInfo;
    int* zero = ::new (zero_storage) int(0);

    const auto expect_zero_representation = [zero](const int* value, const char* name) {
        const unsigned char* value_bytes = reinterpret_cast<const unsigned char*>(value);
        const unsigned char* zero_bytes = reinterpret_cast<const unsigned char*>(zero);
        expect(std::memcmp(value_bytes, zero_bytes, sizeof(*value)) == 0,
               std::string(name) + " should default to zero");
    };

    expect_zero_representation(&node->port, "port");
    expect_zero_representation(&node->pooler_port, "pooler_port");
    expect_zero_representation(&node->forward_port, "forward_port");
    expect_zero_representation(&node->gtm_port, "gtm_port");

    node->~NodeInfo();
}

}  // namespace

int check_port_available(const char *, int port, const char *, const char *, int) {
    ++probe_call_count;
    probed_ports.push_back(port);
    expect(!probe_results.empty(), "unexpected port availability probe");
    const int result = probe_results.front();
    probe_results.pop_front();
    return result;
}

void log_info_fmt(const char *, const char *, int, ...) {
}

void log_error_fmt(const char *, const char *, int, ...) {
}

int main() {
    expect_zero_initialized_port_fields();

    int node_port = -1;
    int pooler_port = -1;
    int forward_port = -1;

    set_probe_results({0, 0, 0});
    expect(get_available_port_pair("127.0.0.1", 11000, node_port, pooler_port, forward_port,
                                   "user", "password", 22) == 0,
           "available group should succeed");
    expect(node_port == 11000 && pooler_port == 11001 && forward_port == 11002,
           "available group should allocate consecutive ports");
    expect_probed_ports({11000, 11001, 11002});

    set_probe_results({1, 0, 0, 0});
    expect(get_available_port_pair("127.0.0.1", 11000, node_port, pooler_port, forward_port,
                                   "user", "password", 22) == 0,
           "occupied group should be skipped");
    expect(node_port == 11003 && pooler_port == 11004 && forward_port == 11005,
           "occupied group should advance by three ports");
    expect_probed_ports({11000, 11003, 11004, 11005});

    set_probe_results({0, 1, 0, 0, 0});
    expect(get_available_port_pair("127.0.0.1", 11000, node_port, pooler_port, forward_port,
                                   "user", "password", 22) == 0,
           "group occupied at pooler offset should be skipped");
    expect(node_port == 11003 && pooler_port == 11004 && forward_port == 11005,
           "pooler-offset occupation should advance by three ports");
    expect_probed_ports({11000, 11001, 11003, 11004, 11005});

    set_probe_results({0, 0, 1, 0, 0, 0});
    expect(get_available_port_pair("127.0.0.1", 11000, node_port, pooler_port, forward_port,
                                   "user", "password", 22) == 0,
           "group occupied at forward offset should be skipped");
    expect(node_port == 11003 && pooler_port == 11004 && forward_port == 11005,
           "forward-offset occupation should advance by three ports");
    expect_probed_ports({11000, 11001, 11002, 11003, 11004, 11005});

    set_probe_results({0, 0, 0});
    expect(get_available_port_pair("127.0.0.1", 65533, node_port, pooler_port, forward_port,
                                   "user", "password", 22) == 0,
           "highest complete three-port group should succeed");
    expect(node_port == 65533 && pooler_port == 65534 && forward_port == 65535,
           "boundary group should include port 65535");
    expect_probed_ports({65533, 65534, 65535});

    expect_probe_error({-1, 0, 0}, {11000});
    expect_probe_error({0, -1, 0}, {11000, 11001});
    expect_probe_error({0, 0, -1}, {11000, 11001, 11002});

    return 0;
}
