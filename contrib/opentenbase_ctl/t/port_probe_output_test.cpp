#include "ssh/remote_ssh.h"

#include <cstdlib>
#include <iostream>

namespace {

void
expect(bool condition, const char *message)
{
    if (!condition) {
        std::cerr << "port probe classifier test failed: " << message << '\n';
        std::exit(EXIT_FAILURE);
    }
}

}  // namespace

int
main()
{
    expect(classify_port_probe_output("OPENTENBASE_PORT_AVAILABLE\n") == 0,
           "available marker should be free");
    expect(classify_port_probe_output("OPENTENBASE_PORT_OCCUPIED\n") == 1,
           "occupied marker should be occupied");
    expect(classify_port_probe_output("Warning: Permanently added host\n"
                                      "OPENTENBASE_PORT_AVAILABLE\n") == 0,
           "SSH warning plus available marker should be free");
    expect(classify_port_probe_output("Warning: Permanently added host\n"
                                      "OPENTENBASE_PORT_OCCUPIED\n") == 1,
           "SSH warning plus occupied marker should be occupied");
    expect(classify_port_probe_output("OPENTENBASE_PORT_OCCUPIED\r\n") == 1,
           "CRLF occupied marker should be occupied");
    expect(classify_port_probe_output("OPENTENBASE_PORT_ERROR\n") == -1,
           "error marker should fail");
    expect(classify_port_probe_output("") == -1,
           "missing marker should fail");
    expect(classify_port_probe_output("OPENTENBASE_PORT_AVAILABLE\n"
                                      "OPENTENBASE_PORT_AVAILABLE\n") == -1,
           "duplicate available markers should be ambiguous");
    expect(classify_port_probe_output(
               "OPENTENBASE_PORT_AVAILABLE-not-a-token\n") == -1,
           "partial available marker line should fail");
    expect(classify_port_probe_output(
               "diagnostic: OPENTENBASE_PORT_OCCUPIED is only text\n") == -1,
           "diagnostic containing occupied marker fragment should fail");
    expect(classify_port_probe_output("OPENTENBASE_PORT_AVAILABLE\n"
                                      "OPENTENBASE_PORT_OCCUPIED\n") == -1,
           "available and occupied markers should be ambiguous");
    return 0;
}
