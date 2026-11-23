#ifndef OTB_AIOPS_PG_CLIENT_H
#define OTB_AIOPS_PG_CLIENT_H

#include <libpq-fe.h>
#include <string>
#include <vector>

namespace otb {

struct ConnConfig {
    std::string host;
    std::string port;
    std::string user;
    std::string dbname;
    std::string password;
    int connect_timeout_sec = 5;
    int statement_timeout_ms = 5000;

    static ConnConfig from_env();
};

struct QueryResult {
    bool ok = false;
    std::string error;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

class PgClient {
public:
    PgClient() = default;
    ~PgClient();

    PgClient(const PgClient&) = delete;
    PgClient& operator=(const PgClient&) = delete;

    bool connect(const ConnConfig& cfg, std::string& err_out);
    void disconnect();
    bool is_connected() const { return conn_ != nullptr; }

    QueryResult query(const std::string& sql);

private:
    PGconn* conn_ = nullptr;
};

}  // namespace otb

#endif  // OTB_AIOPS_PG_CLIENT_H
