#include "db/pg_client.h"

#include <cstdlib>
#include <sstream>

namespace otb {

namespace {

std::string get_env_or(const char* name, const std::string& fallback) {
    const char* val = std::getenv(name);
    return (val && *val) ? std::string(val) : fallback;
}

std::string escape_conn_val(const std::string& v) {
    std::string out = "'";
    for (char c : v) {
        if (c == '\\' || c == '\'') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    out.push_back('\'');
    return out;
}

}  // namespace

ConnConfig ConnConfig::from_env() {
    ConnConfig cfg;
    cfg.host = get_env_or("PGHOST", "127.0.0.1");
    cfg.port = get_env_or("PGPORT", "5432");
    cfg.user = get_env_or("PGUSER", "opentenbase");
    cfg.dbname = get_env_or("PGDATABASE", "postgres");
    cfg.password = get_env_or("PGPASSWORD", "");
    return cfg;
}

PgClient::~PgClient() {
    disconnect();
}

void PgClient::disconnect() {
    if (conn_) {
        PQfinish(conn_);
        conn_ = nullptr;
    }
}

bool PgClient::connect(const ConnConfig& cfg, std::string& err_out) {
    disconnect();

    std::ostringstream oss;
    oss << "host=" << escape_conn_val(cfg.host)
        << " port=" << escape_conn_val(cfg.port)
        << " user=" << escape_conn_val(cfg.user)
        << " dbname=" << escape_conn_val(cfg.dbname)
        << " connect_timeout=" << cfg.connect_timeout_sec
        << " application_name='otb_aiops'";
    if (!cfg.password.empty()) {
        oss << " password=" << escape_conn_val(cfg.password);
    }

    conn_ = PQconnectdb(oss.str().c_str());
    if (!conn_) {
        err_out = "PQconnectdb returned null (out of memory)";
        return false;
    }
    if (PQstatus(conn_) != CONNECTION_OK) {
        err_out = PQerrorMessage(conn_);
        disconnect();
        return false;
    }

    // Enforce read-only session and statement timeout for production safety.
    std::ostringstream guard_sql;
    guard_sql << "SET default_transaction_read_only = on; "
              << "SET statement_timeout = " << cfg.statement_timeout_ms << ";";
    PGresult* res = PQexec(conn_, guard_sql.str().c_str());
    if (res) {
        PQclear(res);
    }
    return true;
}

QueryResult PgClient::query(const std::string& sql) {
    QueryResult qr;
    if (!conn_) {
        qr.ok = false;
        qr.error = "Not connected to database";
        return qr;
    }

    PGresult* res = PQexec(conn_, sql.c_str());
    if (!res) {
        qr.ok = false;
        qr.error = PQerrorMessage(conn_);
        return qr;
    }

    ExecStatusType st = PQresultStatus(res);
    if (st != PGRES_TUPLES_OK && st != PGRES_COMMAND_OK) {
        qr.ok = false;
        qr.error = PQresultErrorMessage(res);
        PQclear(res);
        return qr;
    }

    int ncols = PQnfields(res);
    int nrows = PQntuples(res);
    qr.columns.reserve(ncols);
    for (int c = 0; c < ncols; ++c) {
        qr.columns.emplace_back(PQfname(res, c));
    }

    qr.rows.reserve(nrows);
    for (int r = 0; r < nrows; ++r) {
        std::vector<std::string> row;
        row.reserve(ncols);
        for (int c = 0; c < ncols; ++c) {
            if (PQgetisnull(res, r, c)) {
                row.emplace_back("NULL");
            } else {
                row.emplace_back(PQgetvalue(res, r, c));
            }
        }
        qr.rows.push_back(std::move(row));
    }

    PQclear(res);
    qr.ok = true;
    return qr;
}

}  // namespace otb
