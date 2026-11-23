#ifndef OTB_AIOPS_COLLECTOR_H
#define OTB_AIOPS_COLLECTOR_H

#include "db/pg_client.h"
#include "formatter/formatter.h"

namespace otb {

DiagnosticReport collect_health(PgClient& db);
DiagnosticReport collect_locks(PgClient& db);
DiagnosticReport collect_2pc(PgClient& db);
DiagnosticReport collect_slow_sql(PgClient& db, int top_n);
DiagnosticReport collect_guc(PgClient& db);
DiagnosticReport collect_all(PgClient& db, int top_n);

}  // namespace otb

#endif  // OTB_AIOPS_COLLECTOR_H
