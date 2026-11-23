#ifndef OTB_AIOPS_STORAGE_COLLECTOR_H
#define OTB_AIOPS_STORAGE_COLLECTOR_H

#include "db/pg_client.h"
#include "formatter/formatter.h"

namespace otb {

DiagnosticReport collect_storage(PgClient& db, int top_n);

}  // namespace otb

#endif  // OTB_AIOPS_STORAGE_COLLECTOR_H
