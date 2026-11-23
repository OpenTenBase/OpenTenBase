#ifndef OTB_AIOPS_SQL_AUDIT_H
#define OTB_AIOPS_SQL_AUDIT_H

#include "formatter/formatter.h"

#include <string>
#include <vector>

namespace otb {

struct AuditFinding {
    std::string severity;
    std::string rule;
    std::string detail;
};

std::vector<AuditFinding> static_audit_sql(const std::string& sql);
DiagnosticReport audit_sql(PgClient& db, const std::string& sql);

}  // namespace otb

#endif  // OTB_AIOPS_SQL_AUDIT_H
