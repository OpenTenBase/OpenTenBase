#include "collector/sql_audit.h"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#include <iostream>
#include <string>

namespace {

void test_delete_without_where() {
    const auto findings = otb::static_audit_sql("DELETE FROM users;");
    assert(!findings.empty());
    assert(findings.front().severity == "CRITICAL");
    assert(findings.front().detail.find("WHERE") != std::string::npos);
}

void test_update_without_where() {
    const auto findings = otb::static_audit_sql("UPDATE users SET a=1;");
    assert(!findings.empty());
    assert(findings.front().severity == "CRITICAL");
    assert(findings.front().detail.find("WHERE") != std::string::npos);
}

void test_drop_table() {
    const auto findings = otb::static_audit_sql("DROP TABLE foo;");
    assert(!findings.empty());
    assert(findings.front().severity == "CRITICAL");
}

void test_select_star() {
    const auto findings = otb::static_audit_sql("SELECT * FROM users WHERE id = 1;");
    assert(!findings.empty());
    assert(findings.front().severity == "WARN");
    assert(findings.front().detail.find("SELECT *") != std::string::npos);
}

void test_select_named_column() {
    assert(otb::static_audit_sql("SELECT id FROM users WHERE id = 1;").empty());
}

}  // namespace

int main() {
    test_delete_without_where();
    test_update_without_where();
    test_drop_table();
    test_select_star();
    test_select_named_column();
    std::cout << "All SQL audit tests passed\n";
    return 0;
}
