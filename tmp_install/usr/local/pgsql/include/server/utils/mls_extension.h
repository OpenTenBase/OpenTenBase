/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause
 * 
 */
#ifndef _MLS_EXTENSION_H
#define _MLS_EXTENSION_H
/* the marco of MLS define*/
#define MLS_EXTENSION_NAME "opentenbase_mls"
#define MLS_EXTENSION_NAMESPACE_NAME "mls"
#define MLS_USER_PREFIX      "mls_"
#define MLS_USER_PREFIX_LEN  3

#define MLS_RELATION_ACL_NAME       "pg_mls_relation_acl"
#define Natts_pg_mls_relation_acl          2
#define Anum_pg_mls_relation_acl_userid    1
#define Anum_pg_mls_relation_acl_relid     2

#define MLS_SCHEMA_ACL_NAME         "pg_mls_schema_acl"
#define Natts_pg_mls_schema_acl          2
#define Anum_pg_mls_schema_acl_userid    1
#define Anum_pg_mls_schema_acl_relid     2

#endif /* _MLS_EXTENSION_H */
