/*-------------------------------------------------------------------------
 *
 * objectaddress.h
 *      functions for working with object addresses
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/catalog/objectaddress.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OBJECTADDRESS_H
#define OBJECTADDRESS_H

#include "nodes/pg_list.h"
#include "storage/lockdefs.h"
#include "utils/acl.h"
#include "utils/relcache.h"

/*
 * An ObjectAddress represents a database object of any type.
 */
typedef struct ObjectAddress
{
    Oid            classId;        /* Class Id from pg_class */
    Oid            objectId;        /* OID of the object */
    int32        objectSubId;    /* Subitem within object (eg column), or 0 */
} ObjectAddress;

extern const ObjectAddress InvalidObjectAddress;

/*
 * Compare whether two ObjectAddress are the same
 */
#define ObjectAddressIsEqual(addr1, addr2) \
	 ((addr1).classId == (addr2).classId &&  \
		(addr1).objectId == (addr2).objectId &&  \
		(addr1).objectSubId == (addr2).objectSubId)

#define ObjectAddressSubSet(addr, class_id, object_id, object_sub_id) \
    do { \
        (addr).classId = (class_id); \
        (addr).objectId = (object_id); \
        (addr).objectSubId = (object_sub_id); \
    } while (0)

#define ObjectAddressSet(addr, class_id, object_id) \
    ObjectAddressSubSet(addr, class_id, object_id, 0)

#ifdef __OPENTENBASE__
extern char *GetRemoveObjectName(ObjectType objtype, Node *object);
#endif

extern ObjectAddress get_object_address(ObjectType objtype, Node *object,
                   Relation *relp,
                   LOCKMODE lockmode, bool missing_ok);

extern ObjectAddress get_object_address_rv(ObjectType objtype, RangeVar *rel,
                      List *object, Relation *relp,
                      LOCKMODE lockmode, bool missing_ok);

extern void check_object_ownership(Oid roleid,
                       ObjectType objtype, ObjectAddress address,
                       Node *object, Relation relation);

extern Oid    get_object_namespace(const ObjectAddress *address);

extern bool is_objectclass_supported(Oid class_id);
extern Oid    get_object_oid_index(Oid class_id);
extern int    get_object_catcache_oid(Oid class_id);
extern int    get_object_catcache_name(Oid class_id);
extern AttrNumber get_object_attnum_name(Oid class_id);
extern AttrNumber get_object_attnum_namespace(Oid class_id);
extern AttrNumber get_object_attnum_owner(Oid class_id);
extern AttrNumber get_object_attnum_acl(Oid class_id);
extern AclObjectKind get_object_aclkind(Oid class_id);
extern bool get_object_namensp_unique(Oid class_id);

extern HeapTuple get_catalog_object_by_oid(Relation catalog,
                          Oid objectId);

extern char *getObjectDescription(const ObjectAddress *object);
extern char *getObjectDescriptionOids(Oid classid, Oid objid);

extern int    read_objtype_from_string(const char *objtype);
extern char *getObjectTypeDescription(const ObjectAddress *object);
extern char *getObjectIdentity(const ObjectAddress *address);
extern char *getObjectIdentityParts(const ObjectAddress *address,
                       List **objname, List **objargs);
extern ArrayType *strlist_to_textarray(List *list);

#ifdef __AUDIT__
extern bool IsSameObjectAddress(ObjectAddress * addr1, ObjectAddress * addr2);
extern bool IsValidObjectAddress(ObjectAddress * addr);
extern char * get_object_name(ObjectType objtype, Node *object);
#endif

#endif                            /* OBJECTADDRESS_H */
