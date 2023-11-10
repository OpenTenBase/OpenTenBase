/*-------------------------------------------------------------------------
 * relpath.c
 *        Shared frontend/backend code to compute pathnames of relation files
 *
 * This module also contains some logic associated with fork names.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * IDENTIFICATION
 *      src/common/relpath.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "common/relpath.h"
#include "storage/backendid.h"

#ifdef PGXC
#ifndef FRONTEND
#include "pgxc/pgxc.h"
#endif
#endif

/*
 * Lookup table of fork name by fork number.
 *
 * If you add a new entry, remember to update the errhint in
 * forkname_to_number() below, and update the SGML documentation for
 * pg_relation_size().
 */
const char *const forkNames[] = {
    "main",                        /* MAIN_FORKNUM */
    "fsm",                        /* FSM_FORKNUM */
    "vm",                        /* VISIBILITYMAP_FORKNUM */
#ifdef _SHARDING_
    "extent",                    /* EXTENT_FORKNUM */
#endif
    "init"                        /* INIT_FORKNUM */
};

/*
 * forkname_to_number - look up fork number by name
 *
 * In backend, we throw an error for no match; in frontend, we just
 * return InvalidForkNumber.
 */
ForkNumber
forkname_to_number(const char *forkName)
{
    ForkNumber    forkNum;

    for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
        if (strcmp(forkNames[forkNum], forkName) == 0)
            return forkNum;

#ifndef FRONTEND
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("invalid fork name"),
             errhint("Valid fork names are \"main\", \"fsm\", "
                     "\"vm\", and \"init\".")));
#endif

    return InvalidForkNumber;
}

/*
 * forkname_chars
 *        We use this to figure out whether a filename could be a relation
 *        fork (as opposed to an oddly named stray file that somehow ended
 *        up in the database directory).  If the passed string begins with
 *        a fork name (other than the main fork name), we return its length,
 *        and set *fork (if not NULL) to the fork number.  If not, we return 0.
 *
 * Note that the present coding assumes that there are no fork names which
 * are prefixes of other fork names.
 */
int
forkname_chars(const char *str, ForkNumber *fork)
{
    ForkNumber    forkNum;

    for (forkNum = 1; forkNum <= MAX_FORKNUM; forkNum++)
    {
        int            len = strlen(forkNames[forkNum]);

        if (strncmp(forkNames[forkNum], str, len) == 0)
        {
            if (fork)
                *fork = forkNum;
            return len;
        }
    }
    if (fork)
        *fork = InvalidForkNumber;
    return 0;
}


/*
 * GetDatabasePath - construct path to a database directory
 *
 * Result is a palloc'd string.
 *
 * XXX this must agree with GetRelationPath()!
 */
#ifndef FRONTEND
char *
GetDatabasePath(Oid dbNode, Oid spcNode)
{
    return GetDatabasePath_client(dbNode, spcNode, PGXCNodeName);
}
#endif

char *
GetDatabasePath_client(Oid dbNode, Oid spcNode, const char *nodename)
{
    if (spcNode == GLOBALTABLESPACE_OID)
    {
        /* Shared system relations live in {datadir}/global */
        Assert(dbNode == 0);
        return pstrdup("global");
    }
    else if (spcNode == DEFAULTTABLESPACE_OID)
    {
        /* The default tablespace is {datadir}/base */
        return psprintf("base/%u", dbNode);
    }
    else
    {
        /* All other tablespaces are accessed via symlinks */
#ifdef PGXC        
        return psprintf("pg_tblspc/%u/%s_%s/%u",
                        spcNode, TABLESPACE_VERSION_DIRECTORY,
                        nodename,
                        dbNode);
#else        
        return psprintf("pg_tblspc/%u/%s/%u",
                        spcNode, TABLESPACE_VERSION_DIRECTORY, dbNode);
#endif        
    }
}

/*
 * GetRelationPath - construct path to a relation's file
 *
 * Result is a palloc'd string.
 *
 * Note: ideally, backendId would be declared as type BackendId, but relpath.h
 * would have to include a backend-only header to do that; doesn't seem worth
 * the trouble considering BackendId is just int anyway.
 */
#ifndef FRONTEND
char *
GetRelationPath(Oid dbNode, Oid spcNode, Oid relNode,
                int backendId, ForkNumber forkNumber)
{
    return GetRelationPath_client(dbNode, spcNode, relNode, backendId,
            forkNumber, PGXCNodeName);
}
#endif

char *
GetRelationPath_client(Oid dbNode, Oid spcNode, Oid relNode,
                int backendId, ForkNumber forkNumber,
                const char *nodename)
{// #lizard forgives
    char       *path;

    if (spcNode == GLOBALTABLESPACE_OID)
    {
        /* Shared system relations live in {datadir}/global */
        Assert(dbNode == 0);
        Assert(backendId == InvalidBackendId);
        if (forkNumber != MAIN_FORKNUM)
            path = psprintf("global/%u_%s",
                            relNode, forkNames[forkNumber]);
        else
            path = psprintf("global/%u", relNode);
    }
    else if (spcNode == DEFAULTTABLESPACE_OID)
    {
        /* The default tablespace is {datadir}/base */
        if (backendId == InvalidBackendId)
        {
            if (forkNumber != MAIN_FORKNUM)
                path = psprintf("base/%u/%u_%s",
                                dbNode, relNode,
                                forkNames[forkNumber]);
            else
                path = psprintf("base/%u/%u",
                                dbNode, relNode);
        }
        else
        {
            if (forkNumber != MAIN_FORKNUM)
                path = psprintf("base/%u/t%d_%u_%s",
                                dbNode, backendId, relNode,
                                forkNames[forkNumber]);
            else
                path = psprintf("base/%u/t%d_%u",
                                dbNode, backendId, relNode);
        }
    }
    else
    {
        /* All other tablespaces are accessed via symlinks */
        if (backendId == InvalidBackendId)
        {
            if (forkNumber != MAIN_FORKNUM)
#ifdef PGXC
                path = psprintf("pg_tblspc/%u/%s_%s/%u/%u_%s",
#else                        
                path = psprintf("pg_tblspc/%u/%s/%u/%u_%s",
#endif                    
                                spcNode, TABLESPACE_VERSION_DIRECTORY,
#ifdef PGXC
                /* Postgres-XC tablespaces include node name */
                                nodename,
#endif
                                dbNode,
                                relNode,
                                forkNames[forkNumber]);
            else
#ifdef PGXC
                path = psprintf("pg_tblspc/%u/%s_%s/%u/%u",
#else                        
                path = psprintf("pg_tblspc/%u/%s/%u/%u",
#endif                    
                                spcNode, TABLESPACE_VERSION_DIRECTORY,
#ifdef PGXC
                /* Postgres-XC tablespaces include node name */
                                nodename,
#endif
                                dbNode, relNode);
        }
        else
        {
            if (forkNumber != MAIN_FORKNUM)
#ifdef PGXC
                path = psprintf("pg_tblspc/%u/%s_%s/%u/t%d_%u_%s",
#else                    
                path = psprintf("pg_tblspc/%u/%s/%u/t%d_%u_%s",
#endif                    
                                spcNode, TABLESPACE_VERSION_DIRECTORY,
#ifdef PGXC
                /* Postgres-XC tablespaces include node name */
                                nodename,
#endif
                                dbNode, backendId, relNode,
                                forkNames[forkNumber]);
            else
#ifdef PGXC
                path = psprintf("pg_tblspc/%u/%s_%s/%u/t%d_%u",
#else                    
                path = psprintf("pg_tblspc/%u/%s/%u/t%d_%u",
#endif                    
                                spcNode, TABLESPACE_VERSION_DIRECTORY,
#ifdef PGXC
                /* Postgres-XC tablespaces include node name */
                                nodename,
#endif
                                dbNode, backendId, relNode);
        }
    }
    return path;
}
