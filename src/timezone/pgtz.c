/*-------------------------------------------------------------------------
 *
 * pgtz.c
 *      Timezone Library Integration Functions
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      src/timezone/pgtz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

#include "datatype/timestamp.h"
#include "miscadmin.h"
#include "pgtz.h"
#include "storage/fd.h"
#include "utils/hsearch.h"


/* Current session timezone (controlled by TimeZone GUC) */
pg_tz       *session_timezone = NULL;

/* Current log timezone (controlled by log_timezone GUC) */
pg_tz       *log_timezone = NULL;


static bool scan_directory_ci(const char *dirname,
                  const char *fname, int fnamelen,
                  char *canonname, int canonnamelen);

/*
 * 返回时区数据目录的完整路径名
 */
static const char *
pg_TZDIR(void)
{
#ifndef SYSTEMTZDIR
    /* 正常情况下：时区数据存储在我们的共享目录下 */
    static bool done_tzdir = false;
    static char tzdir[MAXPGPATH];

    if (done_tzdir)
        return tzdir;

    get_share_path(my_exec_path, tzdir);
    strlcpy(tzdir + strlen(tzdir), "/timezone", MAXPGPATH - strlen(tzdir));

    done_tzdir = true;
    return tzdir;
#else
    /* 配置为使用系统的时区数据库 */
    return SYSTEMTZDIR;
#endif
}


/*
 * 给定时区名称，打开时区数据文件。如果成功返回文件描述符，如果失败返回-1。
 *
 * 对输入名称进行不区分大小写的搜索（我们假设时区数据库不包含大小写等价的名称）。
 *
 * 如果“canonname”不为NULL，则在成功时将给定名称的规范拼写存储在那里（缓冲区必须> TZ_STRLEN_MAX 字节！）。
 */
int
pg_open_tzfile(const char *name, char *canonname)
{// #lizard forgives
    const char *fname;
    char        fullname[MAXPGPATH];
    int            fullnamelen;
    int            orignamelen;

    /* 用tzdata目录的基本名称初始化fullname */
    strlcpy(fullname, pg_TZDIR(), sizeof(fullname));
    orignamelen = fullnamelen = strlen(fullname);

    if (fullnamelen + 1 + strlen(name) >= MAXPGPATH)
        return -1;                /* 不会适合 */

    /*
     * 如果调用者不需要规范拼写，首先尝试直接打开名称。如果给定的名称已经是大小写正确的，
     * 或者文件系统不区分大小写，那么可以预期这将成功；如果我们不需要区分这些情况，
     * 那么我们不需要报告规范拼写。
     */
    if (canonname == NULL)
    {
        int            result;

        fullname[fullnamelen] = '/';
        /* 上面的测试确保这会适合： */
        strcpy(fullname + fullnamelen + 1, name);
        result = open(fullname, O_RDONLY | PG_BINARY, 0);
        if (result >= 0)
            return result;
        /* 如果上述尝试失败，则继续使用更复杂的方法 */
        fullname[fullnamelen] = '\0';
    }

    /*
     * Loop to split the given name into directory levels; for each level,
     * search using scan_directory_ci().
     */
    fname = name;
    for (;;)
    {
        const char *slashptr;
        int            fnamelen;

        slashptr = strchr(fname, '/');
        if (slashptr)
            fnamelen = slashptr - fname;
        else
            fnamelen = strlen(fname);
        if (!scan_directory_ci(fullname, fname, fnamelen,
                               fullname + fullnamelen + 1,
                               MAXPGPATH - fullnamelen - 1))
            return -1;
        fullname[fullnamelen++] = '/';
        fullnamelen += strlen(fullname + fullnamelen);
        if (slashptr)
            fname = slashptr + 1;
        else
            break;
    }

    if (canonname)
        strlcpy(canonname, fullname + orignamelen + 1, TZ_STRLEN_MAX + 1);

    return open(fullname, O_RDONLY | PG_BINARY, 0);
}


/*
 * Scan specified directory for a case-insensitive match to fname
 * (of length fnamelen --- fname may not be null terminated!).  If found,
 * copy the actual filename into canonname and return true.
 */
/*
 * 扫描目录中的文件，进行大小写不敏感的匹配查找。
 * 如果找到匹配项，则将匹配的规范名称拷贝到canonname中。
 * 如果成功找到匹配项，则返回true，否则返回false。
 */
static bool
scan_directory_ci(const char *dirname, const char *fname, int fnamelen,
                  char *canonname, int canonnamelen)
{
    bool        found = false;
    DIR           *dirdesc;
    struct dirent *direntry;

    // 打开目录流
    dirdesc = AllocateDir(dirname);
    if (!dirdesc)
    {
        // 如果无法打开目录，则报错并返回false
        ereport(LOG,
                (errcode_for_file_access(),
                 errmsg("could not open directory \"%s\": %m", dirname)));
        return false;
    }

    // 逐个读取目录中的文件项
    while ((direntry = ReadDir(dirdesc, dirname)) != NULL)
    {
        /*
         * 忽略“.”和“..”，以及任何其他“隐藏”文件。
         * 这是一种安全措施，以防止访问时区目录之外的文件。
         */
        if (direntry->d_name[0] == '.')
            continue;

        // 如果文件名长度与要匹配的长度相同，并且忽略大小写后匹配，则认为找到了匹配项
        if (strlen(direntry->d_name) == fnamelen &&
            pg_strncasecmp(direntry->d_name, fname, fnamelen) == 0)
        {
            /* 找到了匹配项 */
            // 将匹配的规范名称拷贝到canonname中
            strlcpy(canonname, direntry->d_name, canonnamelen);
            found = true;
            break;
        }
    }

    // 关闭目录流
    FreeDir(dirdesc);

    return found;
}


/*
 * 我们将加载的时区缓存在哈希表中，以便每次选择时区时都不必重新加载和解析TZ定义文件。
 * 因为我们希望时区名称不区分大小写，所以哈希键是时区名称的大写形式。
 */
typedef struct
{
    /* tznameupper 包含时区的全大写名称 */
    char        tznameupper[TZ_STRLEN_MAX + 1];
    pg_tz        tz;
} pg_tz_cache;

static HTAB *timezone_cache = NULL;



static bool
init_timezone_hashtable(void)
{
    HASHCTL        hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));

    hash_ctl.keysize = TZ_STRLEN_MAX + 1;
    hash_ctl.entrysize = sizeof(pg_tz_cache);

    timezone_cache = hash_create("Timezones",
                                 4,
                                 &hash_ctl,
                                 HASH_ELEM);
    if (!timezone_cache)
        return false;

    return true;
}

/*
 * Load a timezone from file or from cache.
 * Does not verify that the timezone is acceptable!
 *
 * "GMT" is always interpreted as the tzparse() definition, without attempting
 * to load a definition from the filesystem.  This has a number of benefits:
 * 1. It's guaranteed to succeed, so we don't have the failure mode wherein
 * the bootstrap default timezone setting doesn't work (as could happen if
 * the OS attempts to supply a leap-second-aware version of "GMT").
 * 2. Because we aren't accessing the filesystem, we can safely initialize
 * the "GMT" zone definition before my_exec_path is known.
 * 3. It's quick enough that we don't waste much time when the bootstrap
 * default timezone setting is later overridden from postgresql.conf.
 */
pg_tz *
pg_tzset(const char *name)
{// #lizard forgives
    pg_tz_cache *tzp;
    struct state tzstate;
    char        uppername[TZ_STRLEN_MAX + 1];
    char        canonname[TZ_STRLEN_MAX + 1];
    char       *p;

    if (strlen(name) > TZ_STRLEN_MAX)
        return NULL;            /* not going to fit */

    if (!timezone_cache)
        if (!init_timezone_hashtable())
            return NULL;

    /*
     * Upcase the given name to perform a case-insensitive hashtable search.
     * (We could alternatively downcase it, but we prefer upcase so that we
     * can get consistently upcased results from tzparse() in case the name is
     * a POSIX-style timezone spec.)
     */
    p = uppername;
    while (*name)
        *p++ = pg_toupper((unsigned char) *name++);
    *p = '\0';

    tzp = (pg_tz_cache *) hash_search(timezone_cache,
                                      uppername,
                                      HASH_FIND,
                                      NULL);
    if (tzp)
    {
        /* Timezone found in cache, nothing more to do */
        return &tzp->tz;
    }

    /*
     * "GMT" is always sent to tzparse(), as per discussion above.
     */
    if (strcmp(uppername, "GMT") == 0)
    {
        if (!tzparse(uppername, &tzstate, true))
        {
            /* This really, really should not happen ... */
            elog(ERROR, "could not initialize GMT time zone");
        }
        /* Use uppercase name as canonical */
        strcpy(canonname, uppername);
    }
    else if (tzload(uppername, canonname, &tzstate, true) != 0)
    {
        if (uppername[0] == ':' || !tzparse(uppername, &tzstate, false))
        {
            /* Unknown timezone. Fail our call instead of loading GMT! */
            return NULL;
        }
        /* For POSIX timezone specs, use uppercase name as canonical */
        strcpy(canonname, uppername);
    }

    /* Save timezone in the cache */
    tzp = (pg_tz_cache *) hash_search(timezone_cache,
                                      uppername,
                                      HASH_ENTER,
                                      NULL);

    /* hash_search already copied uppername into the hash key */
    strcpy(tzp->tz.TZname, canonname);
    memcpy(&tzp->tz.state, &tzstate, sizeof(tzstate));

    return &tzp->tz;
}

/*
 * Load a fixed-GMT-offset timezone.
 * This is used for SQL-spec SET TIME ZONE INTERVAL 'foo' cases.
 * It's otherwise equivalent to pg_tzset().
 *
 * The GMT offset is specified in seconds, positive values meaning west of
 * Greenwich (ie, POSIX not ISO sign convention).  However, we use ISO
 * sign convention in the displayable abbreviation for the zone.
 *
 * Caution: this can fail (return NULL) if the specified offset is outside
 * the range allowed by the zic library.
 */
pg_tz *
pg_tzset_offset(long gmtoffset)
{
    long        absoffset = (gmtoffset < 0) ? -gmtoffset : gmtoffset;
    char        offsetstr[64];
    char        tzname[128];

    snprintf(offsetstr, sizeof(offsetstr),
             "%02ld", absoffset / SECS_PER_HOUR);
    absoffset %= SECS_PER_HOUR;
    if (absoffset != 0)
    {
        snprintf(offsetstr + strlen(offsetstr),
                 sizeof(offsetstr) - strlen(offsetstr),
                 ":%02ld", absoffset / SECS_PER_MINUTE);
        absoffset %= SECS_PER_MINUTE;
        if (absoffset != 0)
            snprintf(offsetstr + strlen(offsetstr),
                     sizeof(offsetstr) - strlen(offsetstr),
                     ":%02ld", absoffset);
    }
    if (gmtoffset > 0)
        snprintf(tzname, sizeof(tzname), "<-%s>+%s",
                 offsetstr, offsetstr);
    else
        snprintf(tzname, sizeof(tzname), "<+%s>-%s",
                 offsetstr, offsetstr);

    return pg_tzset(tzname);
}


/*
 * Initialize timezone library
 *
 * This is called before GUC variable initialization begins.  Its purpose
 * is to ensure that log_timezone has a valid value before any logging GUC
 * variables could become set to values that require elog.c to provide
 * timestamps (e.g., log_line_prefix).  We may as well initialize
 * session_timestamp to something valid, too.
 */
void
pg_timezone_initialize(void)
{
    /*
     * We may not yet know where PGSHAREDIR is (in particular this is true in
     * an EXEC_BACKEND subprocess).  So use "GMT", which pg_tzset forces to be
     * interpreted without reference to the filesystem.  This corresponds to
     * the bootstrap default for these variables in guc.c, although in
     * principle it could be different.
     */
    session_timezone = pg_tzset("GMT");
    log_timezone = session_timezone;
}


/*
 * Functions to enumerate available timezones
 *
 * Note that pg_tzenumerate_next() will return a pointer into the pg_tzenum
 * structure, so the data is only valid up to the next call.
 *
 * All data is allocated using palloc in the current context.
 */
#define MAX_TZDIR_DEPTH 10

struct pg_tzenum
{
    int            baselen;
    int            depth;
    DIR           *dirdesc[MAX_TZDIR_DEPTH];
    char       *dirname[MAX_TZDIR_DEPTH];
    struct pg_tz tz;
};

/* typedef pg_tzenum is declared in pgtime.h */

pg_tzenum *
pg_tzenumerate_start(void)
{
    pg_tzenum  *ret = (pg_tzenum *) palloc0(sizeof(pg_tzenum));
    char       *startdir = pstrdup(pg_TZDIR());

    ret->baselen = strlen(startdir) + 1;
    ret->depth = 0;
    ret->dirname[0] = startdir;
    ret->dirdesc[0] = AllocateDir(startdir);
    if (!ret->dirdesc[0])
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open directory \"%s\": %m", startdir)));
    return ret;
}

void
pg_tzenumerate_end(pg_tzenum *dir)
{
    while (dir->depth >= 0)
    {
        FreeDir(dir->dirdesc[dir->depth]);
        pfree(dir->dirname[dir->depth]);
        dir->depth--;
    }
    pfree(dir);
}

pg_tz *
pg_tzenumerate_next(pg_tzenum *dir)
{// #lizard forgives
    while (dir->depth >= 0)
    {
        struct dirent *direntry;
        char        fullname[MAXPGPATH * 2];
        struct stat statbuf;

        direntry = ReadDir(dir->dirdesc[dir->depth], dir->dirname[dir->depth]);

        if (!direntry)
        {
            /* End of this directory */
            FreeDir(dir->dirdesc[dir->depth]);
            pfree(dir->dirname[dir->depth]);
            dir->depth--;
            continue;
        }

        if (direntry->d_name[0] == '.')
            continue;

        snprintf(fullname, sizeof(fullname), "%s/%s",
                 dir->dirname[dir->depth], direntry->d_name);
        if (stat(fullname, &statbuf) != 0)
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not stat \"%s\": %m", fullname)));

        if (S_ISDIR(statbuf.st_mode))
        {
            /* Step into the subdirectory */
            if (dir->depth >= MAX_TZDIR_DEPTH - 1)
                ereport(ERROR,
                        (errmsg_internal("timezone directory stack overflow")));
            dir->depth++;
            dir->dirname[dir->depth] = pstrdup(fullname);
            dir->dirdesc[dir->depth] = AllocateDir(fullname);
            if (!dir->dirdesc[dir->depth])
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not open directory \"%s\": %m",
                                fullname)));

            /* Start over reading in the new directory */
            continue;
        }

        /*
         * Load this timezone using tzload() not pg_tzset(), so we don't fill
         * the cache.  Also, don't ask for the canonical spelling: we already
         * know it, and pg_open_tzfile's way of finding it out is pretty
         * inefficient.
         */
        if (tzload(fullname + dir->baselen, NULL, &dir->tz.state, true) != 0)
        {
            /* Zone could not be loaded, ignore it */
            continue;
        }

        if (!pg_tz_acceptable(&dir->tz))
        {
            /* Ignore leap-second zones */
            continue;
        }

        /* OK, return the canonical zone name spelling. */
        strlcpy(dir->tz.TZname, fullname + dir->baselen,
                sizeof(dir->tz.TZname));

        /* Timezone loaded OK. */
        return &dir->tz;
    }

    /* Nothing more found */
    return NULL;
}
