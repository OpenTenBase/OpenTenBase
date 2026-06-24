/*
 * pagehack.cpp
 *
 * A simple hack program for PostgreSQL's internal files
 * Originally written by Nicole Nie (coolnyy@gmail.com).
 *
 * contrib/pagehack/pagehack.cpp
 * Copyright (c) 2000-2011, PostgreSQL Global Development Group
 * ALL RIGHTS RESERVED;
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */

#define MAX_PG_CLASS_ID 10000
#define CLASS_TYPE_NUM 512

#include "postgres.h"
#include "port.h"

#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>
#include <string.h>
#include "access/csnlog.h"
#include "access/nbtree.h"
#include "utils/relmapper.h"
#include "storage/checksum.h"
#include "access/htup_details.h"

typedef unsigned char* binary;
typedef struct PgClass_table {
    const char* class_name;
    Oid ralation_id;
} PgClass_table;

typedef enum HackingType {
    HACKING_HEAP,
    HACKING_INDEX_BTREE,
    HACKING_COLSTORE, 
    HACKING_FILENODE,
    HACKING_CSNLOG,
    NUM_HACKINGTYPE
} HackingType;

int SegNo = 0;
static HackingType hackingtype = HACKING_COLSTORE;
static const char* HACKINGTYPE[] = {
	"heap",
	"btree",
	"col_store",
    "filenode_map",
    "csnlog",
};

static const char HexCharMaps[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

#define Byte2HexChar(_buff, _byte)              \
    do {                                        \
        (_buff)[0] = HexCharMaps[(_byte) >> 4]; \
        (_buff)[1] = HexCharMaps[(_byte)&0x0F]; \
    } while (0)

static void formatBytes(unsigned char* start, int len)
{
    int cnt;
    char byteBuf[4] = {0, 0, ' ', '\0'};
    unsigned char ch;
    int bytesEachLine = 32;
    
    fprintf(stdout, "\t\t\t");
    for (cnt = 0; cnt < len; cnt++) {
        ch = (unsigned char)*start;
        start++;

        // print 32bytes each line
        if (bytesEachLine == 32) {
            fprintf(stdout, "\n\t\t\t");
            bytesEachLine = 0;
        } else if (bytesEachLine % 8 == 0) {
            fprintf(stdout, "");
        }
        ++bytesEachLine;

        Byte2HexChar(byteBuf, ch);
        fprintf(stdout, "%s", byteBuf);
    }
}

static void formatBitmap(const unsigned char* start, int len, char bit1, char bit0)
{
    int bytesOfOneLine = 8;

    for (int i = 0; i < len; ++i) {
        unsigned char ch = start[i];
        unsigned char bitmask = 1;

        // print 8bytes each line
        if (bytesOfOneLine == 8) {
            bytesOfOneLine = 0;
        }
        ++bytesOfOneLine;

        // print 8 bits within a loop
        do {
            fprintf(stdout, "%c", ((ch & bitmask) ? bit1 : bit0));
            bitmask <<= 1;
        } while (bitmask != 0);
        fprintf(stdout, " ");
    }
}

static void usage(const char* progname)
{
    printf("%s is a hacking tool for PostgreSQL.\n"
           "\n"
           "Usage:\n"
           "  %s [OPTIONS]\n"
           "\nHacking options:\n"
           "  -f FILENAME  the database file to hack\n",
        progname,
        progname);

    // print supported type within HACKINGTYPE[]
    //
    printf("  -t {");
    int nTypes = sizeof(HACKINGTYPE) / sizeof(HACKINGTYPE[0]);
    for (int i = 0; i < nTypes; ++i) {
        printf(" %s%s", HACKINGTYPE[i], (i == nTypes - 1) ? " " : "|");
    }
    printf("}\n"
           "       the hacking type (default: col_store)\n");

}

static void ParsePageHeader(const PageHeader page, int blkno, int blknum)
{
    fprintf(stdout, "page information of block %d/%d\n", blkno, blknum);
    fprintf(stdout, "\tpd_lsn: %X/%X\n", (uint32)(PageGetLSN(page) >> 32), (uint32)PageGetLSN(page));

    fprintf(stdout, "\tpd_flags: ");
    if (PageHasFreeLinePointers(page))
        fprintf(stdout, "PD_HAS_FREE_LINES ");
    if (PageIsFull(page))
        fprintf(stdout, "PD_PAGE_FULL ");
    if (PageIsAllVisible(page))
        fprintf(stdout, "PD_ALL_VISIBLE ");

    fprintf(stdout, "\n");
    fprintf(stdout, "\tpd_lower: %u, %s\n", page->pd_lower, PageIsEmpty(page) ? "empty" : "non-empty");
    fprintf(stdout, "\tpd_upper: %u, %s\n", page->pd_upper, PageIsNew(page) ? "new" : "old");
    fprintf(stdout, "\tpd_special: %u, size %u\n", page->pd_special, PageGetSpecialSize(page));
    fprintf(stdout,
        "\tPage size & version: %u, %u\n",
        (uint16)PageGetPageSize(page),
        (uint16)PageGetPageLayoutVersion(page));
    
    fprintf(stdout, "\tpd_prune_xid: %u\n", page->pd_prune_xid);
    if (page->pd_upper < page->pd_lower)
	{
        fprintf(stdout, "WARNING: INVALID PAGE!");
    }
    return;
}

static void parse_special_data(const char* buffer)
{
    PageHeader page = (PageHeader)buffer;
    BTPageOpaque opaque;
    opaque = (BTPageOpaque)PageGetSpecialPointer(page);
    
    fprintf(stdout, "\nbtree index special information:\n");
    fprintf(stdout, "\tbtree left sibling: %u\n", opaque->btpo_prev);
    fprintf(stdout, "\tbtree right sibling: %u\n", opaque->btpo_next);
    if (!P_ISDELETED(opaque))
        fprintf(stdout, "\tbtree tree level: %u\n", opaque->btpo.level);
    else {
           fprintf(stdout, "\tnext txid (deleted): %u\n", opaque->btpo.xact);
    }
    fprintf(stdout, "\tbtree flag: ");
    if (P_ISLEAF(opaque))
        fprintf(stdout, "BTP_LEAF ");
    else
        fprintf(stdout, "BTP_INTERNAL ");
    if (P_ISROOT(opaque))
        fprintf(stdout, "BTP_ROOT ");
    if (P_ISDELETED(opaque))
        fprintf(stdout, "BTP_DELETED ");
    if (P_ISHALFDEAD(opaque))
        fprintf(stdout, "BTP_HALF_DEAD ");
    if (P_HAS_GARBAGE(opaque))
        fprintf(stdout, "BTP_HAS_GARBAGE ");
    fprintf(stdout, "\n");

    fprintf(stdout, "\tbtree cycle ID: %u\n", opaque->btpo_cycleid);
}

static void parse_heap_item(const Item item, unsigned len, int blkno, int lineno)
{
    HeapTupleHeader tup = (HeapTupleHeader)item;
    binary content;

    fprintf(stdout,
        "\t\t\t t_xmin/t_xmax/t_cid: %u/%u/%u\n",
        HeapTupleHeaderGetXmin(tup),
        HeapTupleHeaderGetRawXmax(tup),
        HeapTupleHeaderGetRawCommandId(tup));

    fprintf(stdout,
        "\t\t\t ctid:(block %u/%u, offset %u)\n",
        tup->t_ctid.ip_blkid.bi_hi,
        tup->t_ctid.ip_blkid.bi_lo,
        tup->t_ctid.ip_posid);

    fprintf(stdout, "\t\t\t t_infomask: %hu ", tup->t_infomask);
    if (tup->t_infomask & HEAP_HASNULL)
        fprintf(stdout, "HEAP_HASNULL ");
    if (tup->t_infomask & HEAP_HASVARWIDTH)
        fprintf(stdout, "HEAP_HASVARWIDTH ");
    if (tup->t_infomask & HEAP_HASEXTERNAL)
        fprintf(stdout, "HEAP_HASEXTERNAL ");
    if (tup->t_infomask & HEAP_HASOID)
        fprintf(stdout, "HEAP_HASOID(%d) ", HeapTupleHeaderGetOid(tup));
    if (tup->t_infomask & HEAP_XMAX_KEYSHR_LOCK)
        fprintf(stdout, "HEAP_XMAX_KEYSHR_LOCK ");
    if (tup->t_infomask & HEAP_XMAX_EXCL_LOCK)
        fprintf(stdout, "HEAP_XMAX_EXCL_LOCK ");
    if (tup->t_infomask & HEAP_XMAX_LOCK_ONLY)
        fprintf(stdout, "HEAP_XMAX_LOCK_ONLY ");
    if (tup->t_infomask & HEAP_XMIN_COMMITTED)
        fprintf(stdout, "HEAP_XMIN_COMMITTED ");
    if (tup->t_infomask & HEAP_XMIN_INVALID)
        fprintf(stdout, "HEAP_XMIN_INVALID ");
    if (tup->t_infomask & HEAP_XMIN_FROZEN)
        fprintf(stdout, "HEAP_XMIN_FROZEN ");
    if (tup->t_infomask & HEAP_XMAX_COMMITTED)
        fprintf(stdout, "HEAP_XMAX_COMMITTED ");
    if (tup->t_infomask & HEAP_XMAX_INVALID)
        fprintf(stdout, "HEAP_XMAX_INVALID ");
    if (tup->t_infomask & HEAP_XMAX_IS_MULTI)
        fprintf(stdout, "HEAP_XMAX_IS_MULTI ");
    if (tup->t_infomask & HEAP_UPDATED)
        fprintf(stdout, "HEAP_UPDATED ");
    fprintf(stdout, "\n");

    fprintf(stdout, "\t\t\t t_infomask2:%hu ", tup->t_infomask2);
    if (tup->t_infomask2 & HEAP_KEYS_UPDATED)
        fprintf(stdout, "HEAP_KEYS_UPDATED ");
    if (tup->t_infomask2 & HEAP_HOT_UPDATED)
        fprintf(stdout, "HEAP_HOT_UPDATED ");
    if (tup->t_infomask2 & HEAP_ONLY_TUPLE)
        fprintf(stdout, "HEAP_ONLY_TUPLE ");
    fprintf(stdout, "\n");
    fprintf(stdout, "\t\t\t Attrs Num: %d", HeapTupleHeaderGetNatts(tup));
    fprintf(stdout, "\n");

    fprintf(stdout, "\t\t\t t_infomask3:%hu ", tup->t_infomask3);
    if (tup->t_infomask3 & HEAP_HASROWID)
        fprintf(stdout, "HEAP_HASROWID ");
    if (tup->t_infomask3 & HEAP_FROM_REMOTE)
        fprintf(stdout, "HEAP_FROM_REMOTE ");
    fprintf(stdout, "\n");
    fprintf(stdout, "\t\t\t shard id %d\n", tup->t_shardid);
    fprintf(stdout, "\n");
    fprintf(stdout, "\t\t\t t_hoff: %u\n", tup->t_hoff);
    len -= tup->t_hoff;
    fprintf(stdout, "\n");

    content =((unsigned char*)(tup) + (tup)->t_hoff);
    formatBytes(content, len);

    fprintf(stdout, "\n");
}

static void parse_btree_index_item(const Item item, unsigned len, int blkno, int lineno)
{
    IndexTuple itup = (IndexTuple)item;
    bool hasnull = (itup->t_info & INDEX_NULL_MASK);
    unsigned int tuplen = (itup->t_info & INDEX_SIZE_MASK);
    unsigned int offset = 0;
    unsigned char* null_map = NULL;

    fprintf(stdout,
        "\t\t\tHeap Tid: block %u/%u, offset %u\n",
        itup->t_tid.ip_blkid.bi_hi,
        itup->t_tid.ip_blkid.bi_lo,
        itup->t_tid.ip_posid);

    fprintf(stdout, "\t\t\tLength: %u", tuplen);
    if (itup->t_info & INDEX_VAR_MASK)
        fprintf(stdout, ", has var-width attrs");
    if (hasnull) {
        fprintf(stdout, ", has nulls \n");
        formatBitmap((unsigned char*)item + sizeof(IndexTupleData), sizeof(IndexAttributeBitMapData), 'V', 'N');
        null_map = (unsigned char*)item + sizeof(IndexTupleData);
    } else
        fprintf(stdout, "\n");

    offset = IndexInfoFindDataOffset(itup->t_info);
    formatBytes((unsigned char*)item + offset, tuplen - offset);
}

static void parse_one_item(const Item item, unsigned len, int blkno, int lineno, HackingType type)
{
    switch (type) {
        case HACKING_HEAP:
            parse_heap_item(item, len, blkno, lineno);
            break;

        case HACKING_INDEX_BTREE:
            parse_btree_index_item(item, len, blkno, lineno);
            break;

        default:
            break;
    }
}

static void parse_heap_or_index_page(const char* buffer, int blkno, HackingType type)
{
    const PageHeader page = (const PageHeader)buffer;
    int i;
    int nline, nstorage, nunused, nnormal, ndead;
    ItemId lp;
    Item item;

    nstorage = 0;
    nunused = nnormal = ndead = 0;
    nline = PageGetMaxOffsetNumber((Page)page);
    for (i = FirstOffsetNumber; i <= nline; i++) {
        lp = PageGetItemId(page, i);
        if (ItemIdIsUsed(lp)) {
            if (ItemIdHasStorage(lp))
                nstorage++;

            if (ItemIdIsNormal(lp) ) {
                fprintf(stdout,
                        "\n\t\tTuple #%d is normal: length %u, offset %u\n",
                        i,
                        ItemIdGetLength(lp),
                        ItemIdGetOffset(lp));
                item = PageGetItem(page, lp);
                parse_one_item(item, ItemIdGetLength(lp), blkno, i, type);
            } else if (ItemIdIsDead(lp)) {
                fprintf(stdout,
                    "\n\t\tTuple #%d is dead: length %u, offset %u",
                    i,
                    ItemIdGetLength(lp),
                    ItemIdGetOffset(lp));
                ndead++;
            } else {
                fprintf(stdout,
                    "\n\t\tTuple #%d is redirected: length %u, offset %u",
                    i,
                    ItemIdGetLength(lp),
                    ItemIdGetOffset(lp));
            }
        } else {
            nunused++;
            fprintf(stdout, "\n\t\tTuple #%d is unused\n", i);
        }
    }
    fprintf(stdout, "\n");
    fprintf(stdout, "\tSummary (%d total): %d unused, %d normal, %d dead\n", nline, nunused, nnormal, ndead);

    if (type == HACKING_INDEX_BTREE)
    	parse_special_data(buffer);
    fprintf(stdout, "\n");
    return;
}

static int parse_a_page(const char* buffer, int blkno, int blknum, HackingType type)
{
	const PageHeader page = (const PageHeader)buffer;
    uint16 headersize;

    if (PageIsNew(page)) {
        fprintf(stdout, "Page information of block %d/%d : new page\n\n", blkno, blknum);
        ParsePageHeader(page, blkno, blknum);
        return true;
    }

    headersize = SizeOfPageHeaderData;
    if (page->pd_lower < headersize || page->pd_lower > page->pd_upper || page->pd_upper > page->pd_special ||
        page->pd_special > BLCKSZ || page->pd_special != MAXALIGN(page->pd_special)) {
        fprintf(stderr,
            "The page data is corrupted, corrupted page pointers: lower = %u, upper = %u, special = %u\n",
            page->pd_lower,
            page->pd_upper,
            page->pd_special);
        return false;
    }

    ParsePageHeader(page, blkno, blknum);

    parse_heap_or_index_page(buffer, blkno, type);
    return true;
}

static int parse_page_file(const char* filename, HackingType type, const uint32 start_point, const uint32 number_read)
{
	char buffer[BLCKSZ];
    FILE* fd = NULL;
    long size;
    BlockNumber blknum;
    BlockNumber start = start_point;
    BlockNumber number = number_read;
    size_t result;

    if (NULL == (fd = fopen(filename, "rb+"))) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    fseek(fd, 0, SEEK_END);
    size = ftell(fd);
    rewind(fd);

    if ((0 == size) || (0 != size % BLCKSZ)) {
        fprintf(stderr, "The page file size is not an exact multiple of BLCKSZ\n");
        fclose(fd);
        return false;
    }

    blknum = size / BLCKSZ;
    if (number == InvalidBlockNumber) {
        return false;
    } else if ((start + number) > blknum) {
        fprintf(stderr,
            "don't have %u blocks from block %u in the relation, only %u blocks\n",
            number,
            start,
            (blknum - start));
        number = blknum;
    } else if (number == 0) {
        number = blknum;
    } else {
        number += start;
    }

    Assert((start * BLCKSZ) < size);
    if (start != 0)
        fseek(fd, (start * BLCKSZ), SEEK_SET);

    while (start < number) {
        result = fread(buffer, 1, BLCKSZ, fd);
        if (BLCKSZ != result) {
            fprintf(stderr, "Reading error");
            fclose(fd);
            return false;
        }

        if (!parse_a_page(buffer, start, blknum, type)) {
            fprintf(stderr, "Error during parsing block %u/%u\n", start, blknum);
            fclose(fd);
            return false;
        }
        start++;
    }
    fclose(fd);

    return true;
}

static void fill_filenode_map(char** class_map);
static int parse_filenodemap_file(char* filename)
{
    char buffer[sizeof(RelMapFile)];
    FILE* fd = NULL;
    long size;
    size_t result;
    RelMapFile* mapfile = NULL;
    char* pg_class_map[MAX_PG_CLASS_ID];
    char* pg_class = NULL;
    int i = 0;

    for (i = 0; i < MAX_PG_CLASS_ID; i++) {
        pg_class_map[i] = NULL;
    }
    fill_filenode_map(pg_class_map);

    if (NULL == (fd = fopen(filename, "rb"))) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        return false;
    }

    fseek(fd, 0, SEEK_END);
    size = ftell(fd);
    rewind(fd);
    result = fread(buffer, 1, sizeof(RelMapFile), fd);
    if (sizeof(RelMapFile) != result) {
        fprintf(stderr, "Reading error");
        return false;
    }

    mapfile = (RelMapFile*)buffer;
    fprintf(stdout, "Magic number: 0x%x\n", mapfile->magic);
    fprintf(stdout, "Number of mappings: %u\n", mapfile->num_mappings);
    fprintf(stdout, "Mapping pairs:\n");
    for (i = 0; i < MAX_MAPPINGS; i++) {
        if (0 == mapfile->mappings[i].mapoid && 0 == mapfile->mappings[i].mapfilenode)
            break;

        if (mapfile->mappings[i].mapoid > MAX_PG_CLASS_ID)
            fprintf(stderr,
                "the oid(%u) of catalog is bigger than MAX_PG_CLASS_ID(%u)\n",
                mapfile->mappings[i].mapoid,
                MAX_PG_CLASS_ID);

        pg_class = pg_class_map[mapfile->mappings[i].mapoid];
        fprintf(stdout,
            "\t[%d] relid: %u, relfilenode: %u, catalog: %s\n",
            i,
            mapfile->mappings[i].mapoid,
            mapfile->mappings[i].mapfilenode,
            pg_class);
    }
    fclose(fd);

    return true;
}

static void fill_filenode_map(char** class_map)
{
    int i = 0;
    char* name = NULL;
    const PgClass_table cmap[CLASS_TYPE_NUM] = {{"pg_default_acl", 826},
        {"pg_pltemplate", 1136},
        {"pg_tablespace", 1213},
        {"pg_shdepend", 1214},
        {"pg_type", 1247},
        {"pg_attribute", 1249},
        {"pg_proc", 1255},
        {"pg_class", 1259},
        {"pg_authid", 1260},
        {
            "pg_auth_members",
            1261,
        },
        {"pg_database", 1262},
        {"pg_foreign_server", 1417},
        {"pg_user_mapping", 1418},
        {"pg_foreign_data_wrapper", 2328},
        {"pg_shdescription", 2396},
        {"pg_aggregate", 2600},
        {
            "pg_am",
            2601,
        },
        {"pg_amop", 2602},
        {"pg_amproc", 2603},
        {"pg_attrdef", 2604},
        {"pg_cast", 2605},
        {"pg_constraint", 2606},
        {"pg_conversion", 2607},
        {"pg_depend", 2608},
        {"pg_description", 2609},
        {"pg_index", 2610},
        {"pg_inherits", 2611},
        {"pg_language", 2612},
        {"pg_largeobject", 2613},
        {"pg_namespace", 2615},
        {"pg_opclass", 2616},
        {"pg_operator", 2617},
        {"pg_rewrite", 2618},
        {"pg_statistic", 2619},
        {"pg_trigger", 2620},
        {"pg_opfamily", 2753},
        {"pg_db_role_setting", 2964},
        {"pg_largeobject_metadata", 2995},
        {"pg_extension", 3079},
        {"pg_foreign_table", 3118},
        {"pg_enum", 3501},
        {"pg_seclabel", 3596},
        {"pg_ts_dict", 3600},
        {"pg_ts_parser", 3601},
        {"pg_ts_config", 3602},
        {"pg_ts_config_map", 3603},
        {"pg_ts_template", 3764},
        /* normal catalogs */
        {"pg_attrdef", 2830},
        {"pg_attrdef", 2831},
        {"pg_constraint", 2832},
        {"pg_constraint", 2833},
        {"pg_description", 2834},
        {"pg_description", 2835},
        {"pg_proc", 2836},
        {"pg_proc", 2837},
        {"pg_rewrite", 2838},
        {"pg_rewrite", 2839},
        {"pg_seclabel", 3598},
        {"pg_seclabel", 3599},
        {"pg_statistic", 2840},
        {"pg_statistic", 2841},
        {"pg_trigger", 2336},
        {"pg_trigger", 2337},
        /* shared catalogs */
        {"pg_database", 2844},
        {"pg_database", 2845},
        {"pg_shdescription", 2846},
        {"pg_shdescription", 2847},
        {"pg_db_role_setting", 2966},
        {"pg_db_role_setting", 2967},
        /* indexing */
        {"pg_aggregate_fnoid_index", 2650},
        {"pg_am_name_index", 2651},
        {"pg_am_oid_index", 2652},
        {"pg_amop_fam_strat_index", 2653},
        {"pg_amop_opr_fam_index", 2654},
        {"pg_amop_oid_index", 2756},
        {"pg_amproc_fam_proc_index", 2655},
        {"pg_amproc_oid_index", 2757},
        {"pg_attrdef_adrelid_adnum_index", 2656},
        {"pg_attrdef_oid_index", 2657},
        {"pg_attribute_relid_attnam_index", 2658},
        {"pg_attribute_relid_attnum_index", 2659},
        {"pg_authid_rolname_index", 2676},
        {"pg_authid_oid_index", 2677},
        {"pg_auth_members_role_member_index", 2694},
        {"pg_auth_members_member_role_index", 2695},
        {"pg_cast_oid_index", 2660},
        {"pg_cast_source_target_index", 2661},
        {"pg_class_oid_index", 2662},
        {"pg_class_relname_nsp_index", 2663},
        {"pg_collation_name_enc_nsp_index", 3164},
        {"pg_collation_oid_index", 3085},
        {"pg_constraint_conname_nsp_index", 2664},
        {"pg_constraint_conrelid_index", 2665},
        {"pg_constraint_contypid_index", 2666},
        {"pg_constraint_oid_index", 2667},
        {"pg_conversion_default_index", 2668},
        {"pg_conversion_name_nsp_index", 2669},
        {"pg_conversion_oid_index", 2670},
        {"pg_database_datname_index", 2671},
        {"pg_database_oid_index", 2672},
        {"pg_depend_depender_index", 2673},
        {"pg_depend_reference_index", 2674},
        {"pg_description_o_c_o_index", 2675},
        {"pg_shdescription_o_c_index", 2397},
        {"pg_enum_oid_index", 3502},
        {"pg_enum_typid_label_index", 3503},
        {"pg_enum_typid_sortorder_index", 3534},
        {"pg_index_indrelid_index", 2678},
        {"pg_index_indexrelid_index", 2679},
        {"pg_inherits_relid_seqno_index", 2680},
        {"pg_inherits_parent_index", 2187},
        {"pg_language_name_index", 2681},
        {"pg_language_oid_index", 2682},
        {"pg_largeobject_loid_pn_index", 2683},
        {"pg_largeobject_metadata_oid_index", 2996},
        {"pg_namespace_nspname_index", 2684},
        {"pg_namespace_oid_index", 2685},
        {"pg_opclass_am_name_nsp_index", 2686},
        {"pg_opclass_oid_index", 2687},
        {"pg_operator_oid_index", 2688},
        {"pg_operator_oprname_l_r_n_index", 2689},
        {"pg_opfamily_am_name_nsp_index", 2754},
        {"pg_opfamily_oid_index", 2755},
        {"pg_pltemplate_name_index", 1137},
        {"pg_proc_oid_index", 2690},
        {"pg_proc_proname_args_nsp_index", 2691},
        {"pg_rewrite_oid_index", 2692},
        {"pg_rewrite_rel_rulename_index", 2693},
        {"pg_shdepend_depender_index", 1232},
        {"pg_shdepend_reference_index", 1233},
        {"pg_statistic_relid_att_inh_index", 2696},
        {"pg_tablespace_oid_index", 2697},
        {"pg_tablespace_spcname_index", 2698},
        {"pg_trigger_tgconstraint_index", 2699},
        {"pg_trigger_tgrelid_tgname_index", 2701},
        {"pg_trigger_oid_index", 2702},
        {"pg_ts_config_cfgname_index", 3608},
        {"pg_ts_config_oid_index", 3712},
        {"pg_ts_config_map_index", 3609},
        {"pg_ts_dict_dictname_index", 3604},
        {"pg_ts_dict_oid_index", 3605},
        {"pg_ts_parser_prsname_index", 3606},
        {"pg_ts_parser_oid_index", 3607},
        {"pg_ts_template_tmplname_index", 3766},
        {"pg_ts_template_oid_index", 3767},
        {"pg_type_oid_index", 2703},
        {"pg_type_typname_nsp_index", 2704},
        {"pg_foreign_data_wrapper_oid_index", 112},
        {"pg_foreign_data_wrapper_name_index", 548},
        {"pg_foreign_server_oid_index", 113},
        {"pg_foreign_server_name_index", 549},
        {"pg_user_mapping_oid_index", 174},
        {"pg_user_mapping_user_server_index", 175},
        {"pg_foreign_table_relid_index", 3119},
        {"pg_default_acl_role_nsp_obj_index", 827},
        {"pg_default_acl_oid_index", 828},
        {"pg_db_role_setting_databaseid_rol_index", 2965},
        {"pg_seclabel_object_index", 3597},
        {"pg_extension_oid_index", 3080},
        {"pg_extension_name_index", 3081},
        {"pg_user_status", 3460},
        {"pg_shseclabel", 3592},
        {"pg_shseclabel_object_index", 3593},
        {"pg_user_status_index", 3461},
        {"pg_user_status_oid_index", 3462},
        {"pg_job_oid_index", 3466},
        {"pg_job_proc_oid_index", 3467},
        {"pg_job_schedule_oid_index", 3468},
        {"pg_job_jobid_index", 3469},
        {"pg_job_proc_procid_index", 3470},
        {"pg_job_schedule_schid_index", 3471},
        {"pg_directory_oid_index", 3223},
        {"pg_directory_dirname_index", 3224},
        {"pg_proc_lang_index", 3225},
        {"pg_proc_lang_namespace_index", 3226},
        {"pg_job", 9000},
        {"pg_job_schedule", 9001},
        {"pg_job_proc", 9002},
        {"pg_auth_history", 9108},
        {"pg_auth_history_index", 9110},
        {"pg_auth_history_oid_index", 9111},
        {"pg_directory", 3222},
        {"pgxc_node", 9015 /* PgxcNodeRelationId */},
        {"pgxc_group", 9014 /* PgxcGroupRelationId */},
        {"pg_resource_pool", 3450 /*  ResourcePoolRelationId */},
        {"pg_workload_group", 3451 /* WorkloadGroupRelationId */},
        {"pg_app_workloadgroup_mapping", 3464 /* AppWorkloadGroupMappingRelationId */},
        {"pgxc_node_oid_index", 9010},
        {"pgxc_node_name_index", 9011},
        {"pg_extension_data_source_oid_index", 2717},
        {"pg_extension_data_source_name_index", 2718}};

    for (i = 0; i < CLASS_TYPE_NUM; i++) {
        name = (char*)malloc(64 * sizeof(char));
        if (NULL == name) {
            printf("error allocate space\n");
            return;
        }
        if (NULL != cmap[i].class_name) {
            memcpy(name, cmap[i].class_name, strlen(cmap[i].class_name) + 1);
            class_map[cmap[i].ralation_id] = name;
            name = NULL;
        } else {
            free(name);
            name = NULL;
        }
    }
    return;
}


static bool HexStringToInt(char* path, int* result)
{
    int num = 0;
    int onechar = 0;
    int index = 0;

    if (NULL == path)
        return false;
    
    const char* file_name = basename(path);
    const char* temp = file_name;

    while (*temp++ != '\0')
        num++;

    while (num--) {
        if (file_name[num] >= 'A' && file_name[num] <= 'F')
            onechar = file_name[num] - 55;
        else if (file_name[num] >= '0' && file_name[num] <= '9')
            onechar = file_name[num] - 48;
        else
            return false;

        *result += onechar << (index * 4);
        index++;
    }

    return true;
}

/* parse subtran file */
static int parse_csnlog_file(char* filename)
{
    FILE* fd = NULL;
    int segnum = 0;
    /* one segment file has 32K xids */
    uint32 segnum_xid = CSNLOG_XACTS_PER_PAGE * CSNLOG_XACTS_PER_LSN_GROUP;
    /* One page has 1k xids */
    int page_xid = BLCKSZ / sizeof(CommitSeqNo);
    /* the first xid number of current segment file */
    TransactionId xid = 0;
    CommitSeqNo csn = 0;
    int nread = 0;
    int entry = 0;
    char buffer[BLCKSZ];
    const int failed_size = 128;
    char failed_info[failed_size];

    if (!HexStringToInt(filename, &segnum)) {
        fprintf(stderr, "%s input error \n", filename);
        return false;
    }

    xid = (uint64)segnum * segnum_xid;

    fd = fopen(filename, "rb");
    if (fd == NULL) {
        strerror_r(errno, failed_info, failed_size);
        fprintf(stderr, "%s open failed:%s \n", filename, failed_info);
        return false;
    }

    while ((nread = fread(buffer, 1, BLCKSZ, fd)) != 0) {
        if (nread != BLCKSZ) {
            fprintf(stderr, "read file error!\n");
            fclose(fd);
            return false;
        }

        while (entry < page_xid) {
            csn = *(CommitSeqNo*)(buffer + (entry * sizeof(CommitSeqNo)));
            fprintf(stdout, "xid %u, csn: %lu, is sub xid:%s, is prepare:%s \n", xid, GET_REAL_CSN_VALUE(csn),
                CSN_IS_SUBTRANS(csn) ? "true" : "false", CSN_IS_PREPARED(csn) ? "true" : "false");
            xid++;
            entry++;
        }

        entry = 0;
    }

    fclose(fd);
    return true;
}

int main(int argc, char** argv)
{
    int c;
    char* filename = NULL;

    const char* progname = NULL;
    uint32 start_point = 0;
    uint32 num_block = 0;
    progname = "pagehack";

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0 || strcmp(argv[1], "-h") == 0) {
            usage(progname);
            exit(0);
        }

        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            puts("pagehack (PostgreSQL) " PG_VERSION);
            exit(0);
        }
    }

    while ((c = getopt(argc, argv, "bf:o:t:vs:n:r:i:I:N:uwd")) != -1) {
        switch (c) {
            case 'f':
                filename = optarg;
                break;

            case 't': {
                int i;
                for (i = 0; i < NUM_HACKINGTYPE; ++i) {
                    if (strcmp(optarg, HACKINGTYPE[i]) == 0)
                        break;
                }

                hackingtype = (HackingType)i;
                if (hackingtype >= NUM_HACKINGTYPE) {
                    fprintf(stderr, "invalid hacking type (-t): %s\n", optarg);
                    exit(1);
                }
                break;
            }

            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit(1);
                break;
        }
    }

    if (NULL == filename) {
        fprintf(stderr, "must specify a file to parse.\n");
        exit(1);
    }


    switch (hackingtype) {
		case HACKING_HEAP:
			if (!parse_page_file(filename, HACKING_HEAP, start_point, num_block)) {
	            fprintf(stderr, "Error during parsing heap file %s.\n", filename);
	            exit(1);
	        }
	        break;
		case HACKING_INDEX_BTREE:
			if (!parse_page_file(filename, HACKING_INDEX_BTREE, start_point, num_block)) {
				fprintf(stderr, "Error during parsing index file %s\n", filename);
				exit(1);
			}
			break;

        case HACKING_COLSTORE:
            break;

        case HACKING_FILENODE:
            if (!parse_filenodemap_file(filename)) {
                fprintf(stderr, "Error during parsing filenode_map file %s\n", filename);
                exit(1);
            }
            break;
        case HACKING_CSNLOG:
            if (!parse_csnlog_file(filename)) {
                fprintf(stderr, "Error during parsing suntran file %s\n", filename);
                exit(1);
            }
            break;
        default:
            /* should be impossible to be here */
            Assert(false);
    }

    return 0;
}
