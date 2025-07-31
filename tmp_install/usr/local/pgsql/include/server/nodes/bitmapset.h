/*-------------------------------------------------------------------------
 *
 * bitmapset.h
 *      PostgreSQL generic bitmap set package
 *
 * A bitmap set can represent any set of nonnegative integers, although
 * it is mainly intended for sets where the maximum value is not large,
 * say at most a few hundred.  By convention, a NULL pointer is always
 * accepted by all operations to represent the empty set.  (But beware
 * that this is not the only representation of the empty set.  Use
 * bms_is_empty() in preference to testing for NULL.)
 *
 *
 * Copyright (c) 2003-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/nodes/bitmapset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BITMAPSET_H
#define BITMAPSET_H

/*
 * Forward decl to save including pg_list.h
 */
struct List;

/*
 * Data representation
 */

/* The unit size can be adjusted by changing these three declarations: */
#define BITS_PER_BITMAPWORD 32
typedef uint32 bitmapword;        /* must be an unsigned type */
typedef int32 signedbitmapword; /* must be the matching signed type */

typedef struct Bitmapset
{
    int            nwords;            /* number of words in array */
    bitmapword    words[FLEXIBLE_ARRAY_MEMBER];    /* really [nwords] */
} Bitmapset;


/* result of bms_subset_compare */
typedef enum
{
    BMS_EQUAL,                    /* sets are equal */
    BMS_SUBSET1,                /* first set is a subset of the second */
    BMS_SUBSET2,                /* second set is a subset of the first */
    BMS_DIFFERENT                /* neither set is a subset of the other */
} BMS_Comparison;

/* result of bms_membership */
typedef enum
{
    BMS_EMPTY_SET,                /* 0 members */
    BMS_SINGLETON,                /* 1 member */
    BMS_MULTIPLE                /* >1 member */
} BMS_Membership;

#ifdef _MIGRATE_
#define WORDNUM(x)    ((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)    ((x) % BITS_PER_BITMAPWORD)

#define BITMAPSET_SIZE(nwords)    \
    (offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))
#endif


/*
 * function prototypes in nodes/bitmapset.c
 */

extern Bitmapset *bms_copy(const Bitmapset *a);
extern bool bms_equal(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_make_singleton(int x);

#ifdef _MIGRATE_
extern Bitmapset *bms_make(char *space, int bit_len);
extern void bms_clear(Bitmapset *a);
extern Bitmapset* bms_init_set(int32 n);
#endif

extern void bms_free(Bitmapset *a);

extern Bitmapset *bms_union(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_intersect(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_difference(const Bitmapset *a, const Bitmapset *b);
extern bool bms_is_subset(const Bitmapset *a, const Bitmapset *b);
extern BMS_Comparison bms_subset_compare(const Bitmapset *a, const Bitmapset *b);
extern bool bms_is_member(int x, const Bitmapset *a);
extern bool bms_overlap(const Bitmapset *a, const Bitmapset *b);
extern bool bms_overlap_list(const Bitmapset *a, const struct List *b);
extern bool bms_nonempty_difference(const Bitmapset *a, const Bitmapset *b);
extern int    bms_singleton_member(const Bitmapset *a);
extern bool bms_get_singleton_member(const Bitmapset *a, int *member);
extern int    bms_num_members(const Bitmapset *a);

/* optimized tests when we don't need to know exact membership count: */
extern BMS_Membership bms_membership(const Bitmapset *a);
extern bool bms_is_empty(const Bitmapset *a);

/* these routines recycle (modify or free) their non-const inputs: */

extern Bitmapset *bms_add_member(Bitmapset *a, int x);
extern Bitmapset *bms_del_member(Bitmapset *a, int x);
extern Bitmapset *bms_add_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_add_range(Bitmapset *a, int lower, int upper);
extern Bitmapset *bms_int_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_del_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_join(Bitmapset *a, Bitmapset *b);
#ifdef _SHARDING_
extern Bitmapset *bms_trun_members(Bitmapset *a, int x);
extern Bitmapset *bms_clean_members(Bitmapset *a);
#endif

/* support for iterating through the integer elements of a set: */
extern int    bms_first_member(Bitmapset *a);
extern int    bms_next_member(const Bitmapset *a, int prevbit);

/* support for hashtables using Bitmapsets as keys: */
extern uint32 bms_hash_value(const Bitmapset *a);

#ifdef XCP
extern int    bms_any_member(Bitmapset *a);
#endif

#endif                            /* BITMAPSET_H */
