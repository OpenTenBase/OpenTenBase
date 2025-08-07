/*-------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "fmgr.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#ifdef __OPENTENBASE_C__
#include "nodes/pg_list.h"
#endif


#ifdef _SHARDING_
#define SMGR_TARGBLOCK_MAX_SHARDS MAX_SHARDS

typedef struct ShardTargBlock
{
	ShardID	shardid;
	uint16	hits;
	BlockNumber targblk;
}ShardTargBlock;

#define InitShardTargBlock(tb) \
			(tb)->shardid = InvalidShardID; \
			(tb)->hits = 0; \
			(tb)->targblk = InvalidBlockNumber;
#endif

#ifdef __OPENTENBASE_C__
typedef struct
{
	int64  data_offset;
	int64  data_end;
}smgr_dirty_element;

#define MAX_DIRTY_ELEMENT_NUM 256 /* col blocksize div write blocksize (assumed 4096) */

typedef struct
{
	int buffer;
	BlockNumber blkno;
	smgr_dirty_element dirty_array[MAX_DIRTY_ELEMENT_NUM];
	uint16 dirty_element_num;
	bool flushall; /* true if num equals max value or explictly set to true*/
	bool init;     /* true if has been inited */
} ColDirtyBufferEntry;

#endif

/*
 * smgr.c maintains a table of SMgrRelation objects, which are essentially
 * cached file handles.  An SMgrRelation is created (if not already present)
 * by smgropen(), and destroyed by smgrclose().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.
 * (But smgrclose() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrRelation may have an "owner", which is just a pointer to it from
 * somewhere else; smgr.c will clear this pointer if the SMgrRelation is
 * closed.  We use this to avoid dangling pointers from relcache to smgr
 * without having to make the smgr explicitly aware of relcache.  There
 * can't be more than one "owner" pointer per SMgrRelation, but that's
 * all we need.
 *
 * SMgrRelations that do not have an "owner" are considered to be transient,
 * and are deleted at end of transaction.
 */
typedef struct SMgrRelationData
{
	/* rnode is the hashtable lookup key, so it must be first! */
	RelFileNodeBackend smgr_rnode;	/* relation physical identifier */

	/* pointer to owning pointer, or NULL if none */
	struct SMgrRelationData **smgr_owner;

	/*
	 * These next three fields are not actually used or manipulated by smgr,
	 * except that they are reset to InvalidBlockNumber upon a cache flush
	 * event (in particular, upon truncation of the relation).  Higher levels
	 * store cached state here so that it will be reset when truncation
	 * happens.  In all three cases, InvalidBlockNumber means "unknown".
	 */
	BlockNumber smgr_targblock; /* current insertion target block */
	BlockNumber smgr_fsm_nblocks;	/* last known size of fsm fork */
#ifdef _SHARDING_
	BlockNumber *smgr_shard_targblocks;	
	BlockNumber smgr_ema_nblocks;
	bool		smgr_hasextent;
#endif
	BlockNumber smgr_vm_nblocks;	/* last known size of vm fork */

	/* additional public fields may someday exist here */

	/*
	 * Fields below here are intended to be private to smgr.c and its
	 * submodules.  Do not touch them from elsewhere.
	 */
	int			smgr_which;		/* storage manager selector */

	/*
	 * for md.c; per-fork arrays of the number of open segments
	 * (md_num_open_segs) and the segments themselves (md_seg_fds).
	 */
	int			md_num_open_segs[MAX_FORKNUM + 1];
	struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];

#ifdef __OPENTENBASE_C__
	bool		encrypt;
#endif
	bool		  smgr_haschecksum;

	/* if unowned, list link in list of all unowned SMgrRelations */
	struct SMgrRelationData *next_unowned_reln;
} SMgrRelationData;

typedef SMgrRelationData *SMgrRelation;

/* SMgr storage format */
typedef enum
{
	SMgr_Row_Storage = 0,
	SMgr_COL_Storage
} SMgrStorage;

#define  SMGR_STORE_SECTOR_SIZE 512

extern void smgrinit(void);
extern SMgrRelation smgropen(RelFileNode rnode, BackendId backend);
extern bool smgrexists(SMgrRelation reln, ForkNumber forknum);
extern void smgrsetowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclearowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclose(SMgrRelation reln);
extern void smgrcloseall(void);
extern void smgrclosenode(RelFileNodeBackend rnode);
extern void smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrdounlink(SMgrRelation reln, bool isRedo);
extern void smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo);
extern void smgrdounlinkfork(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrextend(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber blocknum, char *buffer, bool skipFsync);
extern void smgrprefetch(SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum);
extern void smgrread(SMgrRelation reln, ForkNumber forknum,
		 BlockNumber blocknum, char *buffer);
extern void smgrwrite(SMgrRelation reln, ForkNumber forknum,
		  BlockNumber blocknum, char *buffer, bool skipFsync);
extern void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
			  BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum);
extern void smgrtruncate(SMgrRelation reln, ForkNumber forknum,
			 BlockNumber nblocks);
extern void smgrimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void smgrpreckpt(void);
extern void smgrsync(void);
extern void smgrpostckpt(void);
#ifdef _SHARDING_
extern void smgrdealloc(SMgrRelation reln, ForkNumber forknum, BlockNumber from_blk);
extern void smgrrealloc(SMgrRelation reln, ForkNumber forknum, BlockNumber from_blk);
#endif
extern void AtEOXact_SMgr(void);
extern BlockNumber smgr_get_target_block(SMgrRelation rel, ShardID shardid);
extern void smgr_set_target_block(SMgrRelation rel, ShardID shardid, BlockNumber blkno);


/* internals: move me elsewhere -- ay 7/94 */

/* in md.c */
extern void mdinit(void);
extern void mdclose(SMgrRelation reln, ForkNumber forknum);
extern void mdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool mdexists(SMgrRelation reln, ForkNumber forknum);
extern void mdunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void mdextend(SMgrRelation reln, ForkNumber forknum,
		 BlockNumber blocknum, char *buffer, bool skipFsync);
extern void mdprefetch(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber blocknum);
extern void mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer);
extern void mdwrite(SMgrRelation reln, ForkNumber forknum,
		BlockNumber blocknum, char *buffer, bool skipFsync);
extern void mdwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber mdnblocks(SMgrRelation reln, ForkNumber forknum);
extern void mdtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber nblocks);
extern void mdimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void mdpreckpt(void);
extern void mdsync(void);
extern void mdpostckpt(void);
extern void SetForwardFsyncRequests(void);
extern void RememberFsyncRequest(RelFileNode rnode, ForkNumber forknum,
					 BlockNumber segno);
extern void ForgetRelationFsyncRequests(RelFileNode rnode, ForkNumber forknum);
extern void ForgetDatabaseFsyncRequests(Oid dbid);
#ifdef __OPENTENBASE_C__
extern int  mdblocksize(SMgrRelation reln);
extern void mdcolumncreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo);
extern bool mdcolumnexists(SMgrRelation reln, ForkNumber forkNum);
extern void mdcolumnunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo);
extern void mdcolumnextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync);
extern void mdcolumnprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
extern void mdcolumnread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer);
extern void mdcolumnwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,	char *buffer, bool skipFsync);
extern void mdcolumnwriteback(SMgrRelation reln, ForkNumber forknum,	BlockNumber blocknum, BlockNumber nblocks);
extern void mdcolumntruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);
extern void mdcolumnpostckpt(void);
extern BlockNumber mdcolumnsegmentextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
extern int32 mdcolumngetsegmentnumber(RelFileNodeBackend rnode, ForkNumber forknum);
extern int64 mdcolumngetsegmentsize(RelFileNodeBackend rnode, ForkNumber forknum);
extern bool mdcolumnisempty(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber mdcolumnnblocks(SMgrRelation reln, ForkNumber forknum);

#endif

#ifdef _SHARDING_
extern void mddealloc(SMgrRelation reln, ForkNumber forknum, BlockNumber from_blk);
extern void mdrealloc(SMgrRelation reln, ForkNumber forknum, BlockNumber from_blk);
#endif

#endif							/* SMGR_H */
