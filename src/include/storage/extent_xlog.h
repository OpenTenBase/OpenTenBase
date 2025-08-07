/*-------------------------------------------------------------------------
 *
 * extentmapping_xlog.h
 *	  Basic buffer manager data types.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/extentmapping_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _EXTENTMAPPING_XLOG_H_
#define _EXTENTMAPPING_XLOG_H_

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

#define XLOG_EXTENT_NEW_EXTENT			0x00
#define XLOG_EXTENT_UPDATE_EME			0x10
#define XLOG_EXTENT_APPEND_EXTENT		0x20
#define XLOG_EXTENT_ATTACH_EXTENT		0x30
#define XLOG_EXTENT_DETACH_EXTENT		0x40
#define XLOG_EXTENT_MAKE_FULL			0x50
#define XLOG_EXTENT_MAKE_AVAIL			0x60
#define XLOG_EXTENT_FREE_DISK			0x70
#define XLOG_EXTENT_TRUNCATE			0x80
#define XLOG_EXTENT_COMMON				0x90
#define XLOG_EXTENT_EXTEND				0xA0

#define XLOG_EXTENT_OPMASK				0xF0

#define FragTag_EXTENT_XLOG_SETEOB		0x01
#define FragTag_EXTENT_XLOG_EXTENDEOB	0x02
#define FragTag_EXTENT_XLOG_SETEME		0x03
#define FragTag_EXTENT_XLOG_EXTENDEME	0x04
#define FragTag_EXTENT_XLOG_INITEME		0x05
#define FragTag_EXTENT_XLOG_CLEANEME	0x06
#define FragTag_EXTENT_XLOG_SETESA		0x07
#define FragTag_EXTENT_XLOG_TRUNCATE	0x08
#define FragTag_EXTENT_XLOG_TRUNCEOB	0x09
#define FragTag_EXTENT_XLOG_CLEANEOB	0x0a
#define FragTag_EXTENT_XLOG_TRUNCEMA	0x0b
#define FragTag_EXTENT_XLOG_CLEANEMA	0x0c



#define INIT_EXLOG_SETEOB(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_SETEOB)
#define INIT_EXLOG_EXTENDEOB(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_EXTENDEOB)
#define INIT_EXLOG_SETEME(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_SETEME)
#define INIT_EXLOG_EXTENDEME(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_EXTENDEME)
#define INIT_EXLOG_INITEME(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_INITEME)
#define INIT_EXLOG_CLEANEME(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_CLEANEME)
#define INIT_EXLOG_SETESA(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_SETESA)
#define INIT_EXLOG_TRUNCATE(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_TRUNCATE)
#define INIT_EXLOG_TRUNCEOB(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_TRUNCEOB)
#define INIT_EXLOG_CLEANEOB(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_CLEANEOB)
#define INIT_EXLOG_TRUNCEMA(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_TRUNCEMA)
#define INIT_EXLOG_CLEANEMA(xlrecptr) ((xlrecptr)->tag = FragTag_EXTENT_XLOG_CLEANEMA)

typedef struct xl_extent_seteob
{
	int8	tag;
	int16	slot;
	bool 	setfree;
}xl_extent_seteob;

#define SizeOfSetEOB 	sizeof(xl_extent_seteob)


typedef struct xl_extent_extendeob
{
	int8	tag;
	int8	flags;
	int16	slot;
	int16	n_eobs;
	int32 	setfree_start;
	int32	setfree_end;
}xl_extent_extendeob;

#define SizeOfExtendEOB 	sizeof(xl_extent_extendeob)

#define EXTEND_EOB_FLAGS_SETFREE 	0x01


typedef struct xl_extent_seteme
{
	int8	tag;
	int16 	slot;
	int16	setflag;
	ExtentID 	extentid;
	ExtentMappingElement eme;
	RelFileNode	rnode;
	bool		checksum_enabled;
}xl_extent_seteme;

#define SizeOfSetEME 	sizeof(xl_extent_seteme)

typedef struct xl_extent_extendeme
{
	int8	tag;
	int8	flags;
	int16	n_emes;
	int32	setfree_start;
	int32	setfree_end;
}xl_extent_extendeme;

#define SizeOfExtendEME 	sizeof(xl_extent_extendeme)
#define EXTEND_EME_FLAGS_SETFREE 	0x01

typedef struct xl_extent_initeme
{
	int8	tag;
	int16 	slot;
	int8	freespace;
	ShardID	shardid;
}xl_extent_initeme;

#define SizeOfInitEME 	sizeof(xl_extent_initeme)

typedef struct xl_extent_cleaneme
{
	int8	tag;
	int16 	slot;
}xl_extent_cleaneme;

#define SizeOfCleanEME	sizeof(xl_extent_cleaneme)

typedef struct xl_extent_setesa
{
	int8	tag;
	int16 	slot;
	int16	setflag;	
	EMAShardAnchor	anchor;
}xl_extent_setesa;

#define SizeOfSetESA	sizeof(xl_extent_setesa)

typedef struct xl_extent_truncate
{
	int8	tag;
	RelFileNode rnode;
}xl_extent_truncate;

#define SizeOfTruncateExtentSeg	sizeof(xl_extent_truncate)

typedef struct xl_extent_trunceob
{
	int8	tag;
	int16	pageno;
	int32	offset;
	RelFileNode rnode;	
}xl_extent_trunceob;

#define SizeOfTruncEOB sizeof(xl_extent_trunceob)

typedef struct xl_extent_cleaneob
{
	int8	tag;
	int16	pageno;
	RelFileNode rnode;
}xl_extent_cleaneob;

#define SizeOfCleanEOB sizeof(xl_extent_cleaneob)

typedef struct xl_extent_truncema
{
	int8	tag;
	int16	pageno;
	int32	offset;
	RelFileNode rnode;	
}xl_extent_truncema;

#define SizeOfTruncEMA sizeof(xl_extent_truncema)

typedef struct xl_extent_cleanema
{
	int8	tag;
	int16	pageno;
	RelFileNode rnode;
}xl_extent_cleanema;

#define SizeOfCleanEMA sizeof(xl_extent_cleanema)


extern void extent_redo(XLogReaderState *record);
extern void extent_desc(StringInfo buf, XLogReaderState *record);
extern const char *extent_identify(uint8 info);

#endif							/* _EXTENTMAPPING_XLOG_H_ */
