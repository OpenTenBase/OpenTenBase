/*-------------------------------------------------------------------------
 *
 * datapagemap.h
 *
 * Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATAPAGEMAP_H
#define DATAPAGEMAP_H

#include "storage/relfilenode.h"
#include "storage/block.h"


typedef struct estoredatapage
{
	uint32 offset;
	uint32 size;
} estoredatapage;

struct datapagemap
{
	char	   *bitmap;
	int			bitmapsize;
	estoredatapage	*estorearray;
	uint32			estorearray_size;
};

typedef struct datapagemap datapagemap_t;
typedef struct datapagemap_iterator datapagemap_iterator_t;

extern void datapagemap_add(datapagemap_t *map, BlockNumber blkno);
extern datapagemap_iterator_t *datapagemap_iterate(datapagemap_t *map);
extern bool datapagemap_next(datapagemap_iterator_t *iter, BlockNumber *blkno);
extern void datapagemap_print(datapagemap_t *map);

extern void datapagemap_estore_add(datapagemap_t *map, uint32 offset, uint32 size);

#endif							/* DATAPAGEMAP_H */
