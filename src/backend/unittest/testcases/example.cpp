/*-------------------------------------------------------------------------
 *
 * checksum.cpp
 *	  OpenTenBase checksum unit test
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/unittest/storage/page/test_bufpage.cpp
 *
 *-------------------------------------------------------------------------
 */
extern "C" {
#include "postgres.h"
#include "c.h"

#include "access/htup_details.h"
#include "access/itup.h"
#include "access/xlog.h"
#include "storage/checksum.h"
#include "storage/bufpage.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
}

/* C++ conflict with postgres */
#undef Max
#undef StrNCpy

#include "gtest/gtest.h"

BlockNumber
GetRandomPage(Page page)
{
#define RANDOM_ITEM_BUFF 2048
	Size i;
	Item item;
	Size item_len;
	OffsetNumber offsetNumber;
	BlockNumber blocknum = rand();

	/* initialize random seed */
	srand(time(NULL));

	PageInit(page, BLCKSZ, 0);
	item = (Item) malloc(RANDOM_ITEM_BUFF);

	/* fill page with random items */
	while (true)
	{
		item_len = rand() % RANDOM_ITEM_BUFF;

		for (i = 0; i < item_len; i++)
			item[i] = rand() % 256;

		offsetNumber = PageAddItem(page, item,
		                           item_len, InvalidOffsetNumber, false, true);

		/* page is full */
		if (offsetNumber == InvalidOffsetNumber)
		{
			break;
		}
	}
	return blocknum;
}

BlockNumber
GetCheckSumRandomPage(Page page)
{
	BlockNumber blkno;

	blkno = GetRandomPage(page);

	PageSetChecksumInplace(page, blkno);

	return blkno;
}

TEST(PageChecksum, PageOn_TableOn_Success)
{
	Page page;
	BlockNumber blockNumber;

	page = (Page) malloc(BLCKSZ);

	blockNumber = GetCheckSumRandomPage(page);

	ASSERT_EQ(PageIsVerified(page, blockNumber),true);

	free(page);
}

TEST(PageChecksum, PageOn_TableOn_Failure)
{
	Page page;
	PageHeader pageHeader;
	BlockNumber blockNumber;

	page = (Page) malloc(BLCKSZ);
	pageHeader = (PageHeader) page;

	blockNumber = GetCheckSumRandomPage(page);

	pageHeader->pd_checksum++;

	ASSERT_EQ(PageIsVerified(page, blockNumber), false);

	free(page);
}

TEST(PageChecksum, PageOn_TableOff)
{
	Page page;
	BlockNumber blockNumber;

	page = (Page) malloc(BLCKSZ);

	blockNumber = GetCheckSumRandomPage(page);

	ASSERT_EQ(PageIsVerified(page ,blockNumber), true);

	free(page);
}

TEST(PageChecksum, PageOff_TableON_Success)
{
	Page page;
	PageHeader pageHeader;
	BlockNumber blockNumber;

	page = (Page) malloc(BLCKSZ);
	pageHeader = (PageHeader) page;

	blockNumber = GetRandomPage(page);

	ASSERT_EQ(PageIsVerified(page ,blockNumber), false);
	ASSERT_EQ(pageHeader->pd_checksum, 0);

	free(page);
}

TEST(PageChecksum, PageOff_TableON_Failure)
{
	Page page;
	PageHeader pageHeader;
	BlockNumber blockNumber;

	page = (Page) malloc(BLCKSZ);
	pageHeader = (PageHeader) page;

	blockNumber = GetRandomPage(page);

	pageHeader->pd_checksum = 1;

	ASSERT_EQ(PageIsVerified(page ,blockNumber),false);

	free(page);
}
