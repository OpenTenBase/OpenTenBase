/*-------------------------------------------------------------------------
 *
 * tupdesc_details.h
 *      POSTGRES tuple descriptor definitions we can't include everywhere
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * src/include/access/tupdesc_details.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TUPDESC_DETAILS_H
#define TUPDESC_DETAILS_H

/*
 * Structure used to represent value to be used when the attribute is not
 * present at all in a tuple, i.e. when the column was created after the tuple
 */

typedef struct attrMissing
{
    bool        ammissingPresent;    /* true if non-NULL missing value exists */
    Datum        ammissing;        /* value when attribute is missing */
} AttrMissing;

#endif                            /* TUPDESC_DETAILS_H */
