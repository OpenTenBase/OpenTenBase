/*-------------------------------------------------------------------------
 *
 * gdd.h
 *	  Global Deadlock Detector entry
 *
 *
 * Copyright (c) 2018-2020 VMware, Inc. or its affiliates.
 * Copyright (c) 2021-Present OpenTenBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/include/utils/gdd.h
 *-------------------------------------------------------------------------
 */

#ifndef GDD_H
#define GDD_H

extern void GlobalDeadLockDetectorMain(Datum main_arg);

extern void ApplyGlobalDeadLockDetectorRegister(void);
#endif   /* GDD_H */
