/*-------------------------------------------------------------------------
 *
 * map.h
 *
 *	  header of cpp map wrapper
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 *
 * src/include/pgxc/map.h
 *
 *-------------------------------------------------------------------------
 */

#ifdef __cplusplus
	extern "C" {
#endif
		#include "c.h"

		extern void *map_create();
		extern int   map_put(void *map, int64 k1, int k2, void *v, bool overwrite);
		extern void *map_get(void *map, int64 k1, int k2);
		extern void *map_erase(void *map, int64 k1, int k2);
		extern int   map_delete(void *map);
		extern int   map_begin_iter(void *map);
    	extern int   map_iter_next(void *map);
		extern int   map_iter_key(void *map, int64 *k1, int *k2);
		extern void *map_iter_value(void *map);
		extern int   map_size(void *map);

		extern void *fwd_memory_check_map_create();
		extern int fwd_memory_check_map_put(void *map, void *address, void *value);
		extern void *fwd_memory_check_map_get(void *map, void *address);
#ifdef __cplusplus
	}
#endif
