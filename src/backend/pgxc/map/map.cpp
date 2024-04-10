/*-------------------------------------------------------------------------
 *
 * map.cpp
 *
 *	  cpp map wrapper
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 * 
 * src/backend/pgxc/map/map.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <map>

#include "pgxc/map.h"

#define MAPNULL (void *)0

typedef struct Key
{
    int64_t k1;
    int k2;

    bool operator==(const Key &k) const
    {
        if (k1 == k.k1 && k2 == k.k2)
        {
            return true;
        }
        return false;
    }

    bool operator<(const Key &k) const
    {
        if (k1 > k.k1)
        {
            return false;
        }
        else if (k1 < k.k1)
        {
            return true;
        }
        else if (k2 < k.k2)
        {
            return true;
        }
        return false;
    }

} Key;

typedef struct DataChannelMap
{
    std::map<Key, void *> map;
    std::map<Key, void *>::iterator iter;
} DataChannelMap;

extern void *map_create()
{
    return new DataChannelMap();
}

extern int map_put(void *map, int64_t k1, int k2, void *v, bool overwrite)
{
    if (map == MAPNULL)
    {
        return -1;
    }
    DataChannelMap *m = (DataChannelMap *)map;
    std::pair<std::map<Key, void *>::iterator, bool> ret;

    Key k = {k1, k2};
    if (overwrite)
    {
        map_erase(map, k1, k2);
    }

    ret = m->map.insert(std::pair<Key, void *>(k, v));
    if (ret.second == false)
    {
        return -1;
    }
    return 0;
}

extern void *map_get(void *map, int64_t k1, int k2)
{
    if (map == MAPNULL)
    {
        return MAPNULL;
    }
    DataChannelMap *m = (DataChannelMap *)map;
    std::map<Key, void *>::iterator it;

    Key k = {k1, k2};
    it = m->map.find(k);
    if (it != m->map.end())
    {
        return it->second;
    }
    return MAPNULL;
}

extern void *map_erase(void *map, int64_t k1, int k2)
{
    if (map == MAPNULL)
    {
        return MAPNULL;
    }
    DataChannelMap *m = (DataChannelMap *)map;
    std::map<Key, void *>::iterator it;
    void *ret = MAPNULL;

    Key k = {k1, k2};
    it = m->map.find(k);
    if (it != m->map.end())
    {
        ret = it->second;
        m->map.erase(it);
    }
    return ret;
}

extern int map_begin_iter(void *map)
{
    if (map == MAPNULL)
    {
        return -1;
    }
    DataChannelMap *m = (DataChannelMap *)map;

    m->iter = m->map.begin();
    return 0;
}

extern int map_iter_next(void *map)
{
    if (map == MAPNULL)
    {
        return -1;
    }
    DataChannelMap *m = (DataChannelMap *)map;

    if (m->iter != m->map.end())
    {
        ++m->iter;
        return 0;
    }
    return -1;
}

extern int map_iter_key(void *map, int64_t *k1, int *k2)
{
    if (map == MAPNULL)
    {
        return -1;
    }
    DataChannelMap *m = (DataChannelMap *)map;

    if (m->iter != m->map.end())
    {
        *k1 = m->iter->first.k1;
        *k2 = m->iter->first.k2;
        return 0;
    }
    return -1;
}

extern void *map_iter_value(void *map)
{
    if (map == MAPNULL)
    {
        return NULL;
    }
    DataChannelMap *m = (DataChannelMap *)map;

    if (m->iter != m->map.end())
    {
        return m->iter->second;
    }
    return NULL;
}

extern int map_delete(void *map)
{
    if (map == MAPNULL)
    {
        return 0;
    }

    DataChannelMap *m = (DataChannelMap *)map;
    delete m;

    return 0;
}

extern int map_size(void *map)
{
    if (map == MAPNULL)
    {
        return 0;
    }

    DataChannelMap *m = (DataChannelMap *)map;
    return m->map.size();
}


/* Forwarder memory check map */
typedef struct FWDMemoryCheckMap
{
    std::map<void *, void *> map;
    std::map<void *, void *>::iterator iter;
} FWDMemoryCheckMap;

extern void *fwd_memory_check_map_create()
{
    return new FWDMemoryCheckMap();
}

extern int fwd_memory_check_map_put(void *map, void *address, void *value)
{
    if (map == MAPNULL)
    {
        return -1;
    }
    FWDMemoryCheckMap *m = (FWDMemoryCheckMap *)map;
    std::pair<std::map<void *, void *>::iterator, bool> ret;

    ret = m->map.insert(std::pair<void *, void *>(address, value));
    if (ret.second == false)
    {
        return -1;
    }
    return 0;
}


extern void *fwd_memory_check_map_get(void *map, void *address)
{
    if (map == MAPNULL)
    {
        return MAPNULL;
    }
    FWDMemoryCheckMap *m = (FWDMemoryCheckMap *)map;
    std::map<void *, void *>::iterator it;

    it = m->map.find(address);
    if (it != m->map.end())
    {
        return it->second;
    }
    return NULL;
}