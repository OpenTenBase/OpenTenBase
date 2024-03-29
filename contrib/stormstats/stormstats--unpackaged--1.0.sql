/* contrib/stormstats/stormstats--unpackaged--1.0.sql */

-- 添加函数 storm_database_stats() 到 stormstats 扩展
ALTER EXTENSION stormstats ADD function storm_database_stats();

-- 添加视图 storm_database_stats 到 stormstats 扩展
ALTER EXTENSION stormstats ADD view storm_database_stats;
