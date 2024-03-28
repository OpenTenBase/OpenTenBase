
/* contrib/hstore/hstore--1.1--1.2.sql */

/*如果脚本通过 psql 而不是通过 ALTER EXTENSION 命令加载，则输出提示信息并退出 */
\echo Use "ALTER EXTENSION hstore UPDATE TO '1.2'" to load this file. \quit


-- A version of 1.1 was shipped with these objects mistakenly in 9.3.0.
-- Therefore we only add them if we detect that they aren't already there and
-- dependent on the extension.
/*因此，我们仅在检测到它们不存在且依赖于扩展时才添加它们。*/

DO LANGUAGE plpgsql

$$

BEGIN

   -- 检查是否已经存在函数 hstore_to_json_loose，如果不存在，则创建相关对象
   PERFORM 1
   FROM pg_proc p
       JOIN  pg_depend d
          ON p.proname = 'hstore_to_json_loose'
            AND d.classid = 'pg_proc'::regclass
            AND d.objid = p.oid
            AND d.refclassid = 'pg_extension'::regclass
       JOIN pg_extension x
          ON d.refobjid = x.oid
            AND  x.extname = 'hstore';

   IF NOT FOUND
   THEN

        -- 创建函数 hstore_to_json
        CREATE FUNCTION hstore_to_json(hstore)
        RETURNS json
        AS 'MODULE_PATHNAME', 'hstore_to_json'
        LANGUAGE C IMMUTABLE STRICT;

        -- 创建从 hstore 到 json 的转换
        CREATE CAST (hstore AS json)
          WITH FUNCTION hstore_to_json(hstore);

        -- 创建函数 hstore_to_json_loose
        CREATE FUNCTION hstore_to_json_loose(hstore)
        RETURNS json
        AS 'MODULE_PATHNAME', 'hstore_to_json_loose'
        LANGUAGE C IMMUTABLE STRICT;

   END IF;

END;

$$;
