/* contrib/adminpack/adminpack--1.0.sql */

-- 如果脚本通过 psql 运行而不是通过 CREATE EXTENSION 运行，则报错
\echo 使用 "CREATE EXTENSION adminpack" 命令来加载此文件。 \quit

/* ***********************************************
 * PostgreSQL 管理功能
 * *********************************************** */

/* 通用文件访问函数 */

-- 写文件
CREATE FUNCTION pg_catalog.pg_file_write(filename text, data text, append bool)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_file_write'
LANGUAGE C VOLATILE STRICT;

-- 重命名文件
CREATE FUNCTION pg_catalog.pg_file_rename(oldname text, newname text, backup text)
RETURNS bool
AS 'MODULE_PATHNAME', 'pg_file_rename'
LANGUAGE C VOLATILE;

-- 重命名文件（简化版本）
CREATE FUNCTION pg_catalog.pg_file_rename(oldname text, newname text)
RETURNS bool
AS 'SELECT pg_catalog.pg_file_rename($1, $2, NULL::pg_catalog.text);'
LANGUAGE SQL VOLATILE STRICT;

-- 删除文件
CREATE FUNCTION pg_catalog.pg_file_unlink(filename text)
RETURNS bool
AS 'MODULE_PATHNAME', 'pg_file_unlink'
LANGUAGE C VOLATILE STRICT;

-- 列出日志目录下的文件
CREATE FUNCTION pg_catalog.pg_logdir_ls()
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_logdir_ls'
LANGUAGE C VOLATILE STRICT;


/* 重命名现有后端函数以实现与 pgAdmin 的兼容性 */

-- 读文件
CREATE FUNCTION pg_catalog.pg_file_read(filename text, start bigint, length bigint)
RETURNS text
AS 'pg_read_file'
LANGUAGE INTERNAL VOLATILE STRICT;

-- 获取文件长度
CREATE FUNCTION pg_catalog.pg_file_length(filename text)
RETURNS bigint
AS 'SELECT size FROM pg_catalog.pg_stat_file($1)'
LANGUAGE SQL VOLATILE STRICT;

-- 旋转日志文件
CREATE FUNCTION pg_catalog.pg_logfile_rotate()
RETURNS int4
AS 'pg_rotate_logfile'
LANGUAGE INTERNAL VOLATILE STRICT;
