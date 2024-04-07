\echo Use "CREATE EXTENSION eds '1.0'" to load this file. \quit
CREATE SCHEMA eds;
CREATE TABLE eds.eds_instance_table(
    ins_id      serial          NOT NULL,
    ins_loc     varchar(32)     NOT NULL,
    ins_name    varchar(256)    NOT NULL,
    hostname    varchar(15)     NOT NULL,
    port        int8            NOT NULL,
    username    varchar(64)     NOT NULL,
    dbname      varchar(64)     NOT NULL,
    passwd      varchar(256)    NOT NULL,
    params      text                    ,
    isactive    bool        default false

);
CREATE UNIQUE INDEX eds_instance_table_ins_id_index
ON eds.eds_instance_table using btree(ins_id);
CREATE UNIQUE INDEX eds_instance_table_ins_nameloc_index
ON eds.eds_instance_table (ins_name,ins_loc);



CREATE OR REPLACE FUNCTION show_node(
    OUT ins_id      integer,
    OUT ins_loc     text,
    OUT ins_name    text,
    OUT hostname    text,
    OUT port        integer,
    OUT username    text,
    OUT dbname      text,
    OUT params      text,
    OUT isactive    boolean
)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'show_node';


