-- OpenTenBase benchmark 的基础建表脚本。
-- 执行前默认目标数据库中已经存在 default_group。

DROP TABLE IF EXISTS perf_event;
DROP TABLE IF EXISTS perf_order;
DROP TABLE IF EXISTS perf_user;
DROP TABLE IF EXISTS perf_city;

DROP SEQUENCE IF EXISTS perf_event_id_seq;
DROP SEQUENCE IF EXISTS perf_order_id_seq;

CREATE SEQUENCE perf_order_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE perf_event_id_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE perf_user (
    user_id     int NOT NULL,
    user_name   text NOT NULL,
    age         int NOT NULL,
    city_id     int NOT NULL,
    created_at  timestamp NOT NULL DEFAULT now()
) DISTRIBUTE BY SHARD(user_id) TO GROUP default_group;

CREATE TABLE perf_order (
    order_id    bigint NOT NULL DEFAULT nextval('perf_order_id_seq'),
    user_id     int NOT NULL,
    amount      numeric(12,2) NOT NULL,
    status      int NOT NULL,
    created_at  timestamp NOT NULL DEFAULT now()
) DISTRIBUTE BY SHARD(user_id) TO GROUP default_group;

CREATE TABLE perf_city (
    city_id     int NOT NULL,
    city_name   text NOT NULL
) DISTRIBUTE BY REPLICATION TO GROUP default_group;

CREATE TABLE perf_event (
    event_id    bigint NOT NULL DEFAULT nextval('perf_event_id_seq'),
    user_id     int NOT NULL,
    event_type  int NOT NULL,
    payload     text NOT NULL,
    created_at  timestamp NOT NULL DEFAULT now()
) DISTRIBUTE BY SHARD(user_id) TO GROUP default_group;

ALTER SEQUENCE perf_order_id_seq OWNED BY perf_order.order_id;
ALTER SEQUENCE perf_event_id_seq OWNED BY perf_event.event_id;

CREATE INDEX perf_user_user_id_idx ON perf_user (user_id);
CREATE INDEX perf_user_city_id_idx ON perf_user (city_id);

CREATE INDEX perf_order_user_id_idx ON perf_order (user_id);
CREATE INDEX perf_order_status_idx ON perf_order (status);
CREATE INDEX perf_order_created_at_idx ON perf_order (created_at);

CREATE INDEX perf_event_user_id_idx ON perf_event (user_id);
CREATE INDEX perf_event_event_type_idx ON perf_event (event_type);
CREATE INDEX perf_event_created_at_idx ON perf_event (created_at);
