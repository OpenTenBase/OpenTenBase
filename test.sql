drop table t_austria cascade;
drop table t_ger_swiss cascade;
drop table t_rest cascade;
drop table t_turnover cascade;
drop table t_usa cascade;
drop table t_base cascade;
CREATE TABLE t_base (
    id          serial, 
    country     text, 
    t           timestamptz, 
    task        text, 
    turnover    numeric
) PARTITION BY LIST (country);
 
CREATE TABLE t_austria 
    PARTITION OF t_base FOR VALUES IN ('Austria');
 
CREATE TABLE t_usa 
    PARTITION OF t_base FOR VALUES IN ('USA');
 
CREATE TABLE t_ger_swiss 
    PARTITION OF t_base FOR VALUES IN ('Germany', 'Switzerland');

CREATE TABLE t_rest PARTITION OF t_base DEFAULT ;

INSERT INTO t_base (country, t, task, turnover) 
VALUES ('Uganda', now(), 'Some task', 200) ;

INSERT INTO t_base (country, t, task, turnover) 
VALUES ('USA', now(), 'taskUSA', 2) ;



alter table t_base DETACH partition t_usa;

select * from t_base partition  (t_rest);

drop table t_base cascade;
