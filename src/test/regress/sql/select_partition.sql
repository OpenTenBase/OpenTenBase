--
-- PARTITION
--
CREATE TABLE t_base (
    id          serial, 
    country     text, 
    task        text, 
    turnover    numeric
) PARTITION BY LIST (country);

CREATE TABLE t_austria PARTITION OF t_base FOR VALUES IN ('Austria');

CREATE TABLE t_usa PARTITION OF t_base FOR VALUES IN ('USA');

CREATE TABLE t_ger_swiss PARTITION OF t_base FOR VALUES IN ('Germany', 'Switzerland');

CREATE TABLE t_rest PARTITION OF t_base DEFAULT ;

INSERT INTO t_base (country, task, turnover) VALUES ('Uganda', 'Some task', 200);

INSERT INTO t_base (country, task, turnover) VALUES ('USA', 'taskUSA', 2);

INSERT INTO t_base (country, task, turnover) VALUES ('Austria', 'taskAustria', 1000);

INSERT INTO t_base (country, task, turnover) VALUES ('Germany', 'taskGermany', 500);

INSERT INTO t_base (country, task, turnover) VALUES ('Switzerland', 'taskSwiss', 700);

INSERT INTO t_rest (country, task, turnover) VALUES ('Canada', 'taskCanada', 300);


select * from t_base partition (t_rest);
select * from t_base partition (t_austria);
select * from t_base partition (t_usa);
select * from t_base partition (t_ger_swiss);