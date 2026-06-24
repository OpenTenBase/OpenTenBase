\c opentenbase_ora_package_function_regression_ora

SET client_min_messages = error;
create extension if not exists opentenbase_ora_package_function;
reset client_min_messages;

CREATE TABLE invoice (
	invoice_no    integer        PRIMARY KEY,
	seller_no     integer,
	invoice_date  date,
	invoice_amt   numeric(13,2)
);
CREATE MATERIALIZED VIEW sales_summary AS
SELECT
	seller_no,
	invoice_date,
	sum(invoice_amt)::numeric(13,2) as sales_amt
FROM invoice
WHERE invoice_date < CURRENT_DATE
GROUP BY seller_no, invoice_date
ORDER BY seller_no, invoice_date;

CREATE UNIQUE INDEX sales_summary_seller
ON sales_summary (seller_no, invoice_date);

insert into invoice values(1, 1, to_date('2024-05-15 00:00:00', 'yyyy-mm-dd HH24:mi:ss'), 12.3);
insert into invoice values(2, 1, to_date('2024-05-15 00:00:00', 'yyyy-mm-dd HH24:mi:ss'), 10.3);

select * from invoice order by invoice_no;
select * from sales_summary;
call dbms_refresh.refresh('sales_summary');
select * from sales_summary;

insert into invoice values(3, 2, to_date('2024-05-16 00:00:00', 'yyyy-mm-dd HH24:mi:ss'), 13.3);
insert into invoice values(4, 2, to_date('2024-05-16 00:00:00', 'yyyy-mm-dd HH24:mi:ss'), 15.2);

select * from invoice order by invoice_no;
select * from sales_summary;
call dbms_refresh.refresh('sales_summary');
select * from sales_summary;
