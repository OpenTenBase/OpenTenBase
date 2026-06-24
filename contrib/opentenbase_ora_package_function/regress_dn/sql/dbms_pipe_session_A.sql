\c opentenbase_ora_package_function_regression_ora
\set ECHO none
SET client_min_messages = warning;
select dbms_pipe.remove_pipe('pipe_test_owner_created_notifier');
select dbms_pipe.remove_pipe('my_pipe');
select dbms_pipe.create_pipe('my_pipe',10,true); -- explicit pipe creating
select dbms_pipe.pack_message('neco je jinak');
select dbms_pipe.pack_message('anything is else');
select dbms_pipe.send_message('my_pipe',20,0); -- change limit and send without waiting
SELECT name FROM dbms_pipe.__list_pipes() AS (Name varchar, Items int, Size int, "limit" int, "private" bool, "owner" varchar);
