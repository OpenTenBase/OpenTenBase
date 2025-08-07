\c opentenbase_ora_package_function_regression_ora
\set ECHO none
\set VERBOSITY terse
select pg_sleep(1);
select dbms_pipe.receive_message('my_pipe',1); -- wait max 1 sec for message
select dbms_pipe.next_item_type(); -- -> 11, text
select dbms_pipe.unpack_message_text();
select dbms_pipe.next_item_type(); -- -> 11, text
select dbms_pipe.unpack_message_text();
select dbms_pipe.next_item_type(); -- -> 0, no more items
select dbms_pipe.remove_pipe('my_pipe');
