\c opentenbase_ora_package_function_regression_ora
-- pyDE1w==AQAAAAAAAAA=
select dbms_rowid.rowid_block_number('pyDE1w==AAAAAA==AQAAAAAAAAA=');
declare
r rowid;
begin
  select dbms_rowid.rowid_create(dbms_rowid.rowid_type_restricted, 3619954855::number,1, 0, 0) into r;
  raise notice '%', r;
end;
/

declare
  rowid_type number;
  object_number number;
  relative_fno number;
  block_number number;
  row_number number;
begin
  call dbms_rowid.rowid_info ('pyDE1w==AAAAAA==AQAAAAAAAAA=', rowid_type, object_number,
                                relative_fno, block_number, row_number);
  raise notice '%, %, %, %, %', rowid_type, object_number, relative_fno, block_number, row_number;
end;
/

select dbms_rowid.rowid_object ('pyDE1w==AAAAAA==AQAAAAAAAAA=');
select dbms_rowid.rowid_relative_fno ('pyDE1w==AAAAAA==AQAAAAAAAAA=');
select dbms_rowid.rowid_row_number ('pyDE1w==AAAAAA==AQAAAAAAAAA=');
select dbms_rowid.rowid_to_absolute_fno ('pyDE1w==AAAAAA==AQAAAAAAAAA=', 'a', 'b');
select dbms_rowid.rowid_to_extended ('pyDE1w==AAAAAA==AQAAAAAAAAA=', 'a', 'b', 1);
select dbms_rowid.rowid_to_restricted ('pyDE1w==AAAAAA==AQAAAAAAAAA=', 1);
select dbms_rowid.rowid_type ('pyDE1w==AAAAAA==AQAAAAAAAAA=');
select dbms_rowid.rowid_verify ('pyDE1w==AAAAAA==AQAAAAAAAAA=', 'a', 'b', 1);
