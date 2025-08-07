\c opentenbase_ora_package_function_regression_ora
SET client_min_messages = NOTICE;
\set VERBOSITY terse
\set ECHO all
INSERT INTO utl_file.utl_file_dir(dir, dirname) VALUES(utl_file.tmpdir(), 'TMPDIR');
select utl_file.fopen('/tmp', 'sample_opentenbase.txt', 'w');
select utl_file.is_open(1);
call utl_file.put_line(1,'test_utl_file');
call utl_file.put(1,'test_opentenbase');
select utl_file.is_open(1);
call utl_file.fflush(1);
call utl_file.fclose(1);
select utl_file.fopen('/tmp', 'sample_opentenbase.txt', 'r');
select utl_file.is_open(2);
call utl_file.get_nextline(2, '');
call utl_file.get_nextline(2, '');
call utl_file.fflush(2);
call utl_file.fclose_all();
call utl_file.fremove('/tmp','sample_opentenbase.txt');

-- procedure text:get_nextline/get_line
declare
  fhandle  integer;
  fhandle2 integer;
  buffer text;
begin
  fhandle := utl_file.fopen(
                '/tmp'     -- File location
              , 'a.txt' -- File name
              , 'w' -- Open mode: w = write. 
                  );
  call utl_file.put_line(fhandle, 'Hello world!'
                      || CHR(10));
  call utl_file.put_line(fhandle, 'Hello again!');
  call utl_file.fclose(fhandle);
  fhandle2 := utl_file.fopen(
                '/tmp'     -- File location
              , 'a.txt' -- File name
              , 'r' -- Open mode: w = write. 
                  );
  call utl_file.get_line(fhandle2, buffer);
  raise notice '%', buffer;
  call utl_file.get_nextline(fhandle2, buffer);
  raise notice '%', buffer;
  call utl_file.get_nextline(fhandle2, buffer);
  raise notice '%', buffer;
  call utl_file.get_nextline(fhandle2, buffer);
  raise notice '%', buffer;
  call utl_file.fclose(fhandle2);
  call utl_file.fremove('/tmp','a.txt');
end;
/

-- utl_file.file_type
declare
  fhandle  utl_file.file_type;
  fhandle2 utl_file.file_type;
  buffer text;
begin
  fhandle := utl_file.fopen(
                '/tmp'     -- File location
              , 'a.txt' -- File name
              , 'w' -- Open mode: w = write. 
                  );
  call utl_file.put_line(fhandle, 'Hello world! + 1');
  call utl_file.put_line(fhandle, 'Hello world! + 2');
  call utl_file.put(fhandle, 'Hello world! + 3');
  call utl_file.putf(fhandle, 'Hello world! + 4');
  call utl_file.fclose(fhandle);
  fhandle2 := utl_file.fopen(
                '/tmp'     -- File location
              , 'a.txt' -- File name
              , 'r' -- Open mode: w = write. 
                  );
  call utl_file.get_line(fhandle2, buffer);
  raise notice '%', buffer;
  call utl_file.get_nextline(fhandle2, buffer);
  raise notice '%', buffer;
  call utl_file.get_nextline(fhandle2, buffer);
  raise notice '%', buffer;
  call utl_file.get_nextline(fhandle2, buffer);
  raise notice '%', buffer;
  call utl_file.fclose(fhandle2);
  call utl_file.fremove('/tmp','a.txt');
end;
/

declare
  fr utl_file.file_type;
  fw utl_file.file_type;
  buffer text;
begin
  perform not_exist_func(1);
  exception when others then
    fw := utl_file.fopen(
              '/tmp'     -- File location
            , 'a.txt' -- File name
            , 'w' -- Open mode: w = write. 
                );
    call utl_file.put_line(fw, 'Hello world! + 1 ');
    call utl_file.put_line(fw, 'Hello world! + 2 ');
    call utl_file.put(fw, 'Hello world! + 3 ');
    call utl_file.putf(fw, 'Hello world! + 4');
    call utl_file.fclose(fw);
    fr := utl_file.fopen(
                '/tmp'     -- File location
              , 'a.txt' -- File name
              , 'r' -- Open mode: w = write. 
                  );
    call utl_file.get_line(fr, buffer);
    raise notice '%', buffer;
    call utl_file.get_nextline(fr, buffer);
    raise notice '%', buffer;
    call utl_file.get_nextline(fr, buffer);
    raise notice '%', buffer;
    call utl_file.get_nextline(fr, buffer);
    raise notice '%', buffer;
    call utl_file.fclose(fr);
    call utl_file.fremove('/tmp','a.txt');
end;
/

DELETE FROM utl_file.utl_file_dir;

-- begin 124009757: bfilenmae
select bfilename('dirName','fileName') from dual;
select  cast(case when ((1=1) or ( 325.5>=819.67 ) )  or ( 0.>0. ) then initcap( cast('z' as CHAR(1))) else  'L' end  as CHAR(1))  from dual;
select bfilename('dirName',
	      cast(case when ((1=1) or ( 325.5>=819.67 ) )  or ( 0.>0. ) then initcap( cast('z' as CHAR(1))) else  'L' end  as CHAR(1)) ) from dual;
-- end 124009757
