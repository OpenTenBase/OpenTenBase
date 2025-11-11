-- connecting superuser(default)
\connect - opentenbase
SET search_path = sys;
SELECT owner,partitioned,count(*) from dba_tables group by 1,2 order by 1;

-- connecting db using jynnuser(tables owner)
\connect - jynnuser

SET search_path = sys;
SELECT owner,partitioned,count(*) from all_tables group by 1,2 order by 1;

SELECT partitioned,count(*) from user_tables group by 1 order by 1;

-- connecting db using zjaauser(Only able to access some tables under the schema jynnn)
\connect - zjaauser
SET search_path = sys;
SELECT owner,partitioned,count(*) from all_tables group by 1,2 order by 1;

SELECT partitioned,count(*) from user_tables group by 1 order by 1;
