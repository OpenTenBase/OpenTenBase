-- connecting superuser(default)
\connect - opentenbase
SET search_path = sys;
SELECT owner,object_type,count(*) from dba_procedures group by 1,2 order by 1;

-- connecting db using jynnuser(objects owner)
\connect - jynnuser

SET search_path = sys;
SELECT owner,object_type,count(*) from all_procedures group by 1,2 order by 1;

SELECT object_type,count(*) from user_procedures group by 1 order by 1;

-- connecting db using zjaauser(Only able to access some objects under the schema jynnn)
\connect - zjaauser
SET search_path = sys;
SELECT owner,object_type,count(*) from all_procedures group by 1,2 order by 1;

SELECT object_type,count(*) from user_procedures group by 1 order by 1;