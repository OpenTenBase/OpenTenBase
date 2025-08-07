select oid, * from pg_resgroup;
select * from pg_resgroupcapability;
create resource group rg1 with (concurrency=3, cpu_rate_limit=20, memory_limit=30);
create resource group rg1 with concurrency 3 cpu_rate_limit 20 memory_limit 30;
select count(*) from pg_resgroup;
select count(*) from pg_resgroupcapability;
select count(*) from pg_authid;
create role user1 with resource group rg1 password 'pass1';
select count(*) from pg_authid;
select rolname, rolresgroup, rolpriority from pg_authid where rolname = 'user1';
alter role user1 with resource group none priority 'high';
select rolname, rolresgroup, rolpriority from pg_authid where rolname = 'user1';
drop resource group rg1;
select count(*) from pg_resgroup;
select count(*) from pg_resgroupcapability;
drop role user1;
select count(*) from pg_authid;

-- Test: cpuset cannot be specified when group is disabled.
-- CREATE RESOURCE GROUP resource_group1 WITH (memory_limit=5, cpuset='0');
-- CREATE RESOURCE GROUP resource_group1 WITH (memory_limit=5, cpu_rate_limit=5);
-- ALTER RESOURCE GROUP resource_group1 SET cpuset '0';

-- DROP RESOURCE GROUP resource_group1;

-- Test: check gucs
show enable_resource_group;
show resource_group_cpu_priority;
show resource_group_cpu_limit;
show resource_group_memory_limit;
show resource_group_cpu_ceiling_enforcement;
show resource_group_bypass;
show resource_group_bypass;
