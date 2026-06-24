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
create resource group rg1 with cpuset '0';
alter resource group rg1 set cpuset '0';
drop resource group rg1;

-- Test: check gucs
show enable_resource_group;
show resource_group_cpu_priority;
show resource_group_cpu_limit;
show resource_group_memory_limit;
show resource_group_cpu_ceiling_enforcement;
show resource_group_bypass;
show resource_group_queuing_timeout;
show resource_group_name;
show resource_group_bypass_sql;
show resource_group_local;
show resgroup_memory_query_fixed_mem;
show resource_group_leader_coordinator;

select * from  pg_resgroup_get_status_kv('check');
select * from  pg_resgroup_get_status_kv('dump');
select * from  pg_resgroup_get_status_kv('');
