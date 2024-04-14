-- create default group
create default node group default_group with(dn_0, dn_1);
create sharding group to group default_group;
clean sharding;
select * from pgxc_group;