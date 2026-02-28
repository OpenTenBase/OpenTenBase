opentenbase时间序列表分区
=======================================


请注意，`update_partitions.sql`和`update_paartitions_no_unknown_table.sql`是互斥的`update_partitions.sql`创建一个“未知”表，任何没有合适子表的日期插入都会自动转到此处。如果有脏数据，或者希望密切关注用于报告的插入，

update_partitions_no_unknown_table.sql`有一个备用触发器，
当发生“未知”插入时，该触发器将在适当的日期间隔内动态创建一个丢失的子表。如果

有不可预测的数据，这些数据应该始终组织良好，那么这种替代方案可能比管理未知表更有效率
