# OpenTenBase Benchmark Testing Guide

## 1. Benchmark Design

### 1.1 Objectives
- Measure distributed query performance across DataNodes
- Compare distributed vs centralized mode throughput
- Evaluate scaling efficiency when adding DataNodes
- Measure transaction throughput under concurrent load

### 1.2 Metrics

| Category | Metric | Target |
|----------|--------|--------|
| OLTP | TPS (transactions/second) | > 10K TPS at 100 connections |
| OLAP | Query latency (TPC-H Q1) | < 5s on 10GB dataset |
| Scaling | Throughput vs DataNode count | > 0.8x linear scaling up to 8 DNs |
| Latency | P50/P95/P99 read latency | P99 < 100ms for point queries |
| Recovery | Failover time | < 30s GTM failover |

## 2. Benchmark Tools

### 2.1 pgbench (Built-in)
PostgreSQL's standard benchmarking tool. Good for OLTP workloads.

```bash
# Initialize
pgbench -i -s 100 -h <coordinator_host> opentenbase

# Run OLTP benchmark
pgbench -c 50 -j 4 -T 300 -h <coordinator_host> opentenbase
```

### 2.2 sysbench
Similar to pgbench with more workload flexibility.

### 2.3 TPC-H
Industry standard for analytical query benchmarking.

```bash
# Generate data
./dbgen -s 10

# Run queries through Coordinator
for q in 1 2 3 4 5; do
  psql -h <coordinator> -f queries/q${q}.sql
done
```

### 2.4 HammerDB
Full-featured database benchmarking with TPC-C and TPC-H workloads.

## 3. Test Scenarios

### Scenario A: Distributed OLTP
- 3 Coordinators, 6 DataNodes, 1 GTM
- pgbench SELECT-only → mixed read/write → heavy-write
- Vary connections: 10 → 50 → 100 → 200

### Scenario B: Scale-Out Efficiency
- Start with 2 DataNodes, scale to 4, 6, 8
- Measure TPS change at each step
- Calculate scaling factor: TPS(n) / TPS(2) vs n/2

### Scenario C: Distributed OLAP
- TPC-H queries through Coordinator
- Compare with centralized mode on same hardware
- Measure query planning overhead

### Scenario D: High Concurrency
- 200+ concurrent connections
- Mixed workload (70% read, 20% write, 10% DDL)
- Monitor GTM bottleneck

## 4. AI-Assisted Analysis

### 4.1 Automated Report Generation
Use an LLM (e.g., Claude, Hy3) to:
1. Parse raw benchmark output (pgbench, sysbench)
2. Generate performance comparison tables
3. Flag anomalies (latency spikes, throughput drops)
4. Suggest optimization recommendations

### 4.2 Expected Analysis Template

```
## Benchmark Run: Distributed OLTP @ 50 connections

| Metric | Value | Baseline | Delta |
|--------|-------|----------|-------|
| TPS | 12,340 | 10,000 | +23.4% |
| Avg Latency | 4.1ms | 5.0ms | -18% |
| P99 Latency | 45ms | 60ms | -25% |

### AI Observations:
1. Scaling from 4 to 6 DataNodes yielded 1.35x throughput (sub-linear)
2. P99 latency spike at connection count 200 suggests GTM bottleneck
3. Coordinator CPU utilization at 78% - near capacity

### AI Recommendations:
1. Add a second GTM slave for read-heavy transaction workloads
2. Increase coordinator concurrency pool from 8 to 16
3. Consider HASH distribution for the 'orders' table (currently ROUND_ROBIN)
```

## 5. Environment Setup

### Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU per node | 4 cores | 16 cores |
| Memory per node | 8 GB | 32 GB |
| Storage per DataNode | 50 GB SSD | 200 GB NVMe |
| Network | 1 Gbps | 10 Gbps |

### Cluster Configuration

```ini
# opentenbase_config.ini for benchmarking
[instance]
name=benchmark_cluster
type=distributed

[gtm]
master=10.0.1.1
slave=10.0.1.2

[coordinators]
master=10.0.1.3,10.0.1.4
nodes-per-server=1

[datanodes]
master=10.0.1.5,10.0.1.6,10.0.1.7,10.0.1.8,10.0.1.9,10.0.1.10
nodes-per-server=1
```

## 6. Monitoring

During benchmarking, monitor:
- `opentenbase_ctl status` for node health
- `pg_stat_activity` for active queries
- `pg_stat_database` for transaction counts
- System metrics: CPU, memory, disk I/O, network

## 7. References

- [pgbench Documentation](https://www.postgresql.org/docs/current/pgbench.html)
- [TPC-H Specification](https://www.tpc.org/tpch/)
- [HammerDB](https://www.hammerdb.com/)
