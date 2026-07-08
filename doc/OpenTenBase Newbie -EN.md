# OpenTenBase Newbie Starter Pack

## Glossary

### 1. Coordinator Node (CN)

The entry point for user connections. Responsible for receiving SQL queries, generating distributed execution plans, distributing tasks to DNs, and aggregating results. Does not store user data itself.

### 2. DataNode (DN)

The node that actually stores user data. Each DN is a complete PostgreSQL instance responsible for executing read/write requests issued by the CN.

### 3. GTM (Global Transaction Manager)

Responsible for allocating global transaction IDs and managing transaction snapshots, ensuring that distributed transactions across multiple DNs satisfy ACID properties.

### 4. Stateless and Horizontal Scaling

CN nodes do not persist any user data, so adding CN nodes requires no data migration and directly increases the cluster's concurrent processing capacity.

### 5. Primary DataNode (DN-Primary)

The primary node within a shard group responsible for handling read/write requests. It is the core of data services.

### 6. Standby DataNode (DN-Standby)

A backup node that synchronizes data from the primary node in real time via streaming replication and takes over service when the primary node fails.

### 7. Streaming Replication

A PostgreSQL native high-availability technology that maintains data consistency between primary and standby nodes by transmitting WAL logs in real time.

### 8. GTM Proxy

The front-end proxy for the GTM primary node. Responsible for forwarding CN transaction ID requests and automatically switching to the standby node if the GTM primary node fails.

### 9. Load Balancer (HAProxy / LVS)

A traffic distributor deployed in front of CNs. Routes SQL requests to the least busy CN node based on each CN's current load.

### 10. Configuration Center (ConfDB)

A configuration database that stores cluster metadata (node list, sharding rules, primary-standby relationships). CNs load cluster topology information from it at startup.

### 11. Monitoring System (Prometheus + Grafana)

Prometheus collects performance metrics from all nodes; Grafana visualizes this data for cluster status monitoring and troubleshooting.

### 12. Distributed Mode

A deployment mode that fully includes GTM, CN, and DN nodes. Data is horizontally partitioned across multiple shard groups, supporting PB-scale data and parallel query execution.

### 13. Centralized Mode

A deployment mode that includes only DN nodes, without GTM or CN. It is essentially an enhanced single-node PostgreSQL instance, suitable for development and testing environments.

### 14. Distribution Key (Shard Key)

The field specified at table creation time used to calculate which shard group a data row should be routed to. Including the distribution key in query conditions avoids full-cluster scans.

### 15. Shard Group

A physical storage unit consisting of one primary DN and one or more standby DNs. It is the basic unit of data high availability.

### 16. GTM Proxy

The GTM Proxy is a dedicated front-end deployed in front of the GTM primary node. It handles two tasks:

- **Request Forwarding**: Forwards transaction ID requests from CNs to the GTM primary node.

- **Health Monitoring and Failover**: Continuously monitors the health of the GTM primary node and automatically switches requests to the standby node if the primary fails.

---

## Newbie Guide

### 1. What Is OpenTenBase?

OpenTenBase is a relational database cluster platform that provides write reliability and multi-primary-node data synchronization. You can deploy OpenTenBase on one or multiple hosts, with data stored across multiple physical machines. Tables can be stored in two ways: `distributed` or `replicated`. When you send a query SQL to OpenTenBase, it automatically dispatches the query to data nodes and retrieves the final results.

**Key point**: The SQL syntax you write is almost identical to single-node PostgreSQL, making the learning curve low.

### 2. OpenTenBase Architecture Diagram

![](C:\Users\victor\Pictures\2026-07-08-16-12-02-opentenbase架构图.png)

#### Text Description

##### Phase 1: Application Access

1. **Business application initiates SQL request**: The business application does not connect directly to any database node. Instead, it sends the SQL request to the **load balancer** (HAProxy or LVS in the diagram).

2. **Load balancer distributes the request**: The load balancer maintains a list of CN nodes. Based on each CN's current load (connection count, response time, etc.), it routes the SQL request to the least busy **CN (Coordinator node)** — for example, CN-1 in the diagram. This step ensures traffic balancing across multiple CNs.

##### Phase 2: SQL Parsing and Routing Planning

3. **CN reads from the configuration center**: After receiving the SQL request, CN-1 first obtains the cluster's complete metadata from the **configuration center (ConfDB)**, including:
   
   - Which DN nodes exist?
   
   - How many shard groups exist (Shard Group 0, 1, ...)?
   
   - What is the distribution key for each shard group?
   
   - What are the primary-standby relationships of each shard group?

4. **CN parses SQL and generates distributed plan**: The CN performs lexical and syntactic analysis on the SQL, then generates a distributed execution plan based on metadata. Key decisions include:
   
   - If the SQL's WHERE clause contains the distribution key, the CN can **precisely locate** a specific shard group and send requests only to that group.
   
   - If the SQL does not contain the distribution key, the CN **broadcasts** the request to all shard groups and aggregates the results.

##### Phase 3: Obtaining Global Transaction ID

5. **CN requests transaction ID from GTM Proxy**: For write operations or queries requiring transaction support, the CN requests a global transaction ID from the **GTM Proxy**. The proxy acts as the front-end for the GTM primary node, forwarding requests and caching certain information for performance.

6. **GTM Proxy forwards to GTM primary**: The GTM Proxy forwards the request to the **GTM primary node**. The GTM primary allocates a globally unique transaction ID and records the current transaction snapshot, then returns them to the CN via the proxy.

##### Phase 4: Data Routing and DN Execution

7. **CN distributes subtasks to DN primary nodes**: Based on sharding rules, the CN sends SQL subtasks to the **DN-primary nodes** in the corresponding shard groups (e.g., DN-primary in Shard Group 0, DN-primary in Shard Group 1).

8. **DN-primary nodes execute local operations**: Upon receiving subtasks, each DN-primary node performs local data query, update, or insert operations. Each DN-primary is essentially a complete PostgreSQL instance capable of independently handling local transactions.

##### Phase 5: Data Synchronization (Write Scenarios)

9. **DN-primary synchronizes to DN-standby via streaming replication**: For write operations (INSERT, UPDATE, DELETE), after completing the local write, the DN-primary uses **streaming replication** to synchronize transaction logs (WAL logs) to the **DN-standby node** within the same shard group in real time. This ensures data consistency between the primary and standby nodes, providing the foundation for high availability.

##### Phase 6: Result Aggregation and Return

10. **DN primary nodes return results to CN**: Each DN-primary returns execution results (such as retrieved rows, affected row counts, etc.) to the originating CN-1.

11. **CN aggregates results and returns to application**: CN-1 aggregates results from multiple DNs (e.g., performing aggregations, sorting, deduplication) to form the final complete result set, which is returned to the business application via the load balancer.

12. **Business application receives the response**: A complete SQL request processing cycle ends.

##### Throughout: Monitoring and High Availability

- **Monitoring System (Prometheus + Grafana)**: At every step of the entire process, all nodes (load balancer, CNs, DN-primary, DN-standby, GTM-primary, GTM-standby, ConfDB) continuously expose performance metrics (QPS, latency, connection count, CPU, memory, disk I/O, etc.) to Prometheus. Grafana visualizes this data, allowing operations personnel to monitor cluster health in real time and quickly locate issues when anomalies occur.

- **High Availability Mechanisms**:
  
  - If a **DN-primary node** fails, the **DN-standby node** in the same shard group is automatically or manually promoted to the new primary, with no business impact.
  
  - If the **GTM primary node** fails, the GTM Proxy automatically switches requests to the **GTM standby node**, with no impact at the CN layer.
  
  - If a **CN node** goes down, the load balancer automatically removes it from the available list, and subsequent requests are no longer distributed to it.

---

## FAQ (Frequently Asked Questions)

#### Q1: Should I choose centralized or distributed mode for my first deployment?

Choose centralized mode. It has a simple deployment, low resource usage, and can be up and running in 10 minutes, making it ideal for learning and evaluation. Distributed mode is for production environments, has complex configuration, and is not recommended for beginners.

#### Q2: What's the difference between opentenbase_ctl and pgxc_ctl?

opentenbase_ctl is a wrapper-based automated deployment tool. You only need to fill in a single opentenbase_config.ini file to complete deployment with one command. pgxc_ctl is a low-level manual management tool with complex configuration; users generally do not use it directly.

#### Q3: What should I do if SSH connection fails during deployment?

Check that ssh-user, ssh-password, and ssh-port in opentenbase_config.ini are correctly filled in. Verify that the target server is network-reachable and the SSH port is not blocked by a firewall. You can test connectivity by manually executing an ssh command.

#### Q4: Can I deploy distributed mode on a single machine?

Yes. You can deploy distributed mode on a single machine by configuring multiple IPs or using different ports. However, this won't demonstrate disaster recovery capabilities and is only suitable for functional verification.

#### Q5: Why are distribution keys and shard groups needed?

**Why need distribution keys?**  
A single machine cannot hold all data, so data must be split and distributed across multiple machines. The distribution key is the rule that determines "which machine this row of data goes to" by calculating a hash based on the chosen field. Without distribution keys, data cannot be evenly distributed, and queries wouldn't know which machine to query.

**Why need shard groups?**  
A single machine can fail, so each piece of data needs a "backup." A shard group = primary node + standby node. When the primary node fails, the standby automatically takes over, ensuring data is not lost and service does not stop.

#### Q6: How should I choose a distribution key?

**Core principle**: Ensure both **even data distribution** and **query optimization**.

**Priority principle**: If the table has a **primary key**, prioritize using it (for composite primary keys, use the first field) as the distribution key. If there's a **unique index**, use that column.

**Even distribution**: Choose a column with **evenly distributed values**, such as user ID or order ID. **Avoid** columns with only a few fixed values, such as "gender" or "status," as this will cause large amounts of data to concentrate on a few nodes, creating performance bottlenecks.

**Query optimization**: If your queries frequently include a certain field in WHERE clauses (e.g., `WHERE user_id = ?`), setting that field as the distribution key is ideal. This allows the Coordinator node (CN) to directly locate the data node storing that data, making queries extremely efficient.

#### Q7: What happens if a query's WHERE clause doesn't include the distribution key?

The CN broadcasts the query to all shard groups and aggregates results from all of them before returning the final result.

#### Q8: How do sharded tables and replicated tables work together?

Sharded tables store large data (distributed storage), while replicated tables provide complete copies of small tables redundantly across all shard groups. Together, they allow JOIN operations between large and small tables to be completed locally within each shard group, eliminating cross-node data movement and making queries faster with less network overhead.

---

## Installation and Deployment Resources

### Official Documentation

- **Portal**: [OpenTenBase Official Documentation](https://docs.opentenbase.org/)

- **Core Guide**: [Quick Start](https://docs.opentenbase.org/guide/01-quickstart) — covers environment requirements, dependency installation, compilation, and other basic steps.

### Community "Pitfall Guide"

Official documentation covers standard procedures, while community-contributed "pitfall guides" address non-standard issues encountered in real-world practice.

- **Article Links**:
  
  - [OpenTenBase Official Website Community Contribution Section](https://www.opentenbase.org/news/news-post-14/)
  
  - [Modb Community Reprint](https://www.modb.pro/db/1830432843832582144)
