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

![OpenTenBase Architecture Diagram](../images/opentenbase架构图_EN.png)

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
  
  - [Modb Community Reprint](https://www.modb.pro/db/1830432843832582144)**OpenTenBase Newbie Starter Pack**
    
    ## Glossary
    
    ### 1. Coordinator Node (CN)
    
    The entry point for user connections. It receives SQL queries, generates distributed execution plans, dispatches tasks to DataNodes, and aggregates the results. It does not store user data itself.
    
    ### 2. DataNode (DN)
    
    The node that physically stores user data. Each DN is a complete PostgreSQL instance responsible for executing the read/write requests dispatched by the CN.
    
    ### 3. GTM (Global Transaction Manager)
    
    Responsible for allocating global transaction IDs and managing transaction snapshots, ensuring that distributed transactions across multiple DNs satisfy ACID properties.
    
    ### 4. Metadata
    
    Information that describes the data structure, such as table schemas, distribution rules, and node lists. The CN stores metadata, while the DN stores the actual user data – a clear division of labor.
    
    ### 5. Shared-Nothing Architecture
    
    A distributed cluster architecture where each node processes its own data independently. Results are aggregated upwards or passed between nodes. Nodes communicate via network protocols. This offers better parallel processing and scalability, and can be deployed on commodity x86 servers.
    
    ### 6. Master/Slave Nodes
    
    GTM, CN, and DN all support a master-slave architecture. The master node provides services, while the slave node synchronizes data in real-time via streaming replication. If the master fails, the slave automatically takes over.
    
    ### 7. Streaming Replication
    
    PostgreSQL's native high-availability technology. It keeps master and slave node data consistent by transferring WAL (Write-Ahead Log) records in real time.
    
    ### 8. GTM Proxy
    
    A front-end proxy for the GTM master node. It forwards transaction ID requests from CNs and automatically switches to the GTM standby node if the GTM master fails.
    
    ### 9. Load Balancer (HAProxy / LVS)
    
    A traffic distributor deployed in front of the CNs. It routes SQL requests to the least busy CN node based on the current load (connections, response time, etc.).
    
    ### 10. Configuration Center (ConfDB)
    
    A configuration database that stores cluster metadata (node list, sharding rules, master-slave relationships). CNs load the cluster topology information from it during startup.
    
    ### 11. Monitoring System (Prometheus + Grafana)
    
    Prometheus collects performance metrics from all nodes, and Grafana visualizes the data for cluster status monitoring and troubleshooting.
    
    ### 12. Distributed Mode
    
    A deployment topology that includes all three node types: GTM, CN, and DN. Data is horizontally partitioned across multiple shard groups, supporting petabyte-scale data volumes and parallel queries.
    
    ### 13. Centralized Mode
    
    A deployment topology consisting only of DNs, without GTM and CN. It functions as an enhanced standalone PostgreSQL, suitable for development and testing environments.
    
    ### 14. Distribution Key (Shard Key)
    
    A column specified when creating a table, used to calculate which shard group a data row should be routed to. When the query condition includes the distribution key, a full cluster scan can be avoided.
    
    ### 15. Shard Group
    
    A physical storage unit consisting of one primary DN and one or more standby DNs. It is the fundamental unit of data high availability.
    
    ### 16. Replication Table
    
    A table type where each DN stores a complete copy of the data. Suitable for small tables that require frequent JOIN operations. Update performance is lower, but query speed is fast.
    
    ### 17. Distributed Table (Sharded Table)
    
    A table type where data is distributed across DN nodes based on the hash value of the distribution key. Writes only affect the corresponding shard group, resulting in high write performance. Queries that include the distribution key can precisely target a single shard group, yielding extremely high query efficiency. Ideal for large, growing tables (fact tables) and is the most commonly used table type in OpenTenBase.
    
    ### 18. opentenbase_ctl
    
    An automated cluster management tool provided by OpenTenBase. It uses a simplified `opentenbase_config.ini` configuration file to automatically complete software package distribution, node initialization, and cluster startup. Suitable for most users who need quick deployment.
    
    ### 19. pgxc_ctl
    
    The native cluster management tool of Postgres-XL. It requires a complex `pgxc_ctl.conf` configuration file and provides fine-grained control, suitable for advanced users who need a deep understanding of the cluster architecture.
    
    ### 20. Data Sharding
    
    The process of splitting a large table into multiple data fragments based on a distribution key and storing them on different DNs. It is the foundational mechanism enabling horizontal scaling in distributed databases.
    
    ---
    
    ## Newbie Guide
    
    ### 1. What is OpenTenBase?
    
    OpenTenBase is a relational database cluster platform that provides write reliability and multi-master node data synchronization. You can configure OpenTenBase on one or multiple hosts, with data stored across multiple physical machines. Data tables can be stored in two ways: distributed or replicated. When you send a query to OpenTenBase, it automatically dispatches queries to the data nodes and retrieves the final result.  
    Key point: The SQL statements you write are almost identical to standalone PostgreSQL, resulting in a very low learning curve.
    
    ### 2. OpenTenBase Architecture Diagram
    
    #### Text Description:
    
    ##### Phase 1: Application Access
    
    1. **Application initiates SQL request**: The business application (e.g., microservice, web backend) does not directly connect to any database node. Instead, it sends the SQL request to the **Load Balancer** (such as HAProxy or LVS in the diagram).
    
    2. **Load balancer distributes request**: The load balancer maintains a list of CN nodes. Based on the current load of each CN (connection count, response time, etc.), it routes the SQL request to the most idle **CN (Coordinator Node)** , e.g., CN-1 in the diagram. This ensures traffic is balanced across multiple CNs.
    
    ##### Phase 2: SQL Parsing and Route Planning
    
    3. **CN reads from Configuration Center**: Upon receiving the SQL request, CN-1 first retrieves the complete cluster metadata from the **Configuration Center (ConfDB)** , including:
       
       - List of DN nodes
       
       - Shard Groups (e.g., Shard Group 0, 1)
       
       - Distribution keys (sharding rules) for each shard group
       
       - Master-slave relationships for each shard group
    
    4. **CN parses SQL and generates distributed plan**: The CN performs lexical and syntax analysis on the SQL, then combines it with metadata to generate a distributed execution plan. Key decisions include:
       
       - If the SQL WHERE clause contains the distribution key, the CN can **precisely target** a specific shard group and only send the request to that group.
       
       - If the SQL does not contain the distribution key, the CN will **broadcast** the request to all shard groups and then aggregate the results.
    
    ##### Phase 3: Acquiring a Global Transaction ID
    
    5. **CN requests a transaction ID from GTM Proxy**: For write operations or queries requiring transaction support, the CN requests a global transaction ID from the **GTM Proxy**. The GTM Proxy acts as a front-end for the GTM master, forwarding requests and caching some information to improve performance.
    
    6. **GTM Proxy forwards to GTM Master**: The GTM Proxy forwards the request to the **GTM Master Node**. The GTM Master allocates a globally unique transaction ID, records the current transaction snapshot, and returns it to the CN via the GTM Proxy.
    
    ##### Phase 4: Data Routing and DN Execution
    
    7. **CN dispatches sub-tasks to DN primary nodes**: Based on the sharding rules, the CN sends SQL sub-tasks to the **DN-Primary nodes** in the corresponding shard groups (e.g., DN-Primary of Shard Group 0, DN-Primary of Shard Group 1).
    
    8. **DN-Primary nodes execute local operations**: After receiving the sub-tasks, each DN-Primary performs the data query, update, or insertion locally. Each DN-Primary is essentially a complete PostgreSQL instance capable of handling local transactions independently.
    
    ##### Phase 5: Data Synchronization (Write Scenarios)
    
    9. **DN-Primary synchronizes to DN-Standby via streaming replication**: For write operations (INSERT, UPDATE, DELETE), after completing the local write, the DN-Primary uses **streaming replication** technology to synchronize the transaction log (WAL logs) in real-time to the **DN-Standby nodes** within the same shard group. This ensures the standby data remains consistent with the primary, providing a foundation for high availability.
    
    ##### Phase 6: Result Aggregation and Return
    
    10. **DN primary nodes return results to CN**: Each DN-Primary node returns the execution results (e.g., number of rows queried, number of rows affected by a write) to the initiating CN-1.
    
    11. **CN aggregates results and returns to application**: CN-1 aggregates the results from multiple DNs (performing operations like aggregate calculations, sorting, deduplication) to form the final complete result set, which is returned to the business application via the load balancer.
    
    12. **Business application receives response**: One complete SQL request processing cycle ends.
    
    ##### Throughout the Process: Monitoring and High Availability
    
    - **Monitoring System (Prometheus + Grafana)**: At every stage, all nodes (Load Balancer, CN, DN-Primary, DN-Standby, GTM-Master, GTM-Standby, ConfDB) continuously expose performance metrics (QPS, latency, connections, CPU, memory, disk IO) to Prometheus. Grafana visualizes this data, allowing operators to monitor cluster health in real-time and quickly locate issues during anomalies.
    
    - **High Availability Mechanisms**:
      
      - If a **DN-Primary node** fails, the **DN-Standby node** in the same shard group will automatically (or via manual intervention) be promoted to the new primary node, transparent to the business.
      
      - If the **GTM Master node** fails, the GTM Proxy automatically switches requests to the **GTM Standby node**, transparent to the CN layer.
      
      - If a **CN node** crashes, the load balancer automatically removes it from the available list, and subsequent requests are no longer sent to it.
    
    ---
    
    ## Frequently Asked Questions (FAQ)
    
    #### Q1: For the first deployment, should I choose Centralized or Distributed mode?
    
    Choose Centralized mode. It’s simple to deploy, uses fewer resources, and you can have it running in 10 minutes—perfect for learning and experience. Distributed mode is for production environments, with complex configuration that's not beginner-friendly.
    
    #### Q2: What is the difference between `opentenbase_ctl` and `pgxc_ctl`?
    
    `opentenbase_ctl` is a packaged automated deployment tool. You only need to fill in one `opentenbase_config.ini` file and it completes the deployment with one click. `pgxc_ctl` is the underlying manual management tool with complex configuration; users generally don't use it directly.
    
    #### Q3: What if I get an SSH connection failure during deployment?
    
    Check if the `ssh-user`, `ssh-password`, and `ssh-port` in `opentenbase_config.ini` are correct. Confirm that the target server is network-reachable and the SSH port is not blocked by a firewall. You can manually run an `ssh` command first to test connectivity.
    
    #### Q4: Can I deploy Distributed Mode on a single machine?
    
    Yes. You can deploy Distributed Mode on a single machine by configuring multiple IP addresses or using different ports. However, this cannot demonstrate disaster recovery capabilities and is only suitable for functional verification.
    
    #### Q5: Why do I need a Distribution Key and Shard Groups?
    
    ##### Why a Distribution Key:
    
    A single machine cannot hold all the data, so the data must be split and distributed across multiple machines. The distribution key is the rule that determines "which machine this row goes to" by calculating a hash based on the chosen column's value. Without a distribution key, data cannot be distributed evenly, and queries won't know which machine to target.
    
    ##### Why Shard Groups:
    
    A single machine can fail, so each piece of data needs a "backup". A Shard Group = Primary Node + Standby Node. If the primary node fails, the standby automatically takes over, ensuring no data loss and uninterrupted service.
    
    #### Q6: How to choose a Distribution Key?
    
    ##### Core Principles:
    
    **Ensure even data distribution while also optimizing queries**.
    
    **Priority Principle**: If the table has a **primary key**, prioritize choosing the primary key (for composite primary keys, choose the first field) as the distribution key. If there is a **unique index**, choose the unique index column.
    
    **Even Distribution**: Choose a column with evenly distributed values, such as User ID or Order ID. **Avoid** columns with only a few fixed values, like "gender" or "status", otherwise massive amounts of data will crowd onto a few nodes, causing performance bottlenecks.
    
    **Query Optimization**: If your `WHERE` clause frequently uses a particular column (e.g., `WHERE user_id = ?`), setting that column as the distribution key is ideal. This way, the Coordinator Node (CN) can directly locate the single data node storing that data, resulting in extremely high query efficiency.
    
    #### Q7: What happens if the query condition does not include the distribution key?
    
    The CN broadcasts the query condition to all shard groups and then aggregates the results from all of them.
    
    #### Q8: How do Sharded Tables and Replication Tables work together?
    
    Sharded tables store big data (distributed storage), while replication tables provide a complete small table copy to all shard groups (redundant storage). When used together, JOINs between large and small tables can be completed locally within each shard group without moving data across nodes, making queries fast and network-efficient.
    
    #### For deployment and usage issues, visit the official documentation:
    
    - **Entry Point**: [OpenTenBase Official Documentation](https://docs.opentenbase.org/)
    
    - **Core Guide**: [Quick Start](https://docs.opentenbase.org/guide/01-quickstart) covers environment requirements, dependency installation, compilation, and other basic steps.
    
    #### The official documentation mainly covers standard procedures; community-contributed "pitfall guides" specifically solve non-standard problems encountered in practice.
    
    - **Article Links**:
      
      - [OpenTenBase Official Community Contribution Section](https://www.opentenbase.org/news/news-post-14/)
      
      - [Mo Tianlun Community Repost](https://www.modb.pro/db/1830432843832582144)
