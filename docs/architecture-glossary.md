# OpenTenBase Architecture Glossary

A beginner-friendly guide to the core concepts and terms used in OpenTenBase.
Read this before diving into deployment or development.

## Core Components

### OpenTenBase

An enterprise-level distributed HTAP (Hybrid Transactional/Analytical Processing)
database management system built on PostgreSQL (specifically Postgres-XL). It
supports both **distributed** and **centralized** deployment modes and maintains
high compatibility with the PostgreSQL SQL dialect and ecosystem tools.

### GTM (Global Transaction Manager)

The GTM is the **transaction coordinator** for the entire cluster. It issues
globally-unique transaction IDs (XIDs) and manages global snapshots so that all
nodes in the cluster see a consistent view of data. Every distributed cluster
needs at least one GTM (with an optional slave for HA). In centralized mode,
no GTM is needed — transaction management is handled locally.

> **Analogy**: Think of the GTM as the "traffic controller" that hands out
> sequence numbers to every transaction so they don't conflict.

### Coordinator (CN)

Coordinators are the **query front-end**. Users always connect to a Coordinator,
never directly to a DataNode. The Coordinator:

1. Parses and plans the incoming SQL query
2. Splits it into fragments that individual DataNodes can execute
3. Dispatches fragments to the appropriate DataNodes
4. Collects partial results and assembles the final answer

Coordinators hold **only metadata** (table schemas, distribution keys) — no
user data rows.

### DataNode (DN)

DataNodes are the **storage back-end**. All user data lives on DataNodes,
partitioned (sharded) by the distribution key. A DataNode stores a subset of
rows for each distributed table and executes the query fragments it receives
from Coordinators.

DataNodes and Coordinators share the same PostgreSQL-derived schema, so they
understand the same SQL.

## Deployment Modes

### Distributed Mode

A full cluster deployment with GTM + Coordinators + DataNodes. This is the
recommended mode for:

- Large datasets that exceed a single node's storage capacity
- High-concurrency OLTP workloads that benefit from horizontal scaling
- Complex analytical queries that can be parallelised across DataNodes

```
User → Coordinator (CN) ──→ DataNode 1 (DN)
                   │    ──→ DataNode 2 (DN)
                   │    ──→ DataNode 3 (DN)
                   │
                   GTM (transaction IDs & snapshots)
```

### Centralized Mode

A simplified deployment **without GTM or Coordinators** — only DataNodes.
Essentially a single-node or primary-standby PostgreSQL-compatible instance.
Suitable for:

- Development and testing
- Workloads that don't need horizontal scaling
- Migration from standalone PostgreSQL

## Key Concepts

### Node Group

A logical grouping of DataNodes. Tables can be assigned to specific node groups
for data locality or workload isolation. The default node group contains all
DataNodes.

### Sharding / Distribution

Data is horizontally partitioned across DataNodes using a **distribution key**
(the column(s) used to decide which DataNode stores each row). Common strategies:

| Strategy | Behavior |
|----------|----------|
| **HASH** | Rows are distributed by hashing the distribution key. Even spread. |
| **REPLICATION** | Every DataNode stores a full copy. Fast reads, expensive writes. |
| **MODULO** | Simple modulo on an integer key. Deterministic but can be uneven. |
| **ROUND ROBIN** | Rows are distributed in a round-robin fashion. |

### opentenbase_ctl

The **cluster management CLI tool**. It handles:

- Generating configuration templates
- Installing and starting the cluster
- Checking node status
- Stopping and restarting nodes

Main commands: `install`, `start`, `stop`, `status`, `clean`, `prepare config`.

### Package (tar.gz)

A self-contained distribution of the compiled OpenTenBase binaries and
libraries. Created from source via `make install`, or extracted from RPM.
The package is distributed to all cluster nodes during installation and
serves as the runtime for every GTM/CN/DN instance.

## Data Flow

```
1. Client sends SQL to a Coordinator
2. Coordinator parses the query and creates a distributed execution plan
3. Coordinator sends query fragments to DataNodes
4. Each DataNode executes its fragment locally (PostgreSQL engine)
5. DataNodes return partial results to the Coordinator
6. Coordinator merges results and returns the final answer to the client
```

## Relationship to PostgreSQL

OpenTenBase is based on **Postgres-XL**, which extends PostgreSQL with
distributed query processing. It maintains:

- ✅ PostgreSQL wire protocol compatibility
- ✅ PostgreSQL SQL dialect (with extensions for distribution)
- ✅ PostgreSQL ecosystem tools (psql, pg_dump, etc.)
- ✅ User-defined types, functions, foreign keys, transactions

Differences from vanilla PostgreSQL:

| Aspect | PostgreSQL | OpenTenBase |
|--------|-----------|-------------|
| Scale | Single-node | Horizontally scalable |
| Tables | All data on one node | Data sharded across DataNodes |
| Transaction IDs | Local | Globally managed by GTM |
| Query execution | Single process | Distributed across DataNodes |

## Getting Started Path

1. Read this glossary
2. Follow [Quick Deployment](README_ZH.md) to set up a minimal cluster
3. Use `opentenbase_ctl` for cluster management
4. Connect via `psql` to a Coordinator node
5. Create distributed tables with `DISTRIBUTE BY HASH(column)`
