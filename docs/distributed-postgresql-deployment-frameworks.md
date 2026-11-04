# OpenTenBase Integration with Distributed PostgreSQL Deployment Frameworks

## 1. Survey of Frameworks

### 1.1 Patroni
**Description**: Template-based HA manager for PostgreSQL. Uses DCS (etcd/Consul/ZooKeeper)
for leader election.

**Integration Feasibility**: Low-Medium. Patroni assumes standard PostgreSQL binary and
single-node PostgreSQL semantics. OpenTenBase's distributed architecture (GTM+CN+DN)
requires multi-component orchestration beyond Patroni's single-leader model.

**Key Gaps**:
- No multi-component role management (GTM vs CN vs DN)
- No distributed transaction coordinator integration
- pg_ctl compatibility layer needed

### 1.2 CloudNativePG (CNPG)
**Description**: Kubernetes operator for PostgreSQL. Manages the full lifecycle of a
PostgreSQL cluster in Kubernetes.

**Integration Feasibility**: Medium-High. CNPG's operator pattern maps well to
OpenTenBase's component model (GTM/CN/DN as separate statefulsets).

**Key Adaptations Needed**:
- Define CRDs for GTM, CN, and DN roles
- Extend the CNPG reconciliation loop for multi-node deployment
- Integrate opentenbase_ctl as init container

### 1.3 StackGres
**Description**: Enterprise-grade PostgreSQL stack on Kubernetes. Includes connection
pooling, backup, and monitoring.

**Integration Feasibility**: Medium. StackGres bundles many enterprise features that
overlap with OpenTenBase's built-in capabilities.

**Key Adaptations**:
- Replace single-PG assumption with distributed node model
- Map OpenTenBase's built-in sharding to StackGres sharding config

### 1.4 Crunchy PGO
**Description**: Postgres Operator from Crunchy Data. Automated HA, disaster recovery,
and monitoring on Kubernetes.

**Integration Feasibility**: Medium. Similar operator pattern to CNPG, good reference
for Kubernetes-native deployment.

### 1.5 pgEdge (Distributed PostgreSQL)
**Description**: PostgreSQL optimized for the network edge. Multi-master replication.

**Integration Feasibility**: Low. Architecturally different — pgEdge uses multi-master
replication while OpenTenBase uses shared-nothing sharding.

## 2. Recommended Integration Path: CNPG-based Operator

### Rationale
1. CNPG is the most widely adopted PostgreSQL Kubernetes operator (CNCF project)
2. CRD-based model maps naturally to OpenTenBase's component architecture
3. Active community and established extension patterns

### Proposed Architecture

```
┌──────────────────────────────────────────────┐
│            CNPG Operator (extended)           │
├──────────────────────────────────────────────┤
│  OpenTenBaseCluster CRD                      │
│  ├── spec.gtm (replicas, resources)          │
│  ├── spec.coordinators (replicas, resources) │
│  ├── spec.datanodes (replicas, resources)    │
│  └── spec.sharding (distribution config)     │
├──────────────────────────────────────────────┤
│  Reconciliation Loop                         │
│  1. Deploy GTM StatefulSet                    │
│  2. Deploy Coordinator StatefulSet(s)         │
│  3. Deploy DataNode StatefulSet(s)            │
│  4. Initialize cluster (opentenbase_ctl)      │
│  5. Health check → update status              │
└──────────────────────────────────────────────┘
```

### Implementation Phases

**Phase 1: Basic Operator**
- Extend CNPG CRDs with OpenTenBase-specific fields
- Implement basic reconciliation (create → run → stop → delete)
- opentenbase_ctl as init/sidecar container

**Phase 2: HA & Scaling**
- Automatic failover for GTM and CN
- Horizontal scaling of DataNodes
- Rebalancing after scale-out

**Phase 3: Production Readiness**
- Backup/restore integration
- Monitoring (Prometheus metrics)
- Upgrade/migration support

## 3. Alternative: Helm-based Deployment

For non-Kubernetes or simpler deployments, a Helm chart approach is recommended
as an interim solution:

```yaml
# values.yaml
gtm:
  replicas: 1
coordinators:
  replicas: 3
datanodes:
  replicas: 6
  shards: 2
```

This trades Kubernetes-native integration for broader deployment portability.

## 4. References

- [CNPG Documentation](https://cloudnative-pg.io/documentation/)
- [Patroni Documentation](https://patroni.readthedocs.io/)
- [StackGres Documentation](https://stackgres.io/doc/)
- [Crunchy PGO](https://access.crunchydata.com/documentation/postgres-operator/)
