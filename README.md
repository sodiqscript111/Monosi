# Kubernetes Shuffle-Sharding Controller

A Kubernetes controller providing deterministic, multi-tenant workload isolation using the **AWS Route 53 Shuffle-Sharding pattern**.

Each tenant is deterministically assigned to a fixed-size, pseudo-random subset of $k$ shards drawn from a shared pool of $N$ shards. This ensures that any single shard failure or toxic tenant workload only impacts a minor fraction of tenants, and no two tenants share a high-overlap set of shards.

---

## 1. Why Shuffle-Sharding?

In multi-tenant Kubernetes clusters, two common failure patterns arise:
1. **The Shared Cluster (All-in-One)**: All tenant pods run across all nodes. A single toxic request or spike in resource usage from one tenant can cascade across the entire cluster, causing a **100% blast radius**.
2. **Dedicated Sharding ($k = 1$)**: The cluster is split into $N$ isolated silos, each assigned to a tenant. If a silo fails, $100\%$ of that tenant's workload is offline (no redundancy). Furthermore, the number of isolated tenants is capped at $N$.

### The Shuffle-Sharding Solution
Instead of placing a tenant on all shards or just 1 shard, we assign each tenant to a **unique virtual hand of $k$ shards** chosen from a pool of $N$ shards.

```
                  ┌───────────────────────────────────────────────────┐
                  │            Shared Shard Pool (N = 8)              │
                  │  [0]    [1]    [2]    [3]    [4]    [5]    [6]    [7] │
                  └────┬──────┬──────┬──────┬──────┬───────────────────┘
                       │      │      │      │      │
            Tenant A   │      │      │      │      │
           [0, 2] ─────┴──────┼──────┴──────┼──────┼───────┐
                              │             │      │       │
            Tenant B          │             │      │       │
           [1, 3] ────────────┴─────────────┴──────┼───────┼───────┐
                                                   │       │       │
            Tenant C                               │       │       │
           [2, 4] ─────────────────────────────────┴───────┘       │
```

- **Number of combinations**: $\binom{N}{k} = \frac{N!}{k!(N - k)!}$
  - With $N = 8$ and $k = 2$, there are **28 distinct virtual shards**.
  - With $N = 64$ and $k = 4$, there are **635,376 distinct virtual shards**.
- **Pairwise Isolation**: Any two tenants share at most 1 shard (for $k=2$, guaranteed; for $k>2$, mathematically minimized).
- **Fault Tolerance**: If Shard 0 fails, Tenant A still has Shard 2 active and running.

---

## 2. Blast-Radius Mathematics & Concrete Example

### Single Shard Failure ($N = 8, k = 2$)
Suppose we have a pool of $N = 8$ shards and each tenant is assigned $k = 2$ shards:

1. **Fraction of Tenants Affected**:
   The number of combinations containing any specific failing shard $s$ is $\binom{N-1}{k-1}$.
   The fraction of tenants containing shard $s$ is:
   $$\text{Blast Radius} = \frac{\binom{N-1}{k-1}}{\binom{N}{k}} = \frac{k}{N} = \frac{2}{8} = 25\%$$
   - **$75\%$ of tenants experience ZERO degradation.**
   - Only **$25\%$ of tenants** have one of their shards impacted.

2. **Complete Outage Probability (Dual Redundancy)**:
   If each tenant's workload has replicas spread across its assigned shards (e.g., 1 replica on shard $a$, 1 replica on shard $b$), a tenant only experiences a complete service outage if **both** of its assigned shards fail simultaneously.
   - For a single shard failure, **0% of tenants suffer complete outage**.
   - If two random shards fail in the pool, the probability that a specific tenant has both of its assigned shards down is:
     $$P(\text{total outage}) = \frac{1}{\binom{8}{2}} = \frac{1}{28} \approx 3.57\%$$
     Meanwhile, $96.43\%$ of tenants remain operational!

### Comparison Across Sharding Strategies

| Strategy | Total Shards ($N$) | Shards / Tenant ($k$) | Virtual Combinations | Blast Radius (1 Shard Down) | Outage with Dual Failover |
|---|---|---|---|---|---|
| **No Sharding (Shared)** | 8 | 8 | 1 | **100%** | **100%** |
| **Dedicated Silos** | 8 | 1 | 8 | 12.5% | 100% for affected tenant |
| **Shuffle Sharding** | **8** | **2** | **28** | **25%** | **0%** (1 down), **3.57%** (2 down) |
| **Shuffle Sharding** | **64** | **4** | **635,376** | **6.25%** | **0%** (1-3 down), **0.00015%** (4 down) |

---

## 3. Combinatorics & Pairwise Overlap

A primary requirement of shuffle-sharding is minimizing the pairwise overlap between any two tenants $A$ and $B$:

$$\text{Overlap}(A, B) = |S_A \cap S_B|$$

### Pairwise Overlap for $k = 2$
When $k = 2$, each tenant's assignment is a 2-element set $\{u, v\}$.
- If two assignments are distinct ($\{u, v\} \neq \{x, y\}$), they **cannot share 2 elements**, because sharing 2 elements would make the sets identical.
- Therefore, for $k = 2$, **any two distinct tenant assignments share AT MOST 1 shard by mathematical definition**.
- The overlap is strictly:
  $$\text{Overlap} \in \{0, 1\}$$

### Hypergeometric Overlap Distribution
For general $k$, the probability that two randomly assigned tenants share exactly $j$ shards follows the hypergeometric distribution:

$$P(|S_A \cap S_B| = j) = \frac{\binom{k}{j} \binom{N - k}{k - j}}{\binom{N}{k}}$$

For $N = 8, k = 2$:
- $P(j = 0) = \frac{\binom{2}{0} \binom{6}{2}}{\binom{8}{2}} = \frac{15}{28} \approx 53.57\%$ (completely disjoint)
- $P(j = 1) = \frac{\binom{2}{1} \binom{6}{1}}{\binom{8}{2}} = \frac{12}{28} \approx 42.86\%$ (share 1 shard)
- $P(j = 2) = \frac{\binom{2}{2} \binom{6}{0}}{\binom{8}{2}} = \frac{1}{28} \approx 3.57\%$ (identical assignment collision)

Empirical results from our test suite (`pkg/sharder/sharder_test.go`) across 124,750 tenant pairs:
- **Overlap 0**: 53.83%
- **Overlap 1**: 42.58%
- **Identical (Collision)**: 3.59%
- **Distinct pairs sharing $> 1$ shard**: **0 (0.00%)**

---

## 4. Deterministic Seeded PRNG Architecture

To ensure the reconciler produces identical assignments across controller restarts, reconciles, and platforms without relying on global state:
1. **Domain-Separated SHA-256 Hashing**:
   $$\text{Seed} = \text{uint64}(\text{SHA256}(\text{"monosi.io/shuffle-sharding/v1alpha1:"} + \text{tenantID})[0..8])$$
2. **SplitMix64 Generator**:
   A deterministic, 64-bit generator with 64 bits of state, free of Go standard library version fluctuations.
3. **Unbiased Rejection Sampling**:
   Eliminates modulo bias when selecting indices during the Fisher-Yates shuffle.
4. **Partial Fisher-Yates Shuffle**:
   Draws $k$ elements out of $N$ in $O(k)$ time, then sorts the resulting slice in ascending order.

---

## 5. Kubernetes Workload Scheduling & Deletion Lifecycle

The controller maps the assigned shards to Kubernetes nodes using **Node Affinity** and **Tolerations**.

### 1. Workload Reconciliation
When a `ShuffleShardAssignment` specifies `targetWorkload` (e.g. a Deployment), the controller updates the Deployment:
1. **Node Affinity**: Injects `requiredDuringSchedulingIgnoredDuringExecution` requiring `topology.kubernetes.io/shard In [<assignedShards>]`.
2. **Tolerations**: Injects tolerations for the assigned shards:
   ```yaml
   tolerations:
   - key: topology.kubernetes.io/shard
     operator: Equal
     value: "2"
     effect: NoSchedule
   ```
3. **Labels & Annotations**: Injects tenant tracking labels (`sharding.monosi.io/tenant`, `sharding.monosi.io/sharded`, `sharding.monosi.io/assigned-shards`).

### 2. Finalizer & Deletion Cleanup
To prevent orphaned scheduling constraints when a `ShuffleShardAssignment` is deleted:
- The controller registers a finalizer `sharding.monosi.io/finalizer` upon creation.
- On deletion (`DeletionTimestamp != nil`), the controller intercepts the event, fetches the target `Deployment`, and strips:
  - The injected `NodeAffinity` selector terms for the shard key.
  - The injected `Tolerations` for the shard key.
  - The injected `sharding.monosi.io/*` labels and annotations.
- Only after the target workload is restored to an unconstrained state does the controller remove the finalizer, allowing garbage collection to proceed cleanly.

### 3. Reconcile Loop Protection
The controller tracks status conditions and generation state with diff guards to ensure status writes only occur when state actually changes, preventing runaway hot reconcile loops.

---

## 6. Apply-Time Validation (CEL & Admission Webhook)

Invalid specifications (e.g., `shardsPerTenant > shardPoolSize`, `shardPoolSize < 1`, or empty `tenantID`) are prevented at `kubectl apply` time through two complementary layers:

1. **Declarative CEL Rules in CRD Schema**:
   The CRD includes `x-kubernetes-validations` directly in the OpenAPI schema:
   ```yaml
   x-kubernetes-validations:
     - rule: "self.shardsPerTenant <= self.shardPoolSize"
       message: "shardsPerTenant cannot exceed shardPoolSize"
     - rule: "self.shardsPerTenant >= 1"
       message: "shardsPerTenant must be at least 1"
     - rule: "self.shardPoolSize >= 1"
       message: "shardPoolSize must be at least 1"
     - rule: "size(self.tenantID) > 0"
       message: "tenantID must not be empty"
   ```
   This rejects invalid resources at the API server before they are ever stored.

2. **Validating Admission Webhook**:
   A controller-runtime admission webhook is implemented in `api/v1alpha1/shuffleshardassignment_webhook.go` and can be enabled via `--enable-webhook`.

---

## 7. Custom Resource Definition (CRD)

### Spec
- `tenantID` (`string`, required): Unique identifier for the tenant.
- `shardPoolSize` (`int`, optional, default: 8): Total pool size ($N \ge 1$).
- `shardsPerTenant` (`int`, optional, default: 2): Shards assigned to this tenant ($1 \le k \le N$).
- `nodeSelectorKey` (`string`, optional, default: `topology.kubernetes.io/shard`): Node label key.
- `targetWorkload` (`object`, optional): Reference to target Deployment (`name`, `namespace`, `kind`, `apiVersion`).

### Status
- `assignedShards` (`[]int`): Sorted list of deterministically assigned shard IDs.
- `phase` (`string`): `Assigned` or `Failed`.
- `conditions` (`[]metav1.Condition`): `Ready` and `WorkloadConfigured`.
- `observedGeneration` (`int64`): Observed generation.
- `lastReconciled` (`metav1.Time`): Timestamp of last reconciliation.

---

## 8. Verification & Running Tests

### Run Unit Tests
```bash
go test -v ./...
```

The test suite covers:
- **Assignment Stability**: 500 repeated iterations per tenant asserting 100% reproducible assignments across reconciler invocations.
- **Even Distribution**: 20,000 synthetic tenants evaluated over $N=8, k=2$; asserts empirical frequencies cluster evenly around the expected 25% ($\pm 3\%$).
- **Pairwise Overlap Bounds**: 124,750 tenant pairs evaluated; verifies that distinct assignments share at most 1 shard.
- **Controller Reconciler**: Fake client tests verifying CR status updates, Deployment NodeAffinity injection, tolerations injection, hot-loop protection, and finalizer-based cleanup on deletion.
- **Admission Webhook**: Validates rejection of invalid configurations ($k > N$, empty tenant, $N < 1$) on create and update.
