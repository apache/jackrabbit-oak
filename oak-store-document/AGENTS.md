# AGENTS.md — oak-store-document

## Module Overview

Document-oriented `NodeStore` implementation persisting repository content as documents.
Source: `oak-store-document/src/main/java/org/apache/jackrabbit/oak/plugins/document/`

Three `DocumentStore` backends:
- **MongoDB** (`mongo/`) — production backend for clustered deployments
- **RDB** (`rdb/`) — production backend for relational databases (JDBC)
- **Memory** (`memory/`) — in-memory, **testing only** (no persistence, no clustering)

**Documentation:** `oak-doc/src/site/markdown/nodestore/documentmk.md`,
`nodestore/document/mongo-document-store.md`, `nodestore/document/rdb-document-store.md`,
https://jackrabbit.apache.org/oak/docs/nodestore/documentmk.html

## Key Concepts

### Storage Model

Two collections/tables:
- **`nodes`** — one document per JCR node, keyed by `_id` (`<depth>:<path>`, e.g. `2:/a/b`)
- **`blobs`** — binary data when using the built-in BlobStore

| Field | Description |
|---|---|
| `_id` | Primary key: `<depth>:<path>` |
| `_deleted` | Map of revision → true/false (creation/deletion history) |
| `_lastRev` | Last revision written per cluster node ID |
| `_modCount` | Monotonic modification counter for optimistic locking |
| `_children` | Boolean: true if the node has child nodes |
| `_revisions` | Commit status map: revision → `c` (committed) or `b` (branch) |
| `_bc` | Branch commit marker map (Oak 1.8+) |
| `_prev` | Links to previous (split) documents |
| `_sdType` | Split document type (10=DEFAULT, 50=LEAF, 60=COMMIT_ROOT_ONLY, 70=NO_BRANCH) |

Property updates append new revision-stamped entries rather than overwriting, preserving full history for MVCC reads.

### Revision Model

A `Revision` has three components — e.g. `r13f38835063-2-1`:
- **Timestamp** — 11-char hex from `System.currentTimeMillis()`
- **Counter** — disambiguates revisions created at the same millisecond
- **Cluster node ID** — identifies which cluster node created it

A `RevisionVector` tracks the head revision across all cluster nodes simultaneously.

### MVCC and Commit Queue

Reads are always consistent snapshots based on a `RevisionVector`. Concurrent commits are
serialised through `CommitQueue`, which advances the head revision atomically. Optimistic
concurrency control detects conflicts via `_modCount` and revision comparisons (`Collision`).

### Branching

In-memory branches accumulate changes before merge. After 100,000 pending changes a branch
is automatically spilled to the backend (`_bc` field). Branch commits are identified by
revision prefix `b`. `UnmergedBranches` tracks all active in-memory branches.

### Previous (Split) Documents

When a document grows beyond 100 revisions or ~1 MB, aged revision history is moved to
split documents via `SplitOperations`. Split documents are linked via `_prev` and form a
hierarchical tree (type 40 = intermediate node).

### Sweep

`NodeDocumentSweeper` (sweep 1) fixes up uncommitted branch revisions left by a crashed
cluster node. `MissingBcSweeper2` (sweep 2) backfills the `_bc` field on documents written
before Oak 1.8. The sweep revision (`_sweepRev`) tracks how far sweep has progressed.

## Package Layout

| Package | Role |
|---|---|
| _(root)_ | Core model: `DocumentNodeStore`, `NodeDocument`, `Revision`, `Commit`, `CommitQueue`, `Branch`, GC classes, cluster management |
| `mongo/` | `MongoDocumentStore`, `MongoDocumentNodeStoreBuilder`, blob store, throttling, metrics |
| `rdb/` | `RDBDocumentStore`, `RDBDocumentNodeStoreBuilder`, JDBC helpers, serialization |
| `cache/` | `NodeDocumentCache`, cache invalidation, `ForwardingListener` |
| `persistentCache/` | Off-heap / disk-backed persistent cache (MapDB) for immutable caches |
| `prefetch/` | `PrefetchDispatcher` — async prefetch of child nodes |
| `secondary/` | Secondary store support for read replicas |
| `spi/` | Internal SPI interfaces (e.g. `DocumentStoreFilter`) |
| `locks/` | `DocumentNodeStoreLocks` — cluster-level distributed locking |
| `memory/` | `MemoryDocumentStore` — test-only in-memory `DocumentStore`; no persistence, no external dependencies, no cluster support |
| `init/` | Repository initialisation helpers |
| `util/` | `Utils`, `MongoConnection`, and other shared utilities |

## Key Classes

| Class | Role |
|---|---|
| `DocumentNodeStore` | Central `NodeStore` implementation; owns all background threads and caches |
| `DocumentStore` | Interface for the storage backend (MongoDB or RDB) |
| `NodeDocument` | Single document in the `nodes` collection; contains all revision history for a node |
| `Revision` | A single point-in-time identifier (timestamp + counter + cluster node ID) |
| `RevisionVector` | Vector clock tracking head revision across all cluster nodes |
| `Commit` / `CommitBuilder` | Represents an in-flight commit; builds `UpdateOp` instances |
| `CommitQueue` | Serialises concurrent commits; manages head revision advancement |
| `UpdateOp` | Atomic set of update operations on a single document — always use this for mutations |
| `Branch` / `UnmergedBranches` | Tracks active in-memory branch state before merge |
| `SplitOperations` | Logic for splitting large node documents into previous documents |
| `NodeDocumentSweeper` | Marks uncommitted branch revisions as rolled back after crash recovery |
| `VersionGarbageCollector` | GC entry point — orchestrates Revision GC (phase 1, MongoDB+RDB) and Full GC (phase 2, MongoDB only) |
| `JournalGarbageCollector` | Cleans up expired journal collection entries |
| `ClusterNodeInfo` | Manages cluster node ID registration and lease renewal |
| `LastRevRecoveryAgent` | Recovers `_lastRev` for crashed cluster nodes |
| `DocumentNodeStoreBuilder` | Fluent builder holding all runtime configuration |
| `DocumentNodeStoreService` | OSGi `@Component` — reads config and constructs the builder |
| `Configuration` | OSGi `@ObjectClassDefinition` — declares all configurable properties |
| `FormatVersion` | Controls read/write compatibility during rolling upgrades |

## OSGi Configuration

```
Configuration.java                ← OSGi @ObjectClassDefinition (all property declarations)
    ↓
DocumentNodeStoreService          ← OSGi @Component; reads config, constructs builder
    ↓
DocumentNodeStoreBuilder          ← Fluent builder; holds all runtime settings
    ↓  (RDB variant: RDBDocumentNodeStoreBuilder)
DocumentNodeStore                 ← Runtime instance
```

OSGi PID: `org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService`

When adding a new OSGi config property, all four files above must be updated plus tests.
Follow the workflow in
`oak-store-document/.claude/skills/oak-store-document-add-config/SKILL.md`
and the supporting files in the same directory. In Claude, this workflow is exposed as
the `/oak-store-document-add-config` skill.

## Background Operations

| Operation | Frequency | Purpose |
|---|---|---|
| Lease renewal | Every 10s | Extends cluster node lease 2 minutes ahead |
| Background write | Periodic | Flushes accumulated `_lastRev` updates to ancestor nodes |
| Background read | Every 1s | Polls root node for external changes; invalidates mutable caches |
| Document splitting | Periodic | Splits documents exceeding 100 revisions or ~1 MB |
| Revision GC | Configurable | Removes revisions older than retention period (default 24h) |
| Journal GC | Configurable | Cleans up `journal` collection entries |

## Caching

| Cache | % | Contents | Invalidation |
|---|---|---|---|
| `nodeCache` | 35% | Immutable `DocumentNodeState` views | None needed (immutable) |
| `diffCache` | 30% | Diffs between revisions | None needed; also backed by persistent cache |
| `documentCache` | 16% | Mutable `NodeDocument` instances | Via background reads |
| `childrenCache` | 15% | Immutable child node lists | None needed (immutable) |
| `prevDocCache` | 4% | Immutable previous (split) documents | None needed (immutable) |

Immutable caches (`nodeCache`, `childrenCache`, `prevDocCache`, `diffCache`) can be backed
by the **persistent cache** (MapDB) for warm restarts.

## Garbage Collection

GC runs in two phases via `VersionGarbageCollector`, controlled by `VersionGCOptions` / `FullGCMode`.

**Phase 1 — Revision GC**: supports **MongoDB and RDB**
- Removes old property revisions, deleted nodes, and split documents past the retention window
- On MongoDB: runs continuously every 5 seconds (Oak 1.8+)
- On RDB: invoked manually via `RepositoryManagementMBean.startRevisionGC()` or oak-run
- CLI: `java -jar oak-run.jar revisions <store-uri> collect`

**Phase 2 — Full GC**: **MongoDB only — explicitly blocked for RDB**
- Cleans orphaned property revisions within the same time window as Revision GC
- Disabled by default; must be explicitly enabled via `fullGCEnabled=true`
- Behaviour controlled by `fullGCMode` (see `FullGCMode` enum below)

**Journal GC** (`JournalGarbageCollector`): cleans `journal` collection entries past the retention period; applies to both MongoDB and RDB.

### Key classes

| Class | Role |
|---|---|
| `VersionGarbageCollector` | Main GC entry point — orchestrates both Revision GC and Full GC phases |
| `VersionGCOptions` | Runtime options passed to a GC run (max age, Full GC mode, generation, etc.) |
| `VersionGCRecommendations` | Computes when GC should run and what to target based on current store state |
| `VersionGCSupport` | Base class for backend-specific document/revision queries needed by GC |
| `MongoVersionGCSupport` | `VersionGCSupport` implementation for MongoDB (`mongo/` package) |
| `RDBVersionGCSupport` | `VersionGCSupport` implementation for RDB (`rdb/` package) |
| `FullGCStatsCollector` / `FullGCStatsCollectorImpl` | Collects per-run Full GC metrics (documents scanned, revisions removed, duration) |
| `FullGCMetricsExporter` | Exports Full GC metrics to Prometheus via pushgateway |

### FullGCMode enum

| Value | What it cleans |
|---|---|
| `NONE` (0) | Full GC disabled |
| `GAP_ORPHANS` | Orphaned revisions in gaps only |
| `GAP_ORPHANS_EMPTYPROPS` | Gap orphans + empty properties |
| `ALL_ORPHANS` | All orphaned revisions |
| `ALL_ORPHANS_EMPTYPROPS` | All orphans + empty properties |
| `ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS` | Above + keep one value for user properties |
| `ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS` | Above + keep one value for all properties |
| `ORPHANS_EMPTYPROPS_UNMERGED_BC` | Above + unmerged branch commits |
| `ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC` | Above, scoped between checkpoints, no unmerged BC |
| `ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC` | Above, scoped between checkpoints, with unmerged BC |

### OSGi configuration

**Revision GC:**

| Property | Default | Purpose |
|---|---|---|
| `versionGcMaxAgeInSecs` | `86400` (24h) | Retention window — revisions older than this are eligible for collection |

**Full GC (MongoDB only):**

| Property | Default | Purpose |
|---|---|---|
| `fullGCEnabled` | `false` | Enable Full GC phase |
| `fullGcMaxAgeInSecs` | `86400` (24h) | Retention window for Full GC (same semantics as `versionGcMaxAgeInSecs`) |
| `fullGCMode` | `0` (`NONE`) | Controls which orphan/property cleanup to perform (see `FullGCMode` enum) |
| `fullGCGeneration` | `0` | Generation counter to scope Full GC runs |
| `embeddedVerificationEnabled` | `true` | Verify cleaned documents are consistent before removing revisions |

## Throttling

**MongoDB only** — throttling is not available for RDB. Enabled via `throttlingEnabled=true`.

`ThrottlingDocumentStoreWrapper` wraps `MongoDocumentStore` at startup and sleeps for
`throttlingTime()` ms before every write. `MongoDocumentStoreThrottlingFactorUpdater` runs
on a background scheduler and reads a throttling factor from MongoDB's `settings` collection
every `throttlingJobSchedulePeriodSecs` seconds, storing it in an `AtomicReference<Integer>`
shared with the wrapper. `ThrottlingStatsCollector` records per-operation throttling metrics.

### OSGi configuration

| Property | Default | Purpose |
|---|---|---|
| `throttlingEnabled` | `false` | Enable throttling; system property `oak.documentstore.throttlingEnabled` |
| `throttlingTimeMillis` | — | Base delay in milliseconds |
| `throttlingJobSchedulePeriodSecs` | — | How often the factor is re-read from MongoDB |
| `disableThrottling` | `false` | Emergency kill-switch to disable throttling at runtime |

### Key classes

| Class | Location | Role |
|---|---|---|
| `Throttler` | root | Interface: `throttlingTime()` returns delay in ms; `NO_THROTTLING` constant returns 0 always |
| `MongoThrottlerFactory` | `mongo/` | Factory that creates `Throttler` instances — see throttler types below |
| `ThrottlingDocumentStoreWrapper` | `util/` | `DocumentStore` decorator that sleeps for `throttlingTime()` ms before every write |
| `MongoDocumentStoreThrottlingFactorUpdater` | `mongo/` | Background scheduler that reads throttling factor from MongoDB `settings` collection into an `AtomicReference<Integer>` |
| `ThrottlingStatsCollector` / `ThrottlingStatsCollectorImpl` | root | Metrics collection for throttling events per operation and collection |

### Throttler types (`MongoThrottlerFactory`)

| Factory method | Throttler | Behaviour |
|---|---|---|
| `exponentialThrottler(threshold, oplogWindow, time)` | `ExponentialThrottler` | Scales delay by oplog window vs threshold: `> threshold` → 0ms, `≤ threshold` → 1×, `≤ threshold/2` → 2×, `≤ threshold/4` → 4×, `≤ threshold/8` → 8× the base `throttlingTime` |
| `extFactorThrottler(factor, time)` | `ExtFactorThrottler` | Delay = `time × factor` where `factor` is read from MongoDB `settings` collection by `MongoDocumentStoreThrottlingFactorUpdater`; factor ≤ 0 → no throttling |
| `noThrottler()` | `NO_THROTTLING` | Always returns 0 — used when throttling is disabled |

## Lease Updates

Every cluster node must continuously prove it is alive by renewing its lease in the
`clusterNodes` collection. `ClusterNodeInfo` acquires a cluster node ID on startup and
renews `leaseEnd` every **10 seconds** (pushing it 120 seconds forward). Before every
`DocumentStore` read or write, `LeaseCheckDocumentStoreWrapper` calls
`ClusterNodeInfo.performLeaseCheck()` — if `leaseEnd` has passed, the check fails per the
configured `LeaseCheckMode` and `LeaseFailureHandler` is invoked (typically stops the OSGi
bundle) to prevent a node with an expired lease from corrupting shared state.

### LeaseCheckMode

| Mode | Behaviour |
|---|---|
| `STRICT` | Fails immediately when `leaseEnd` is reached. Default since Oak 1.10. |
| `LENIENT` | Gives the renewal thread a short grace period to renew before failing. |
| `DISABLED` | No lease check at all. Only for testing — never use in production. |

### Key classes

| Class | Location | Role |
|---|---|---|
| `ClusterNodeInfo` | root | Registers cluster node ID, holds lease state, drives renewal. Constants: `DEFAULT_LEASE_DURATION_MILLIS` (120s), `DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS` (10s), `DEFAULT_LEASE_FAILURE_MARGIN_MILLIS` (20s) |
| `LeaseCheckDocumentStoreWrapper` | `util/` | `DocumentStore` decorator that calls `performLeaseCheck()` before every read and write; throws `DocumentStoreException` on failure |
| `LeaseCheckMode` | root | Enum: `STRICT`, `LENIENT`, `DISABLED` |

### OSGi configuration

| Property | Default | Purpose |
|---|---|---|
| `leaseCheckMode` | `STRICT` | Controls how lease expiry is handled (`STRICT` / `LENIENT` / `DISABLED`) |

System property overrides:
- `oak.documentMK.leaseDurationSeconds` — overrides lease duration (default 120s)
- `oak.documentMK.leaseCheckDisabled` — set `true` to disable lease checks (testing only)

## Secondary Store

An optional read-only `NodeStore` (e.g. a local Segment store) that shadows a subset of the
document store's content via `PathFilter`, acting as a path-filtered read cache.
`SecondaryStoreObserver` keeps it in sync by observing primary changes. When a node read
falls within an included path and the secondary store's revision is current, it is served from
there; otherwise falls back to the primary. Only path inclusion is supported — exclusion within
an included subtree is not possible.

| Class | Role |
|---|---|
| `SecondaryStoreCacheService` | OSGi `@Component`; wires secondary `NodeStore`, builds cache and observer |
| `SecondaryStoreCache` | `DocumentNodeStateCache` implementation; path-filtered read delegation |
| `SecondaryStoreObserver` | Observes primary store changes and applies them to the secondary store |

## Checkpoints

Checkpoints pin a `RevisionVector` so GC cannot remove still-needed revisions — primarily
used by the async indexer. Created via `DocumentNodeStore.checkpoint(lifetime, info)`, released
via `DocumentNodeStore.release(checkpoint)`. All active checkpoints are stored in a single
`_id=checkpoint` document in `nodes`, keyed by revision → expiry + info map.
`Checkpoints.getOldestRevisionToKeep()` is called by GC before collecting — revisions newer
than the oldest valid checkpoint are never removed. Expired checkpoints are cleaned up lazily
every 10 `create` calls.

## Prefetch

`PrefetchDispatcher` pre-fetches visible external changes in a background thread before delivery
to local observers, hiding MongoDB/RDB read latency during observer dispatch. Controlled by
feature toggle `FT_PREFETCH_OAK-9780` (disabled by default); wired via
`DocumentNodeStoreBuilder.setPrefetchFeature()`.

## Persistent Cache

Backs immutable caches (`nodeCache`, `childrenCache`, `prevDocCache`, `diffCache`) with an
off-heap MapDB store on disk for warm restarts. `documentCache` (mutable) is never persisted.
Configured via `DocumentNodeStoreBuilder.setPersistentCache()`. Supports async write mode
(`asyncCache=true` by default) and a broadcast mechanism (`broadcast/` sub-package) for cache
invalidation across nodes.

Key classes: `PersistentCache` (main entry point), `NodeCache`, `CacheActionDispatcher`
(async write queue), `PersistentCacheStats` (metrics).

## DocumentDiscoveryLiteService

OSGi `@Component` that derives and publishes a **cluster view** — a snapshot of which cluster
nodes are active, deactivating, or inactive — as a repository descriptor via `ClusterRepositoryInfo`.

Node states (from `clusterNodes` collection):
- **active** — running with a valid up-to-date lease
- **deactivating** — lease expired but recovery not yet complete
- **inactive** — not running and all changes recovered

The view carries a monotonically increasing **sequence number**; a `final` flag indicates
stability. If `final=false`, consumers must not act on deactivating/inactive states until the
view becomes final. Primarily consumed by Sling's discovery framework for cluster topology events.

## Feature Toggles

Registered in `DocumentNodeStoreService`, wired into `DocumentNodeStoreBuilder`. All follow
`FT_<DESCRIPTION>_OAK-<issue>` naming and are **disabled by default**.

| Toggle name | Builder method | Purpose |
|---|---|---|
| `FT_PREFETCH_OAK-9780` | `setPrefetchFeature` | Enable async pre-fetching of external changes |
| `FT_THROTTLING_OAK-9909` | `setDocStoreThrottlingFeature` | Enable write throttling on MongoDB |
| `FT_DISABLE_THROTTLING_OAK-12119` | `setDocStoreDisableThrottlingFeature` | Runtime kill-switch to disable throttling |
| `FT_NOCOCLEANUP_OAK-10660` | `setNoChildOrderCleanupFeature` | Disable child-order property cleanup |
| `FT_CANCELINVALIDATION_OAK-10595` | `setCancelInvalidationFeature` | Cancel in-flight cache invalidations |
| `FT_FULL_GC_OAK-10199` | `setDocStoreFullGCFeature` | Enable Full GC phase (MongoDB only) |
| `FT_EMBEDDED_VERIFICATION_OAK-10633` | `setDocStoreEmbeddedVerificationFeature` | Enable embedded verification during Full GC |
| `FT_AVOID_MERGE_LOCK_OAK-11720` | `setDocStoreAvoidMergeLockFeature` | Avoid acquiring merge lock where safe |
| `FT_PREV_NO_PROP_OAK-11184` | `setPrevNoPropCacheFeature` | Skip caching previous documents with no properties |

## Cluster Support

Each cluster node registers a `ClusterNodeInfo` document in `clusterNodes`, acquiring a unique
numeric ID (inactive IDs from crashed nodes are reused) and holding a lease renewed every 10s
(2-minute window). `LeaseCheckMode.STRICT` (default since Oak 1.10) immediately rejects writes
on lease expiry.

**Clock skew:** Oak tolerates up to **2 seconds** of clock difference between the application
server and the database server. NTP synchronisation is required across all nodes.

**Recovery:** When a cluster node crashes, `LastRevRecoveryAgent` recovers its `_lastRev`
updates. `NodeDocumentSweeper` then marks any uncommitted branch changes as rolled back.

**Format version:** `FormatVersion` in the `settings` collection controls read/write
compatibility. Use `oak-run unlockUpgrade` when all cluster nodes are inactive to advance
the version.

## Important System Properties

| Property | Default | Purpose |
|---|---|---|
| `oak.documentMK.disableBranches` | false | Disable branch commits (not recommended for production) |
| `oak.documentMK.childrenCacheLimit` | 16 | Max child node entries per cache entry |
| `oak.mongo.maxReplicationLagMillis` | 60000 | Max MongoDB replication lag before warning |
| `oak.mongo.socketKeepAlive` | true | Enable TCP keep-alive on MongoDB connections |
| `oak.fullGC.enable` | false | Enable Full GC mode (cleans all orphaned revisions) |

## Testing

**JUnit 4** with Mockito 5.x. Three backends available:

| Backend | Class | When to use |
|---|---|---|
| Memory | `MemoryDocumentStore` | Default for unit tests — no external dependencies, fast, no persistence |
| MongoDB | `MongoDocumentStore` | Required when testing cluster behaviour, replication lag, or Mongo-specific paths |
| RDB | `RDBDocumentStore` | Required when testing RDB-specific paths or JDBC behaviour |

- `DocumentStoreFixture` — fixture abstraction supporting `MONGO`, `RDB_*`, and `MEMORY`
  backends; used by `AbstractDocumentStoreTest` and related base classes
- `MongoUtils` / `AbstractMongoConnectionTest` — requires `mongod` on port 27017 (`-Dmongo.url` to override)
- `AbstractRDBConnectionTest` — configured via JDBC URL system properties
- Integration tests (IT suffix) require `-PintegrationTesting`
- The `DOCUMENT_NS` fixture in the broader test suite maps to MongoDB

```bash
# Unit tests (uses MemoryDocumentStore — no MongoDB needed)
mvn test -pl oak-store-document -Dtest=NodeDocumentTest

# Full test suite against MongoDB (requires mongod on localhost:27017)
mvn test -pl oak-store-document

# Integration tests
mvn verify -pl oak-store-document -PintegrationTesting
```

## Common Pitfalls

- **MongoDB required for `DOCUMENT_NS` fixture** — start `mongod` on port 27017 before
  running any test that uses the `DOCUMENT_NS` fixture
- **Never bypass lease checks** — do not mock or disable `ClusterNodeInfo` lease logic in
  production code paths; it prevents split-brain in multi-node clusters
- **Always use `UpdateOp` for mutations** — never write raw documents directly to
  `DocumentStore`; `UpdateOp` ensures correct revision stamping and `_modCount` tracking
- **Document size limit** — MongoDB has a 16 MB hard limit per document; `SplitOperations`
  must fire before this is reached. Do not accumulate unbounded revision history
- **Format version gating** — code that changes the on-disk format must be gated behind
  a `FormatVersion` check to preserve rolling upgrade compatibility
- **Clock skew** — code using `Revision` timestamps must account for the 2-second tolerance;
  never assume the local clock matches the backend clock
- **Adding OSGi config** — changes to `Configuration.java` require matching updates in
  `DocumentNodeStoreService`, `DocumentNodeStoreBuilder`, `RDBDocumentNodeStoreBuilder`,
  and all relevant test files; follow
  `oak-store-document/.claude/skills/oak-store-document-add-config/SKILL.md`
  and its supporting files. In Claude, this workflow is exposed as the
  `/oak-store-document-add-config` skill
