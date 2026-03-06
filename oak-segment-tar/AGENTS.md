# AGENTS.md — oak-segment-tar

## Module Overview

Immutable segment-based content storage for Oak, using TAR files as the default
persistence format. This is the default NodeStore for single-instance deployments.

This module has two roles:
1. **SPI definition** — the `spi` packages define the storage abstraction that cloud
   backends (oak-segment-azure, oak-segment-aws) implement
2. **TAR implementation** — the concrete FileStore/TarMK storage engine

Docs: `oak-doc/src/site/markdown/nodestore/segment/overview.md`,
`oak-doc/src/site/markdown/nodestore/segmentmk.md`

## Key Concepts

### Segments
Immutable byte containers (max 256 KiB). Two kinds:
- **Data segments** — contain node records, templates, and references to other segments
- **Bulk segments** — contain raw binary data (strings, blobs)

Segments are identified by a 128-bit UUID (`SegmentId`). Records within a segment are
addressed by `RecordId` (segment + offset). Records are 4-byte aligned.

### Record Types (`RecordType`)
`NODE`, `TEMPLATE`, `LEAF`, `BRANCH`, `BUCKET`, `LIST`, `VALUE`, `BLOCK`, `BLOB_ID`.
Templates act as "hidden classes" — they encode the structure (property names, types,
child node layout) of a node and are shared across nodes with the same shape.

### TAR Files
Segments are packed into TAR archives. Each TAR file contains:
- Segment entries (variable size, up to 256 KiB each)
- A binary index at the end for O(log n) segment lookup
- Default max TAR file size: 256 MB

### Generations and Garbage Collection
Each compaction cycle creates a new **GC generation**. Segments carry their generation
number. Old generations are reclaimed during cleanup. By default, the last 2 generations
are retained (`RETAINED_GENERATIONS_DEFAULT = 2`).

### Copy-on-Write / MVCC
Segments are never modified after creation. Content changes create new segments along
the modified path. Concurrent readers see a consistent snapshot; a single writer commits
atomically by updating the HEAD revision in the journal.

## Package Layout

| Package | Role |
|---------|------|
| `segment.spi.persistence` | **Public SPI** — `SegmentNodeStorePersistence`, `RepositoryLock`, `SegmentArchiveManager`, `JournalFile`, `GCJournalFile`, `ManifestFile`. Cloud backends implement these. |
| `segment.spi.monitor` | **Public SPI** — `IOMonitor`, `FileStoreMonitor`, `RemoteStoreMonitor` |
| `segment.spi.persistence.split` | **Public SPI** — split persistence (different backends per subtree) |
| `segment` | Core model — `Segment`, `SegmentId`, `RecordId`, `RecordType`, `SegmentNodeState`, `SegmentNodeStore`, `SegmentReader`, `SegmentWriter`, `SegmentTracker` |
| `segment.file` | `FileStore`, `FileStoreBuilder`, `ReadOnlyFileStore`, `GarbageCollector`, GC/compaction strategies |
| `segment.file.tar` | TAR file I/O — `TarFiles`, `TarReader`, `TarWriter`, `TarRevisions`, `TarPersistence` |
| `segment.file.tar.index` | TAR index structures (v1/v2) |
| `segment.compaction` | GC configuration — `SegmentGCOptions` (GCType, CompactorType), `SegmentRevisionGCMBean` |
| `segment.data` | Segment binary format — `SegmentData`, `SegmentDataV13` |
| `segment.standby` | Master–slave replication over Netty (codec, client, server, JMX) |
| `segment.scheduler` | Commit coordination — `LockBasedScheduler` |
| `segment.tool` | CLI commands — `Check`, `Compact`, `Backup`, `Restore`, debug tools |
| `segment.osgi` | OSGi service components — `TarPersistenceService`, `SplitPersistenceService` |
| `backup` | Backup/restore — `FileStoreBackup`, `FileStoreRestore` |
| `segment.memory` | In-memory segment store (for testing) |

## OSGi Exports

Only the `spi` packages are exported — everything else is internal:
```
org.apache.jackrabbit.oak.segment.spi
org.apache.jackrabbit.oak.segment.spi.monitor
org.apache.jackrabbit.oak.segment.spi.persistence
org.apache.jackrabbit.oak.segment.spi.persistence.split
org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache
```

Changes to exported packages trigger OSGi baseline checks. Changes to internal packages
do not affect downstream bundles.

## FileStore Lifecycle

### Opening (`FileStore` constructor via `FileStoreBuilder.build()`)
1. Acquires exclusive `RepositoryLock` via `persistence.lockRepository()`
2. Checks/updates manifest (store version)
3. Creates `SegmentWriter` for system writes
4. Initializes `TarFiles` (loads existing TAR archives)
5. Creates `GarbageCollector` with configured strategies

### Closing (`FileStore.close()`)
1. Stops the background scheduler
2. Flushes pending writes
3. Releases closeables in order: `repositoryLock` → `tarFiles` → `revisions`
4. Forces GC and reaps pending file deletions

### Key builder options (`FileStoreBuilder`)
- `withMaxFileSize(int MB)` — TAR file size limit (default 256 MB)
- `withMemoryMapping(boolean)` — memory-mapped I/O (default true on 64-bit JVMs)
- `withSegmentCacheSize(int MB)` — segment cache (default 256 MB)
- `withGCOptions(SegmentGCOptions)` — GC/compaction configuration
- `withBlobStore(BlobStore)` — external blob store (null = inline blobs)

## Garbage Collection and Compaction

### GC Types (`SegmentGCOptions.GCType`)
- `FULL` — compacts the entire HEAD state
- `TAIL` — compacts only the diff since the last compaction

### Compactor Types (`SegmentGCOptions.CompactorType`)
- `CLASSIC_COMPACTOR` — simple single-threaded compaction
- `CHECKPOINT_COMPACTOR` — checkpoint-aware diff compaction
- `PARALLEL_COMPACTOR` — multithreaded (default)

### Strategy Chain
```
GarbageCollectionStrategy          — orchestrates the full GC cycle
  ├─ EstimationStrategy            — estimates whether compaction is worthwhile
  ├─ CompactionStrategy            — executes the compaction
  │   ├─ FullCompactionStrategy
  │   ├─ TailCompactionStrategy
  │   └─ FallbackCompactionStrategy (tries primary, falls back to secondary)
  └─ CleanupStrategy               — removes unreferenced old-generation segments
```

Two GC strategy implementations:
- `DefaultGarbageCollectionStrategy` — standard (estimate → compact → cleanup)
- `CleanupFirstGarbageCollectionStrategy` — cleanup before compaction

### GC Defaults
| Parameter | Default |
|-----------|---------|
| Retry count | 5 |
| Force timeout | 60 seconds |
| Retained generations | 2 |
| Size delta estimation | 1 GB |
| Memory threshold | 15% |
| Compaction concurrency | 1 |

## Persistence SPI

To implement a custom storage backend (as oak-segment-azure and oak-segment-aws do),
implement `SegmentNodeStorePersistence`:

| Method | Purpose |
|--------|---------|
| `createArchiveManager(...)` | Factory for `SegmentArchiveManager` — manages segment archive reading/writing |
| `segmentFilesExist()` | Check if the store has existing segments |
| `getJournalFile()` | `JournalFile` — revision journal (append-only log of HEAD record IDs) |
| `getGCJournalFile()` | `GCJournalFile` — GC history log |
| `getManifestFile()` | `ManifestFile` — store version metadata |
| `lockRepository()` | `RepositoryLock` — exclusive lock preventing concurrent access |

The `RepositoryLock` contract requires that the lock is released automatically if the
process crashes (no manual cleanup). The TAR implementation uses file-system locks;
cloud implementations use blob leases (Azure) or DynamoDB locks (AWS).

## Important System Properties

| Property | Default | Purpose |
|----------|---------|---------|
| `access.off.heap` | false | Off-heap segment access |
| `gc.classic` | false | Use synchronized classic GC strategy |
| `oak.gc.backoff` | — | Minimum interval between GC runs (ms) |
| `oak.segment.compaction.gcSizeDeltaEstimation` | 1 GB | Minimum size delta to trigger compaction |
| `oak.segmentNodeStore.commitFairLock` | true | Fair locking for commit serialization |
| `oak.checkpoints.lockWaitTime` | 10s | Checkpoint lock acquisition timeout |

## Testing

- **JUnit 4** with Mockito 5.x
- `TemporaryFileStore` — JUnit rule that creates an ephemeral FileStore for a test
  (`src/test/java/.../segment/test/TemporaryFileStore.java`)
- `SegmentTarFixture` — `NodeStoreFixture` implementation for cross-backend test suites
- In-memory segment store (`segment.memory` package) for fast unit tests
- Standby tests use `NetworkErrorProxy` for simulating network failures
- Integration tests (IT suffix) run with `-PintegrationTesting`

## Common Pitfalls

- Changing anything in `segment.spi.*` packages affects cloud backends
  (oak-segment-azure, oak-segment-aws) — rebuild with `-pl oak-segment-tar -amd`
- Memory-mapping is enabled by default on 64-bit JVMs; some tests may behave differently
  on 32-bit environments
- GC tests can be slow — use `-Dtest=ClassName` to run specific ones
- The standby subsystem embeds Netty — be aware of optional import resolution in OSGi