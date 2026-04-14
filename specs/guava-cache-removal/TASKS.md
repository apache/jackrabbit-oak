# Oak Cache API Migration — Task Breakdown

This document decomposes the Oak Cache API migration plan (PLAN.md) into independently mergeable JIRA-sized tasks. Each task produces one PR that compiles and passes tests without requiring any sibling task to be in-progress. Tasks are numbered sequentially (OAK-12147 through OAK-12162). Batch 0 splits into two tasks (API then implementations), Batch 6 splits into three (cache infra, diff caches, persistent cache), and Batch 7 splits into two (Guava-shim caches, CacheLIRS-based caches). All other batches map one-to-one.

## Current local status

- OAK-12147 API interfaces are implemented locally.
- OAK-12148 hidden implementations and builder are implemented locally: `CacheBuilder` creates Caffeine-backed caches only; `CacheLIRS` instances are exposed via `CacheLIRS.asOakCache()`. Separate manual/loading adapters per backend, builder-side validation, and Javadocs are all in place.
- OAK-12149 through OAK-12162 remain planning tasks in this document.

## Dependency Graph

```
OAK-12147  (API interfaces)
  |
OAK-12148  (hidden impls + builder)
  |
  +--+--+--+--+--+--+--+--+--+--+--+
  |  |  |  |  |  |  |  |  |  |  |  |
 149 150 151 152 153 154 155 156 157 158 159 160
  |  |  |  |  |  |  |  |  |  |  |  |
  +--+--+--+--+--+--+--+--+--+--+--+
                  |
              OAK-12161
                  |
              OAK-12162

Parallel groups (all depend only on OAK-12148, can run concurrently):
  OAK-12149 — oak-blob-cloud
  OAK-12150 — oak-blob-cloud-azure
  OAK-12151 — oak-blob
  OAK-12152 — oak-search-elastic
  OAK-12153 — oak-search
  OAK-12154 — oak-store-document cache infra
  OAK-12155 — oak-store-document diff caches
  OAK-12156 — oak-store-document persistent cache
  OAK-12157 — oak-segment-tar Guava-shim caches
  OAK-12158 — oak-segment-tar CacheLIRS-based caches
  OAK-12159 — oak-blob-plugins
  OAK-12160 — oak-run-commons

Sequential tail:
  OAK-12161 — oak-it-osgi verification (depends on OAK-12149 through OAK-12160)
  OAK-12162 — final cleanup (depends on OAK-12161)
```

## Migration rules (enforced for every task)

**1. Guava-free check** — Before declaring a migration task done, run:
```bash
grep -rn "org.apache.jackrabbit.guava.common.cache" <module>/src/
```
This must return **zero results** — both `src/main/java` and `src/test/java` must be clean.
Test files that reference Guava cache types (e.g. for reflective access checks) must be
migrated in the same PR as the production code.

**2. Cross-module return-type cascade** — Whenever a task changes the return type of any
public method (e.g. `getCacheStats()`, `getCurrentStats()`), every caller across **all
modules** must be updated in the same PR. Before closing the task:
```bash
grep -rn "methodName()" $(git rev-parse --show-toplevel)
```
A caller in an unrelated module that still expects the old return type will compile
locally (if that module is not rebuilt) but will fail in CI's full build. The list of
known callers must be explicitly enumerated in the task's "What changes" section.

**Cascade is transitive** — if caller B is a helper that re-exposes the changed method's
return type (e.g. `DocumentNodeStoreHelper.getNodesCache()` wraps `getNodeCache()`), then
callers of B are **also** cascaded. After fixing each level, re-run the grep above for
the helper's method name to find the next level of callers.

---

## TASK-1 — Oak Cache API interfaces [oak-core-spi] — [OAK-12147](https://issues.apache.org/jira/browse/OAK-12147)

**Depends on:** none
**Independent of:** none (all other tasks depend on this transitively)

### What changes

- `oak-core-spi/.../cache/api/Cache.java` — **new** interface:
  - `getIfPresent(K)` → `@Nullable V`
  - `get(K, Function<? super K, ? extends V>)` → `@Nullable V` (matches Caffeine's manual-cache contract)
  - `put(K, V)`
  - `invalidate(K)`
  - `invalidateAll()`
  - `invalidateAll(Iterable)` — _(no Oak module currently calls this; can be removed if decided)_
  - `estimatedSize()` → `long` — _(no Oak module currently calls this directly; can be removed if decided)_
  - `stats()` → `CacheStatsSnapshot`
  - `asMap()` → `ConcurrentMap<K, V>`
  - `getAllPresent(Iterable)` → `Map<K, V>` — _(no Oak module currently calls this; CacheLIRS throws `UnsupportedOperationException` for it; can be removed if decided)_
  - `cleanUp()` — _(no Oak module currently calls this; CacheLIRS is a no-op; can be removed if decided)_

- `oak-core-spi/.../cache/api/LoadingCache.java` — **new** interface extending `Cache`:
  - `get(K)` → `@NotNull V` (runtime exceptions propagate directly; checked loader failures surface as `CompletionException`)
  - `refresh(K)` → `CompletableFuture<V>`

- `oak-core-spi/.../cache/api/CacheLoader.java` — **new** functional interface (`V load(K) throws Exception`)
- `oak-core-spi/.../cache/api/Weigher.java` — **new** functional interface (`int weigh(K, V)`)
- `oak-core-spi/.../cache/api/EvictionCause.java` — **new** enum (`EXPLICIT`, `REPLACED`, `SIZE`, `EXPIRED`, `COLLECTED`)
- `oak-core-spi/.../cache/api/EvictionListener.java` — **new** functional interface (`void onEviction(K, V, EvictionCause)`)
- `oak-core-spi/.../cache/api/CacheStatsSnapshot.java` — **new** immutable value object: `hitCount`, `missCount`, `loadSuccessCount`, `loadFailureCount`, `totalLoadTime`, `evictionCount`; methods `minus()`, `hitRate()`, `missRate()`, `requestCount()`
- `oak-core-spi/.../cache/AbstractCacheStats.java` — **no change** (still returns Guava shim `CacheStats` from `getCurrentStats()`; decoupled in OAK-12162)
- `oak-core-spi/.../cache/CacheStats.java` — **no change** (existing Guava-wrapping JMX class; deferred to OAK-12162)

### Acceptance criteria
- `oak-core-spi` compiles; no new Caffeine or Guava types in the public API surface
- All existing `oak-core-spi` tests pass (`CacheTest`, `CacheSizeTest`, `ConcurrentTest`, `ConcurrentPerformanceTest`)
- No consumer module is changed; all existing `CacheLIRS.newBuilder()` and `new CacheStats(guavaCache, ...)` call sites still compile

---

## TASK-2 — Hidden implementations and builder [oak-core-spi] — [OAK-12148](https://issues.apache.org/jira/browse/OAK-12148)

**Depends on:** OAK-12147
**Independent of:** none (all consumer tasks depend on this)

### What changes
- `oak-core-spi/.../cache/api/CacheBuilder.java` — **new** public final class for creating Caffeine-backed Oak caches only.

  Builder fields: `maximumWeight`, `maximumSize`, `weigher(Weigher)`, `evictionListener(EvictionListener)`, `recordStats`, `expireAfterAccess`, `expireAfterWrite`, `refreshAfterWrite`.
  Methods: `build()` → `Cache`, `build(CacheLoader)` → `LoadingCache`.
  `build()` must always return a manual-cache adapter that does not implement `LoadingCache`; `build(CacheLoader)` must always return a loading-cache adapter.
  Validation rules are enforced in the builder before cache construction:
  - exactly one of `maximumSize` or `maximumWeight` must be configured
  - `maximumWeight` requires `weigher(...)`
  - `weigher(...)` requires `maximumWeight(...)`
  - `refreshAfterWrite(...)` is valid only with `build(CacheLoader)`
  `CacheBuilder` contains no `CacheLIRS` references; callers that still need LIRS must build
  loading `CacheLIRS` instances directly and expose them through `CacheLIRS.asOakCache()`.

- `oak-core-spi/.../cache/CacheLIRS.java` — add `asOakCache()` returning an Oak `LoadingCache` view for `CacheLIRS` instances created with a loader

- `oak-core-spi/.../cache/impl/lirs/LirsCacheAdapter.java` — **new** package-private class implementing `Cache`, wrapping `CacheLIRS`; paired with `LirsLoadingCacheAdapter` for loading caches
  - Adapts `Weigher` → Guava shim `Weigher`
  - Adapts `EvictionListener`/`EvictionCause` → `CacheLIRS.EvictionCallback`/Guava `RemovalCause`
  - `get(K, Function)`: adapts the key-aware mapping function to `CacheLIRS.get(K, Callable)` and converts checked failures to `CompletionException`
  - `LirsLoadingCacheAdapter.get(K)`: delegates to `CacheLIRS.get(K)` and converts checked failures to `CompletionException`
  - `LirsLoadingCacheAdapter.refresh(K)`: runs CacheLIRS refresh and returns a completed future representing the best-effort synchronous refresh result
  - `stats()`: converts Guava shim `CacheStats` → `CacheStatsSnapshot`
  - `invalidateAll(Iterable)`, `estimatedSize()`, `getAllPresent()`, `cleanUp()`: delegate directly to CacheLIRS

- `oak-core-spi/.../cache/impl/caffeine/CaffeineCacheAdapter.java` — **new** package-private class implementing `Cache`, wrapping Caffeine `Cache`; paired with `CaffeineLoadingCacheAdapter` for loading caches:
  - `get(K, Function)`: delegates directly to Caffeine's manual-cache API
  - `CaffeineLoadingCacheAdapter.get(K)`: delegates directly to Caffeine's loading-cache API
  - `CaffeineLoadingCacheAdapter.refresh(K)`: delegates directly to Caffeine and returns the refresh future
  - Adapts `Weigher` → Caffeine `Weigher`, `EvictionListener`/`EvictionCause` → Caffeine `RemovalListener`/`RemovalCause`
  - `stats()`: converts Caffeine `CacheStats` → `CacheStatsSnapshot`

- `oak-core-spi/.../cache/api/CacheStatsAdapter.java` — **new** package-private class; extends `AbstractCacheStats`; overrides `getCurrentStats()` returning Guava shim `CacheStats` converted from the wrapped `Cache`'s `CacheStatsSnapshot` — Guava return type kept until OAK-12162 changes the base class

- `oak-core-spi/.../cache/EmpiricalWeigher.java` — modify to implement `Weigher<CacheValue, CacheValue>` while keeping a temporary Guava-compatible bridge for existing callers until OAK-12162 cleanup

- `oak-core-spi/pom.xml` — add Caffeine as `compile` scope dependency (used only inside `CaffeineCacheAdapter`; not re-exported)

**Note on `put(K, V, int memory)` (CacheLIRS-specific):** `DefaultSegmentWriter` calls
`nodeCache.put(key, value, memoryCost)`. This method has no equivalent in `Cache` or
Caffeine — Caffeine derives weight from the configured `Weigher` at insertion time.
Migration for this call site (OAK-12158): replace with `Cache.put(key, value)` and
ensure an `Weigher` is set on the builder that computes the same cost from the
key/value. `LirsCacheAdapter` does **not** expose `put(K, V, int memory)` on the interface.

### Restore javadoc links from OAK-12147
OAK-12147 deferred `{@link CacheBuilder}` references as plain `{@code}` text with `TODO OAK-TASK2` comments.
Restore all of them to proper `{@link}` in this task:

| File | Links to restore |
|------|-----------------|
| `Cache.java` | `{@link CacheBuilder}`, `{@link CacheBuilder#recordStats()}` |
| `CacheLoader.java` | `{@link CacheBuilder#build(CacheLoader)}` |
| `Weigher.java` | `{@link CacheBuilder#weigher(Weigher)}`, `{@link CacheBuilder#maximumWeight(long)}` |
| `EvictionListener.java` | `{@link CacheBuilder#removalListener(EvictionListener)}` |
| `LoadingCache.java` | `{@link CacheBuilder#build(CacheLoader)}` |

Remove all `<!-- TODO OAK-TASK2: ... -->` HTML comments after restoring the links.

### Acceptance criteria
- All existing `oak-core-spi` tests pass
- New focused unit tests cover:
  - `CacheBuilderTest`
  - `build()` creates a Caffeine-backed manual cache
  - `build()` returns a manual `Cache` that does not implement `LoadingCache`
  - `build(CacheLoader)` creates a Caffeine-backed loading cache
  - `weigher` and `evictionListener` wiring
  - `build(CacheLoader)` produces `LoadingCache`; checked loader failure surfaces as `CompletionException`
  - runtime loader failures propagate directly
  - `Cache.get(key, mappingFunction)` uses Caffeine's `Function` contract and propagates runtime failures directly
  - `LoadingCache.refresh(key)` returns a non-null `CompletableFuture`
  - invalid builder combinations are rejected consistently before cache construction
  - `CacheStatsAdapter` bridges `CacheStatsSnapshot` back to Guava shim `CacheStats`
  - `stats()` returns non-null `CacheStatsSnapshot` with correct counts
  - `CaffeineCacheAdapterTest`
  - Caffeine removal-cause mapping, stats snapshot conversion, and iterable invalidation
  - `LirsCacheAdapterTest`
  - LIRS removal-cause mapping, checked/runtime/error exception translation, and stats snapshot conversion
  - `LirsLoadingCacheAdapterTest`
  - loading LIRS get/refresh behavior and checked/runtime loader failure translation
  - `CacheLIRSOakAdapterTest`
  - `CacheLIRS.asOakCache()` succeeds only for loading caches and rejects manual caches
- No `TODO OAK-TASK2` comments remain in any file
- No consumer module is changed; existing call sites still compile

### Compatibility note for downstream tasks
For OAK-12149 through OAK-12160, the Oak cache API follows the Caffeine cache contract.
Migration in those tasks must update manual cache loads from `Callable` to
`Function`, and callers of `loadingCache.get(key)` must stop relying on checked
`ExecutionException`.

---

## TASK-3 — Migrate oak-blob-cloud to Oak Cache API [oak-blob-cloud] — [OAK-12149](https://issues.apache.org/jira/browse/OAK-12149)

**Depends on:** OAK-12148
**Independent of:** OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12159, OAK-12160

### What changes
- `oak-blob-cloud/.../s3/S3Backend.java` — replace `CacheBuilder.newBuilder()...build()` with `CacheBuilder.newBuilder()...build()`; replace `Cache<DataIdentifier, URI>` field with `Cache<DataIdentifier, URI>`; remove Guava shim cache imports

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-blob-cloud/src/` (main and test)
- `S3Backend` cache tests pass (presigned URI caching, expiry, hit/miss)

---

## TASK-4 — Migrate oak-blob-cloud-azure to Oak Cache API [oak-blob-cloud-azure] — [OAK-12150](https://issues.apache.org/jira/browse/OAK-12150)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12159, OAK-12160

### What changes
- `oak-blob-cloud-azure/.../AzureBlobStoreBackend.java` — replace `CacheBuilder.newBuilder()` with `CacheBuilder`; replace `Cache<String, URI>` with `Cache<String, URI>`; remove Guava shim cache imports
- `oak-blob-cloud-azure/.../v8/AzureBlobStoreBackendV8.java` — same changes

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-blob-cloud-azure/src/` (main and test)
- Azure backend cache tests pass

---

## TASK-5 — Migrate oak-blob to Oak Cache API [oak-blob] — [OAK-12151](https://issues.apache.org/jira/browse/OAK-12151)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12159, OAK-12160

### What changes
- `oak-blob/.../split/BlobIdSet.java` — replace `CacheBuilder.newBuilder()` with `CacheBuilder`; replace `Cache<String, Boolean>` with `Cache<String, Boolean>`; remove Guava shim cache imports

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-blob/src/` (main and test)
- `BlobIdSet` membership and bounded-cache tests pass

---

## TASK-6 — Migrate oak-search-elastic to Oak Cache API [oak-search-elastic] — [OAK-12152](https://issues.apache.org/jira/browse/OAK-12152)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12159, OAK-12160

### What changes
- `oak-search-elastic/.../ElasticIndexStatistics.java` — replace `CacheBuilder.newBuilder()` with `CacheBuilder`; replace `LoadingCache` fields with `LoadingCache`; replace `CacheLoader` with `CacheLoader<K,V>` passed to `CacheBuilder.build(loader)`; remove Guava shim cache imports

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### CacheBuilder ticker API (added to `oak-core-spi`)
- `ticker(Supplier<Long> ticker)` — raw nanosecond supplier
- `ticker(Clock clock)` — convenience overload; delegates to the `Supplier<Long>` overload via `() -> TimeUnit.MILLISECONDS.toNanos(clock.millis())`

### Acceptance criteria
- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-search-elastic/src/` (main and test)
- `ElasticIndexStatisticsTest` passes covering expiry, refresh, and loader failure behavior
- Both `ticker(Supplier<Long>)` and `ticker(Clock)` present on `CacheBuilder`

---

## TASK-7 — Migrate oak-search to Oak Cache API [oak-search] — [OAK-12153](https://issues.apache.org/jira/browse/OAK-12153)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12159, OAK-12160

### What changes
- `oak-search/.../ExtractedTextCache.java` — replace `CacheBuilder.newBuilder()` with `CacheBuilder`; replace `Cache<String,String>` with `Cache<String,String>`; replace local `EmpiricalWeigher` inner class (Guava `Weigher`) with `Weigher` lambda; replace `new CacheStats(guavaCache, ...)` with `CacheStatsAdapter`; remove all Guava shim cache imports
- `oak-lucene/pom.xml` — add `org/apache/jackrabbit/oak/cache/api/CacheStatsAdapter.class` to the `Embed-Dependency` inline list for `oak-core-spi`; fixes OSGi classloader split where `oak-lucene`'s inlined `AbstractCacheStats` was a different class than `oak-core-spi` bundle's `AbstractCacheStats` (the superclass of `CacheStatsAdapter`), causing `VerifyError` in `IndexVersionSelectionIT`

**Return-type cascade** — `ExtractedTextCache.getCacheStats()` changes from `CacheStats` to
`AbstractCacheStats`. All callers across all modules must be updated in the same PR:
- `oak-lucene/.../LuceneIndexProviderService.java` — `CacheStats` → `AbstractCacheStats`
- `oak-run-commons/.../DocumentStoreIndexerBase.java` — `CacheStats` → `AbstractCacheStats`

Run `grep -rn "getCacheStats()"` at the repo root before closing to confirm no caller is missed.

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-search/src/` (main and test)
- `ExtractedTextCache` cache tests pass, including stats reporting
- `DocumentStoreIndexerBase` compiles cleanly in `oak-run-commons`

---

## TASK-8 — Migrate oak-store-document cache infrastructure [oak-store-document] — [OAK-12154](https://issues.apache.org/jira/browse/OAK-12154)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12159, OAK-12160

### What changes
- `DocumentNodeStoreBuilder.java` — replace `CacheLIRS.newBuilder()` calls with `CacheBuilder` for Caffeine-backed caches
- `cache/NodeDocumentCache.java` — `Cache` types to `Cache`; `asMap()` calls unchanged (method exists on `Cache`)
- `cache/ForwardingListener.java` — `RemovalCause` to `EvictionCause`; update to use `EvictionListener`
- `persistentCache/EvictionListener.java` — `RemovalCause` to `EvictionCause`
- `CachingCommitValueResolver.java` — `Cache` to `Cache`
- `CacheStats` construction sites — replace with stats obtained from `Cache` via `CacheStatsAdapter`
- `BranchTest.java` — update `Cache` import from Guava shim to Oak API (`org.apache.jackrabbit.oak.cache.api.Cache`)

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- `NodeDocumentCacheTest`, `CacheChangesTrackerTest`, `AsyncCacheTest`, `DisableCacheTest`, `BranchTest` pass
- Checked loader failures surface as `CompletionException` on the Oak-visible API
- Synchronous eviction callback timing preserved

---

## TASK-9 — Migrate oak-store-document diff caches [oak-store-document] — [OAK-12155](https://issues.apache.org/jira/browse/OAK-12155)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12156, OAK-12157, OAK-12158, OAK-12159, OAK-12160

### What changes
- `MemoryDiffCache.java` — `Cache` to `Cache`; replace Guava/CacheLIRS builder with `CacheBuilder`
- `LocalDiffCache.java` — `Cache` to `Cache`; replace builder
- `TieredDiffCache.java` — `Cache` to `Cache`; update type references

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- `MemoryDiffCacheTest`, `LocalDiffCacheTest` pass
- Diff cache eviction behavior unchanged

---

## TASK-10 — Migrate oak-store-document persistent cache and stores [oak-store-document] — [OAK-12156](https://issues.apache.org/jira/browse/OAK-12156)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12157, OAK-12158, OAK-12159, OAK-12160

### What changes
- `persistentCache/NodeCache.java` — `Cache` to `Cache`
- `persistentCache/PersistentCache.java` — `Cache` to `Cache`
- `DocumentNodeStore.java` — change `Cache` types to `Cache`
- `NodeDocument.java` — update cache type references
- `MongoDocumentStore.java` — remove direct Caffeine/Guava cache references
- `RDBDocumentStore.java` — remove direct Caffeine/Guava cache references
- `MemoryDocumentStore.java` — update if it references cache types
- `JournalDiffLoader.java` — update if it references cache types
- Various test classes — update to use `Cache` types
- **`oak-run-commons/.../DocumentNodeStoreHelper.java`** — cross-module return-type cascade: `DocumentNodeStore.getNodeCache()` return type changed in this task, so this caller must be updated in the same PR (per migration rule 2). Change `import org.apache.jackrabbit.guava.common.cache.Cache` → `import org.apache.jackrabbit.oak.cache.api.Cache`.
- **`oak-benchmarks/.../PersistentCacheTest.java`** — second-level cascade: calls `DocumentNodeStoreHelper.getNodesCache()`, whose return type also changed (see above). Same import fix required.

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-store-document/src/` (main and test)
- Persistent cache eviction behavior unchanged
- `persistentCache.CacheTest`, `persistentCache.NodeCacheTest` pass

---

## TASK-11 — Migrate oak-segment-tar Guava-shim caches [oak-segment-tar] — [OAK-12157](https://issues.apache.org/jira/browse/OAK-12157)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12158, OAK-12159, OAK-12160

### What changes
- `SegmentCache.java` — `CacheBuilder.newBuilder()` (Guava shim) to `CacheBuilder`; `Cache` to `Cache`; Guava `RemovalCause` to `EvictionCause`; inner `Stats` class updated to convert `CacheStatsSnapshot` → Guava shim `CacheStats` in `getCurrentStats()` (Guava return type kept until OAK-12162)
- `RecordCache.java` — `CacheBuilder.newBuilder()` (Guava shim) to `CacheBuilder`; `Cache` to `Cache`; Guava `Weigher` to `Weigher`
- `CacheWeights.java` — `Weigher` to `Weigher`
- `CachingSegmentReader.java` — update cache type references

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- `SegmentCacheTest` passes
- Segment unload timing and eviction callback timing unchanged

---

## TASK-12 — Migrate oak-segment-tar CacheLIRS-based caches [oak-segment-tar] — [OAK-12158](https://issues.apache.org/jira/browse/OAK-12158)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12159, OAK-12160

### What changes
- `ReaderCache.java` — `CacheLIRS.newBuilder()` to `CacheBuilder`; `CacheLIRS<K,V>` to `Cache<K,V>`
- `WriterCacheManager.java` — update cache type references
- `PriorityCache.java` — update if it references Guava shim cache types directly
- `RecordCacheStats.java` — update to obtain stats from the migrated `Cache`; convert `CacheStatsSnapshot` → Guava shim `CacheStats` in `getCurrentStats()` (Guava return type kept until OAK-12162)
- `spi/persistence/persistentcache/SegmentCacheStats.java` — same: convert `CacheStatsSnapshot` → Guava shim `CacheStats` in `getCurrentStats()` until OAK-12162
- `SegmentNodeStoreRegistrar.java` — update if it references cache builder types
- **`DefaultSegmentWriter.java`** — `nodeCache.put(key, value, memoryCost)` must be replaced with `nodeCache.put(key, value)`. Add an `Weigher` to the `CacheBuilder` configuration that computes the same memory cost from the key/value, so Caffeine can use it at insertion time. The CacheLIRS-specific 3-arg `put` is not on `Cache`.

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-segment-tar/src/` (main and test)
- `PriorityCacheTest`, `ConcurrentPriorityCacheTest`, `ReaderCacheTest` pass
- Memoization behavior unchanged

---

## TASK-13 — Migrate oak-blob-plugins to Oak Cache API [oak-blob-plugins] — [OAK-12159](https://issues.apache.org/jira/browse/OAK-12159)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12160

### What changes
- `FileCache.java` — `CacheLIRS.newBuilder()` to `CacheBuilder`; `Cache` to `Cache`; `CacheLIRS.EvictionCallback` to `EvictionListener`; `CacheStatsSnapshot` to stats from `CacheBuilder`
- `UploadStagingCache.java` — update cache types
- `CompositeDataStoreCache.java` — update cache types
- `AbstractSharedCachingDataStore.java` — update cache types
- `CachingBlobStore.java` — update cache types
- `DataStoreCacheUpgradeUtils.java` — update cache types

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-blob-plugins/src/` (main and test)
- No `CacheLIRS` references in `oak-blob-plugins/src/` (main and test)
- File cache eviction/deletion behavior unchanged
- All blob-plugins cache tests pass

---

## TASK-14 — Migrate oak-run-commons and remaining modules [oak-run-commons] — [OAK-12160](https://issues.apache.org/jira/browse/OAK-12160)

**Depends on:** OAK-12148
**Independent of:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12159

### What changes
- `oak-run-commons/.../DocumentNodeStoreHelper.java` — **already migrated in OAK-12156** (cross-module return-type cascade from `DocumentNodeStore.getNodeCache()`)
- `oak-run-commons/.../DocumentStoreIndexerBase.java` — update if it references cache types
- Scan for any other modules with residual Caffeine/Guava cache imports and migrate them

### Exception handling migration
- Callers of `cache.get(key, callable)` must switch to `cache.get(key, k -> ...)`
- Callers of `loadingCache.get(key)` must stop catching checked `ExecutionException`; runtime failures propagate directly and checked loader failures surface as `CompletionException`

### Acceptance criteria
- No direct Caffeine or Guava cache usage in any module's `src/` (main or test) except `oak-core-spi`
- `DocumentNodeStoreHelper` tests pass

---

## TASK-15 — OSGi integration verification [oak-it-osgi] — [OAK-12161](https://issues.apache.org/jira/browse/OAK-12161)

**Depends on:** OAK-12149, OAK-12150, OAK-12151, OAK-12152, OAK-12153, OAK-12154, OAK-12155, OAK-12156, OAK-12157, OAK-12158, OAK-12159, OAK-12160
**Independent of:** none

### What changes
- Verify all OSGi bundle manifests: consumer bundles must not import `com.github.benmanes.caffeine.cache` or `org.apache.jackrabbit.guava.common.cache`
- Remove Caffeine from `Import-Package` of consumer bundles if still present
- Verify `oak-core-spi` is the only bundle importing Caffeine packages
- Run OSGi integration tests (`oak-it-osgi`)

**Known limitation (OAK-3598):** `oak-lucene` selectively inlines `AbstractCacheStats.class`, `CacheStats.class`, and (after OAK-12153) `CacheStatsAdapter.class` from `oak-core-spi`. The rest of `org.apache.jackrabbit.oak.cache.api` (`Cache`, `Weigher`, `CacheBuilder`) is not auto-added to `Import-Package` by bnd due to the split-package heuristic. This means `CacheStatsAdapter` methods that resolve `Cache`/`Weigher` at runtime will fail in OSGi if `maxWeight > 0`. The `IndexVersionSelectionIT` test is safe because `LuceneIndexEditorProvider` defaults to `maxWeight = 0`. The full fix requires OAK-3598 (remove selective inlining, add proper `Import-Package`).

### Acceptance criteria
- `mvn verify -pl oak-it-osgi -PintegrationTesting` passes
- No consumer bundle imports Caffeine packages

---

## TASK-16 — Final cleanup and deprecation [oak-core-spi] — [OAK-12162](https://issues.apache.org/jira/browse/OAK-12162)

**Depends on:** OAK-12161
**Independent of:** none

> **This task is a cleanup gate.** `CacheBuilder` already produces only Caffeine-backed
> caches. What remains is removing the transitional LIRS adapter code and the `CacheLIRS`
> class itself once every consumer has migrated away from `CacheLIRS.asOakCache()`.
> Execute this task only after OAK-12149 through OAK-12160 are all merged and no module
> outside `oak-core-spi` references `CacheLIRS` or its adapters.

### What changes
- `AbstractCacheStats.java` — change `getCurrentStats()` return type from Guava shim `CacheStats` to `CacheStatsSnapshot`; rewrite internal `lastSnapshot` field and `stats()` method to use `CacheStatsSnapshot` arithmetic
- `CacheStats.java` — update `getCurrentStats()` to return `CacheStatsSnapshot` (converting from its held Guava snapshot)
- `CacheStatsAdapter.java` — update `getCurrentStats()` to return `CacheStatsSnapshot` directly (drop Guava conversion shim)
- `GuavaCompatibleEmpiricalWeigher` — remove the temporary Guava-compatibility bridge introduced during the migration
- `RecordCacheStats.java`, `SegmentCache.Stats`, `SegmentCacheStats.java` — update `getCurrentStats()` to return `CacheStatsSnapshot` directly (drop Guava conversion shims added in OAK-12157/OAK-12158)
- Mark old `CacheStats` constructor (`Cache<?,?>` Guava shim) as `@Deprecated(forRemoval = true)`
- **Remove `LirsCacheAdapter` and `LirsLoadingCacheAdapter`** — no longer needed once no consumer calls `CacheLIRS.asOakCache()`
- **Remove `CacheLIRS`** (or mark `@Deprecated(forRemoval = true)` — since it was already `@Internal`, outright removal is acceptable)
- Verify no module outside `oak-core-spi` references `CacheLIRS` or its `asOakCache()` bridge
- Grep: confirm no module outside `oak-core-spi` imports `com.github.benmanes.caffeine.cache` or `org.apache.jackrabbit.guava.common.cache`
- Remove any Guava cache shim re-exports if they still exist

**Return-type cascade** — `AbstractCacheStats.getCurrentStats()` changes from Guava shim
`CacheStats` to `CacheStatsSnapshot`. Every override and every direct call site across all
modules must be updated in the same PR. Before starting, enumerate all callers:
```bash
grep -rn "getCurrentStats()" $(git rev-parse --show-toplevel)
```
Known overrides that must be updated (accumulated across OAK-12149 through OAK-12162):
- `oak-core-spi/.../CacheStats.java` — override
- `oak-core-spi/.../CacheStatsAdapter.java` — override
- `oak-segment-tar/.../RecordCacheStats.java` — override (shim added in OAK-12158)
- `oak-segment-tar/.../SegmentCache.Stats` — override (shim added in OAK-12157)
- `oak-segment-tar/.../SegmentCacheStats.java` — override (shim added in OAK-12158)
- Any other `AbstractCacheStats` subclass introduced in OAK-12149 through OAK-12160

All callers of `getCurrentStats()` that store the result as `CacheStats` must also be updated
to `CacheStatsSnapshot`.

### Acceptance criteria
- `mvn clean install -DskipTests` succeeds
- `mvn clean install` succeeds (full test suite)
- `AbstractCacheStats.getCurrentStats()` returns `CacheStatsSnapshot`; no Guava types in `AbstractCacheStats` or its subclasses
- No Caffeine or Guava cache types in any public API surface outside `oak-core-spi`
- No `CacheLIRS` or `LirsCacheAdapter` classes remain (or `CacheLIRS` is `@Deprecated(forRemoval = true)`)
- `CacheBuilder` has no `lirs` code path — Caffeine is the sole implementation
- OSGi integration tests pass
