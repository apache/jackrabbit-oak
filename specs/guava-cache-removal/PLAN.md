# Oak Cache API Migration Plan

## Summary

Introduce an Oak-owned cache API in `oak-core-spi` that hides all implementation details
(CacheLIRS, Caffeine, or any future backend). Migrate every module from direct Guava/Caffeine
cache usage to the Oak cache API. Each batch is an independently-deliverable PR.

## Current local status

Batch 0 is implemented locally through the end of OAK-12148:
- `CacheBuilder` creates Caffeine-backed caches only; no `CacheImplementation` enum or global toggle
- `CacheLIRS` instances are exposed through the `Cache` API via `CacheLIRS.asOakCache()`
- separate manual/loading adapters for both backends (`LirsCacheAdapter`, `CaffeineCacheAdapter`)
- builder validation rejects inconsistent configurations up front
- only the remaining Guava-compatibility bridge (`EmpiricalWeigher`) and stats conversion shim
  (`CacheStatsAdapter`) are tagged for Final Cleanup

### Principles

1. **Oak owns the cache contract.** No module outside `oak-core-spi` references Caffeine or
   Guava cache types.
2. **Caffeine is the sole implementation created by `CacheBuilder`.** New caches use
   `CacheBuilder`, which always creates Caffeine-backed instances. `CacheLIRS` is not
   created by the builder; consumers that still need it build `CacheLIRS` instances directly
   and expose them through `CacheLIRS.asOakCache()`.
3. **Both `CacheLIRS` (via `asOakCache()`) and `CaffeineCacheAdapter` (via `CacheBuilder`) implement
   the `Cache` interface**, so call sites are uniform regardless of which backend is in use.
4. **Every module imports only** `org.apache.jackrabbit.oak.cache.api` types.
5. **Each batch is a self-contained PR** that can be merged independently.
6. **Test-first.** Every batch strengthens tests before migration, then re-runs them after.
   Before declaring a batch done, grep the **entire module** (`src/main/java` **and**
   `src/test/java`) for `org.apache.jackrabbit.guava.common.cache` — test code must be
   migrated in the same PR as the production code.
7. **Behavioral preservation where practical.** Same eviction timing, same toggles. The
   Oak-visible cache API follows Caffeine's retrieval contract, so downstream callers migrate
   toward the end-state API instead of a temporary Guava-shaped compatibility layer.
8. **Migration end-game.** Once all modules that should move to the Oak cache API are
   migrated and validated, final cleanup can remove temporary compatibility shims and
   decide whether `CacheLIRS` remains only for legacy/internal use or is removed entirely.
9. **Cross-module return type cascades.** Whenever a method's return type changes (e.g.
   `getCacheStats()` from `CacheStats` → `AbstractCacheStats`), every caller across
   **all modules** must be updated in the same PR — including modules not otherwise in
   scope for that batch. Use `grep -rn "methodName()"` across the entire repository
   before declaring a task done. A missed caller causes a cross-module compilation error
   in CI even if the originating module builds cleanly.

---

## Batch 0 — Foundation: Oak Cache API in `oak-core-spi`

### Goal

Define the Oak cache abstraction and two hidden implementations (LIRS and Caffeine) so that
subsequent batches can migrate modules one at a time.

### New interfaces — package `org.apache.jackrabbit.oak.cache.api`

All interfaces are `@ProviderType` (Oak owns the contract; consumers do not implement them).

#### `CacheLoader<K, V>`

A new loader interface added alongside `Cache`. It is key-aware (like Caffeine's
`CacheLoader`) and declares a checked exception:

```java
/**
 * Computes or loads a value for a missing cache entry.
 *
 * <p>Provides a key-aware loader for builder-created loading caches while preserving
 * Caffeine's checked-loader SPI and unchecked caller-facing contract.</p>
 */
@FunctionalInterface
public interface CacheLoader<K, V> {

    /**
     * Compute the value for {@code key}.
     *
     * @param key the key whose value should be loaded (never null)
     * @return the loaded value (never null)
     * @throws Exception if the value cannot be loaded
     */
    @NotNull
    V load(@NotNull K key) throws Exception;
}
```

#### `Cache<K, V>`

```java
/**
 * A size-bounded, thread-safe cache. Implementations may use different eviction
 * strategies (LIRS, W-TinyLFU/Caffeine, etc.) but callers see only this interface.
 *
 * <p>The {@code get(key, mappingFunction)} signature matches Caffeine's manual-cache
 * contract: callers supply a key-aware {@code Function} and failures are exposed as
 * unchecked exceptions.</p>
 */
@ProviderType
public interface Cache<K, V> {

    /** Return the cached value, or {@code null} if absent. */
    @Nullable
    V getIfPresent(@NotNull K key);

    /**
     * Return the cached value. If absent, invoke {@code mappingFunction}, cache the
     * result, and return it.
     *
     * <p>Matches Caffeine's manual-cache contract: the mapping function receives
     * the cache key and failures are exposed as unchecked exceptions.</p>
     *
     * @param key             the key whose associated value is to be returned
     * @param mappingFunction the function used to compute a value if absent
     * @return the current (existing or computed) value, or {@code null} if the
     *         mapping function returns {@code null}
     */
    @Nullable
    V get(@NotNull K key, @NotNull Function<? super K, ? extends V> mappingFunction);

    /** Unconditionally put a value into the cache. */
    void put(@NotNull K key, @NotNull V value);

    /** Remove the mapping for {@code key}, if present. */
    void invalidate(@NotNull K key);

    /** Remove all mappings from the cache. */
    void invalidateAll();

    /**
     * Remove all mappings whose keys are in {@code keys}.
     *
     * <p><em>Note: no Oak module currently calls this method. It can be removed
     * from the interface if deemed unnecessary.</em></p>
     */
    void invalidateAll(@NotNull Iterable<? extends K> keys);

    /**
     * Return the approximate number of entries in the cache.
     *
     * <p><em>Note: no Oak module currently calls this method directly
     * ({@code asMap().size()} is used instead). It can be removed from the
     * interface if deemed unnecessary.</em></p>
     */
    long estimatedSize();

    /** Return a snapshot of this cache's cumulative statistics. */
    @NotNull
    CacheStatsSnapshot stats();

    /** Return a concurrent, live view of the cache entries. */
    @NotNull
    ConcurrentMap<K, V> asMap();

    /**
     * Return all cached values for the given keys (cache hits only).
     *
     * <p><em>Note: no Oak module currently calls this method (CacheLIRS throws
     * {@code UnsupportedOperationException} for it). It can be removed from the
     * interface if deemed unnecessary.</em></p>
     */
    @NotNull
    Map<K, V> getAllPresent(@NotNull Iterable<? extends K> keys);

    /**
     * Clean up expired or otherwise reclaimable entries.
     *
     * <p><em>Note: no Oak module currently calls this method (CacheLIRS
     * implementation is a no-op). It can be removed from the interface if
     * deemed unnecessary.</em></p>
     */
    void cleanUp();
}
```

**`put(K, V, int memory)` (CacheLIRS-specific weighted put):** `DefaultSegmentWriter` in
`oak-segment-tar` calls `nodeCache.put(key, value, memoryCost)`. This is a CacheLIRS internal
that has no equivalent in the Caffeine `Cache` interface — Caffeine derives weight from the
configured `Weigher` at insertion time. **Migration for this call site:** replace with
`Cache.put(key, value)` and ensure an `Weigher` is configured on the builder. The
weigher receives the key and value at insertion time, making per-call weight unnecessary.

#### `LoadingCache<K, V>` extends `Cache<K, V>`

Used by `ElasticIndexStatistics` (loading cache with refresh semantics).

```java
/**
 * A cache that automatically loads absent entries from a pre-configured
 * {@link CacheLoader}.
 *
 * <p>{@code get(K)} follows Caffeine's loading-cache contract: runtime exceptions
 * propagate directly and checked loader failures are wrapped in
 * {@link CompletionException}. {@code refresh(K)} returns a future representing
 * the refresh work.</p>
 */
@ProviderType
public interface LoadingCache<K, V> extends Cache<K, V> {

    /**
     * Return the cached value, loading it via the pre-configured loader if absent.
     *
     */
    @NotNull
    V get(@NotNull K key);

    /**
     * Trigger a reload of the value for {@code key}. The stale value remains
     * available until the reload completes. The returned future matches
     * Caffeine's refresh contract; the LIRS adapter completes it after its
     * best-effort synchronous refresh path runs.
     */
    CompletableFuture<V> refresh(@NotNull K key);
}
```

#### `Weigher<K, V>`

```java
/**
 * Determines the weight of a cache entry. Replaces direct use of Caffeine's
 * {@code Weigher} and Guava's {@code Weigher} in consumer modules.
 */
@FunctionalInterface
public interface Weigher<K, V> {

    /**
     * Return the weight of the entry. The unit is typically bytes but is
     * cache-specific. Must be non-negative.
     */
    int weigh(@NotNull K key, @NotNull V value);
}
```

> **Decision — `EmpiricalWeigher`:** `EmpiricalWeigher` currently implements
> `org.apache.jackrabbit.guava.common.cache.Weigher<CacheValue, CacheValue>` (Guava shim).
> It will be changed to implement `Weigher<CacheValue, CacheValue>` instead. The adapter
> layer inside `oak-core-spi` wraps `Weigher` into the backend-specific weigher type
> (`Guava Weigher` for `LirsCacheAdapter`, `Caffeine Weigher` for `CaffeineCacheAdapter`).
> `EmpiricalWeigher` remains public because existing callers reference it. A temporary
> compatibility base keeps it assignable to the Guava shim `Weigher` until Final Cleanup.

#### `EvictionCause` (enum)

```java
/**
 * Reason an entry was removed from the cache. Mirrors the common subset of
 * Caffeine's and CacheLIRS's removal causes without exposing either.
 */
public enum EvictionCause {
    /** Manually invalidated. */
    EXPLICIT,
    /** Replaced by a new value for the same key. */
    REPLACED,
    /** Evicted due to size/weight constraint. */
    SIZE,
    /** Expired. */
    EXPIRED,
    /** Collected (weak/soft reference reclaimed). */
    COLLECTED
}
```

#### `EvictionListener<K, V>`

```java
/**
 * Callback invoked when an entry is removed from the cache.
 *
 * <p><b>Warning:</b> it is unsafe to call cache methods from within the listener.
 * Some implementations hold internal locks during the callback.</p>
 */
@FunctionalInterface
public interface EvictionListener<K, V> {

    void onEviction(@NotNull K key, @Nullable V value, @NotNull EvictionCause cause);
}
```

#### `CacheBuilder<K, V>`

`CacheBuilder` is the Oak API entry point for creating Caffeine-backed caches. It no
longer switches between backends. Call sites that still require loading `CacheLIRS`
keep using `CacheLIRS.newBuilder()` and expose the resulting cache through
`CacheLIRS.asOakCache()`.

```java
/**
 * Fluent builder for Caffeine-backed {@link Cache} instances.
 *
 * <p>Usage:
 * <pre>
 * Cache&lt;String, byte[]&gt; cache = CacheBuilder.&lt;String, byte[]&gt;newBuilder()
 *     .maximumWeight(64 * 1024 * 1024)
 *     .weigher((k, v) -&gt; v.length)
 *     .removalListener((k, v, cause) -&gt; LOG.info("evicted {}", k))
 *     .recordStats()
 *     .build();
 * </pre>
 */
public final class CacheBuilder<K, V> {

    // --- Builder state ---
    private String module;
    private long maximumWeight = -1;
    private long maximumSize = -1;
    private int initialCapacity = -1;
    private Weigher<K, V> weigher;
    private EvictionListener<K, V> removalListener;
    private boolean recordStats;
    private Duration expireAfterAccess;
    private Duration expireAfterWrite;
    private Duration refreshAfterWrite;     // Caffeine-only (loading caches)

    public static <K, V> CacheBuilder<K, V> newBuilder() { ... }

    public CacheBuilder<K, V> maximumWeight(long maximumWeight) { ... }
    public CacheBuilder<K, V> maximumSize(long maximumSize) { ... }
    public CacheBuilder<K, V> initialCapacity(int initialCapacity) { ... }
    public CacheBuilder<K, V> weigher(Weigher<K, V> weigher) { ... }
    public CacheBuilder<K, V> removalListener(EvictionListener<K, V> listener) { ... }
    public CacheBuilder<K, V> recordStats() { ... }
    public CacheBuilder<K, V> expireAfterAccess(Duration duration) { ... }
    public CacheBuilder<K, V> expireAfterWrite(Duration duration) { ... }
    public CacheBuilder<K, V> refreshAfterWrite(Duration duration) { ... }
    public CacheBuilder<K, V> ticker(Supplier<Long> ticker) { ... }
    public CacheBuilder<K, V> ticker(Clock clock) { ... }

    /**
     * Build a non-loading cache.
     */
    public Cache<K, V> build() { ... }

    /**
     * Build a loading cache with the given loader.
     * The loader is key-aware and may throw a checked exception (see {@link CacheLoader}).
     */
    public LoadingCache<K, V> build(CacheLoader<K, V> loader) { ... }
}
```

### Stats: `CacheStatsSnapshot` value object (Batch 0) vs. `AbstractCacheStats` decoupling (Final Cleanup)

On trunk, `AbstractCacheStats.getCurrentStats()` returns Guava shim `CacheStats`. There are
**four subclasses** across two modules that override it:

| Class | Module |
|-------|--------|
| `CacheStats` | `oak-core-spi` |
| `RecordCacheStats` | `oak-segment-tar` |
| `SegmentCache.Stats` (inner class) | `oak-segment-tar` |
| `SegmentCacheStats` | `oak-segment-tar` |

Changing `AbstractCacheStats.getCurrentStats()` in Batch 0 would break the three
`oak-segment-tar` subclasses immediately — they are not migrated until Batches 7/8.
Therefore the `AbstractCacheStats` return-type change is **deferred to Final Cleanup**,
when all subclasses have already been updated.

In Batch 0 we only:

1. **Add `CacheStatsSnapshot`** — an Oak-owned stats snapshot class (immutable value object) with
   the same fields as Guava's `CacheStats` (`hitCount`, `missCount`, `loadSuccessCount`,
   `loadFailureCount`, `totalLoadTime`, `evictionCount`). Includes `minus(CacheStatsSnapshot)`,
   `requestCount()`, `hitRate()`, `missRate()`, etc. `AbstractCacheStats` is **not changed**.

2. **Add `CacheStatsAdapter`** (package-private) — extends `AbstractCacheStats` and wraps
   an `Cache`. Its `getCurrentStats()` still returns Guava shim `CacheStats` (converted
   from the `Cache`'s `CacheStatsSnapshot`) so it compiles against the unchanged
   `AbstractCacheStats`. This is the stats handle returned by `CacheBuilder` and used by
   consumer modules after migration.

Consumer modules that currently call `new CacheStats(guavaCache, ...)` switch to
`CacheStatsAdapter` as part of their individual migration task. The existing `CacheStats`
class and `AbstractCacheStats` are left untouched until Final Cleanup.

**In each per-module migration task** (Batches 1–9), if the module has a custom
`AbstractCacheStats` subclass (e.g. `RecordCacheStats`), that subclass is updated to
obtain stats from the new `Cache` and convert them to Guava `CacheStats` inside
`getCurrentStats()`. The return type stays Guava until Final Cleanup.

**In Final Cleanup** (Batch 10), once every subclass has been updated:
- `AbstractCacheStats.getCurrentStats()` return type changes from Guava `CacheStats` to
  `CacheStatsSnapshot`; internal `lastSnapshot` field and `stats()` method rewritten accordingly
- `CacheStats.getCurrentStats()` updated to return `CacheStatsSnapshot` (converting from Guava)
- All other subclasses drop their Guava conversion shim — they already have `CacheStatsSnapshot`

### Hidden implementations — packages `org.apache.jackrabbit.oak.cache.impl.lirs` and `...caffeine` (package-private)

#### `LirsCacheAdapter<K, V>` + `LirsLoadingCacheAdapter<K, V>`

- `LirsCacheAdapter` wraps the existing `CacheLIRS<K, V>` for manual caches.
- `LirsLoadingCacheAdapter` wraps the loading `CacheLIRS<K, V>` path for `LoadingCache`.
- `CacheLIRS.asOakCache()` exposes loading `CacheLIRS` instances through the Oak
  `LoadingCache` API.
- Delegates all operations to `CacheLIRS`.
- Adapts `Weigher` to `org.apache.jackrabbit.guava.common.cache.Weigher` (the Guava shim
  interface that `CacheLIRS` currently uses).
- Adapts `EvictionListener` / `EvictionCause` to `CacheLIRS.EvictionCallback` /
  `org.apache.jackrabbit.guava.common.cache.RemovalCause`.
- **Exception exposure — `get(K, Function)`:** adapts the key-aware mapping function to
  `CacheLIRS.get(K, Callable)` and converts checked failures to `CompletionException`
  while propagating runtime failures directly.
- **Exception exposure — `LoadingCache.get(K)`:** delegates to `CacheLIRS.get(K)` and
  converts checked failures to `CompletionException` while propagating runtime failures directly.
- **Refresh exposure — `LoadingCache.refresh(K)`:** runs CacheLIRS refresh and returns a
  completed future representing the best-effort synchronous refresh result.
- `CacheLIRS` now exposes `asOakCache()` for instances created with a loader while
  remaining a concrete class implementing the Guava shim `LoadingCache` for internal use only.
  Its Guava dependency is fully hidden behind the Oak `LoadingCache` view returned by
  `asOakCache()`.

#### Migration pitfall — `e.getCause()` in catch blocks

Old Guava / CacheLIRS code that called `cache.get(key, callable)` wrapped **all** thrown
exceptions — including `RuntimeException` — inside `ExecutionException`. Callers therefore
wrote `catch (ExecutionException e)` and unwrapped the real cause with `e.getCause()`.

After migration, `cache.get(key, k -> ...)` (Oak Cache API) propagates `RuntimeException`
**directly** through both the Caffeine and LIRS paths (the latter via
`LirsCacheAdapter.toCaffeineException()`, which unwraps the `RuntimeException` from the
`ExecutionException` before re-throwing). The catch block changes to
`catch (RuntimeException e)`, where `e` IS the original exception — `e.getCause()` returns
`null`.

Any `e.getCause()` left inside a `catch (RuntimeException` block after migration silently
nullifies the cause of the wrapping exception. The symptom is a `DocumentStoreException`
(or similar wrapper) with a non-null message but a `null` cause, causing assertions like
`assertTrue(e.getCause() instanceof SomeException)` to fail.

**Fix:** replace `e.getCause()` with `e` everywhere inside the migrated catch block.
Also search for the `@NotNull Throwable` overloads of conversion helpers — passing `null`
(from `e.getCause()`) to a `@NotNull` parameter causes NPE at the `t.getMessage()` call
inside the helper.

#### Migration pitfall — OSGi impl package visibility

The `impl.lirs` and `impl.caffeine` sub-packages are **not exported** by `oak-core-spi`.
Any consumer module that directly references a class from these packages (e.g.
`LirsCacheAdapter.toOakCause()`) will fail OSGi bundle resolution: the consumer bundle stays
in state `INSTALLED` (state 2) and never reaches `ACTIVE` (state 32).

The symptom in `OSGiIT`:
```
AssertionError: Bundle org.apache.jackrabbit.oak-store-document not active. expected:<32> but was:<2>
ClassNotFoundException: org.apache.jackrabbit.oak.plugins.document.spi.lease.LeaseFailureHandler
```

The second error (class not found) is a downstream effect: because the bundle never activated,
none of its exported packages are available either.

**The `RemovalCause` → `EvictionCause` conversion** was initially placed in
`LirsCacheAdapter.toOakCause()` (impl package). When `DocumentNodeStoreBuilder` referenced it,
the bundle failed. Fix: move `toOakCause(RemovalCause)` to `CacheLIRS` (package
`org.apache.jackrabbit.oak.cache`, which IS exported). `LirsCacheAdapter.toOakCause()` now
delegates to `CacheLIRS.toOakCause()`, keeping backward compat inside `oak-core-spi`'s own code.
Consumer modules call `CacheLIRS.toOakCause(cause)` directly.

Adding this new public method to `CacheLIRS` also triggers the **OSGi baseline check**: the
`org.apache.jackrabbit.oak.cache` package version must be bumped in `package-info.java`.

**Rule for all subsequent batches:** before closing any task that wires up a LIRS eviction
callback in a consumer module, grep for `impl` references:
```bash
grep -rn "cache.impl" <module>/src/main/java
```
This must return zero results.

#### `CaffeineCacheAdapter<K, V>` + `CaffeineLoadingCacheAdapter<K, V>`

- `CaffeineCacheAdapter` wraps a `com.github.benmanes.caffeine.cache.Cache<K, V>` for manual caches.
- `CaffeineLoadingCacheAdapter` wraps a `LoadingCache<K, V>` for `LoadingCache`.
- Delegates all operations to Caffeine.
- **`get(K, Function)`** delegates directly to Caffeine's manual-cache API.
- **`LoadingCache.get(K)`** delegates directly to Caffeine's loading-cache API.
- **`LoadingCache.refresh(K)`** delegates directly to Caffeine and returns the refresh future.
- Adapts `Weigher` to `com.github.benmanes.caffeine.cache.Weigher`.
- Adapts `EvictionListener` / `EvictionCause` to Caffeine `RemovalListener` / `RemovalCause`.
- `build()` returns `CaffeineCacheAdapter`; `build(CacheLoader)` returns
  `CaffeineLoadingCacheAdapter`. Manual caches do not implement `LoadingCache`.

#### Builder validation rules

`CacheBuilder` validates configuration before constructing the Caffeine adapter.

- Exactly one of `maximumSize` or `maximumWeight` must be configured.
- `maximumWeight` requires `weigher(...)`.
- `weigher(...)` requires `maximumWeight(...)`.
- `refreshAfterWrite(...)` is valid only for `build(CacheLoader)`.

### OSGi changes

- `oak-core-spi` exports both `org.apache.jackrabbit.oak.cache` (existing) and the new
  `org.apache.jackrabbit.oak.cache.api` package. The public API types, including
  `CacheBuilder` and `CacheStatsAdapter`, live under
  `org.apache.jackrabbit.oak.cache.api`. The hidden adapter sub-packages
  (`org.apache.jackrabbit.oak.cache.impl.lirs`, `org.apache.jackrabbit.oak.cache.impl.caffeine`)
  are not re-exported.
- On trunk, `oak-core-spi` has no Caffeine dependency. Batch 0 adds Caffeine as a compile
  dependency of `oak-core-spi` (confined to the hidden `CaffeineCacheAdapter`). Consumer bundles
  do **not** need to import Caffeine packages once they migrate to the Oak cache API.

### Files changed

| File | Change |
|------|--------|
| `oak-core-spi/.../cache/api/Cache.java` | **New** (`get(K, Function)` — matches Caffeine's manual-cache contract) |
| `oak-core-spi/.../cache/api/LoadingCache.java` | **New** (`get(K)` unchecked, `refresh(K)` returns `CompletableFuture<V>`) |
| `oak-core-spi/.../cache/api/CacheLoader.java` | **New** (functional interface; `V load(K) throws Exception`) |
| `oak-core-spi/.../cache/api/Weigher.java` | **New** |
| `oak-core-spi/.../cache/api/EvictionCause.java` | **New** |
| `oak-core-spi/.../cache/api/EvictionListener.java` | **New** (`void onEviction(K, V, EvictionCause)`) |
| `oak-core-spi/.../cache/api/CacheStatsSnapshot.java` | **New** (immutable stats snapshot record) |
| `oak-core-spi/.../cache/api/CacheBuilder.java` | **New** (public final class; Caffeine-backed cache builder) |
| `oak-core-spi/.../cache/api/CacheStatsAdapter.java` | **New** (package-private; extends `AbstractCacheStats`; `getCurrentStats()` returns Guava shim `CacheStats` converted from `CacheStatsSnapshot` — Guava return type preserved until Final Cleanup) |
| `oak-core-spi/.../cache/impl/lirs/LirsCacheAdapter.java` | **New** (package-private) |
| `oak-core-spi/.../cache/impl/lirs/LirsLoadingCacheAdapter.java` | **New** (package-private) |
| `oak-core-spi/.../cache/impl/caffeine/CaffeineCacheAdapter.java` | **New** (package-private) |
| `oak-core-spi/.../cache/impl/caffeine/CaffeineLoadingCacheAdapter.java` | **New** (package-private) |
| `oak-core-spi/.../cache/EmpiricalWeigher.java` | Modify: implement `Weigher<CacheValue, CacheValue>` (Oak API) with temporary Guava-shim compatibility bridge |
| `oak-core-spi/.../cache/AbstractCacheStats.java` | **No change** (deferred to Final Cleanup) |
| `oak-core-spi/.../cache/CacheStats.java` | **No change** (existing Guava-wrapping JMX class; deferred to Final Cleanup) |
| `oak-core-spi/.../cache/CacheLIRS.java` | Modified: add `asOakCache()` (returns `LirsLoadingCacheAdapter`) and `toOakCause(RemovalCause)` (static; converts to `EvictionCause`; moved here from `LirsCacheAdapter` so the exported `cache` package is the call site for consumer modules) |

### Acceptance criteria

- `oak-core-spi` compiles with no new Caffeine or Guava types in the public API surface.
- All existing `oak-core-spi` tests pass (`CacheTest`, `CacheSizeTest`, `ConcurrentTest`,
  `ConcurrentPerformanceTest`).
- New focused tests cover the new Oak-facing adapters and builder:
  `CacheBuilderTest`, `CaffeineCacheAdapterTest`, `LirsCacheAdapterTest`,
  `LirsLoadingCacheAdapterTest`, and `CacheLIRSOakAdapterTest`.
- No existing consumer module is changed in this batch — all current `CacheLIRS.newBuilder()`
  and direct `Caffeine.newBuilder()` call sites still compile.

---

## Batch 1 — `oak-blob-cloud`

### Prerequisite
Batch 0 merged.

### Scope
Migrate `S3Backend` from direct Guava-shim cache (`CacheBuilder.newBuilder()`) to `CacheBuilder`.

On trunk, `S3Backend` uses:
- `org.apache.jackrabbit.guava.common.cache.Cache<DataIdentifier, URI>`
- `org.apache.jackrabbit.guava.common.cache.CacheBuilder`

### Files changed

| File | Change |
|------|--------|
| `oak-blob-cloud/.../s3/S3Backend.java` | Replace `CacheBuilder.newBuilder()...build()` with `CacheBuilder.newBuilder()...build()`. Replace `Cache<DataIdentifier, URI>` field with `Cache<DataIdentifier, URI>`. Remove Guava shim cache imports. |

### Acceptance criteria

- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-blob-cloud/src/` (main and test).
- `S3Backend` cache tests pass (strengthen before migration).
- Presigned URI caching semantics preserved (expiry, max-size).

---

## Batch 2 — `oak-blob-cloud-azure`

### Prerequisite
Batch 0 merged.

### Scope
Migrate `AzureBlobStoreBackend` and `AzureBlobStoreBackendV8` from direct Guava-shim cache
to `CacheBuilder`.

On trunk, both backends use:
- `org.apache.jackrabbit.guava.common.cache.Cache<String, URI>`
- `org.apache.jackrabbit.guava.common.cache.CacheBuilder`

### Files changed

| File | Change |
|------|--------|
| `oak-blob-cloud-azure/.../AzureBlobStoreBackend.java` | Replace `CacheBuilder.newBuilder()` with `CacheBuilder`. Replace `Cache<String, URI>` with `Cache<String, URI>`. Remove Guava shim cache imports. |
| `oak-blob-cloud-azure/.../v8/AzureBlobStoreBackendV8.java` | Same. |

### Acceptance criteria

- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-blob-cloud-azure/src/` (main and test).
- Azure backend cache tests pass (strengthen before migration).

---

## Batch 3 — `oak-blob`

### Prerequisite
Batch 0 merged.

### Scope
Migrate `BlobIdSet` from direct Guava-shim cache to `CacheBuilder`.

On trunk, `BlobIdSet` uses `org.apache.jackrabbit.guava.common.cache.Cache` and
`org.apache.jackrabbit.guava.common.cache.CacheBuilder`.

### Files changed

| File | Change |
|------|--------|
| `oak-blob/.../split/BlobIdSet.java` | Replace `CacheBuilder.newBuilder()` with `CacheBuilder`. Replace `Cache<String, Boolean>` with `Cache<String, Boolean>`. Remove Guava shim cache imports. |

### Acceptance criteria

- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-blob/src/` (main and test).
- `BlobIdSet` membership and bounded-cache tests pass.

---

## Batch 4 — `oak-search-elastic`

### Prerequisite
Batch 0 merged.

### Scope
Migrate `ElasticIndexStatistics` from Guava-shim `LoadingCache` to `LoadingCache` via
`CacheBuilder.build(loader)`.

On trunk, `ElasticIndexStatistics` uses:
- `org.apache.jackrabbit.guava.common.cache.CacheBuilder`
- `org.apache.jackrabbit.guava.common.cache.CacheLoader`
- `org.apache.jackrabbit.guava.common.cache.LoadingCache`

### Files changed

| File | Change |
|------|--------|
| `oak-search-elastic/.../ElasticIndexStatistics.java` | Replace `CacheBuilder.newBuilder()` with `CacheBuilder`. Replace `LoadingCache` fields with `LoadingCache`. Replace `CacheLoader` with `CacheLoader<K,V>` (key-aware, throws checked exception) passed to `CacheBuilder.build(loader)`. Remove Guava shim cache imports. |

### Acceptance criteria

- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-search-elastic/src/` (main and test).
- `ElasticIndexStatisticsTest` passes covering expiry, refresh, and failure behavior.

### Ticker support
`ElasticIndexStatistics` uses a controllable clock for deterministic time in tests.
`CacheBuilder` exposes two overloads:
- `ticker(Supplier<Long> ticker)` — raw nanosecond supplier, for custom time sources
- `ticker(Clock clock)` — convenience overload wrapping `java.time.Clock`; internally delegates to `ticker(() -> TimeUnit.MILLISECONDS.toNanos(clock.millis()))`

---

## Batch 5 — `oak-search`

### Prerequisite
Batch 0 merged.

### Scope
Migrate `ExtractedTextCache` from Guava-shim cache + Oak `CacheStats` (which wraps a Guava
`Cache`) to `CacheBuilder` + `CacheStatsAdapter`.

On trunk, `ExtractedTextCache` uses:
- `org.apache.jackrabbit.guava.common.cache.Cache`
- `org.apache.jackrabbit.guava.common.cache.CacheBuilder`
- `org.apache.jackrabbit.guava.common.cache.Weigher` (local `EmpiricalWeigher` inner class)
- `org.apache.jackrabbit.oak.cache.CacheStats` (passing a Guava `Cache` to its constructor)

### Files changed

| File | Change |
|------|--------|
| `oak-search/.../ExtractedTextCache.java` | Replace `CacheBuilder.newBuilder()` with `CacheBuilder`. Replace `Cache<String,String>` with `Cache<String,String>`. Replace the local `EmpiricalWeigher` inner class (implements Guava `Weigher`) with an `Weigher` lambda. Replace `new CacheStats(guavaCache, ...)` with `CacheStatsAdapter` obtained from the builder. Remove all Guava shim cache imports. |
| `oak-lucene/.../LuceneIndexProviderService.java` | `getCacheStats()` return type cascade: `CacheStats` → `AbstractCacheStats`. |
| `oak-run-commons/.../DocumentStoreIndexerBase.java` | `getCacheStats()` return type cascade: `CacheStats` → `AbstractCacheStats`. |
| `oak-lucene/pom.xml` | Add `org/apache/jackrabbit/oak/cache/api/CacheStatsAdapter.class` to the `Embed-Dependency` inline list for `oak-core-spi`. Without this, `ExtractedTextCache` (inlined in oak-lucene) assigns a `CacheStatsAdapter` instance — whose superclass `AbstractCacheStats` is resolved from `oak-core-spi` bundle classloader — to a field declared as `AbstractCacheStats` from oak-lucene's own inlined copy; the two classes are distinct at JVM verification time, causing a `VerifyError` in `IndexVersionSelectionIT`. Known limitation: `org.apache.jackrabbit.oak.cache.api` is still absent from `Import-Package` (bnd split-package heuristic); `maxWeight > 0` usage in OSGi requires OAK-3598. |

### Acceptance criteria

- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-search/src/` (main and test).
- `ExtractedTextCache` cache tests pass, including stats reporting.
- `DocumentStoreIndexerBase` compiles cleanly in `oak-run-commons`.

---

## Batch 6 — `oak-store-document`

### Prerequisite
Batch 0 merged.

### Scope
This is the largest and most complex migration. `oak-store-document` has 38 files referencing
cache types. The key changes:

1. **`DocumentNodeStoreBuilder.buildCache()`** — replace the `CacheLIRS.newBuilder()` call
   with a single `CacheBuilder` call for Caffeine-backed caches. On trunk there is no
   `oak.documentMK.lirsCache` toggle to remove (that was introduced only in the rejected
   PR #2807).

2. **All `Cache<K,V>` fields and parameters** across the module become `Cache<K,V>`.

3. **`EvictionListener`** — replace `org.apache.jackrabbit.guava.common.cache.RemovalCause`
   (Guava shim, used on trunk) with `EvictionCause`. The module's own `EvictionListener<K,V>`
   interface changes its signature from `evicted(K, V, RemovalCause)` to
   `evicted(K, V, EvictionCause)`.

4. **`ForwardingListener`** — update to use `EvictionListener`.

5. **`CacheStats` construction sites** — replace with stats obtained from the `Cache`
   (via `CacheStatsAdapter`).

6. **`Weigher` references** — replace Caffeine `Weigher` with `Weigher` where the weigher
   is configured.

7. **Persistent cache integration** (`PersistentCache`, `NodeCache`) — these wrap an
   underlying cache. They must be updated to accept `Cache` instead of `Cache`.

8. **`asMap()` usage** in `NodeDocumentCache.keys()` and `NodeDocumentCache.values()` —
   `Cache.asMap()` provides this.

### Files changed

| File | Change |
|------|--------|
| `DocumentNodeStoreBuilder.java` | Replace `CacheLIRS.newBuilder()` calls with `CacheBuilder`. |
| `DocumentNodeStore.java` | Change `Cache` types to `Cache`. |
| `NodeDocument.java` | Update cache type references. |
| `cache/NodeDocumentCache.java` | `Cache` → `Cache`. `asMap()` calls stay (method exists on `Cache`). |
| `cache/ForwardingListener.java` | `RemovalCause` → `EvictionCause`. |
| `persistentCache/EvictionListener.java` | `RemovalCause` → `EvictionCause`. |
| `persistentCache/NodeCache.java` | `Cache` → `Cache`. |
| `persistentCache/PersistentCache.java` | `Cache` → `Cache`. |
| `MemoryDiffCache.java` | `Cache` → `Cache`. |
| `LocalDiffCache.java` | `Cache` → `Cache`. |
| `TieredDiffCache.java` | `Cache` → `Cache`. |
| `CachingCommitValueResolver.java` | `Cache` → `Cache`. |
| `MongoDocumentStore.java` | Remove direct Caffeine/Guava cache references. |
| `RDBDocumentStore.java` | Remove direct Caffeine/Guava cache references. |
| `MemoryDocumentStore.java` | Update if it references cache types. |
| `JournalDiffLoader.java` | Update if it references cache types. |
| Various `util/` classes | Update `CacheValue` usage (no change needed if they only reference the interface). |
| `BranchTest.java` | `Cache` import: Guava shim → Oak API. |
| Various test classes | Update to use `Cache` types. |
| **`oak-run-commons/.../DocumentNodeStoreHelper.java`** | Cross-module cascade: `getNodeCache()` return type changed here, so this caller must be updated in the same PR. |
| **`oak-benchmarks/.../PersistentCacheTest.java`** | Second-level cascade: calls `getNodesCache()`, whose return type also changed. |

### OSGi wiring fix — eviction callback in `buildCache()` (discovered in Batch 6)

`DocumentNodeStoreBuilder.buildCache()` wires up a LIRS eviction callback that originally
called `LirsCacheAdapter.toOakCause(cause)`. `LirsCacheAdapter` is in the unexported
`impl.lirs` package, so `oak-store-document` failed OSGi bundle resolution (never reached
ACTIVE state). Symptoms in `OSGiIT`:

```
AssertionError: Bundle oak-store-document not active. expected:<32> but was:<2>
ClassNotFoundException: LeaseFailureHandler not found
```

**Fix:** call `CacheLIRS.toOakCause(cause)` instead (`org.apache.jackrabbit.oak.cache`,
exported). Adding `toOakCause()` to `CacheLIRS` required a minor version bump on
`org.apache.jackrabbit.oak.cache` in `package-info.java` (OSGi baseline check).

**Rule for future batches:** before closing, grep consumer modules for impl references:
```bash
grep -rn "cache.impl" <module>/src/main/java   # must return zero results
```

### Acceptance criteria

- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-store-document/src/` (main and test).
- No `org.apache.jackrabbit.oak.cache.impl` imports in `oak-store-document/src/main/java`.
- Persistent cache eviction behavior unchanged.
- Checked loader failures surface as `CompletionException` on the Oak-visible API.
- Synchronous eviction callback timing preserved where persistent cache depends on it.
- All existing tests pass: `NodeDocumentCacheTest`, `CacheChangesTrackerTest`,
  `AsyncCacheTest`, `DisableCacheTest`, `BranchTest`, `MemoryDiffCacheTest`, `LocalDiffCacheTest`,
  `persistentCache.CacheTest`, `persistentCache.NodeCacheTest`.
- `OSGiIT.bundleStates` passes (`oak-store-document` bundle reaches ACTIVE state).

---

## Batch 7 — `oak-segment-tar`

### Prerequisite
Batch 0 merged.

### Scope

`oak-segment-tar` has three distinct cache subsystems. On trunk all use the Guava shim
(`org.apache.jackrabbit.guava.common.cache.*`) or `CacheLIRS`:

1. **`SegmentCache`** — uses Guava-shim `CacheBuilder.newBuilder()` with a custom
   `RemovalListener` and manual stats tracking. Migrate to `CacheBuilder`.

2. **`RecordCache`** — uses Guava-shim `CacheBuilder.newBuilder()` with a `Weigher`.
   Migrate to `CacheBuilder`.

3. **`ReaderCache`** — uses `CacheLIRS.newBuilder()` directly. Expose as `Cache<K,V>`
   by calling `CacheLIRS.asOakCache()` on the existing instance; retain the `CacheLIRS`
   reference for stats (which still uses the CacheLIRS-specific `CacheStats` constructor
   until OAK-12162). The field type changes from `CacheLIRS<CacheKey, T>` to
   `org.apache.jackrabbit.oak.cache.api.Cache<CacheKey, T>`; all call sites switch to
   the Oak API methods (`getIfPresent`, `put`, `invalidateAll`).

4. **`PriorityCache`** — check trunk; update if it references Guava shim cache types directly.

5. **`WriterCacheManager`** — update if it references Guava shim cache builder types.

6. **`RecordCacheStats`**, **`SegmentCache.Stats`**, **`SegmentCacheStats`** — these are
   `AbstractCacheStats` subclasses. In this batch they are updated to obtain stats from the
   migrated `Cache`, converting `CacheStatsSnapshot` → Guava shim `CacheStats` inside
   `getCurrentStats()`. The Guava return type is kept until Final Cleanup changes the base
   class.

### Files changed

| File | Change |
|------|--------|
| `SegmentCache.java` | `CacheBuilder.newBuilder()` (Guava shim) → `CacheBuilder`. `Cache` → `Cache`. Guava `RemovalCause` → `EvictionCause`. |
| `RecordCache.java` | `CacheBuilder.newBuilder()` (Guava shim) → `CacheBuilder`. `Cache` → `Cache`. Guava `Weigher` → `Weigher`. |
| `ReaderCache.java` | Keep `CacheLIRS` instance; expose as `Cache<K,V>` via `CacheLIRS.asOakCache()`. Field type `CacheLIRS<CacheKey,T>` → `Cache<CacheKey,T>`. |
| `PriorityCache.java` | Update if it references Caffeine types directly. |
| `WriterCacheManager.java` | Update cache type references. |
| `RecordCacheStats.java` | Update to use `CacheStatsSnapshot`. |
| `CacheWeights.java` | `Weigher` → `Weigher`. |
| `spi/persistence/persistentcache/SegmentCacheStats.java` | Update. |
| `CachingSegmentReader.java` | Update cache type references. |
| `SegmentNodeStoreRegistrar.java` | Update if it references cache builder types. |

### Acceptance criteria

- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-segment-tar/src/` (main and test).
- `SegmentCacheTest`, `PriorityCacheTest`, `ConcurrentPriorityCacheTest` pass.
- Segment unload timing and memoization behavior unchanged.
- Eviction callback timing unchanged.

---

## Batch 8 — `oak-blob-plugins`

### Prerequisite
Batch 0 merged.

### Scope

`oak-blob-plugins` mixes `CacheLIRS.newBuilder()`, Guava-shim `CacheBuilder.newBuilder()`,
and Oak `CacheStats` (which wraps a Guava shim `Cache`). All paths migrate to `CacheBuilder`
+ `Cache`.

### Files changed

| File | Change |
|------|--------|
| `FileCache.java` | `CacheLIRS.newBuilder()` → `CacheBuilder`. `Cache` → `Cache`. `CacheLIRS.EvictionCallback` → `EvictionListener`. `CacheStatsSnapshot` → stats from `CacheBuilder`. |
| `UploadStagingCache.java` | Update cache types. |
| `CompositeDataStoreCache.java` | Update cache types. |
| `AbstractSharedCachingDataStore.java` | Update cache types. |
| `CachingBlobStore.java` | Update cache types. |
| `DataStoreCacheUpgradeUtils.java` | Update cache types. |

### Acceptance criteria

- No `org.apache.jackrabbit.guava.common.cache` imports in `oak-blob-plugins/src/` (main and test).
- No `CacheLIRS` references in `oak-blob-plugins/src/` (main and test).
- File cache eviction/deletion behavior unchanged.
- All blob-plugins cache tests pass.

---

## Batch 9 — `oak-run-commons` and remaining modules

### Prerequisite
Batch 0 merged.

### Scope

- `DocumentNodeStoreHelper` — **already migrated in Batch 6 (OAK-12156)** as a cross-module return-type cascade from `DocumentNodeStore.getNodeCache()`.
- `DocumentStoreIndexerBase` — update if it references cache types.
- Scan for any other modules with residual Caffeine/Guava cache imports and migrate them.

### Files changed

| File | Change |
|------|--------|
| `oak-run-commons/.../DocumentNodeStoreHelper.java` | Migrated in Batch 6 (OAK-12156) — cross-module cascade. |
| `oak-run-commons/.../DocumentStoreIndexerBase.java` | Update if needed. |

### Acceptance criteria

- No direct Caffeine or Guava cache usage in any module's `src/` (main or test) except
  `oak-core-spi` (where the adapters live).

---

## Batch 10 — Final Cleanup

### Prerequisite
All previous batches merged.

### Scope

1. **Decouple `AbstractCacheStats` from Guava shim.** All `AbstractCacheStats` subclasses
   have been updated by their module tasks and already produce `CacheStatsSnapshot` internally.
   Now make the base class match:
   - Change `AbstractCacheStats.getCurrentStats()` return type from Guava shim `CacheStats`
     to `CacheStatsSnapshot`
   - Rewrite internal `lastSnapshot` field and `stats()` method to use `CacheStatsSnapshot`
   - Update `CacheStats.getCurrentStats()` to return `CacheStatsSnapshot` (converting from its
     Guava snapshot)
   - Update `CacheStatsAdapter.getCurrentStats()` to return `CacheStatsSnapshot` directly
     (drop the Guava conversion shim it carried since Batch 0)
   - Remove the temporary Guava-compatibility bridge
     (`GuavaCompatibleEmpiricalWeigher`) if it is still present
   - Update `RecordCacheStats`, `SegmentCache.Stats`, `SegmentCacheStats` to return
     `CacheStatsSnapshot` directly (drop their Guava conversion shims)

2. **Verify isolation.** Grep the entire codebase: no module outside `oak-core-spi`
   imports `com.github.benmanes.caffeine.cache` or `org.apache.jackrabbit.guava.common.cache`.

3. **Remove CacheLIRS and the LIRS fallback.** With all modules on `Cache` API and
   Caffeine validated as the production backend:
   - Remove `LirsCacheAdapter` entirely
   - Remove `CacheLIRS` (or mark it `@Deprecated(forRemoval = true)` if a deprecation
     cycle is preferred — but since it was already `@Internal`, outright removal is acceptable)
   - Remove any remaining `CacheLIRS.asOakCache()` bridge usage from migrated modules

4. **Mark the old `CacheStats` class** (the one accepting a Guava shim `Cache<?,?>` in its
   constructor) as `@Deprecated(forRemoval = true)`. Consumers should use
   `CacheStatsAdapter` obtained from the builder.

5. **Remove Caffeine from OSGi `Import-Package`** of consumer bundles. Only `oak-core-spi`
   should import Caffeine packages.

6. **Remove any Guava cache shim re-exports** if they still exist.

### Acceptance criteria

- `mvn clean install -DskipTests` succeeds.
- `mvn clean install` succeeds (full test suite).
- No Caffeine or Guava cache types in any public API surface outside `oak-core-spi`.
- No `CacheLIRS` or `LirsCacheAdapter` classes remain (or `CacheLIRS` is `@Deprecated(forRemoval = true)`).
- `CacheBuilder` has no `lirs` code path — Caffeine is the sole implementation.
- OSGi integration tests pass (`oak-it-osgi`).

---

## Test Plan

Every batch follows this sequence:

1. **Before migration:** Add or strengthen unit tests covering the existing cache behavior
   in the target module. Run them against the unmodified code to establish a baseline.
2. **Migrate the implementation.**
3. **After migration:** Re-run the same tests. They must pass with identical behavior.
4. **OSGi check:** Inspect generated bundle manifests — consumer bundles must not import
   Caffeine packages after migration.

### Per-batch test focus

| Batch | Key tests |
|-------|-----------|
| 0 | `CacheTest`, `CacheSizeTest`, `ConcurrentTest`, `ConcurrentPerformanceTest`, **new** `CacheBuilderTest`, `CaffeineCacheAdapterTest`, `LirsCacheAdapterTest`, `LirsLoadingCacheAdapterTest`, `CacheLIRSOakAdapterTest` |
| 1 | `S3Backend` cache tests (presigned URI caching, expiry, hit/miss) |
| 2 | Azure backend cache tests |
| 3 | `BlobIdSet` membership and bounded cache tests |
| 4 | `ElasticIndexStatisticsTest` (expiry, refresh, loader failure) |
| 5 | `ExtractedTextCache` tests (weighing, expiry, stats reporting) |
| 6 | `NodeDocumentCacheTest`, `CacheChangesTrackerTest`, `AsyncCacheTest`, `DisableCacheTest`, `MemoryDiffCacheTest`, `LocalDiffCacheTest`, `persistentCache.CacheTest`, `persistentCache.NodeCacheTest` |
| 7 | `SegmentCacheTest`, `PriorityCacheTest`, `ConcurrentPriorityCacheTest`, `ReaderCacheTest` |
| 8 | `FileCache` tests, `UploadStagingCache` tests, `CompositeDataStoreCache` tests |
| 9 | `DocumentNodeStoreHelper` tests |
| 10 | Full build (`mvn clean install`), OSGi integration tests (`oak-it-osgi`) |

---

## Assumptions and Design Decisions

### Why `Cache` instead of Caffeine directly?

The previous approach (PR #2807 / OAK-11946) migrated modules from Guava types to Caffeine
types. This creates a hard dependency on Caffeine's API surface across the entire codebase —
exactly the same coupling problem we had with Guava. If Caffeine ever introduces breaking
changes, or if Oak needs to switch to a different cache library, every module would require
another mass migration.

`Cache` is a thin, stable interface owned by Oak. The Caffeine dependency is confined to
`oak-core-spi`'s hidden implementation package. Consumer modules depend only on Oak types.

### Trunk baseline

On trunk (`origin/trunk`), all cache usage goes through the `org.apache.jackrabbit.guava`
shim, which wraps the actual Guava library. Specifically:
- `CacheLIRS` implements `org.apache.jackrabbit.guava.common.cache.LoadingCache`
- `AbstractCacheStats.getCurrentStats()` returns `org.apache.jackrabbit.guava.common.cache.CacheStats`
- `CacheStats` wraps `org.apache.jackrabbit.guava.common.cache.Cache<?,?>`
- `EmpiricalWeigher` implements `org.apache.jackrabbit.guava.common.cache.Weigher<CacheValue, CacheValue>`

No Caffeine dependency exists in `oak-core-spi` or any consumer module on trunk. Batch 0
introduces Caffeine into `oak-core-spi` as a hidden implementation detail only.

### How CacheLIRS is preserved during migration

`CacheLIRS` is **not removed**. It remains the existing Guava-backed LIRS
implementation in `oak-core-spi`. For loading caches, `CacheLIRS.asOakCache()` wraps it
behind the Oak `LoadingCache` interface. Existing code that directly uses `CacheLIRS`
continues to compile until the consumer module is migrated in a later batch.

`CacheBuilder` is now Caffeine-only. LIRS call sites that still need Oak loading-cache API
exposure use `CacheLIRS.asOakCache()` directly instead of routing through `CacheBuilder`.

### OSGi / bundle implications

- `oak-core-spi` exports `org.apache.jackrabbit.oak.cache`. All new interfaces live in this
  package.
- `oak-core-spi` imports `com.github.benmanes.caffeine.cache` (private implementation detail).
- After migration, consumer bundles (e.g., `oak-store-document`) **stop** importing
  `com.github.benmanes.caffeine.cache` because they only reference `Cache` types.
- This is a net reduction in coupling and a simpler OSGi wiring graph.

### `Cache` API — Oak-owned contract with backend bridging

The `Cache` API follows Caffeine's retrieval contract. Backend differences are
bridged inside the hidden adapters so the transitional LIRS fallback remains
transparent to callers:

| Concern | CacheLIRS (trunk) | Caffeine | `Cache` decision |
|---------|-------------------|----------|---------------------|
| On-demand loader signature | `Callable<V>` (no key) | `Function<K,V>` (key-aware) | `Function<K,V>` — matches Caffeine's manual-cache contract |
| Loader exception | `ExecutionException` (checked) | `CompletionException` (unchecked) | Runtime failures propagate directly; checked failures surface as `CompletionException` |
| Loading cache loader | `CacheLoader.load(K)` (Guava shim) | `CacheLoader.load(K)` (Caffeine) | `CacheLoader<K,V>.load(K) throws Exception` — key-aware, checked |
| `LoadingCache.get(K)` exception | `ExecutionException` (checked) | `CompletionException` (unchecked) | Runtime failures propagate directly; checked failures surface as `CompletionException` |
| Async refresh | Not supported | `refreshAfterWrite` + `refresh(K)` | `LoadingCache.refresh(K)` returns `CompletableFuture<V>` + `CacheBuilder.refreshAfterWrite(Duration)` |
| Expiry | Not supported | `expireAfterAccess`, `expireAfterWrite` | Supported by `CacheBuilder`; `LirsCacheAdapter` ignores these (passed only when wrapping existing LIRS instances) |
| Eviction callback | `EvictionCallback` (key, value, cause) | `RemovalListener` (key, value, cause) | `EvictionListener.onEviction(key, value, EvictionCause)` |
| Stats snapshot | Guava `CacheStats` | Caffeine `CacheStats` | `CacheStatsSnapshot` (Oak-owned, same fields) |

`LirsCacheAdapter.get(K, Function)` adapts the key-aware mapping function to
`CacheLIRS.get(K, Callable)` and converts checked failures to `CompletionException`.
`CaffeineCacheAdapter.get(K, Function)` delegates directly to Caffeine.

### Why not put the API in a new `oak-cache-api` module?

Adding a new module increases the build graph complexity and requires every consumer to add a
new dependency. The `oak-core-spi` module already serves as the SPI home for Oak's internal
contracts, and it already exports the `org.apache.jackrabbit.oak.cache` package. Placing the
new interfaces there is the simplest path with zero new module wiring.

### `CacheValue` stays unchanged

`CacheValue` is a simple interface (`int getMemory()`) with no Guava or Caffeine dependency.
It stays as-is. Modules that use `CacheValue`-typed keys/values continue to work.

### Batches 1-5 and 6-8 are independently orderable

Batches 1-5 (simple modules) have no dependencies on each other — only on Batch 0. They can
be merged in any order. Batches 6-8 (complex modules) also depend only on Batch 0 and can be
done in parallel if staffing allows. The numbering reflects a suggested order from simplest to
most complex.
