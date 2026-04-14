# Oak Cache API

This is part of the broader Guava removal effort tracked in [OAK-10685](https://issues.apache.org/jira/browse/OAK-10685).

## Goals

- Introduce a small Oak-owned cache API in `oak-core-spi` that mirrors Caffeine's contract but contains no Caffeine-specific types in the public interfaces.
- Consumers import only from `org.apache.jackrabbit.oak.cache.api`; the Caffeine-backed implementation stays hidden behind package-private adapters.
- Swapping the underlying cache implementation must not require touching any consumer code.

## Design Decisions

- CacheLIRS is kept as-is for backward compatibility; it is not replaced.
- `CacheBuilder` creates Caffeine-backed caches only; callers that still need CacheLIRS build it directly and expose it via `CacheLIRS.asOakCache()`.

## Migration Constraints

- Migrate one module per PR so each change is small, reviewable, and mergeable independently.
- The `oak-core-spi` foundation must land first; after that all consumer modules can migrate in parallel.
- Each module PR must pass the cache compatibility tests introduced in [OAK-12145](https://issues.apache.org/jira/browse/OAK-12145) / [PR #2811](https://github.com/apache/jackrabbit-oak/pull/2811).
- Every PR must leave the codebase in a state that compiles and is realistically mergeable into Oak as-is; no PR may rely on a follow-up PR to restore buildability.
- If a module cannot switch from Guava to Oak Cache in one PR without forcing simultaneous updates across many other modules, introduce an adapter to bridge the old and new cache contracts and use it to stage the migration incrementally.
- When a method's return type changes as part of a migration, update all callers in all modules in the same PR; a missed caller compiles locally but breaks CI's full build.
- Before declaring a PR done, grep the entire module (`src/main/java` and `src/test/java`) for `org.apache.jackrabbit.guava.common.cache`; test code must be migrated in the same PR as production code.
- After migration, inspect the generated bundle manifests — consumer bundles must not import Caffeine packages.
- Preserve behavioral equivalence: same eviction timing, same toggles, same observable cache semantics.
- **`e.getCause()` in migrated catch blocks:** Guava wrapped loader exceptions in `ExecutionException`; the new API throws them directly. Any leftover `e.getCause()` silently returns `null`. Before closing a task, run `grep -rn "getCause()"` on touched files and replace `e.getCause()` with `e`.
