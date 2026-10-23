<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Caffeine cache plan

## Implementation and PR order

1. **OAK-12436 / #3185**: shared per-cache SYNC/ASYNC maintenance selection; preserve the existing ASYNC API default and caller behavior.
2. **OAK-12438 / #3186**: Segment Tar's independent bundle-local maintenance override. Remove obsolete shared toggle fields, update the internal cache API export and rebuild consumers together.
3. **OAK-12425 / #3176**: minimal DocumentNodeStore Caffeine migration, gated by `FT_CAFFEINE_CACHE_OAK-12425`, disabled by default. Standalone processes explicitly opt in with `-Doak.documentMK.caffeineCache=true`. Every Document Caffeine cache uses SYNC maintenance. Include LOCAL_DIFF in the Caffeine migration with the original memory budget, weights and persistence wrapper; preserve the zero-weight no-cache path. A builder samples implementation selection once; changes require a replacement builder/store. Preserve the existing NodeCache, CacheMetadata, raw values, weights, live map and persistent-write behavior. Register the eviction listener before the persistent wrapper attaches its callback. Include further production fixes only when a test demonstrates a Caffeine SYNC regression.
4. **OAK-12437 / #3190**: ASYNC-only metadata work and per-CacheType selection, based on #3176. `FT_OAK-12437`, disabled by default, gates ASYNC maintenance and the entry-owned metadata path together. Caffeine must also be enabled. Standalone ASYNC requires both startup opt-ins. Each builder snapshots feature selection once; existing caches retain their implementation and mode until restart. Unconfigured caches remain SYNC; LOCAL_DIFF honors the same per-cache requests and feature gates as the other eligible caches. Only persistent caches actually selected as ASYNC use CacheEntry/CacheEntries and the ASYNC NodeCache wrapper. FT off or SYNC selection uses the unchanged #3176 NodeCache/CacheMetadata path, with no entry overhead, mutation monitors, guarded write protocol or metadata map adapter from the ASYNC implementation. Retain the persistent file format and raw public values.
5. **OAK-12426 / #3174**: compare CacheLIRS, Caffeine SYNC with the original metadata path, and Caffeine ASYNC with entry-owned metadata through the production DocumentNodeStore cache. Verify effective mode and implementation; reject mismatched labels. Use the same node-cache memory budget, raw-value weights, workloads, persistent-write settings and JVM settings. ASYNC entry overhead consumes that budget and reduces its effective entry capacity; working-set ratios refer to the raw-value base capacity. Rerun results whenever these paths change.

Publish and review the native `gh stack` in order: **#3185 → #3186 → #3176 → #3190 → #3174**. Rebase each dependent PR onto its direct parent.

## Behavioral coverage

- #3176: disabled feature retains CacheLIRS; explicit opt-in selects Caffeine SYNC; all cache types retain the original metadata/weight representation. Existing builders retain implementation selection across feature changes. Service registration is removed on deactivation and preserves the selected opt-in across the restart required to apply it.
- #3176: size evictions reach existing persistent-cache callbacks inline. Existing persistence, broadcast, loader, raw map, custom weigher and cache statistics tests continue passing. Check cold node loading with CacheLIRS and Caffeine SYNC before adding any concurrency fix.
- #3176: LOCAL_DIFF follows implementation selection, preserves raw weights and its memory budget under eviction pressure. Verify diff correctness and loader fallback, zero-budget handling and builder selection stability.
- #3190: all combinations of Caffeine FT, ASYNC FT and per-cache selection, including both toggle-transition directions, replacement builders and unconfigured caches. Verify effective mode and selected persistent wrapper.
- #3190: FT off gives the #3176 wrapper, metadata behavior and exact custom weight even when ASYNC is requested. With Caffeine off, ASYNC requests resolve to SYNC and keep CacheLIRS.
- #3190: entry metadata lives with its entry; delayed callbacks refer to the exact removed entry, retired/replaced values and invalidated generations cannot persist stale data, raw map mutation preserves metadata, and weights include entry overhead only on the ASYNC path.
- #3190: ASYNC persistent wrapper covers NODE, CHILDREN, DIFF, LOCAL_DIFF and PREV_DOCUMENT; DOCUMENT is not persisted and PREV_NOPROP stays SYNC. Verify LOCAL_DIFF across both features, SYNC/ASYNC requests, persistence on/off and zero/nonzero budgets, including entry weights and callback threads. Journal-cache selection and persisted data survive reopen in both modes. Reuse behavior tests across both wrappers and isolate tests of ASYNC internals to the ASYNC wrapper.
- #3190: cross-cache loading and guarded persistent-write changes are confined to the ASYNC path unless a separate SYNC reproduction demonstrates necessity. Cancelled writes do not inflate successful-put or disk-usage statistics.
- #3174: fresh-JVM opt-in rejection/acceptance, workload accounting and three-mode production cache measurements. Add separate real DocumentNodeStore point reads, child traversal, commits/diff comparisons, concurrent readers/writers and persistent-cache reopen workloads. Validate returned data and report actual document reads/queries and disk hits; retain direct-cache microbenchmarks separately. State that MemoryDocumentStore results do not measure remote database latency or clustering. Published results describe the selected implementation, mode, metadata path and equal memory budgets.
- #3174: both Caffeine policies include LOCAL_DIFF in the named implementation and maintenance mode. Reject effective-mode mismatches and publish all-Caffeine comparisons; remove the obsolete hybrid configuration from the final result table.

## Jira scope

Use Caffeine throughout oak-store-document when its implementation feature is
enabled, including LOCAL_DIFF. Keep SYNC as the default maintenance mode and
require the independent ASYNC feature and per-cache requests for ASYNC.
Validate normal commits/readback without forced diffs as well as explicit diff
consumers. Use 256 MB as the primary repository budget and 384 MB for a 50%
increase, including LOCAL_DIFF in the named policy. Measure merge, explicit diff
and readback separately, with local-diff memory hits/misses and backend finds.
Use a bounded hierarchy with configurable folder width (100 data children per
group by default, 100 groups per bucket); node count means total data nodes.
Use the production one-second background interval so bulk-load journal paths
are flushed normally. Label 1,000-child folders and disabled-background runs as
explicit stress diagnostics. Separate direct NODE microbenchmark capacities
from the production-distributed total repository budget.
Warm each repository workload's code in a disposable store before measuring a
fresh store; initial dataset reads alone do not warm commit/diff code. Measure
for a minimum duration and report actual completed operations and elapsed time.
Use separate JVMs and rotate policy order. The focused sustained comparison uses
10 seconds of warm-up, 20 seconds of measurement and two repetitions for point
reads, normal commits/readback and commits with explicit diffs at 256 MiB.
Published policies include LOCAL_DIFF in Caffeine; the prior hybrid control is
only a diagnostic of admission sensitivity. Do not combine warmed
duration-based results with older fixed-count measurements in the result table.
Also run all-Caffeine 16/24 MB comparisons with normal background work,
plus 1,000-child wide-folder controls. Treat small budgets as memory-pressure diagnostics; do not equate that stress scenario
with typical production traffic. Preserve cache budgets and persistence semantics
while measuring the admission and metadata costs of the complete migration.

OAK-12425 contains the FT-gated SYNC migration and necessary SYNC compatibility fixes only. OAK-12437 contains the optional ASYNC mode and entry-owned metadata implementation. OAK-12438 remains Segment Tar only. OAK-12426 covers comparative measurements after the rescope. Both Document features are disabled by default and require a new builder/store after changes.
