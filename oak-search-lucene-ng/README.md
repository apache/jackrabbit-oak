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

# oak-search-lucene-ng

Lucene NG index provider for Oak (`type="luceneNg"`), using Lucene 9.

The index type is independent of the Lucene library version. Segment data is
stored in the hidden `:luceneNg` child under the index definition. The OSGi
service remains enabled by default; defining `type="luceneNg"` is the explicit
opt-in to this experimental backend.

The OSGi bundle inlines the shared `oak-search` code and embeds compile/runtime
Lucene dependencies, including transitives, as nested jars on `Bundle-ClassPath`.
Each Lucene jar retains its service-provider registrations and multi-release
manifest, allowing the framework to load the appropriate Java-specific classes.

## Local segment caching

`enableCopyOnReadSupport` enables reader caching and `enableReadBeforeWriteSupport`
enables writer read caching; both default to `true` and can be configured
independently. Both use `localIndexDir` (or `repository.home/index` by default).
`prefetchIndexFiles` controls reader prefetch only. Incremental writers prefetch
existing segments before opening, read unchanged files locally, and write new
files synchronously to Oak storage. Reindexing does not prefetch obsolete segments.
Local cache setup failures are logged and fall back to remote reads; failures
opening the actual index are reported through the shared bad-index tracker.

## Feature parity

| Feature | Legacy Lucene | Elastic | LuceneNg |
|---|---|---|---|
| Property restrictions, path/type filters | ✓ | ✓ | ✓ |
| Fulltext search | ✓ | ✓ | ✓ |
| Configurable analyzers | ✓ | ✓ | ✗ |
| Index-time aggregation | ✓ | ✓ | ✓ |
| Facets (insecure / statistical / secure) | ✓ | ✓ | ✓ |
| Excerpts | ✓ | ✓ | ✓ |
| Ordering / sorting | ✓ | ✓ | ✓ |
| Suggestions | ✓ | ✓ | ✗ |
| Spellcheck | ✓ | ✓ | ✗ |
| Similarity / More Like This | ✓ | ✓ (+ KNN) | ✗ |
| Native queries | ✓ | ✓ | ✗ |
| Index statistics / JMX | ✓ | ✓ | ✗ |
| Index augmentors [^1] | ✓ | ✗ | ✗ |
| NRT / hybrid indexing | ✓ | ✗ | ✗ |
| Index copier (CopyOnRead) | ✓ | ✗ | ✓ |
| Read-before-write / locally cached writer reads | ✓ | ✗ | ✓ |
| Index copier (CopyOnWrite) | ✓ | ✗ | ✗ |
| Composite node store queries [^2] | ✓ | ✗ | ✗ |
| Inference / vector search | ✗ | ✓ | ✗ |

[^1]: Index augmentors are OSGi services (`IndexFieldProvider`, `FulltextQueryTermsProvider`) that let third-party code inject additional fields into indexed documents or expand fulltext queries, without modifying the index definition.
[^2]: When the repository is backed by a composite node store (e.g. a read-only `/apps`+`/libs` mount combined with a writeable store), the Lucene index runs one query per mount and merges the results. This feature is not required for a single-store deployment.

## Known limitations and deferred work

These items were identified during code review of the initial MVP. They are consciously deferred — not overlooked. Each is noted here so future contributors have the full picture without re-reading the review history.

### Performance

**Batched results and excerpts.**
The cursor uses `searchAfter`, starting at 50 results and doubling up to 100K.
Each batch acquires and releases its searcher independently; result rows load
only stored PATH, while fulltext excerpts are generated separately for that
batch. Candidate rows count towards Oak's configured query read limit even
when Oak later rejects them during post-filtering. Exceeding the limit fails
the query rather than silently truncating it. String/boolean sort metadata is
loaded lazily and cached per immutable reader generation.

**Facet aggregation is eager.**
Only facet queries aggregate facets, but they process the complete match set
before returning rows so counts reflect the entire query. Lazy facet
evaluation remains deferred.

**`LuceneNgIndexTracker` does not override `isUpdateNeeded`.**
It relies on the inherited `FulltextIndexTracker` default, which only compares the `:status` and `:index-definition` hidden child nodes between commits — not a full-subtree diff of the index definition (which would also walk the Lucene segment storage on every commit and is expensive on large indexes). This is safe for two independent reasons, covering the two ways content changes reach the index:
- **Incremental (non-reindex) updates.** `LuceneNgIndexEditor` (via the shared `FulltextIndexEditorContext.closeWriter()`) writes `:status/lastUpdated` whenever `LuceneNgFulltextIndexWriter.close()` reports that a write actually happened (its `indexUpdated` flag, set by `updateDocument`/`deleteDocumentTree`/`deleteDocument`).
- **Reindex — including the edge case of a reindex that ends up matching zero documents.** This is the case that actually matters and is easy to get wrong: `LuceneNgFulltextIndexWriter`'s `indexUpdated` flag is *not* a reliable signal here, because a reindex that matches no documents (a misconfigured rule, or all matching content already gone) still opens the `IndexWriter` with `OpenMode.CREATE` and calls `indexWriter.commit()` in `close()` without ever calling `updateDocument`/`deleteDocumentTree`/`deleteDocument` — so `indexUpdated` stays `false` even though the reindex wipes any previously-existing segments. (Legacy `oak-lucene`'s `DefaultIndexWriter.close()` has an explicit generation-number fallback for exactly this gap; `LuceneNgFulltextIndexWriter` does not.) The actual safety net for reindex is upstream of this module entirely: `oak-core`'s `IndexUpdate.removeIndexState()` unconditionally strips all hidden child nodes — including `:status` and `:index-definition` — from the index definition before every reindex, regardless of what this module's writer does. That guarantees a real diff (e.g. `:status` losing `lastUpdated`/`indexedNodes`, or disappearing entirely) that the inherited default's `isStatusChanged`/`isIndexDefinitionChanged` checks pick up, even for a reindex-to-empty.

If a future LuceneNg-specific reindex path were ever added that bypasses `oak-core`'s standard `IndexUpdate` reindex machinery (e.g. a bespoke out-of-band reindex tool), it would need its own way of touching `:status`/`:index-definition` — relying on `LuceneNgFulltextIndexWriter`'s `indexUpdated` dirty-tracking alone would silently reintroduce a stale-index-node bug for the reindex-to-zero-documents case.

### Index discovery

**Query support is root-only.**
Define query indexes at `/oak:index/<name>`. One backend uses the inherited
planner to discover definitions and executes the index selected by each plan
for results, sorting and facets. Nested definitions (for example,
`/content/dam/oak:index/damAssets`) are excluded from query plans because
subtree-relative query and result paths are not supported yet. The editor and
standalone tracker can still maintain/open nested definitions; that does not
make them query-supported.

**Disable older versions explicitly during experimental migration.**
Shared cross-type supersession does not recognize `luceneNg` as a replacement
for legacy Lucene. Do not rely on automatic replacement filtering: explicitly
disable older index versions when switching queries to this backend.

### Query configuration

**StandardAnalyzer only.**
Indexing, fulltext queries and excerpts use StandardAnalyzer. Custom analyzer
configuration is not honored; configurable analyzers remain a missing feature.

**Path filtering is always supported.**
PATH and parent fields are indexed regardless of `evaluatePathRestrictions`.
The planner therefore advertises path support when that flag is omitted,
false or true.

**Configured null checks use markers.**
`nullCheckEnabled` writes/query-matches `:nullProps` markers. Without that
configuration the backend does not claim indexed null checks. Existing
`notNullCheckEnabled` markers and typed existence queries remain supported.

### Error handling

**`IllegalArgumentException` in query construction propagates uncaught.**
Unsupported or inconsistent property restrictions fail the query rather than
returning success-shaped fallback results. Malformed DATE bounds/sets are
rejected explicitly, never replaced by epoch or open bounds. Fulltext
tokenization failures also propagate instead of returning partial tokens.
DATE/fulltext error diagnostics omit input values and conversion/analysis
exception causes that may contain those values.

### Concurrency

**`getFacetReaderState()` uses `get`/check/`putIfAbsent` instead of `computeIfAbsent`.**
Under high concurrency, N threads can simultaneously construct a `DefaultSortedSetDocValuesReaderState` (which reads all ordinals). Only one wins the race; the rest are discarded. Replace with `computeIfAbsent` to guarantee at-most-one construction.

### Observability

**No JMX / metrics instrumentation.**
Errors are logged and index-open failures use the shared bad-index tracker,
but there are no module-specific query/error counters or index-size MBeans.
Some existing I/O error paths return empty results after logging. The legacy
module exposes richer query counts, error rates, and index sizes via JMX.

**`IndexPrinter` does not recognise `luceneNg`.**
`oak-core`'s `IndexPrinter` identifies known index types for inventory output. It does not include `luceneNg`, so luceneNg indexes appear with reduced diagnostic information in the Oak repository inventory.

### Storage and data consistency

**`BlobDeletionCallback` is hardcoded to NOOP.**
When index files are deleted from `OakDirectory`, the blob store is not notified. Unreferenced blobs accumulate until a full blob GC scan. The legacy module wires a real callback; this is a known incomplete feature (see TODO in `OakDirectory`).

**`IndexWriter.commit()` and Oak `NodeStore` commit are not atomic.**
A JVM crash between the two orphans blobs in the blob store. The blob GC will collect them eventually. This is the same accepted trade-off as `oak-lucene` (documented in OAK-7066 context).
Lucene's commit writes to the staged NodeBuilder, not the published NodeStore
root. A rejected Oak merge leaves the previously published index readable;
no separate Lucene rollback mechanism is required.

**Asynchronous CopyOnWrite is not ported.** Existing segments are prefetched and
writer reads reuse the local cache, but new segment files are written directly to
the remote-backed `OakDirectory`. Local buffering and background uploads remain
deferred.

### Minor

**Per-field excerpts (`rep:excerpt(propertyName)`) are not supported.**
Only the unqualified `rep:excerpt` output column is served, generated from the shared
`FULLTEXT` field. A query requesting an excerpt scoped to a specific property gets no
excerpt for that column rather than an error. The legacy module supports field-scoped
excerpts directly from the index.

**`OakDirectory.fileLength()` opens a full `OakIndexInput` on every call** to read blob metadata. Lucene calls this frequently during segment selection. Lengths should be cached on the file node to avoid repeated blob reads.

**`buildQuery()` is called twice per query** — once in `getPlanDescription()` and once in `query()`. The cost is low in absolute terms but avoidable.

**`OakBufferedIndexFile` computes wrong read length if `PROP_UNIQUE_KEY` is externally deleted.** Under normal operation this property is written atomically with file creation and is never absent. Same design as legacy (see OAK-7066).

**Statistical facet sampling seed is logged at `DEBUG` and is deterministic** (inherited from legacy). Requires `DEBUG` log access, statistical facet mode, and precise document placement control to exploit.

**Binary content is not extracted for fulltext indexing.** `LuceneNgDocumentMaker.addBinary` is a
documented no-op: `jcr:content/jcr:data` binaries (PDFs, office documents, etc.) contribute nothing
to fulltext search, unlike the legacy module's Tika-based text extraction. This has always been true
of this module — the hand-rolled editor never indexed binaries either — but adopting the shared
`FulltextDocumentMaker` framework makes the gap reachable for the first time: index-time aggregation
now pulls a matched child node's *string* properties into the parent's `:fulltext`, yet any binary
property on that aggregated node is still skipped. Binary/Tika text extraction is deferred work.

**Per-property fulltext boost (`PropertyDefinition.boost`) is not applied to node-scope fulltext relevance.**
The legacy module expands a boosted property's value into the shared `:fulltext` field with an
index-time boost so node-scope `CONTAINS(*, ...)`/`CONTAINS(., ...)` queries rank documents higher
when the match is in a boosted property. Lucene 9 removed per-field index-time boosts, and this
module does not replicate the effect via an alternative (e.g. query-time boosting per field). Both
node-scope and property-scoped (`CONTAINS(propertyName, ...)`) fulltext matching are functionally
correct here; only this relevance-tuning refinement is absent.
