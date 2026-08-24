# oak-search-lucene-ng

Lucene 9 index provider for Oak (`type="lucene9"`).

## Feature parity

| Feature | Legacy Lucene | Elastic | LuceneNg |
|---|---|---|---|
| Property restrictions, path/type filters | ✓ | ✓ | ✓ |
| Fulltext search | ✓ | ✓ | ✓ |
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
| Index copier (CopyOnRead/Write) | ✓ | ✗ | ✗ |
| Composite node store queries [^2] | ✓ | ✗ | ✗ |
| Inference / vector search | ✗ | ✓ | ✗ |

[^1]: Index augmentors are OSGi services (`IndexFieldProvider`, `FulltextQueryTermsProvider`) that let third-party code inject additional fields into indexed documents or expand fulltext queries, without modifying the index definition.
[^2]: When the repository is backed by a composite node store (e.g. a read-only `/apps`+`/libs` mount combined with a writeable store), the Lucene index runs one query per mount and merges the results. This feature is not required for a single-store deployment.

## Known limitations and deferred work

These items were identified during code review of the initial MVP. They are consciously deferred — not overlooked. Each is noted here so future contributors have the full picture without re-reading the review history.

### Performance

**No result batching (`searchAfter`).**
`query()` fetches `Math.max(1, maxDoc())` results in a single Lucene call. On large indexes with broad queries this allocates O(N) `ScoreDoc` entries on the heap. The legacy module uses a 50→100K batch doubling strategy via `searchAfter`. Implementing that here requires the cursor to hold the `IndexSearcher` reference across batch boundaries; the cursor already does this via its Cleaner-based lifecycle.

**Excerpts generated for all matched documents.**
`generateExcerpts()` passes the full `TopDocs` to `UnifiedHighlighter`, which loads stored fields and re-analyzes text for every matched document, not just the visible page. Combined with the batching gap above, a fulltext query matching 50 K docs blocks until all highlights are computed before the first result is returned.

### Index discovery

**`LuceneNgQueryIndexProvider.getQueryIndexes()` only discovers `lucene9` indexes one level under `/oak:index`.**
`LuceneNgIndexTracker` itself can resolve and serve a `lucene9` index at any nesting depth once given its exact path (`acquireIndexNode(path)` does a lazy, per-path lookup with no depth restriction). The remaining limitation is query-time *discovery*: `LuceneNgQueryIndexProvider.getQueryIndexes()` — the method that tells the Oak query engine which `lucene9` indexes exist so it can hand the tracker an exact path — only enumerates direct children of `/oak:index`. An index defined deeper (e.g. `/content/dam/oak:index/damAssets`) is still maintained correctly by the editor, but a real query against it will never be offered that index as a query plan candidate and silently falls back to traversal. For this version, `type=lucene9` index definitions must still be placed at `/oak:index/<name>` for queries to find them.

### Error handling

**`IllegalArgumentException` in query construction propagates uncaught.**
`createNumericQuery`, `createBooleanQuery`, and `createStringQuery` throw `IllegalArgumentException` for unsupported or inconsistent restriction combinations. The caller catches only `IOException`, so an unusual restriction pattern can propagate to the query engine and fail the entire query instead of falling back to another index or traversal.

### Concurrency

**`getFacetReaderState()` uses `get`/check/`putIfAbsent` instead of `computeIfAbsent`.**
Under high concurrency, N threads can simultaneously construct a `DefaultSortedSetDocValuesReaderState` (which reads all ordinals). Only one wins the race; the rest are discarded. Replace with `computeIfAbsent` to guarantee at-most-one construction.

### Observability

**No JMX / metrics instrumentation.**
Query errors return empty cursors with no counter incremented. Operations cannot distinguish an empty result set from a corrupted or unresponsive index without enabling `DEBUG` logging. The legacy module exposes query counts, error rates, and index sizes via JMX.

**`IndexPrinter` does not recognise `lucene9`.**
`oak-core`'s `IndexPrinter` identifies known index types for inventory output. It does not include `lucene9`, so lucene9 indexes appear with reduced diagnostic information in the Oak repository inventory.

### Storage and data consistency

**`BlobDeletionCallback` is hardcoded to NOOP.**
When index files are deleted from `OakDirectory`, the blob store is not notified. Unreferenced blobs accumulate until a full blob GC scan. The legacy module wires a real callback; this is a known incomplete feature (see TODO in `OakDirectory`).

**`OakDirectory.close()` is the sole point where the in-memory file listing is persisted.**
If a JVM crash occurs after files are created but before `close()` is called, the in-memory listing is lost. On next open, `getListing()` rebuilds it by scanning child node names — a documented recovery path, same as the legacy design.

**`IndexWriter.commit()` and Oak `NodeStore` commit are not atomic.**
A JVM crash between the two orphans blobs in the blob store. The blob GC will collect them eventually. This is the same accepted trade-off as `oak-lucene` (documented in OAK-7066 context).

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
