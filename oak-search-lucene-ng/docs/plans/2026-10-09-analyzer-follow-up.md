# LuceneNg analyzer follow-up

## Scope

Implement the decisions collected for all 21 findings in the October 8 report,
against starting revision `0bd1fca7694f767c5f96e2a34405a887ab5abf6f`.
All decisions were collected before implementation.

- Production changes remain inside `oak-search-lucene-ng`.
- Shared comparison-test changes are allowed; shared production code is not.
- Keep `type=luceneNg`; internal segment storage becomes `:luceneNg`.
- The code has not been deployed: no old-storage compatibility or migration.
- Preserve independent reader/writer caching and synchronous remote writes.
- No new production dependencies, tools, executor policy, or feature toggles.
  The user separately approved test-scoped `logback-classic`, following Oak
  Core's existing setup, for captured-log regressions.
- The user authorized committing and pushing the approved follow-up.
  Do not post reviews or resolve review threads in this task.

## Decisions

| Item | Decision |
| --- | --- |
| 1 | Fix property LIKE using the existing escaped SQL-wildcard conversion. |
| 2 | Defer configurable analyzers; document StandardAnalyzer-only support. |
| 3 | Reject malformed dates explicitly; never substitute epoch/open bounds. |
| 4 | Push down node types only when their type fields are property-indexed. |
| 5 | Keep path support always on and align the planner, regardless of the flag. |
| 6 | Leave shared competition code unchanged; require disabling older versions. |
| 7 | One backend query object; execute the path selected by its plan everywhere. |
| 8 | Load only stored PATH for result-row materialization; preserve excerpts. |
| 9 | Skip operational MBeans/IndexInfoProvider implementation. |
| 10 | Keep default activation; explicit type=luceneNg is the opt-in gate. |
| 11 | Implement configured null markers at indexing and query time. |
| 12 | Keep explicit root-only query support; exclude nested plans and document it. |
| 13 | Hide storage as :luceneNg and verify reindex/cache lifecycle behavior. |
| 14 | Enforce configured candidate-read limits, not new wall-clock timers. |
| 15 | Defer lazy facets; document eager aggregation over the full match set. |
| 16 | Cache merged sort metadata lazily per reader generation. |
| 17 | Skip executor changes, including idle-thread-timeout parity. |
| 18 | Add focused module-local facet edge-case tests. |
| 19 | Remove raw input from module error diagnostics, including exception leakage. |
| 20 | Add shared valid DATE-range/timezone and actual LIKE comparisons. |
| 21 | Add failed-reindex/publication regression; no new rollback mechanism. |

## Implementation and functional scenarios

### A. Query restrictions and planner agreement (1, 3, 4, 5, 11, 20)

Write regressions before fixing query construction.

- LIKE prefix, leading wildcard, underscore and escaped patterns must return
  exact expected paths in both backends. Use
  `QueryUtils.sqlLikeToLuceneWildcardQuery`, not naive replacements.
  Leave separate local-name LIKE behavior outside this change.
- Shared valid DATE queries exercise inclusive/exclusive bounds, two-sided and
  open-ended ranges, timezone-equivalent boundaries and real epoch values.
- Module-level malformed DATE restrictions exercise equality, range, IN and NOT
  without relying on a parser that may reject the literal before the backend.
  They must fail visibly with safe diagnostics, rather than construct a
  different query. Retain valid epoch and existing typed-query behavior.
- For mixed-type indexes, indexed primary/mixin type fields narrow Lucene
  candidates using the filter's subtype-aware type sets. If those fields are
  absent, leave type evaluation to Oak; do not query nonexistent fields.
- Planner support and actual path filtering agree with omitted, false and true
  `evaluatePathRestrictions`. Existing root, descendant, direct-child,
  exact and parent semantics remain correct.
- With `nullCheckEnabled`, write/query `NULL_PROPS` markers so null predicates
  do not scan all documents. Without that configuration, retain existing
  planner/post-filter behavior. Preserve declared NOT NULL support.

Use existing module helpers and shared planner decisions. Do not change shared
type-resolution, planner or query-engine production code.

### B. Backend/plan relationship and root-only support (7, 12)

- Return one LuceneNg backend object rather than one per root definition.
- Keep synchronous tracker refresh; no new observer or discovered-path cache.
- A selected plan supplies the path for execution, sorting, facets, explain
  output and diagnostics. Preserve existing lower-level hand-built-plan
  compatibility where a constructor path is explicitly supplied; an unbound
  backend must not guess a path when plan metadata is missing.
- Root-only support is intentional: nested definitions must not produce
  executable query plans, even if a root-level definition also exists.
- Tests use multiple definitions with different indexed properties/content,
  verifying distinct plans, selected-index results, sorting and facet results.
- Nested-only and mixed root/nested fixtures prove unsupported plans are
  excluded, rather than exposed by the provider change.

### C. Cursor reads and generation metadata (8, 14, 16)

- Use Lucene 9's selective stored-field API for PATH when building result rows.
  Keep per-batch highlighting separate and preserve paths, scores and excerpts.
- Pass actual query limits to the cursor. Count examined candidate rows,
  including rows later rejected by Oak, and fail explicitly above the read
  limit. Do not silently truncate, count only accepted results, or introduce
  unrelated wall-clock settings.
- Tests cover exactly the limit and the first candidate beyond it, including
  post-filter rejection and cleanup after failure. Keep batching/searchAfter
  and release the index node between batches.
- Cache merged FieldInfos lazily with the immutable searcher/reader generation,
  following the module's existing generation lifecycle. Do not retain readers
  in a global map. Test reuse, refresh and single-/multi-valued sorting.

### D. Hidden storage and failed publication (13, 21)

- Change the storage constant and repository-path literals to `:luceneNg`.
  Do not rename the backend type. Verify portable cache directory handling,
  missing-storage shutdown behavior and writer/read cache reuse.
- Existing standard reindex cleanup must remove the hidden storage; a reindex
  to zero documents must still refresh the query generation.
- Add a regression starting with a readable committed index, rebuilding in an
  uncommitted builder, and injecting a failure before publication. The previous
  committed index must remain readable even if the rebuild's Lucene writer
  already committed its own staged generation.
- Do not add rollback machinery: OakDirectory mutates the supplied builder and
  the NodeStore merge is the publication boundary.

### E. Focused facet tests and safe diagnostics (18, 19)

- Retain the substantial existing shared ACL/statistical facet integration
  suite; add only missing focused tests for absent stored PATH, valid empty/null
  matching-document sets, and deterministic scaling/rounding/zero labels.
- Do not rewrite the facet algorithms or loosen security assertions. Only
  address defects actually demonstrated by approved regressions.
- Remove raw fulltext/date input from module ERROR diagnostics, including
  faceting date errors. Verify captured error logs do not reproduce sensitive
  fixture values through direct messages or exception causes.
- Preserve visible failure diagnostics and avoid silently successful-looking
  error paths. Coordinate date failure work with section A.
- This is not repository-wide log redaction; shared production logging and
  existing DEBUG query diagnostics remain outside scope.

## Documentation

Update the module README for hidden storage, root-only query support, always-on
path support, configured null queries, read-limit enforcement, and:

- StandardAnalyzer-only support; custom analyzers are a deferred feature.
- Experimental migration requires explicitly disabling older index versions
  because shared cross-type supersession does not recognize luceneNg.
- Eager full-match facet aggregation remains a performance limitation.
- Default-enabled OSGi activation is unchanged; type=luceneNg is explicit opt-in.

Keep descriptions of touched cursor/planning behavior accurate, removing stale
claims that batching or generation-safe searcher release are absent. Do not
implement skipped monitoring or executor work while editing documentation.

## Validation

1. Run new targeted regressions against the unfixed code and retain meaningful
   failures, not fixture/setup failures.
2. Implement coherent module-local changes and rerun targeted coverage.
3. Run the shared comparison scenarios against both LuceneNg and legacy Lucene.
   Use reactor packaging where sibling test jars are required; never install
   SNAPSHOT siblings into the local Maven repository.
4. Run LuceneNg reactor clean verify, checking actual test counts and RAT.
   The known unrelated legacy minimal-test license issue remains out of scope.
5. Exercise the packaged bundle with a real Oak repository: LIKE/DATE/null
   predicates, multi-index plan selection, always-on path restrictions,
   traversal limits, hidden storage, incremental updates and cache lifecycle.
6. Verify the final changed-file set contains no shared production modifications
   and no skipped/deferred feature implementation.

## Packaging follow-up

The user approved Elastic-style transitive embedding after bundle-only checks
exposed missing Lucene helper classes. Enable `Embed-Transitive` and restrict the
existing Lucene embedding selector to compile/runtime dependencies. Keep
oak-search inline, but embed Lucene dependencies as nested jars, as Elastic does
for most dependencies. This preserves each jar's service registrations and
manifest. Keep existing library versions; add no production dependency.

Rebuild and verify the bundle contains the required transitive Lucene classes,
without embedding test libraries. Repeat bundle-only functional checks with no
external Lucene or standalone oak-search production jars. Verify Lucene loading
through an existing local Felix framework, since a plain Java classpath does not
interpret the bundle's nested-jar classpath.

Leave the Oak bundle's `Multi-Release` header unchanged. Lucene's original
multi-release manifest remains inside its nested core jar; Felix loads the
Java-specific implementation directly from that jar.

## Completion and verification

All approved implementation work is complete. Deferred/skipped features remain
deferred/skipped, and shared production code is unchanged.

- The genuine targeted baseline had 14 failures, with no fixture/setup errors.
- Final module reactor `clean verify`: 235 tests in 41 suites, zero
  failures/errors/skips; RAT reports 83 approved licenses and no unknown or
  unapproved licenses.
- Shared legacy comparison scenarios: 23 tests, zero failures/errors.
- Packaged-content functional checks: 59 tests, zero failures/skips. Lucene jars
  were extracted from the produced bundle and byte-verified against its entries;
  no external Lucene or standalone oak-search production jar supplied classes.
- Real Felix 7.0.5/SCR 2.1.28 loading on Java 25: LuceneNg, shared search, Lucene,
  SpanCollector and the versioned MMap provider use the bundle classloader.
  Real Oak LIKE/typed queries, incremental update/delete, writer cache reuse and
  shutdown cache preservation pass. The host supplies Oak/provided imports;
  this is not a full AEM deployment.
- All eight embedded Lucene jars use 9.12.2. Service registrations and Lucene
  core's multi-release manifest are intact; Logback/JUnit/Mockito are absent
  from the production bundle. No new library, version, runtime flag or Oak
  multi-release header was added.

The user authorized publication after verification. Review replies and thread
resolutions remain outside this task.
