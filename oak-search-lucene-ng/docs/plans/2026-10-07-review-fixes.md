# LuceneNg review fixes

## Scope

Address the five October 5 comments on apache/jackrabbit-oak#2817 and use
`type=luceneNg`. NRT, full asynchronous CopyOnWrite, and unrelated parity gaps
remain out of scope. Use `luceneNg` for the storage child; this undeployed code
needs no compatibility or migration support.

The approved write-side approach is read-before-write prefetch plus a writable
read-through directory: existing immutable segments are read locally; new files
are written synchronously to OakDirectory. Reader prefetch configuration remains
independent of writer prefetch.

## Implementation sequence

1. Add shared query regressions for typed IN and IS NOT NULL, including declared
   types and string-literal conversion. Pass planner property definitions into
   query construction, reuse shared type resolution, and index/query configured
   not-null markers. Preserve unindexed-property post-filtering.
2. Add a deterministic concurrent-copy regression that blocks remote openInput
   before local output creation. Claim files across directory generations,
   publish local validity after sync, and release claims/accounting on every
   failure. Cover failed copy and failed sync.
3. Add lifecycle regressions for missing storage, shutdown, index removal,
   recoverable local-cache failure, and corrupt remote storage. Guard absent
   storage before opening directories; fall back only for local caching errors;
   let genuine index failures reach BadIndexTracker.
4. Add writable-directory tests for prefetched reads, newly written files,
   deletion, rename, reindex, and cleanup. Wire the shared copier through the
   editor context/writer factory. Use an immutable reader snapshot separate from
   the writer's mutable NodeBuilder; invalidate cached names on writes.
5. Rename the type constant and all type selectors, registrations, test
   definitions, diagnostics and documentation to luceneNg. Keep storage-path
   references consistently named `luceneNg`.

## Validation

- Run regressions before and after each relevant fix.
- Run the shared query scenarios against legacy Lucene and LuceneNg.
- Use Maven's reactor so sibling test jars come from this checkout, not cached
  snapshots. Do not install dependencies or change package/tool versions.
- Run module verification and exercise real Oak commits/queries with caching
  enabled, including incremental writes and reader refresh.
- Check that all requested regressions ran and that no legacy routing changed.
- Do not push, post review replies, or resolve GitHub threads without a separate
  request.

## Outcome

All five requested fixes and the `luceneNg` type are implemented locally.
Reader and writer caching have independent switches; full asynchronous
CopyOnWrite and NRT remain deferred. Both the type and storage child are named
`luceneNg`.

Query and cache/lifecycle regressions reproduced the defects before the fixes.
Clean reactor packaging passed all 211 LuceneNg tests and all 21 legacy shared
comparison tests, with no failures, errors, or skips. The full suite also caught
and verified a compatibility fix for hand-built query plans.

Final `clean verify -pl oak-search-lucene-ng -am` passed, including RAT.
The packaged bundle passed real Oak typed queries, incremental update/deletion,
writer cache reuse, and shutdown cache preservation. Its OSGi metadata includes
both caching switches.

Combined verification including `oak-lucene` remains blocked by its existing
`LuceneIndexMinimalTest.java` missing a license header; that unrelated file was
left untouched. The touched LuceneNg README now has the standard license header.
PR replies and thread resolutions remain pending.

The user subsequently confirmed that this code has never been deployed and
approved consistent storage/cache naming without migration support. Obsolete
old-type fixtures were removed, storage paths and test names updated, and literal
storage-name assertions added. Fresh LuceneNg reactor `clean verify` passed all
211 tests in 39 suites, with no failures, errors, or skips, and passed RAT.
