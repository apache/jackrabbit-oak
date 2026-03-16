# Implementation Notes

## 2026-03-11: Phase Prioritization Decision

**Context:** While implementing Phase 2 Step 5 (Highlighting), realized the feature requires coordinated changes across 4 files and integration with Lucene's FastVectorHighlighter API. Test written and verified to fail correctly.

**Decision:** Pivot to Phase 2.5 (Multi-target write capability) first, as it's marked CRITICAL for migrations. Highlighting is a query enhancement feature that can be completed after migration infrastructure is in place.

**Rationale:**
- Multi-target write enables safe migrations (shadow indexing)
- Index flipping (Phase 3) depends on multi-target write
- Highlighting is valuable but not blocking for production migrations
- User emphasized autonomous continuation on critical path

**Status:**
- ✅ Phase 1: Write path complete
- ✅ Phase 2 Steps 1-4: Query support (text, property, sorting, faceting)
- ⏸️  Phase 2 Step 5: Highlighting (test written, implementation deferred)
- 🎯 Next: Phase 2.5 Multi-target write (spec complete, ready for implementation)

**Highlighting Test Location:**
- Test file: `LuceneNgHighlightingTest.java`
- Status: Compiles, fails at expected point (rep:excerpt returns null)
- Ready for implementation when prioritized

## Implementation Order (Revised):

1. ✅ Phase 1 + Phase 2 Steps 1-4 (Complete)
2. 🎯 Phase 2.5: Multi-target Write Capability (Next)
3. Phase 3: Index Flipping & Validation
4. Phase 2 Step 5: Highlighting (Return to this)
5. Phase 4: NRT Support
6. Phase 5: Feature Parity (Aggregates, Suggestions, etc.)
7. Phase 6: Production Hardening
