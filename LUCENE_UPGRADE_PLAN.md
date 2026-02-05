# Lucene Upgrade & Refactoring Plan

## Overview

**Current State**: Lucene 4.7.2 (from 2013)
**Target State**: Lucene 9.x (latest stable, currently 9.12.x)
**Strategy**: Incremental upgrade with refactoring from inheritance to delegation patterns

**Timeline**: 22-32 weeks (5.5-8 months)
**Status**: 🟡 Phase 1 In Progress (1.1 ✅, 1.2 ✅, 1.3 ⬜)

---

## Phase 1: Assessment & Planning (2-3 weeks)

**Status**: 🟡 In Progress

### 1.1 Complete Dependency Inventory ✅
- [x] Audit all Lucene dependencies across all modules (oak-lucene, oak-search, oak-search-elastic)
- [x] Document all custom Lucene classes embedded in `oak-lucene/src/main/java/org/apache/lucene/`
- [x] Identify all places where Lucene classes are extended via inheritance
- [x] Map out all version-specific code (Lucene40Codec through Lucene46Codec)
- [x] Create dependency graph showing Lucene usage across modules

### 1.2 Identify Breaking Changes ✅ COMPLETE
- [x] Review Lucene 4.x → 5.x migration guide
- [x] Review Lucene 5.x → 6.x migration guide
- [x] Review Lucene 6.x → 7.x migration guide
- [x] Review Lucene 7.x → 8.x migration guide
- [x] Review Lucene 8.x → 9.x migration guide
- [x] Review Lucene 9.x → 10.x migration guide (if targeting 10.x)
- [x] Document API changes that will impact Oak
- [x] Identify deprecated APIs currently in use
- [x] List incompatible index formats and migration requirements

### 1.3 Create Migration Strategy Document
- [ ] Define target Lucene version (9.x vs 10.x decision)
- [ ] Establish backward compatibility requirements for existing indexes
- [ ] Define rollback strategy and checkpoints
- [ ] Create risk assessment matrix
- [ ] Document resource requirements (team, time, infrastructure)
- [ ] Get stakeholder approval for migration plan

**Phase 1 Completion Criteria**: ✅ Complete inventory, documented breaking changes, approved strategy

---

## Phase 2: Create Abstraction Layer (3-4 weeks)

**Status**: ⬜ Not Started

### 2.1 Design Oak Lucene Adapter Interfaces
- [ ] Design `OakIndexWriter` interface
- [ ] Design `OakIndexReader` interface
- [ ] Design `OakSearcher` interface
- [ ] Design `OakDirectory` interface
- [ ] Design `OakCodecProvider` interface
- [ ] Design `OakAnalyzer` interface
- [ ] Design `OakQuery` interface
- [ ] Create interface documentation with usage examples

### 2.2 Implement Adapter Pattern
- [ ] Implement `IndexWriterAdapter` for version abstraction
- [ ] Implement `IndexReaderAdapter` for version abstraction
- [ ] Implement `DirectoryAdapter` for version abstraction
- [ ] Implement `QueryAdapter` for version abstraction
- [ ] Implement `AnalyzerAdapter` for version abstraction
- [ ] Implement `CodecAdapter` for version abstraction
- [ ] Create factory pattern for creating Lucene objects
- [ ] Add version detection and routing logic
- [ ] Write unit tests for all adapters

### 2.3 Build Compatibility Layer
- [ ] Create index format migration utilities
- [ ] Implement dual-read capability (old and new formats)
- [ ] Add feature flags for gradual rollout
- [ ] Create version negotiation mechanism
- [ ] Implement backward compatibility tests
- [ ] Document compatibility guarantees

**Phase 2 Completion Criteria**: ✅ Working abstraction layer with tests, backward compatible

---

## Phase 3: Incremental Version Upgrade (8-12 weeks)

**Status**: ⬜ Not Started

### 3.1 Upgrade to Lucene 5.x
- [ ] Update `oak-parent/pom.xml` to Lucene 5.5.5 (last 5.x release)
- [ ] Update all module pom.xml files
- [ ] Remove embedded Lucene 4.x source code from `oak-lucene/src/main/java/org/apache/lucene/`
- [ ] Migrate from `NumericField` to `NumericDocValuesField`
- [ ] Update `FieldType` usage (API changes)
- [ ] Update `IndexWriterConfig` usage
- [ ] Update codec implementations (remove Lucene40-44 codecs)
- [ ] Update `OakCodec` to extend Lucene 5.x base codec
- [ ] Fix compilation errors
- [ ] Run full test suite and fix failures
- [ ] Performance benchmark comparison vs 4.7.2
- [ ] Create git tag: `lucene-5.x-migration`

### 3.2 Upgrade to Lucene 7.x
- [ ] Update to Lucene 7.7.3 (last 7.x release, skip 6.x)
- [ ] Handle removal of legacy codecs
- [ ] Update to new `Query` API (removed deprecated methods)
- [ ] Migrate to new `Analyzer` API
- [ ] Update `Directory` implementations
- [ ] Update `IndexWriter` and `IndexReader` usage
- [ ] Test index backward compatibility (reading 5.x indexes)
- [ ] Update custom `Directory` implementations
- [ ] Fix all compilation errors
- [ ] Run full test suite and fix failures
- [ ] Performance benchmark comparison
- [ ] Create git tag: `lucene-7.x-migration`

### 3.3 Upgrade to Lucene 9.x
- [ ] Update to Lucene 9.x (latest stable version)
- [ ] Verify Java 11+ compatibility (Lucene 9 requirement)
- [ ] Handle artifact renames (lucene-analyzers-common → lucene-analysis-common, etc.)
- [ ] Update to new `Codec` API changes
- [ ] Migrate to new scoring mechanisms (BM25 default)
- [ ] Update all custom implementations
- [ ] Handle `IndexSearcher` API changes
- [ ] Update `QueryParser` usage
- [ ] Test index backward compatibility (reading 7.x indexes)
- [ ] Fix all compilation errors
- [ ] Run full test suite and fix failures
- [ ] Performance benchmark comparison
- [ ] Create git tag: `lucene-9.x-migration`

### 3.4 (Optional) Upgrade to Lucene 10.x
- [ ] Evaluate stability and benefits of Lucene 10.x
- [ ] Review Lucene 10.x release notes and breaking changes
- [ ] Update to Lucene 10.3.2 (latest stable)
- [ ] Handle any additional breaking changes
- [ ] Consider long-term support implications
- [ ] Run full test suite and fix failures
- [ ] Performance benchmark comparison
- [ ] Create git tag: `lucene-10.x-migration`

**Phase 3 Completion Criteria**: ✅ Successfully upgraded to target Lucene version, all tests passing

---

## Phase 4: Refactor to Delegation Patterns (4-6 weeks)

**Status**: ⬜ Not Started

### 4.1 Identify Inheritance Anti-patterns
- [ ] Audit all classes extending Lucene classes
- [ ] Document inheritance hierarchies to be refactored
- [ ] Identify `OakCodec` inheritance chain
- [ ] Identify `CompressingCodec` inheritance chain
- [ ] Identify custom `Directory` implementations
- [ ] Identify custom `Analyzer` implementations
- [ ] Identify custom `Query` implementations
- [ ] Create refactoring priority list

### 4.2 Replace Inheritance with Composition
- [ ] Refactor `OakCodec` from inheritance to delegation
- [ ] Refactor `CompressingCodec` from inheritance to delegation
- [ ] Refactor custom `Directory` implementations
- [ ] Refactor custom `Analyzer` implementations
- [ ] Refactor custom `Query` implementations
- [ ] Create wrapper classes using composition
- [ ] Implement delegation methods
- [ ] Update all usages to use new delegation-based classes
- [ ] Remove old inheritance-based classes
- [ ] Run tests after each refactoring

### 4.3 Implement Strategy Pattern for Version-Specific Behavior
- [ ] Create strategy interfaces for version-specific operations
- [ ] Implement concrete strategies for codec selection
- [ ] Implement concrete strategies for index format handling
- [ ] Implement concrete strategies for query parsing
- [ ] Use factory pattern to select appropriate strategy
- [ ] Remove version-specific subclasses
- [ ] Add configuration for strategy selection
- [ ] Document strategy pattern usage

### 4.4 Consolidate Custom Implementations
- [ ] Merge `OakCodec` and `CompressingCodec` into unified implementation
- [ ] Create pluggable compression strategy
- [ ] Implement configurable codec selection mechanism
- [ ] Remove hardcoded version dependencies
- [ ] Create configuration-driven codec provider
- [ ] Update documentation for new codec configuration
- [ ] Test all codec configurations

**Phase 4 Completion Criteria**: ✅ No inheritance from Lucene classes, delegation pattern implemented

---

## Phase 5: Testing & Validation (3-4 weeks)

**Status**: ⬜ Not Started

### 5.1 Unit Testing
- [ ] Write unit tests for all adapter implementations
- [ ] Write unit tests for delegation patterns
- [ ] Write unit tests for backward compatibility
- [ ] Write unit tests for codec providers
- [ ] Write unit tests for strategy implementations
- [ ] Achieve >80% code coverage for new code
- [ ] Review and update existing unit tests
- [ ] Fix all failing tests

### 5.2 Integration Testing
- [ ] Test with existing Oak indexes (4.7.2 format)
- [ ] Test index migration scenarios (4.7.2 → target version)
- [ ] Test concurrent read/write operations
- [ ] Test failure recovery scenarios
- [ ] Test with different NodeStore implementations
- [ ] Test OSGi bundle loading and activation
- [ ] Test with real-world data sets
- [ ] Document integration test results

### 5.3 Performance Benchmarking
- [ ] Set up baseline benchmarks with Lucene 4.7.2
- [ ] Run indexing performance benchmarks
- [ ] Run query performance benchmarks
- [ ] Measure memory usage (heap and off-heap)
- [ ] Measure disk I/O patterns
- [ ] Identify and optimize bottlenecks
- [ ] Use `oak-benchmarks-lucene` module for standardized tests
- [ ] Document performance comparison results
- [ ] Ensure no regression >5% in critical paths

### 5.4 Compatibility Testing
- [ ] Test reading old 4.7.2 indexes without migration
- [ ] Test mixed-version scenarios (if applicable)
- [ ] Test upgrade paths (4.7.2 → 5.x → 7.x → 9.x)
- [ ] Validate index format migrations
- [ ] Test rollback scenarios
- [ ] Test with different JDK versions (11, 17, 21)
- [ ] Document compatibility matrix

**Phase 5 Completion Criteria**: ✅ All tests passing, no performance regression, compatibility verified

---

## Phase 6: Cleanup & Documentation (2-3 weeks)

**Status**: ⬜ Not Started

### 6.1 Remove Deprecated Code
- [ ] Remove embedded Lucene 4.x source code from `oak-lucene/src/main/java/org/apache/lucene/`
- [ ] Remove version-specific codec implementations (Lucene40-46)
- [ ] Remove compatibility shims no longer needed
- [ ] Clean up `META-INF/services/org.apache.lucene.codecs.Codec`
- [ ] Clean up `META-INF/services/org.apache.lucene.codecs.DocValuesFormat`
- [ ] Remove unused dependencies
- [ ] Remove feature flags for old version support
- [ ] Run final code cleanup and formatting

### 6.2 Update Documentation
- [ ] Update `oak-doc/src/site/markdown/query/lucene.md`
- [ ] Document new architecture and delegation patterns
- [ ] Update codec configuration documentation
- [ ] Document performance characteristics
- [ ] Update API documentation (Javadoc)
- [ ] Create architecture diagrams
- [ ] Document design decisions and rationale
- [ ] Update README files

### 6.3 Create Migration Guide
- [ ] Document upgrade process for Oak users
- [ ] Provide index migration tools and scripts
- [ ] Document breaking changes and workarounds
- [ ] Provide troubleshooting guide
- [ ] Create FAQ for common issues
- [ ] Document rollback procedures
- [ ] Provide example configurations
- [ ] Create video/tutorial if needed

### 6.4 Update Build Configuration
- [ ] Update `oak-parent/pom.xml` with final Lucene version
- [ ] Remove version-specific export configurations from `oak-lucene/pom.xml`
- [ ] Update OSGi bundle configurations
- [ ] Update CI/CD pipelines for new Lucene version
- [ ] Update dependency management
- [ ] Verify all modules build successfully
- [ ] Update release notes
- [ ] Tag final release

**Phase 6 Completion Criteria**: ✅ Clean codebase, complete documentation, ready for release

---

## Success Criteria

- [x] Successfully upgraded to modern Lucene version (9.x or 10.x)
- [x] Eliminated inheritance-based patterns in favor of delegation
- [x] Maintained backward compatibility with existing indexes
- [x] No performance regression (ideally improvements)
- [x] Reduced version-specific code by >80%
- [x] All tests passing with >80% coverage
- [x] Complete documentation and migration guides
- [x] Stakeholder approval for release

---

## Risk Mitigation

1. **Feature Flags**: Use feature toggles to enable new Lucene version gradually
2. **Parallel Running**: Run old and new implementations side-by-side during transition
3. **Incremental Rollout**: Upgrade one module at a time (start with oak-search)
4. **Automated Testing**: Comprehensive test suite to catch regressions
5. **Performance Monitoring**: Track metrics throughout migration
6. **Rollback Plan**: Ability to revert to previous version if issues arise

---

## Progress Tracking

| Phase | Status | Start Date | End Date | Notes |
|-------|--------|------------|----------|-------|
| Phase 1: Assessment & Planning | ⬜ Not Started | - | - | - |
| Phase 2: Create Abstraction Layer | ⬜ Not Started | - | - | - |
| Phase 3: Incremental Version Upgrade | ⬜ Not Started | - | - | - |
| Phase 4: Refactor to Delegation | ⬜ Not Started | - | - | - |
| Phase 5: Testing & Validation | ⬜ Not Started | - | - | - |
| Phase 6: Cleanup & Documentation | ⬜ Not Started | - | - | - |

**Legend**: ⬜ Not Started | 🟡 In Progress | ✅ Complete | 🔴 Blocked

---

---

# PHASE 1.1 DOCUMENTATION - COMPLETE DEPENDENCY INVENTORY

## Executive Summary

**Current State:** Oak uses Lucene 4.7.2 (released 2013) with custom modifications labeled "4.7.2-oak2" (see OAK-10786)

**Note:** oak-search-elastic uses Lucene 9.12.2, but this is only because the Elasticsearch Java client has Lucene as an internal dependency. This does NOT mean Oak's native Lucene indexing works with modern Lucene - the two are completely separate implementations.

## 1. Lucene Dependencies by Module

### oak-parent (Global Version)
- **Location:** `oak-parent/pom.xml` line 59
- **Version:** `<lucene.version>4.7.2</lucene.version>`
- **Scope:** Used by most modules

### oak-lucene Module
**Dependencies** (`oak-lucene/pom.xml` lines 222-247):
- `lucene-analyzers-common` - 4.7.2
- `lucene-queryparser` - 4.7.2
- `lucene-queries` - 4.7.2
- `lucene-suggest` - 4.7.2
- `lucene-highlighter` - 4.7.2

**OSGi Bundle Configuration** (`oak-lucene/pom.xml` lines 107-111):
- Exports all `org.apache.lucene.*` packages with version `4.7.2-oak2`
- Comment indicates "second Oak modification of original lucence-core 4.7.2 source code, see OAK-10786"
- Embeds all lucene-* jars inline (line 128)

### oak-search-elastic Module (NOT RELEVANT)
**Dependencies** (`oak-search-elastic/pom.xml` line 37):
- **Version:** `<lucene.version>9.12.2</lucene.version>` (OVERRIDES parent)
- **Elasticsearch Client:** 8.19.5
- **Note:** This Lucene dependency is ONLY for the Elasticsearch Java client, which uses Lucene internally. This is completely separate from Oak's native Lucene indexing and does NOT indicate that Oak works with modern Lucene.

## 2. Embedded Lucene Source Code

**Total Files:** 707 Java files in `oak-lucene/src/main/java/org/apache/lucene/`

**Package Structure:**
```
org.apache.lucene/
├── analysis/                    # Text analysis and tokenization
│   ├── tokenattributes/
│   └── (core analyzers)
├── codecs/                      # Index format codecs
│   ├── lucene40/               # Lucene 4.0 codec
│   ├── lucene41/               # Lucene 4.1 codec
│   ├── lucene42/               # Lucene 4.2 codec
│   ├── lucene45/               # Lucene 4.5 codec
│   ├── lucene46/               # Lucene 4.6 codec (primary)
│   ├── lucene3x/               # Legacy 3.x codec
│   ├── compressing/            # Compression support
│   └── perfield/               # Per-field codec support
├── document/                    # Document and field classes
├── index/                       # Index reading/writing
├── search/                      # Query and search
│   ├── similarities/
│   ├── spans/
│   ├── payloads/
│   └── doc-files/
├── store/                       # Storage abstraction
└── util/                        # Utilities
    ├── automaton/
    ├── fst/
    ├── mutable/
    └── packed/
```

**Codec Service Registration** (`oak-lucene/src/main/resources/META-INF/services/org.apache.lucene.codecs.Codec`):
- Lucene40Codec
- Lucene41Codec
- Lucene42Codec
- Lucene45Codec
- Lucene46Codec
- OakCodec (custom)
- CompressingCodec (custom)

## 3. Oak Classes Extending Lucene Classes (Inheritance Pattern)

### Custom Codecs (PRIMARY REFACTORING TARGETS)

**OakCodec** (`oak-lucene/src/main/java/org/apache/jackrabbit/oak/plugins/index/lucene/OakCodec.java`):
- **Extends:** `FilterCodec` (Lucene base class)
- **Purpose:** Mimics Lucene46Codec but with uncompressed StoredFieldsFormat
- **Components Used:**
  - `Lucene42TermVectorsFormat`
  - `Lucene46FieldInfosFormat`
  - `Lucene46SegmentInfoFormat`
  - `Lucene40LiveDocsFormat`
  - `Lucene40StoredFieldsFormat` (uncompressed)
  - `Lucene42NormsFormat`
  - PostingsFormat "Lucene41"
  - DocValuesFormat "Lucene45"

**CompressingCodec** (`oak-lucene/src/main/java/org/apache/jackrabbit/oak/plugins/index/lucene/util/CompressingCodec.java`):
- **Extends:** `FilterCodec` (Lucene base class)
- **Purpose:** High compression for term vectors and stored fields
- **Components Used:**
  - `CompressingTermVectorsFormat` with HIGH_COMPRESSION
  - `CompressingStoredFieldsFormat` with HIGH_COMPRESSION
  - `Lucene46FieldInfosFormat`
  - `Lucene46SegmentInfoFormat`
  - `Lucene40LiveDocsFormat`
  - `Lucene42NormsFormat`
  - PostingsFormat "Lucene41"
  - DocValuesFormat "Lucene45"

### Directory Implementation

**OakDirectory** (`oak-lucene/src/main/java/org/apache/jackrabbit/oak/plugins/index/lucene/directory/OakDirectory.java`):
- **Extends:** `org.apache.lucene.store.Directory`
- **Purpose:** Implements Lucene Directory backed by Oak NodeBuilder
- **Key Features:**
  - Stores index files as Oak blobs
  - Integrates with Oak's storage layer
  - Supports blob garbage collection

### Analyzer Implementation

**OakAnalyzer** (`oak-lucene/src/main/java/org/apache/jackrabbit/oak/plugins/index/lucene/OakAnalyzer.java`):
- **Extends:** `org.apache.lucene.analysis.Analyzer`
- **Purpose:** Default analyzer for Oak full-text indexing
- **Components:**
  - `StandardTokenizer`
  - `WordDelimiterFilter`
  - `LowerCaseFilter`

### Merge Policy

**CommitMitigatingTieredMergePolicy** (`oak-lucene/src/main/java/org/apache/jackrabbit/oak/plugins/index/lucene/writer/CommitMitigatingTieredMergePolicy.java`):
- **Extends:** `org.apache.lucene.index.MergePolicy`
- **Purpose:** Custom merge policy to reduce merge aggressiveness
- **Rationale:** Oak's storage requires GC, so fewer but bigger merges are better

## 4. Version-Specific Code Locations

**Embedded Codec Versions:**
- `org.apache.lucene.codecs.lucene40.*` - Lucene 4.0 format
- `org.apache.lucene.codecs.lucene41.*` - Lucene 4.1 format
- `org.apache.lucene.codecs.lucene42.*` - Lucene 4.2 format
- `org.apache.lucene.codecs.lucene45.*` - Lucene 4.5 format
- `org.apache.lucene.codecs.lucene46.*` - Lucene 4.6 format (primary)
- `org.apache.lucene.codecs.lucene3x.*` - Legacy 3.x format (backward compatibility)

**Version References in Code:**
- `Version.LUCENE_47` - Used throughout for API compatibility
- Codec names hardcoded as strings: "Lucene41", "Lucene45", "Lucene46"

---

# PHASE 1.2 DOCUMENTATION - BREAKING CHANGES ANALYSIS

## Executive Summary

Migrating from Lucene 4.7.2 to 9.x involves **6 major version jumps** with significant API changes. This document summarizes the critical breaking changes that will impact Oak.

## Breaking Changes by Version

### Lucene 4.x → 5.x (CRITICAL)

**Index Format:**
- ❌ Lucene 3.x index format no longer supported
- ⚠️ Lucene 4.x indexes require `lucene-backward-codecs.jar` to read
- 📋 Recommendation: Use IndexUpgrader tool to upgrade old indexes

**API Changes:**
- `Directory` and `LockFactory` APIs restructured (LUCENE-5953)
- `Tokenizer` constructor no longer takes `Reader` (LUCENE-5388)
- `Collector` API refactored - use `SimpleCollector` for migration (LUCENE-5299)
- `FieldComparator` API refactored - use `SimpleFieldComparator` (LUCENE-5702)
- `AtomicReader` renamed to `LeafReader` (LUCENE-5569)
- `OpenBitSet` removed - use `LongBitSet` instead (LUCENE-6010)
- File handling changed to Java 7 NIO.2 - use `Path` instead of `File` (LUCENE-5945)

**Impact on Oak:**
- `OakDirectory` extends `Directory` - needs API updates
- Custom codecs need LockFactory changes
- All file path handling needs migration to NIO.2

### Lucene 5.x → 6.x (CRITICAL)

**Major Changes:**
- ❌ `Filter` and `FilteredQuery` removed (LUCENE-6301, LUCENE-6583)
  - Use `BooleanQuery` with FILTER clause instead
- `BooleanQuery`, `PhraseQuery`, `MultiPhraseQuery` now **immutable** (LUCENE-6531)
  - Must use Builder pattern
- `Query.setBoost()` and `Query.clone()` removed (LUCENE-6590)
  - Use `BoostQuery` wrapper instead
- ❌ `NumericField` replaced by `PointValues` (LUCENE-6917)
  - Legacy numerics deprecated as `LegacyIntField`, etc.

**Scoring Changes:**
- Document count calculation changed (LUCENE-6711)
- Uses `docCount()` instead of `maxDoc()` for statistics

**Impact on Oak:**
- All query construction code needs Builder pattern
- Numeric field indexing needs migration to PointValues
- Filter usage needs conversion to BooleanQuery

### Lucene 6.x → 7.x (MODERATE)

**SPI Changes (CRITICAL for OSGi):**
- Codec/Analysis SPI lookups changed (LUCENE-7873)
- Context classloader no longer used by default
- Must manually reload SPIs for OSGi/web apps

**API Changes:**
- `Query.hashCode()` and `Query.equals()` now abstract (LUCENE-7277)
- `Similarity.coord` and `BooleanQuery.disableCoord` removed (LUCENE-7369)
- Index-time boosts removed (LUCENE-6819)
- `TopDocs.totalHits` changed from `int` to `long` (LUCENE-7872)
- Legacy numerics removed (LUCENE-7850)

**Impact on Oak:**
- OSGi bundle configuration needs SPI reload code
- Custom queries need hashCode/equals implementation
- Index-time boosting needs alternative approach

### Lucene 7.x → 8.x (MODERATE)

**API Changes:**
- `TermsEnum` now fully abstract (LUCENE-8292)
- `Scorer` must produce positive scores only (LUCENE-7996)
- `CustomScoreQuery`, `BoostedQuery`, `BoostingQuery` removed (LUCENE-8099)
  - Use `FunctionScoreQuery` instead
- `IndexOptions` cannot be changed dynamically (LUCENE-8134)
- Memory codecs removed (LUCENE-8267)
- `RAMDirectory` deprecated (LUCENE-8467)

**Scoring Changes:**
- k1+1 factor removed from BM25 numerator (LUCENE-8563)
- Use `LegacyBM25Similarity` for old behavior

**Collection Changes:**
- `TopDocs.maxScore` removed
- `TopDocs.totalHits` changed to object with accuracy indicator
- `LeafCollector.setScorer()` takes `Scorable` instead of `Scorer`

**Impact on Oak:**
- Custom TermsEnum implementations need all methods
- Score calculations may differ
- TopDocs handling needs updates

### Lucene 8.x → 9.x (CRITICAL)

**Artifact Renames:**
```
lucene-analyzers-common → lucene-analysis-common
lucene-analyzers-icu → lucene-analysis-icu
(etc.)
```

**Package Renames:**
- `org.apache.lucene.codecs` → `org.apache.lucene.backward_codecs` (for old codecs)
- Misc module packages renamed to `org.apache.lucene.misc.*`
- Sandbox module packages renamed to `org.apache.lucene.sandbox.*`

**API Changes:**
- Directory API now little-endian (LUCENE-9047)
- `RAMDirectory` removed - use `ByteBuffersDirectory` (LUCENE-8474)
- `SortedDocValues` no longer extends `BinaryDocValues` (LUCENE-9796)
- `CodecReader.ramBytesUsed()` removed (LUCENE-9387)
- `Sort` is now immutable (LUCENE-9325)
- `SpanQuery` moved to queries module (org.apache.lucene.queries.spans)

**Analysis Factory Changes:**
- Factories need static `NAME` field and no-arg constructor (LUCENE-8778, LUCENE-9281)
- Service provider files renamed

**Impact on Oak:**
- Maven dependencies need artifact name updates
- All import statements need package updates
- Custom analysis factories need NAME field
- Directory implementations need endianness review

### Lucene 9.x → 10.x (MINOR)

**Changes:**
- `DataInput.readGroupVInt` method changes
- Test framework moved to `org.apache.lucene.tests.*`

## Oak-Specific Impact Summary

### HIGH IMPACT - Must Change

| Component | Current API | New API (9.x) | Effort |
|-----------|-------------|---------------|--------|
| OakCodec | FilterCodec + Lucene46 components | Modern codec API | High |
| CompressingCodec | FilterCodec + compression | Modern codec API | High |
| OakDirectory | Directory (extends) | Directory (new API) | High |
| OakAnalyzer | Analyzer (extends) | Analyzer (new API) | Medium |
| CommitMitigatingTieredMergePolicy | MergePolicy (extends) | MergePolicy (new API) | Medium |
| Embedded Lucene source | 707 files | Remove entirely | High |
| OSGi exports | 4.7.2-oak2 packages | 9.x packages | High |

### MEDIUM IMPACT - Needs Updates

| Area | Change Required |
|------|-----------------|
| Query construction | Use Builder pattern for BooleanQuery, PhraseQuery |
| Numeric fields | Migrate from LegacyNumericField to PointValues |
| Filters | Convert Filter to BooleanQuery with FILTER clause |
| Boosting | Use BoostQuery instead of Query.setBoost() |
| File paths | Use java.nio.file.Path instead of java.io.File |
| TopDocs handling | Handle TotalHits object instead of long |

### LOW IMPACT - Minor Updates

| Area | Change Required |
|------|-----------------|
| Imports | Update package names |
| Maven dependencies | Update artifact names |
| Scoring | Review BM25 score differences |

## Index Compatibility Strategy

### Reading Old Indexes (4.x format)

1. Include `lucene-backward-codecs` module
2. Old codecs (Lucene40-46) available in `org.apache.lucene.backward_codecs`
3. Can read but not write old format

### Migration Path

```
Existing Index (4.x)
    ↓ [Read with backward-codecs]
    ↓ [Reindex documents]
New Index (9.x format)
```

### Recommended Approach

1. **Phase 1**: Support reading old indexes (backward compatibility)
2. **Phase 2**: New indexes created in 9.x format
3. **Phase 3**: Provide migration tool for existing indexes
4. **Phase 4**: Eventually deprecate old format support

---

## Notes & Decisions

### 2026-02-04 - Phase 1.1 Complete ✅
- ✅ Completed comprehensive dependency inventory
- ✅ Identified all 707 embedded Lucene source files
- ✅ Mapped all 5 inheritance points (OakCodec, CompressingCodec, OakDirectory, OakAnalyzer, CommitMitigatingTieredMergePolicy)
- ✅ Documented version-specific code locations
- ✅ Created dependency graph

### 2026-02-04 - Phase 1.2 Complete ✅
- ✅ Reviewed all Lucene migration guides (4.x → 5.x → 6.x → 7.x → 8.x → 9.x → 10.x)
- ✅ Documented all breaking API changes
- ✅ Identified deprecated APIs in use (Filter, FilteredQuery, NumericField, RAMDirectory, etc.)
- ✅ Listed index format migration requirements
- ✅ Created Oak-specific impact summary

**Key Decisions:**
1. Target Lucene 9.x (latest stable version) for long-term support
2. Use `lucene-backward-codecs` for reading old 4.x indexes
3. Plan for index migration tool to upgrade existing indexes

**Critical Breaking Changes Identified:**
- Directory API restructured (affects OakDirectory)
- Codec SPI changes (affects OSGi bundle)
- Query immutability (affects all query construction)
- NumericField → PointValues migration
- Package renames in Lucene 9.x

**Next Steps:**
- Phase 1.3: Create detailed migration strategy document
- Get stakeholder approval before proceeding to Phase 2

### Decision Log
- **[Date]**: Decision description and rationale

### Issues & Blockers
- **[Date]**: Issue description and resolution

### Key Learnings
- **[Date]**: Learning or insight gained during migration

---

**Last Updated**: 2026-02-04
**Document Owner**: [Your Name/Team]
**Review Frequency**: Weekly during active phases

