# Lucene 9 Parallel Implementation Design

**Date:** 2026-03-06
**Status:** Approved - Ready for Implementation
**Last Updated:** 2026-03-06

## Executive Summary

This document defines the design for adding Lucene 9 indexing capability to Jackrabbit Oak as a parallel implementation alongside the existing Lucene 4.7.2 (oak-lucene) and Elasticsearch (oak-search-elastic) implementations. The solution includes multi-target write capability and index version flipping functionality.

**Key Innovation:** Separation of index definition from storage location - Lucene 9 indexes store data in `/var/indexing/lucene/<indexName>/` rather than under the index definition node.

## Table of Contents

1. [Current State Analysis](#current-state-analysis)
2. [Requirements](#requirements)
3. [Approved Design Decisions](#approved-design-decisions)
4. [Architecture](#architecture)
5. [Storage Strategy](#storage-strategy)
6. [Multi-Target Write Capability](#multi-target-write-capability)
7. [Index Version Flipping](#index-version-flipping)
8. [Query Safety and Validation](#query-safety-and-validation)
9. [NRT Strategy](#nrt-strategy)
10. [Implementation Phases](#implementation-phases)
11. [Configuration Examples](#configuration-examples)

---

## Current State Analysis

### Existing Implementations

#### 1. oak-lucene (Lucene 4.7.2)
- **Size:** 707 embedded Lucene source files
- **Version:** Lucene 4.7.2-oak2 (customized with CVE fixes)
- **Storage:** `:data` node under index definition
- **Features:**
  - Full-text and property indexing
  - NRT/Hybrid indexing (property index + Lucene)
  - Async indexing
  - Support for aggregates, facets, suggestions, spellcheck
  - Index copier for local caching
  - Directory abstraction (OakDirectory with BlobStore)

#### 2. oak-search-elastic (Elasticsearch)
- **Size:** 59 Java files
- **Version:** Uses Lucene 9.11.1 internally (via Elasticsearch client)
- **Storage:** Remote Elasticsearch cluster
- **Pattern:** Clean implementation following oak-search abstractions

#### 3. oak-search (Common Module)
- **Purpose:** Shared abstractions and utilities
- **Key Classes:**
  - `IndexDefinition` - Base index configuration
  - `PropertyDefinition` - Property-level configuration
  - `FieldNames` - Field naming conventions
  - `ExtractedTextCache` - Text extraction caching
  - `spi.editor.FulltextIndexEditor` - Base editor
  - `update/` - Refresh policies (NRT, timed, on-read/write)

### Index Type Registration

Each index implementation registers via OSGi services:
- **IndexEditorProvider** - Handles writes/indexing
- **QueryIndexProvider** - Handles queries

Current types:
- Lucene 4.7: `type = "lucene"`
- Elasticsearch: `type = "elasticsearch"`
- **New:** Lucene 9: `type = "lucene9"`

### Current Storage Structure (Lucene 4.7)

```
/oak:index/myIndex
  - jcr:primaryType = "oak:QueryIndexDefinition"
  - type = "lucene"
  - async = ["async"]
  + :data/                          ← Lucene index files stored here
    - dirListing = ["segments_1", "_0.cfs", ...]
    + segments_1                    ← Each file as child node
      - jcr:data = <blob>
      - blobSize = 12345
    + _0.cfs
      - jcr:data = <blob>
```

---

## Requirements

1. ✅ **Keep Existing Lucene 4.7 Untouched:** No changes to oak-lucene codebase
2. ✅ **Lucene 9 Implementation:** New from-scratch implementation, following Elasticsearch pattern
3. ✅ **Multi-Target Write:** Ability to write to multiple index types simultaneously
4. ✅ **Version Flipping:** Mechanism to switch active index version via index definition property
5. ✅ **Upgrade Prevention:** Avoid the "707 embedded files" trap - use pure dependencies
6. ✅ **Straightforward:** Keep it simple and fully usable

---

## Approved Design Decisions

### 1. Architecture
**Decision:** Minimal Clone Pattern - create `oak-search-luceneNg` module (~60-80 files) following the Elasticsearch model.

**Rationale:**
- Clean, maintainable codebase
- Proven pattern (Elasticsearch shows it works)
- No coupling to oak-lucene
- Easy to understand and maintain

### 2. Module Name
**Decision:** `oak-search-luceneNg`

**Rationale:** Follows the `oak-search-elastic` naming pattern.

### 3. Property Names
**Decision:**
- `storeTargets` - Array of storage types to write to (e.g., `['lucene47', 'lucene9']`)
- `activeTarget` - The storage type used for queries
- `type` - Backwards compatibility fallback (if storeTargets/activeTarget missing)

**Examples:**
```
storeTargets = ["lucene47", "lucene9"]  // Write to both
activeTarget = "lucene47"                // Query from lucene47
```

### 4. Storage Location
**Decision:** Implementation-specific storage locations:
- **Lucene 4.7:** Unchanged, uses `:data` under index definition
- **Lucene 9:** `/var/indexing/lucene/<indexName>/` (auto-created if missing)
- **Elasticsearch:** Remote cluster (unchanged)

**Rationale:**
- Separation of concerns - definition vs storage
- Lucene 4.7 remains untouched
- Future implementations can choose their own strategy
- Clean namespace for each implementation

**Path Derivation:**
```java
// Auto-derived for Lucene 9
String storagePath = "/var/indexing/lucene/" + indexName;
```

### 5. Dependencies
**Decision:** No dependency on oak-lucene. Extract shared utilities to oak-search if needed.

**Rationale:**
- Clean separation between implementations
- Avoids coupling and potential conflicts
- Forces proper abstraction of shared code

### 6. Lucene Version
**Decision:** Lucene 9.x (likely 9.11.1 or 9.12.2)

**Constraints:**
- Must stay on 9.x until Oak upgrades to Java 17 (Lucene 10 requires Java 17)
- Pure Maven dependencies only - NO embedded source code

### 7. Upgrade Prevention Strategy
**Decision:**
- ✅ Pure Maven dependencies (no embedded code)
- ✅ Prefer public stable Lucene APIs
- ✅ Watch for version-specific leakage into higher layers
- ✅ Refactor if coupling appears (pragmatic, not dogmatic)

**Rationale:** The 707 embedded files in oak-lucene made upgrades impossible. Never embed Lucene source code again.

### 8. NRT Implementation
**Decision:** Defer to Phase 4 (research required)

**Approach:**
- Phase 1-3: Async-only indexing
- Phase 4: Research native Lucene 9 NRT vs property index hybrid
- Deep dive into why property index hybrid was needed
- Prototype and compare approaches

### 9. Query Safety
**Decision:** Fail fast with commit hook validation

**Behavior:**
- When `activeTarget` is updated, commit hook validates:
  - ✅ Target exists in `storeTargets`
  - ✅ Target index is built and ready
  - ❌ Reject commit if validation fails

**Rationale:** Prevents accidental queries to unbuilt indexes, forces explicit control.

### 10. Initial Build
**Decision:** Automatic async reindex when new target added

**Behavior:**
- When a type is added to `storeTargets`, the async indexer automatically detects and builds that index from scratch
- Similar to how adding `async` property triggers reindex today

### 11. Cleanup
**Decision:**
- Phase 1: Manual cleanup (data remains after removal from storeTargets)
- Future: Background async cleanup task (runs hours/days after removal)

**Rationale:**
- Safe - allows rollback if issues discovered
- Removal from storeTargets is already a conscious decision
- Manual cleanup gives full control initially

---

## Architecture

### Module Structure

```
oak-search-luceneNg/
├── pom.xml
│   └── Dependencies:
│       ├── org.apache.lucene:lucene-core:9.11.1
│       ├── org.apache.lucene:lucene-queryparser:9.11.1
│       ├── org.apache.lucene:lucene-analyzers-common:9.11.1
│       └── oak-search (for common abstractions)
│
└── src/main/java/.../luceneNg/
    ├── LuceneNgIndexProviderService.java        ← OSGi service
    ├── LuceneNgIndexDefinition.java             ← Extends IndexDefinition
    ├── LuceneNgIndexTracker.java                ← Manages index lifecycle
    │
    ├── index/                                   ← Write path
    │   ├── LuceneNgIndexEditorProvider.java     ← Implements IndexEditorProvider
    │   ├── LuceneNgIndexEditor.java
    │   ├── LuceneNgIndexWriter.java
    │   └── OakDirectory.java               ← Custom Directory for /var storage
    │
    └── query/                                   ← Read path
        ├── LuceneNgIndexProvider.java           ← Implements QueryIndexProvider
        ├── LuceneNgIndex.java
        ├── LuceneNgPlanner.java
        └── LuceneNgSearcher.java
```

### Key Components

#### 1. OakDirectory
Custom Lucene `Directory` implementation that stores files in `/var/indexing/lucene/<indexName>/`:

```java
public class OakDirectory extends Directory {
    private final NodeBuilder varBuilder;
    private final String indexName;

    public OakDirectory(NodeStore nodeStore, String indexName) {
        this.indexName = indexName;
        // Navigate to /var/indexing/lucene/<indexName>
        // Auto-create if missing
        this.varBuilder = getOrCreateVarNode(nodeStore, indexName);
    }

    private NodeBuilder getOrCreateVarNode(NodeStore nodeStore, String indexName) {
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder var = root.child("var");
        NodeBuilder indexing = var.child("indexing");
        NodeBuilder lucene9 = indexing.child("lucene9");
        return lucene9.child(indexName);
    }

    // Implement Directory methods to read/write files in varBuilder
}
```

#### 2. LuceneNgIndexEditorProvider
Handles write operations:

```java
public class LuceneNgIndexEditorProvider implements IndexEditorProvider {

    @Override
    public Editor getIndexEditor(String type, NodeBuilder definition,
                                 NodeState root, IndexUpdateCallback callback) {
        if (!"lucene9".equals(type)) {
            return null;
        }

        String indexPath = getIndexPath(callback);
        String indexName = PathUtils.getName(indexPath);

        LuceneNgIndexDefinition indexDef =
            new LuceneNgIndexDefinition(root, definition.getNodeState(), indexPath);

        OakDirectory directory =
            new OakDirectory(getNodeStore(callback), indexName);

        return new LuceneNgIndexEditor(indexDef, directory, callback);
    }
}
```

#### 3. LuceneNgIndexProvider
Handles query operations:

```java
public class LuceneNgIndexProvider implements QueryIndexProvider {
    private final LuceneNgIndexTracker indexTracker;

    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        return List.of(new LuceneNgIndex(indexTracker));
    }
}
```

---

## Storage Strategy

### Lucene 4.7 Storage (Unchanged)

```
/oak:index/myIndex
  - type = "lucene"
  + :data/
    - dirListing = ["segments_1", "_0.cfs", ...]
    + segments_1
      - jcr:data = <blob>
```

### Lucene 9 Storage (New)

```
/oak:index/myIndex
  - type = "lucene9"
  - async = ["async"]
  (NO :data node here)

/var/indexing/lucene/myIndex/
  - dirListing = ["segments_1", "_0.cfs", ...]
  + segments_1
    - jcr:data = <blob>
  + _0.cfs
    - jcr:data = <blob>
```

### Multi-Target Storage

```
/oak:index/myIndex
  - storeTargets = ["lucene47", "lucene9"]
  - activeTarget = "lucene47"

/oak:index/myIndex/:data/              ← Lucene 4.7 storage (unchanged)
  + segments_1
  + ...

/var/indexing/lucene/myIndex/         ← Lucene 9 storage (separate)
  + segments_1
  + ...
```

**Key Points:**
- Each storage type manages its own location
- No "primary" vs "shadow" distinction
- All targets are equal
- Clean separation enables independent lifecycle

---

## Multi-Target Write Capability

### Configuration

Add two new properties to index definitions:

```java
// In FulltextIndexConstants.java (oak-search)
public static final String STORE_TARGETS = "storeTargets";
public static final String ACTIVE_TARGET = "activeTarget";
```

### Example Index Definition

```json
{
  "jcr:primaryType": "oak:QueryIndexDefinition",
  "type": "lucene47",
  "storeTargets": ["lucene47", "lucene9"],
  "activeTarget": "lucene47",
  "async": ["async"],
  "indexRules": {
    "nt:base": {
      "properties": {
        "title": {
          "name": "jcr:title",
          "analyzed": true
        }
      }
    }
  }
}
```

### Property Semantics

- **`type`**: Backwards compatibility (if storeTargets missing, defaults to [type])
- **`storeTargets`**: Array of storage types to write to
- **`activeTarget`**: Which target to use for queries (must be in storeTargets)

### Implementation

Enhance `CompositeIndexEditorProvider` (or create new provider) to fan out writes:

```java
@Override
public Editor getIndexEditor(String type, NodeBuilder definition,
                              NodeState root, IndexUpdateCallback callback) {
    List<Editor> editors = new ArrayList<>();

    // Get storeTargets or fallback to type
    PropertyState storeTargetsProperty = definition.getProperty(STORE_TARGETS);
    List<String> storeTargets = storeTargetsProperty != null
        ? Lists.newArrayList(storeTargetsProperty.getValue(Type.STRINGS))
        : List.of(type);

    // Create editor for each storeTarget
    for (String targetType : storeTargets) {
        IndexEditorProvider provider = getProviderForType(targetType);
        if (provider != null) {
            Editor editor = provider.getIndexEditor(targetType, definition, root, callback);
            if (editor != null) {
                editors.add(new ErrorTolerantEditor(editor, targetType));
            }
        }
    }

    return editors.isEmpty() ? null : CompositeEditor.compose(editors);
}
```

### Error Handling

**Critical:** Failures in secondary targets must not block primary writes.

```java
public class ErrorTolerantEditor implements Editor {
    private final Editor delegate;
    private final String targetType;

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        try {
            delegate.leave(before, after);
        } catch (Exception e) {
            // Log error but don't propagate
            LOG.error("Index write failed for target {}: {}", targetType, e.getMessage());
            // Increment JMX metric for monitoring
            metrics.incrementFailureCount(targetType);
        }
    }

    // Similar for other methods
}
```

### Query Handling

Only `activeTarget` is queried:

```java
// In QueryEngineImpl or equivalent
String activeTarget = indexDef.getString(ACTIVE_TARGET);
if (activeTarget == null) {
    // Fallback to type for backwards compatibility
    activeTarget = indexDef.getString(TYPE_PROPERTY_NAME);
}

// Use activeTarget to select query provider
QueryIndexProvider provider = getProviderForType(activeTarget);
```

---

## Index Version Flipping

### Three-Phase Migration Process

#### Phase 1: Shadow Writing (Validation)

```json
{
  "type": "lucene47",
  "storeTargets": ["lucene47", "lucene9"],
  "activeTarget": "lucene47",
  "async": ["async"]
}
```

**What happens:**
- ✅ Writes go to both lucene47 and lucene9
- ✅ Queries use lucene47
- ✅ Async indexer automatically builds lucene9 index
- ✅ Monitor lucene9 health via JMX

**Duration:** Until lucene9 index is fully built and verified

#### Phase 2: Flip Reads (Monitoring)

```json
{
  "type": "lucene47",
  "storeTargets": ["lucene47", "lucene9"],
  "activeTarget": "lucene9",            ← Changed
  "async": ["async"]
}
```

**What happens:**
- ✅ Writes still go to both
- ✅ Queries now use lucene9
- ✅ Monitor query performance, error rates
- ✅ Commit hook validates lucene9 is ready before allowing this change

**Duration:** Monitoring period (hours to days)

#### Phase 3: Finalize (Cleanup)

```json
{
  "type": "lucene9",
  "storeTargets": ["lucene9"],          ← Removed lucene47
  "activeTarget": "lucene9",
  "async": ["async"]
}
```

**What happens:**
- ✅ Only writes to lucene9
- ✅ Only queries lucene9
- ⏳ Lucene47 data remains at `/oak:index/myIndex/:data/` (manual cleanup)

### Rollback Support

At any phase, can rollback by changing `activeTarget`:

```json
// Emergency rollback in Phase 2
{
  "type": "lucene47",
  "storeTargets": ["lucene47", "lucene9"],
  "activeTarget": "lucene47",  ← Flip back
  "async": ["async"]
}
```

**Commit hook validation ensures:**
- Can only flip to targets in storeTargets
- Target must be ready (index exists and has data)

---

## Query Safety and Validation

### Commit Hook: ActiveTargetValidator

```java
public class ActiveTargetValidator extends DefaultEditor {

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        if (ACTIVE_TARGET.equals(after.getName())) {
            String newActiveTarget = after.getValue(Type.STRING);
            validateActiveTarget(newActiveTarget);
        }
    }

    private void validateActiveTarget(String activeTarget) {
        // 1. Check activeTarget is in storeTargets
        PropertyState storeTargets = builder.getProperty(STORE_TARGETS);
        if (storeTargets == null ||
            !Lists.newArrayList(storeTargets.getValue(Type.STRINGS)).contains(activeTarget)) {
            throw new CommitFailedException(
                "activeTarget '" + activeTarget + "' must be in storeTargets");
        }

        // 2. Check target index exists and is ready
        if (!isIndexReady(activeTarget)) {
            throw new CommitFailedException(
                "Index for target '" + activeTarget + "' is not ready. " +
                "Wait for async indexing to complete.");
        }
    }

    private boolean isIndexReady(String targetType) {
        switch (targetType) {
            case "lucene47":
                return checkLucene47Ready();
            case "lucene9":
                return checkLucene9Ready();
            case "elasticsearch":
                return checkElasticReady();
            default:
                return false;
        }
    }

    private boolean checkLucene9Ready() {
        // Check if /var/indexing/lucene/<indexName>/ exists and has index files
        NodeState var = root.getChildNode("var");
        if (!var.exists()) return false;

        NodeState indexing = var.getChildNode("indexing");
        if (!indexing.exists()) return false;

        NodeState lucene9 = indexing.getChildNode("lucene9");
        if (!lucene9.exists()) return false;

        NodeState indexNode = lucene9.getChildNode(indexName);
        if (!indexNode.exists()) return false;

        // Check for essential Lucene files (segments_N)
        PropertyState dirListing = indexNode.getProperty("dirListing");
        if (dirListing == null) return false;

        List<String> files = Lists.newArrayList(dirListing.getValue(Type.STRINGS));
        return files.stream().anyMatch(f -> f.startsWith("segments_"));
    }
}
```

### Index Readiness Checks

Before flipping `activeTarget`, verify:

1. **Index Completeness:** Entry count reasonable
2. **Query Correctness:** Sample queries return expected results
3. **Performance:** Response times acceptable

**JMX operations** (optional Phase 2+):
```java
public interface IndexFlipperMBean {
    boolean isTargetReady(String indexPath, String targetType);
    void validateBeforeFlip(String indexPath, String newTarget);
}
```

---

## NRT Strategy

### Decision: Defer to Phase 4

**Rationale:**
- Current property index hybrid is complex but proven
- Lucene 9 has improved native NRT capabilities
- Need research to determine best approach:
  - Why was property index hybrid needed?
  - Can native Lucene 9 NRT meet the same requirements?
  - Performance comparison?

**Phase 1-3:** Async-only indexing (simpler, validates core functionality)

**Phase 4:** NRT research and implementation
- Deep dive into hybrid-index.md use cases
- Prototype native Lucene 9 NRT (IndexWriter.commit + DirectoryReader.openIfChanged)
- Compare approaches
- Implement chosen solution

---

## Implementation Phases

### Phase 1: Core Lucene 9 Module (4 weeks)

**Scope:**
- Create oak-search-luceneNg module structure
- Implement basic write path (IndexEditorProvider, IndexEditor)
- Implement OakDirectory (storage in /var/indexing/lucene/)
- Implement basic read path (QueryIndexProvider, Index, Planner)
- Async-only indexing (no NRT)
- OSGi service registration

**Deliverables:**
- ✅ `oak-search-luceneNg` module with pom.xml
- ✅ `LuceneNgIndexEditorProvider` (writes)
- ✅ `LuceneNgIndexProvider` (queries)
- ✅ `OakDirectory` (/var storage)
- ✅ `LuceneNgIndexDefinition`
- ✅ Unit tests
- ✅ Integration test: full indexing + query roundtrip

**Success Criteria:**
- Can create index with `type="lucene9"`
- Async indexer indexes content
- Queries return correct results
- Index stored in `/var/indexing/lucene/<indexName>/`

### Phase 2: Multi-Target Write (2 weeks)

**Scope:**
- Implement `storeTargets` and `activeTarget` properties
- Enhance CompositeIndexEditorProvider for multi-target writes
- Error-tolerant editor wrapper
- Query provider selection based on activeTarget
- JMX monitoring for target health

**Deliverables:**
- ✅ Multi-target write capability
- ✅ Error handling for secondary target failures
- ✅ Backwards compatibility (type fallback)
- ✅ Integration tests (dual write scenarios)
- ✅ JMX metrics per target

**Success Criteria:**
- Can write to multiple targets simultaneously
- Primary target failure propagates, secondary failure logged
- Queries use activeTarget correctly

### Phase 3: Index Flipping and Validation (1 week)

**Scope:**
- Implement ActiveTargetValidator commit hook
- Index readiness checks
- Rollback support
- Documentation (migration runbook)

**Deliverables:**
- ✅ `ActiveTargetValidator` commit hook
- ✅ Readiness validation logic
- ✅ Fail-fast on invalid flip attempts
- ✅ Migration guide document
- ✅ Integration tests (flip scenarios, rollback)

**Success Criteria:**
- Cannot flip to unready index (commit fails)
- Can safely flip between targets
- Can rollback if issues occur

### Phase 4: NRT Support (3 weeks) - DEFERRED

**Scope:** Research and implement NRT based on findings

**Approach:**
1. Research phase (1 week):
   - Analyze property index hybrid requirements
   - Prototype native Lucene 9 NRT
   - Compare approaches
   - Decide on implementation

2. Implementation (2 weeks):
   - Based on research findings
   - Likely: property index hybrid initially (proven)
   - Future: native NRT if benefits proven

### Phase 5: Feature Parity (4 weeks)

**Scope:**
- Aggregates
- Facets
- Suggestions/Spellcheck
- Similarity search
- Function indexes
- Analyzers
- Full query feature parity with Lucene 4.7

**Deliverables:**
- ✅ All features from oak-lucene supported
- ✅ Test coverage matching oak-lucene
- ✅ Performance benchmarks

### Phase 6: Production Hardening (2 weeks)

**Scope:**
- Performance testing and optimization
- Error recovery and edge cases
- Comprehensive documentation
- Migration tools and scripts

**Deliverables:**
- ✅ Performance benchmarks vs Lucene 4.7
- ✅ Production deployment guide
- ✅ Migration runbook
- ✅ Troubleshooting guide

**Total Estimated Effort:** 16 weeks (4 months)

---

## Configuration Examples

### Simple Lucene 9 Index (Async Only)

```json
{
  "jcr:primaryType": "oak:QueryIndexDefinition",
  "type": "lucene9",
  "async": ["async"],
  "indexRules": {
    "nt:base": {
      "properties": {
        "title": {
          "name": "jcr:title",
          "analyzed": true,
          "nodeScopeIndex": true
        },
        "description": {
          "name": "jcr:description",
          "analyzed": true
        }
      }
    }
  }
}
```

**Storage:**
- Definition: `/oak:index/myIndex`
- Data: `/var/indexing/lucene/myIndex/`

### Multi-Target Migration Index

```json
{
  "jcr:primaryType": "oak:QueryIndexDefinition",
  "type": "lucene47",
  "storeTargets": ["lucene47", "lucene9"],
  "activeTarget": "lucene47",
  "async": ["async"],
  "indexRules": {
    "nt:base": {
      "properties": {
        "title": {
          "name": "jcr:title",
          "analyzed": true
        }
      }
    }
  }
}
```

**Storage:**
- Definition: `/oak:index/myIndex`
- Lucene47 data: `/oak:index/myIndex/:data/`
- Lucene9 data: `/var/indexing/lucene/myIndex/`

**Writes:** Both lucene47 and lucene9
**Queries:** lucene47 (activeTarget)

### After Successful Migration

```json
{
  "jcr:primaryType": "oak:QueryIndexDefinition",
  "type": "lucene9",
  "storeTargets": ["lucene9"],
  "activeTarget": "lucene9",
  "async": ["async"],
  "indexRules": {
    "nt:base": {
      "properties": {
        "title": {
          "name": "jcr:title",
          "analyzed": true
        }
      }
    }
  }
}
```

**Storage:**
- Definition: `/oak:index/myIndex`
- Lucene9 data: `/var/indexing/lucene/myIndex/`
- Lucene47 data: `/oak:index/myIndex/:data/` (remains, manual cleanup)

**Writes:** lucene9 only
**Queries:** lucene9

### Backwards Compatibility (No New Properties)

```json
{
  "jcr:primaryType": "oak:QueryIndexDefinition",
  "type": "lucene9",
  "async": ["async"],
  "indexRules": { ... }
}
```

**Behavior:**
- `storeTargets` defaults to `["lucene9"]` (derived from type)
- `activeTarget` defaults to `"lucene9"` (derived from type)
- Works exactly like explicit single-target configuration

---

## Migration Runbook

### Step-by-Step Migration: Lucene 4.7 → Lucene 9

#### Prerequisites
1. Oak includes oak-search-luceneNg bundle
2. `/var/indexing/lucene/` will be auto-created
3. Index health monitoring in place

#### Step 1: Enable Shadow Writing

Update index definition:
```json
{
  "type": "lucene47",
  "storeTargets": ["lucene47", "lucene9"],   ← Add
  "activeTarget": "lucene47",                ← Add
  "async": ["async"],
  ...
}
```

**What happens:**
- Async indexer detects new target, starts building lucene9 index
- Writes go to both targets
- Queries still use lucene47

**Monitor:**
- Check async indexer logs for lucene9 progress
- JMX: Index entry counts should match
- Sample queries against both indexes (via JMX or oak-run)

**Wait:** Until lucene9 index is fully built (all content indexed)

#### Step 2: Flip to Lucene 9

Update index definition:
```json
{
  "type": "lucene47",
  "storeTargets": ["lucene47", "lucene9"],
  "activeTarget": "lucene9",                 ← Changed
  "async": ["async"],
  ...
}
```

**Validation:**
- Commit hook validates lucene9 is ready
- If not ready, commit fails with error message

**What happens:**
- Writes still go to both
- Queries now use lucene9

**Monitor:**
- Query performance metrics
- Error rates
- Response time percentiles

**Duration:** 24-72 hours of monitoring recommended

#### Step 3: Remove Old Target (Optional)

If satisfied with lucene9:
```json
{
  "type": "lucene9",
  "storeTargets": ["lucene9"],               ← Removed lucene47
  "activeTarget": "lucene9",
  ...
}
```

**What happens:**
- Only writes to lucene9
- Lucene47 data remains at `/oak:index/myIndex/:data/`

**Cleanup (Manual):**
```bash
# Via oak-run or JCR API
# Delete /oak:index/myIndex/:data node
```

#### Rollback Procedure

If issues discovered in Phase 2:
```json
{
  "type": "lucene47",
  "storeTargets": ["lucene47", "lucene9"],
  "activeTarget": "lucene47",                ← Flip back
  ...
}
```

**Effect:** Immediately returns to lucene47 for queries, both still receiving writes.

---

## Open Questions for Future Phases

### Phase 4 (NRT)
- Should we implement property index hybrid or native Lucene 9 NRT?
- What are the exact requirements that drove hybrid indexing?
- Performance comparison needed

### Phase 5+ (Optimization)
- Should IndexCopier support be added for Lucene 9?
- Can we optimize /var storage structure for better performance?
- Background async cleanup task design

### Production
- JMX operations for index health monitoring?
- Automated migration tooling (oak-run command)?
- Index size estimation and capacity planning?

---

## Success Metrics

### Phase 1
- ✅ Can create and query lucene9 indexes
- ✅ Index data stored in /var/indexing/lucene/
- ✅ No changes to oak-lucene code

### Phase 2
- ✅ Can write to multiple targets
- ✅ Primary target failures propagate, secondary logged
- ✅ Query routing based on activeTarget

### Phase 3
- ✅ Cannot flip to unready index
- ✅ Can rollback safely
- ✅ Migration runbook tested

### Overall
- ✅ Zero embedded Lucene code
- ✅ Clean module structure (<100 files)
- ✅ Straightforward upgrade path for future Lucene versions
- ✅ Production-ready in 4 months

---

## Risks and Mitigations

### Risk: /var not existing in all Oak deployments
**Mitigation:** Auto-create /var/indexing/lucene/ if missing (approved)

### Risk: Storage separation increases complexity
**Mitigation:** Clear documentation, simple path derivation logic

### Risk: Commit hook validation too strict
**Mitigation:** Comprehensive readiness checks, clear error messages

### Risk: Performance of /var storage location
**Mitigation:** Use same storage strategy as :data (BlobStore), benchmark in Phase 6

### Risk: Multi-target write failures
**Mitigation:** Error-tolerant wrapper, monitoring, fail open for secondary targets

---

## Conclusion

This design provides a clean, safe path to adding Lucene 9 indexing to Jackrabbit Oak. Key innovations:

1. **Storage Separation:** `/var/indexing/lucene/` breaks the definition/storage coupling
2. **Multi-Target Writing:** Enables safe migrations and A/B testing
3. **Fail-Fast Validation:** Prevents accidental misconfigurations
4. **No Embedded Code:** Ensures future upgradability
5. **Phased Delivery:** 6 phases over 4 months, each delivering value

**Next Steps:**
1. ✅ Design approved
2. Create implementation plan (detailed task breakdown)
3. Set up oak-search-luceneNg module skeleton
4. Begin Phase 1 implementation

---

**Document Status:** Approved - Ready for Implementation
**Last Updated:** 2026-03-06
**Approved By:** Stakeholder Review
**Implementation Start:** TBD
