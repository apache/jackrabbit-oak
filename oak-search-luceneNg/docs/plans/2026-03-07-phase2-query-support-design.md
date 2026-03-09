<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Phase 2: Lucene 9 Query Support (Read Path) - Design

**Date:** 2026-03-07
**Status:** Approved
**Goal:** Implement full query support for Lucene 9 with feature parity to Elasticsearch integration

---

## Overview

Phase 1 implemented the write path (indexing). Phase 2 adds the read path (querying) to enable full search functionality. This will be implemented incrementally across 5 steps, each building on the previous.

## Architecture

### Core Components

1. **LuceneNgQueryIndexProvider** (`QueryIndexProvider` implementation)
   - Routes queries to Lucene 9 indexes
   - Returns `List<QueryIndex>` for indexes that can satisfy a query
   - Integrates with Oak's query engine

2. **LuceneNgIndexPlanner** (created per query)
   - Analyzes query filters and available indexes
   - Creates execution plan (cost estimation, property selection)
   - Determines if index can satisfy the query

3. **LuceneNgIndex** (`AdvancedQueryIndex` implementation)
   - Executes queries against Lucene IndexSearcher
   - Translates Oak filters to Lucene Query objects
   - Returns result iterator with scores

4. **IndexSearcherHolder** (resource management)
   - Manages IndexSearcher lifecycle and NRT (near-real-time) reopening
   - Thread-safe access to searchers
   - Cleanup on index updates

### Data Flow

```
Oak Query Engine
    ↓ (getPlans)
LuceneNgQueryIndexProvider
    ↓ (analyze query)
LuceneNgIndexPlanner
    ↓ (creates plan)
Oak Query Engine (selects best plan)
    ↓ (query)
LuceneNgIndex
    ↓ (builds Lucene Query)
IndexSearcher
    ↓ (searches)
TopDocs → ResultIterator
```

---

## Implementation Steps (Incremental)

### Step 1: Foundation (Basic Text Search)

**Components:**
- `LuceneNgQueryIndexProvider`
- `LuceneNgIndexPlanner` (basic cost estimation only)
- `LuceneNgIndex` (text queries only)
- `IndexSearcherHolder` (manages searcher lifecycle)

**Queries Supported:**
- Full-text search: `jcr:contains(*, 'keyword')`
- Single term queries
- Phrase queries

**Test Coverage:**
- Create index, index documents with text properties
- Execute full-text search queries
- Verify correct documents returned with scores
- Test IndexSearcher opens Lucene 9 indexes correctly

**Validates:**
- End-to-end read path works
- Integration with Oak query engine
- OakDirectory reads work correctly

---

### Step 2: Property Queries + Filtering

**Components (extend Step 1):**
- Enhanced `LuceneNgIndexPlanner` (property index selection)
- Enhanced `LuceneNgIndex` (property queries)
- Query builder for boolean combinations

**Queries Supported:**
- Property exact match: `title = 'Introduction'`
- Range queries: `age > 25`, `date BETWEEN x AND y`
- Boolean combinations: `(title = 'Oak' OR text CONTAINS 'lucene') AND status = 'published'`
- NOT queries: `title != 'Draft'`

**Test Coverage:**
- Property-based filtering on StringField, NumericField, DateField
- Boolean queries (AND, OR, NOT)
- Combining full-text with property filters
- Query optimization (use indexed properties)

**Validates:**
- IndexPlanner correctly identifies indexed properties
- Cost estimation favors property indexes over full scans
- Boolean query builder handles complex conditions

---

### Step 3: Sorting

**Components (extend Step 2):**
- SortField handling in `LuceneNgIndex`
- DocValues support for sortable fields
- Multi-field sorting

**Queries Supported:**
- Single field sort: `ORDER BY title ASC`
- Multi-field sort: `ORDER BY date DESC, title ASC`
- Sort by score (relevance)
- Sort by indexed fields (text, numeric, date)

**Test Coverage:**
- Sort by text fields (alphabetical)
- Sort by numeric fields (age, price)
- Sort by date fields (temporal order)
- Multi-level sorting
- Sort + pagination (offset/limit)

**Validates:**
- DocValues fields stored correctly during indexing
- SortField types match field types
- Sort order correctness (ASC/DESC)
- Performance with large result sets

---

### Step 4: Aggregations

**Components (extend Step 3):**
- Facet collectors (terms, range, date histogram)
- Stats collectors (count, sum, avg, min, max)
- Aggregation result builders

**Queries Supported:**
- Terms facets: "Group by author, show counts"
- Range facets: "Price buckets: 0-10, 10-50, 50+"
- Date histograms: "Documents per month"
- Metric aggregations: "Average rating", "Total sales"
- Nested aggregations: "Average price per category"

**Test Coverage:**
- Terms faceting on string fields
- Numeric range faceting
- Date histogram aggregations (day/month/year)
- Stats aggregations (count/sum/avg/min/max)
- Nested aggregations (sub-buckets)
- Aggregation + query filtering

**Validates:**
- Facet collectors work with Lucene 9
- Correct bucket counts
- Stats calculations accurate
- Memory efficiency for large cardinality facets

---

### Step 5: Highlighting

**Components (extend Step 4):**
- Fragment extractor
- Hit highlighting with FastVectorHighlighter
- Snippet formatting

**Queries Supported:**
- Highlight matching keywords in results
- Control fragment size and count
- Custom pre/post tags (e.g., `<em>...</em>`)

**Test Coverage:**
- Highlight single term matches
- Highlight phrase matches
- Multiple fragments per document
- Fragment size control
- Custom highlight tags

**Validates:**
- FastVectorHighlighter works with Lucene 9
- Term vectors stored correctly
- Snippet extraction accurate
- Performance with large documents

---

## Feature Parity with Elasticsearch

This implementation provides functional equivalence to the current Elasticsearch integration:

| Feature | Elasticsearch | Lucene 9 | Step |
|---------|--------------|----------|------|
| Full-text search | ✓ | ✓ | 1 |
| Property filtering | ✓ | ✓ | 2 |
| Boolean queries | ✓ | ✓ | 2 |
| Sorting | ✓ | ✓ | 3 |
| Terms aggregations | ✓ | ✓ | 4 |
| Stats aggregations | ✓ | ✓ | 4 |
| Highlighting | ✓ | ✓ | 5 |

---

## Testing Strategy

**Unit Tests:**
- Component tests for each class (QueryProvider, IndexPlanner, Index)
- Mock IndexSearcher for isolated testing
- Query builder tests (Oak Filter → Lucene Query)

**Integration Tests:**
- End-to-end tests: index + query
- Compare results with expected output
- Test all query types supported in each step

**High-Level Tests (after Phase 3):**
- Real Oak instance with MemoryNodeStore
- Index with both Lucene 4.7 and Lucene 9
- Compare query results (should be identical)
- Migration tests (hot migration + reindex)

---

## Dependencies

**Oak APIs:**
- `QueryIndexProvider`, `QueryIndex`, `AdvancedQueryIndex`
- `Filter`, `FilterImpl` (query representation)
- `IndexPlanner`, `IndexPlan` (cost estimation)

**Lucene 9 APIs:**
- `IndexSearcher`, `IndexReader`
- `Query`, `BooleanQuery`, `TermQuery`, `PhraseQuery`
- `TopDocs`, `ScoreDoc`
- `SortField`, `Sort`
- `FacetsCollector`, `FastVectorHighlighter`

**Existing Components:**
- `OakDirectory` (read index files)
- `LuceneNgIndexTracker` (track index updates)
- `LuceneNgIndexDefinition` (index configuration)

---

## Non-Goals (Deferred to Later Phases)

- Multi-index write (storeTargets) - Phase 3
- Index flipping (activeTarget) - Phase 3
- Migration tests - Phase 3
- Near-real-time (NRT) search - Future
- Distributed search - Future
- Advanced Lucene features (MLT, spatial, etc.) - Future

---

## Success Criteria

**Step 1:** Can execute full-text queries and get correct results
**Step 2:** Can filter by properties and combine conditions
**Step 3:** Can sort results by any indexed field
**Step 4:** Can aggregate results (facets + stats)
**Step 5:** Can highlight matching keywords in results

**Overall:** Query results match Elasticsearch behavior for equivalent queries
