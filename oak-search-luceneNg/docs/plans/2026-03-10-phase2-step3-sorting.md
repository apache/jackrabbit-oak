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

# Phase 2 Step 3: Sorting Implementation Plan

**Date:** 2026-03-10
**Status:** Planning
**Dependencies:** Phase 2 Step 2 (Property Queries) completed
**Goal:** Add sorting support for query results

---

## Overview

Implement sorting capabilities for Lucene 9 queries, allowing results to be ordered by:
- Text fields (alphabetical)
- Numeric fields (age, price, counts)
- Date fields (temporal order)
- Relevance scores
- Multiple sort fields (multi-level sorting)

This requires:
1. Adding DocValues fields during indexing (for efficient sorting)
2. Implementing SortField handling in query execution
3. Supporting Oak's OrderEntry specification

---

## Current State (After Step 2)

**✅ Completed:**
- Basic indexing with StringField, TextField, LongPoint, DoublePoint
- Full-text search queries
- Property equality and range queries
- Boolean query combinations
- Cost estimation

**❌ Missing:**
- DocValues fields for sorting
- Sort field handling in query execution
- Multi-field sorting
- Integration with Oak's OrderEntry

---

## Reference Implementation

### Legacy Lucene (oak-lucene)

**Indexing with DocValues:**
```java
// LuceneIndexEditor.java - legacy Lucene 4.7
private void addTypedFields(List<Field> fields, PropertyState property, String pname) {
    int tag = property.getType().tag();

    for (int i = 0; i < values.size(); i++) {
        if (Type.BINARY.tag() == tag) {
            // ...
        } else if (Type.LONG.tag() == tag) {
            fields.add(new LongField(pname, value, Field.Store.NO));
            fields.add(new NumericDocValuesField(pname, value)); // For sorting
        } else if (Type.DOUBLE.tag() == tag) {
            fields.add(new DoubleField(pname, value, Field.Store.NO));
            fields.add(new DoubleDocValuesField(pname, Double.doubleToRawLongBits(value)));
        } else if (Type.DATE.tag() == tag) {
            long dateValue = FieldFactory.convertToDate(value);
            fields.add(new LongField(pname, dateValue, Field.Store.NO));
            fields.add(new NumericDocValuesField(pname, dateValue));
        } else if (Type.BOOLEAN.tag() == tag) {
            fields.add(new StringField(pname, value, Field.Store.NO));
            fields.add(new SortedDocValuesField(pname, new BytesRef(value)));
        } else {
            fields.add(new StringField(pname, value, Field.Store.NO));
            fields.add(new SortedDocValuesField(pname, new BytesRef(value)));
        }
    }
}
```

**Query with Sorting:**
```java
// LuceneIndex.java - legacy query execution
private TopDocs search(Query query, int numDocs, IndexSearcher searcher,
                       List<OrderEntry> sortOrder) throws IOException {
    if (sortOrder.isEmpty()) {
        return searcher.search(query, numDocs);
    }

    // Build Lucene Sort from Oak OrderEntry list
    Sort sort = createSort(sortOrder);
    return searcher.search(query, numDocs, sort);
}

private Sort createSort(List<OrderEntry> sortOrder) {
    if (sortOrder.isEmpty()) {
        return null;
    }

    List<SortField> fields = new ArrayList<>();
    for (OrderEntry o : sortOrder) {
        SortField sf;
        if (OrderEntry.ORDER_SCORE.equals(o.getPropertyName())) {
            sf = SortField.FIELD_SCORE;
        } else {
            sf = new SortField(o.getPropertyName(),
                             getSortFieldType(o.getPropertyType()),
                             o.getOrder() == OrderEntry.Order.DESCENDING);
        }
        fields.add(sf);
    }

    return new Sort(fields.toArray(new SortField[0]));
}

private SortField.Type getSortFieldType(int propertyType) {
    switch (propertyType) {
        case PropertyType.LONG:
        case PropertyType.DATE:
            return SortField.Type.LONG;
        case PropertyType.DOUBLE:
            return SortField.Type.DOUBLE;
        case PropertyType.BOOLEAN:
        case PropertyType.STRING:
        default:
            return SortField.Type.STRING;
    }
}
```

### Elastic (oak-search-elastic)

**Sort handling:**
```java
// ElasticIndex.java
private SearchSourceBuilder buildSearchSource(Filter filter, List<OrderEntry> sortOrder) {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

    // Add query
    sourceBuilder.query(buildQuery(filter));

    // Add sorting
    for (OrderEntry order : sortOrder) {
        String propertyName = order.getPropertyName();

        if (OrderEntry.ORDER_SCORE.equals(propertyName)) {
            sourceBuilder.sort(SortBuilders.scoreSort()
                .order(getElasticOrder(order.getOrder())));
        } else {
            sourceBuilder.sort(SortBuilders.fieldSort(propertyName)
                .order(getElasticOrder(order.getOrder())));
        }
    }

    return sourceBuilder;
}
```

---

## Implementation Plan

### Task 1: Add DocValues Support to Indexing

**Files to modify:**
- `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexEditor.java`

**Step 1: Add DocValues fields alongside existing fields**

Update `indexNode()` method to add DocValues fields for sortable properties:

```java
// Current code (simplified):
case PropertyType.LONG:
    if (!prop.isArray()) {
        long value = prop.getValue(Type.LONG);
        doc.add(new LongPoint(propName, value));
        doc.add(new StoredField(propName, value));
    }
    break;

// Updated code:
case PropertyType.LONG:
    if (!prop.isArray()) {
        long value = prop.getValue(Type.LONG);
        doc.add(new LongPoint(propName, value));           // For range queries
        doc.add(new StoredField(propName, value));         // For retrieval
        doc.add(new NumericDocValuesField(propName, value)); // For sorting
    }
    break;
```

Add DocValues for all property types:
- `NumericDocValuesField` for Long and Date
- `DoubleDocValuesField` for Double (convert with Double.doubleToRawLongBits)
- `SortedDocValuesField` for String and Boolean

**Step 2: Add imports**

```java
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.util.BytesRef;
```

**Step 3: Verify DocValues fields are indexed**

Run: `mvn test -Dtest=LuceneNgIndexEditorTest`

---

### Task 2: Add Sorting Support to Query Execution

**Files to modify:**
- `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndex.java`

**Step 1: Update query() method signature**

The `AdvancedQueryIndex` interface requires implementing:
```java
Cursor query(Filter filter, NodeState rootState, OrderEntry.Order order,
             Filter.PropertyRestriction restriction);
```

But we also need to handle the basic `QueryIndex.query()` method which Oak's query engine calls.

Check current signature and update if needed.

**Step 2: Extract sort order from Filter**

```java
@Override
public Cursor query(Filter filter, NodeState rootState) {
    try {
        // Get index node
        LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
        if (indexNode == null) {
            LOG.warn("Index node not found: {}", indexPath);
            return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
        }

        // Get searcher
        NodeBuilder definitionBuilder = getDefinitionBuilder(rootState, indexPath);
        IndexSearcherHolder holder = new IndexSearcherHolder(
            definitionBuilder,
            indexNode.getDefinition().getIndexName()
        );
        IndexSearcher searcher = holder.getSearcher();

        // Build Lucene query
        Query query = buildQuery(filter);
        LOG.debug("Executing query: {}", query);

        // Get sort order from filter
        List<OrderEntry> sortOrder = createSortOrder(filter);

        // Execute query with or without sorting
        TopDocs docs;
        if (sortOrder.isEmpty()) {
            docs = searcher.search(query, 100);
        } else {
            Sort sort = createSort(sortOrder);
            LOG.debug("Sorting by: {}", sort);
            docs = searcher.search(query, 100, sort);
        }

        LOG.debug("Found {} hits", docs.totalHits);

        // Return cursor
        return new LuceneNgCursor(docs, searcher, holder);

    } catch (IOException e) {
        LOG.error("Error executing query on index: " + indexPath, e);
        return Cursors.newPathCursor(Collections.emptyList(), filter.getQueryLimits());
    }
}
```

**Step 3: Implement createSortOrder() method**

```java
private List<OrderEntry> createSortOrder(Filter filter) {
    // Oak stores sort information in the Filter's sort order
    // This is typically accessed through filter.getSortOrder() or similar
    // For now, return empty list - will enhance based on actual Oak API
    return Collections.emptyList();
}
```

**Step 4: Implement createSort() method**

```java
/**
 * Creates Lucene Sort from Oak OrderEntry list.
 * Based on legacy LuceneIndex implementation.
 */
private Sort createSort(List<OrderEntry> sortOrder) {
    if (sortOrder == null || sortOrder.isEmpty()) {
        return null;
    }

    List<SortField> fields = new ArrayList<>();
    for (OrderEntry order : sortOrder) {
        SortField sf = createSortField(order);
        if (sf != null) {
            fields.add(sf);
        }
    }

    return new Sort(fields.toArray(new SortField[0]));
}

private SortField createSortField(OrderEntry order) {
    String propertyName = order.getPropertyName();

    // Special case: sort by relevance score
    if (OrderEntry.ORDER_SCORE.equals(propertyName)) {
        return SortField.FIELD_SCORE;
    }

    // Determine sort field type based on property type
    SortField.Type fieldType = getSortFieldType(order.getPropertyType());

    // Create sort field (reverse = descending order)
    boolean reverse = (order.getOrder() == OrderEntry.Order.DESCENDING);

    return new SortField(propertyName, fieldType, reverse);
}

private SortField.Type getSortFieldType(int propertyType) {
    switch (propertyType) {
        case PropertyType.LONG:
        case PropertyType.DATE:
            return SortField.Type.LONG;
        case PropertyType.DOUBLE:
            return SortField.Type.DOUBLE;
        case PropertyType.BOOLEAN:
        case PropertyType.STRING:
        default:
            return SortField.Type.STRING;
    }
}
```

**Step 5: Add imports**

```java
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.jackrabbit.oak.spi.query.Filter.OrderEntry;
import javax.jcr.PropertyType;
```

**Step 6: Verify sorting works**

Run: `mvn test -Dtest=LuceneNgIndexTest`

---

### Task 3: Add Sorting Tests

**Files to modify:**
- `src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexTest.java`

**Step 1: Add test for numeric sorting**

```java
@Test
public void testSortByNumericField() throws Exception {
    // Setup: Create index
    NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
    NodeBuilder oakIndex = builder.child("oak:index");
    NodeBuilder indexDef = oakIndex.child("test");
    indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

    OakDirectory directory = new OakDirectory(indexDef, "test", false);
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter writer = new IndexWriter(directory, config);

    // Add documents with ages: 45, 25, 35
    Document doc1 = new Document();
    doc1.add(new StringField("path", "/person1", Field.Store.YES));
    doc1.add(new LongPoint("age", 45L));
    doc1.add(new StoredField("age", 45L));
    doc1.add(new NumericDocValuesField("age", 45L));
    writer.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new StringField("path", "/person2", Field.Store.YES));
    doc2.add(new LongPoint("age", 25L));
    doc2.add(new StoredField("age", 25L));
    doc2.add(new NumericDocValuesField("age", 25L));
    writer.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(new StringField("path", "/person3", Field.Store.YES));
    doc3.add(new LongPoint("age", 35L));
    doc3.add(new StoredField("age", 35L));
    doc3.add(new NumericDocValuesField("age", 35L));
    writer.addDocument(doc3);

    writer.commit();
    writer.close();
    directory.close();

    NodeState root = builder.getNodeState();

    // Create index and tracker
    LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
    tracker.update(root);

    LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

    // Create filter with sort order: age ASC
    Filter filter = mock(Filter.class);
    when(filter.getFullTextConstraint()).thenReturn(null);

    // Create PropertyRestriction that matches all documents (no filtering)
    PropertyRestriction pr = new PropertyRestriction();
    pr.propertyName = "age";
    pr.first = PropertyValues.newLong(0L);
    pr.firstIncluding = true;
    when(filter.getPropertyRestrictions()).thenReturn(Collections.singletonList(pr));
    when(filter.getQueryLimits()).thenReturn(null);

    // Add sort order
    OrderEntry orderEntry = mock(OrderEntry.class);
    when(orderEntry.getPropertyName()).thenReturn("age");
    when(orderEntry.getPropertyType()).thenReturn(PropertyType.LONG);
    when(orderEntry.getOrder()).thenReturn(OrderEntry.Order.ASCENDING);
    when(filter.getSortOrder()).thenReturn(Collections.singletonList(orderEntry));

    // Execute query
    Cursor cursor = index.query(filter, root);

    // Should return in order: person2 (25), person3 (35), person1 (45)
    assertTrue("Should find results", cursor.hasNext());
    assertEquals("First should be /person2", "/person2", cursor.next().getPath());
    assertTrue("Should have second result", cursor.hasNext());
    assertEquals("Second should be /person3", "/person3", cursor.next().getPath());
    assertTrue("Should have third result", cursor.hasNext());
    assertEquals("Third should be /person1", "/person1", cursor.next().getPath());
    assertFalse("Should have no more results", cursor.hasNext());
}
```

**Step 2: Add test for string sorting**

```java
@Test
public void testSortByStringField() throws Exception {
    // Test sorting by title alphabetically (ASC and DESC)
    // Add documents: "Zebra", "Apple", "Mango"
    // Sort ASC: Apple, Mango, Zebra
    // Sort DESC: Zebra, Mango, Apple
}
```

**Step 3: Add test for multi-field sorting**

```java
@Test
public void testMultiFieldSort() throws Exception {
    // Test sorting by category (ASC), then age (DESC)
    // Documents: (tech, 30), (tech, 25), (science, 40)
    // Result: (science, 40), (tech, 30), (tech, 25)
}
```

**Step 4: Add test for relevance score sorting**

```java
@Test
public void testSortByRelevanceScore() throws Exception {
    // Test sorting by relevance score (default for full-text queries)
    // Documents with different keyword frequencies
    // Should return highest scoring documents first
}
```

**Step 5: Run tests**

Run: `mvn test -Dtest=LuceneNgIndexTest`

---

### Task 4: Add Sorting to Comparison Tests

**Files to modify:**
- `src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgComparisonTest.java`

**Step 1: Update createLuceneNgIndex() to mark fields as sortable**

```java
private Tree createLuceneNgIndex() throws Exception {
    IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    builder.noAsync();
    builder.evaluatePathRestrictions();

    // Configure index rules for property search with sorting
    builder.indexRule("nt:base")
        .property("title").propertyIndex().ordered()  // Enable sorting
        .property("age").propertyIndex().type("Long").ordered()
        .property("price").propertyIndex().type("Double").ordered()
        .property("status").propertyIndex().ordered();

    Tree index = builder.build(root.getTree("/").getChild("oak:index").addChild("luceneNgTestIndex"));
    index.setProperty("type", "lucene9");

    root.commit();
    return index;
}
```

**Step 2: Add sort test with SQL2**

```java
@Test
public void testSortByAgeAscending() throws Exception {
    createLuceneNgIndex();
    createTestContent();

    // SQL2 query with ORDER BY
    String query = "SELECT * FROM [nt:base] WHERE [age] > 0 ORDER BY [age] ASC";

    // Execute and verify order
    List<String> result = executeQuery(query, "JCR-SQL2", false);

    // Should return: page1 (age=25), page2 (age=35), page3 (age=45)
    assertEquals("Should have 3 results", 3, result.size());
    assertEquals("First should be page1", "/content/page1", result.get(0));
    assertEquals("Second should be page2", "/content/page2", result.get(1));
    assertEquals("Third should be page3", "/content/page3", result.get(2));
}
```

**Step 3: Add descending sort test**

```java
@Test
public void testSortByPriceDescending() throws Exception {
    createLuceneNgIndex();
    createTestContent();

    // SQL2 query with ORDER BY DESC
    String query = "SELECT * FROM [nt:base] WHERE [price] > 0 ORDER BY [price] DESC";

    // Should return: page3 (75.00), page2 (45.50), page1 (15.99)
    assertQuery(query, "JCR-SQL2",
                List.of("/content/page3", "/content/page2", "/content/page1"));
}
```

**Step 4: Run tests**

Run: `mvn test -Dtest=LuceneNgComparisonTest`

---

## Verification

**Unit Tests:**
1. `testSortByNumericField()` - Sort by Long field (age)
2. `testSortByStringField()` - Sort by String field (title)
3. `testMultiFieldSort()` - Sort by multiple fields
4. `testSortByRelevanceScore()` - Sort by score
5. `testSortDescending()` - Test DESC order

**Integration Tests:**
1. `testSortByAgeAscending()` - SQL2 query with ORDER BY ASC
2. `testSortByPriceDescending()` - SQL2 query with ORDER BY DESC
3. `testSortByTitle()` - Alphabetical sorting

**Manual Verification:**
1. Inspect Lucene index to verify DocValues fields present
2. Check Sort object creation with debugger
3. Verify SortField types match property types
4. Compare results with legacy Lucene (should be identical order)

---

## Success Criteria

✅ DocValues fields added to all indexed properties
✅ Sort object created correctly from Oak OrderEntry
✅ Single-field sorting works (numeric, string, date)
✅ Multi-field sorting works correctly
✅ Sort by relevance score works
✅ Both ASC and DESC orders work
✅ All tests passing (70+ tests expected)
✅ Query results correctly ordered

---

## Notes

**DocValues vs Stored Fields:**
- Stored fields: Retrieve original values (slow for sorting)
- DocValues: Column-oriented storage (fast for sorting, aggregation)
- Must use DocValues for efficient sorting on large result sets

**SortField Types:**
- `SortField.Type.STRING` - For text fields (uses SortedDocValuesField)
- `SortField.Type.LONG` - For Long and Date fields
- `SortField.Type.DOUBLE` - For Double fields
- `SortField.FIELD_SCORE` - Special field for relevance score

**Performance:**
- DocValues are loaded into memory for fast access
- Multi-field sorting uses hierarchical comparison
- Large cardinality fields may require more memory

**Oak Integration:**
- Oak passes sort order through Filter.getSortOrder()
- OrderEntry contains property name, type, and direction
- Must handle special case: ORDER_SCORE for relevance sorting

---

## Generated by

Generated-by: Claude Sonnet 4.5 (Anthropic)
