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

# Phase 2 Step 2: Property Queries + Filtering Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add support for property-based queries with range queries, NOT queries, and complex boolean combinations

**Patterns:** Follow legacy LuceneIndex and ElasticIndex patterns for property restrictions. Use Lucene 9 equivalents of legacy Lucene 4.7 query types.

**Tech Stack:** Java 11, Lucene 9.11.1, JUnit 4, Mockito, Oak query SPI

---

## Current State Analysis

**What we have:**
- ✅ Basic property equality: `title = 'value'` (StringField exact match)
- ✅ Full-text search with boolean combinations (AND/OR for full-text)
- ✅ Simple cost estimation in `getCost()`

**What we need to add:**
- ❌ Range queries: `age > 25`, `date BETWEEN x AND y`
- ❌ NOT queries: `title != 'Draft'`
- ❌ Complex boolean combinations mixing properties and full-text
- ❌ Proper IndexPlanner for better cost estimation
- ❌ Support for numeric types (Long, Double) and Date types

---

## Legacy Patterns Reference

### Legacy Lucene Pattern (oak-lucene/LuceneIndex.java)
```java
// Line 720-816
for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
    if (pr.first != null && pr.first.equals(pr.last)) {
        // Equality: title = 'value'
        qs.add(new TermQuery(new Term(name, value)));
    } else if (pr.first != null || pr.last != null) {
        // Range: age > 25, age BETWEEN 10 AND 100
        qs.add(TermRangeQuery.newStringRange(name, first, last,
                pr.firstIncluding, pr.lastIncluding));
    }
}
```

### Elastic Pattern (oak-search-elastic/util/TermQueryBuilderFactory.java)
```java
// Line 120-150: Handles all property restriction cases
public static Query newPropertyRestrictionQuery(String field, PropertyRestriction pr,
                                                Function<PropertyValue, R> propToObj) {
    if (pr.first != null && pr.first.equals(pr.last)) {
        return termQuery(field, first);  // Equality
    } else if (pr.first != null && pr.last != null) {
        return rangeQuery(field, first, last, ...);  // Both bounds
    } else if (pr.first != null) {
        return rangeQuery(field, first, null, ...);  // Lower bound only (>= or >)
    } else if (pr.last != null) {
        return rangeQuery(field, null, last, ...);   // Upper bound only (<= or <)
    } else if (pr.list != null) {
        return inQuery(field, pr.list);  // IN query
    } else if (pr.isNot && pr.not != null) {
        return boolQuery().mustNot(termQuery(field, not));  // NOT equal
    }
}
```

---

## Task 1: Add Range Query Support

**Files to modify:**
- `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndex.java`
- `src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexTest.java`

**Step 1: Write failing test for numeric range query**

Add to `LuceneNgIndexTest.java`:

```java
@Test
public void testNumericRangeQuery() throws Exception {
    // Setup: Create index with numeric property
    NodeBuilder builder = INITIAL_CONTENT.builder();
    NodeBuilder oakIndex = builder.child("oak:index");
    NodeBuilder indexDef = oakIndex.child("test");
    indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

    // Index documents with age property
    OakDirectory directory = new OakDirectory(indexDef, "test", false);
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter writer = new IndexWriter(directory, config);

    // Document 1: age = 25
    Document doc1 = new Document();
    doc1.add(new StringField("path", "/person1", Field.Store.YES));
    doc1.add(new LongPoint("age", 25L));
    doc1.add(new StoredField("age", 25L));
    writer.addDocument(doc1);

    // Document 2: age = 35
    Document doc2 = new Document();
    doc2.add(new StringField("path", "/person2", Field.Store.YES));
    doc2.add(new LongPoint("age", 35L));
    doc2.add(new StoredField("age", 35L));
    writer.addDocument(doc2);

    // Document 3: age = 45
    Document doc3 = new Document();
    doc3.add(new StringField("path", "/person3", Field.Store.YES));
    doc3.add(new LongPoint("age", 45L));
    doc3.add(new StoredField("age", 45L));
    writer.addDocument(doc3);

    writer.commit();
    writer.close();
    directory.close();

    NodeState root = builder.getNodeState();

    // Create index and tracker
    LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
    tracker.update(root);

    LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

    // Create filter for: age > 30
    Filter filter = mock(Filter.class);
    when(filter.getFullTextConstraint()).thenReturn(null);
    PropertyValue pv30 = PropertyValues.newLong(30L);
    PropertyRestriction pr = new PropertyRestriction();
    pr.propertyName = "age";
    pr.first = pv30;
    pr.firstIncluding = false;  // exclusive: >
    when(filter.getPropertyRestrictions()).thenReturn(Collections.singletonList(pr));
    when(filter.getQueryLimits()).thenReturn(null);

    // Execute query
    Cursor cursor = index.query(filter, root);

    // Should return person2 (35) and person3 (45), not person1 (25)
    assertTrue("Should find results", cursor.hasNext());
    List<String> paths = new ArrayList<>();
    while (cursor.hasNext()) {
        paths.add(cursor.next().getPath());
    }

    assertEquals("Should find 2 results", 2, paths.size());
    assertTrue("Should contain /person2", paths.contains("/person2"));
    assertTrue("Should contain /person3", paths.contains("/person3"));
    assertFalse("Should not contain /person1", paths.contains("/person1"));
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=LuceneNgIndexTest#testNumericRangeQuery`
Expected: FAIL with "No supported constraint found" or similar

**Step 3: Implement range query support in buildQuery()**

Update `LuceneNgIndex.java` `buildQuery()` method to handle range queries:

```java
private Query buildQuery(Filter filter) {
    FullTextExpression ft = filter.getFullTextConstraint();

    // Handle full-text queries
    if (ft != null) {
        Analyzer analyzer = new StandardAnalyzer();
        Query ftQuery = getFullTextQuery(ft, analyzer);
        LOG.debug("Building full-text query: {}", ftQuery);

        // Combine with property restrictions if present
        List<PropertyRestriction> propRestrictions = filter.getPropertyRestrictions();
        if (!propRestrictions.isEmpty()) {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(ftQuery, Occur.MUST);
            for (PropertyRestriction pr : propRestrictions) {
                Query propQuery = createPropertyQuery(pr);
                if (propQuery != null) {
                    bq.add(propQuery, Occur.MUST);
                }
            }
            return bq.build();
        }
        return ftQuery;
    }

    // Handle property restriction queries only
    List<PropertyRestriction> propRestrictions = filter.getPropertyRestrictions();
    if (propRestrictions.isEmpty()) {
        throw new IllegalArgumentException("No supported constraint found");
    }

    if (propRestrictions.size() == 1) {
        return createPropertyQuery(propRestrictions.get(0));
    }

    // Multiple property restrictions - combine with AND
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (PropertyRestriction pr : propRestrictions) {
        Query propQuery = createPropertyQuery(pr);
        if (propQuery != null) {
            bq.add(propQuery, Occur.MUST);
        }
    }
    return bq.build();
}

/**
 * Creates a Lucene Query for a property restriction.
 * Handles equality, range, NOT, and IN queries.
 * Based on legacy LuceneIndex pattern.
 */
private Query createPropertyQuery(PropertyRestriction pr) {
    String propertyName = pr.propertyName;

    // Skip special properties
    if (propertyName.startsWith("rep:") || propertyName.startsWith("oak:")) {
        return null;
    }

    // Determine property type from first/last/not value
    int propertyType = determinePropertyType(pr);

    switch (propertyType) {
        case PropertyType.LONG:
            return createLongQuery(propertyName, pr);
        case PropertyType.DOUBLE:
            return createDoubleQuery(propertyName, pr);
        case PropertyType.DATE:
            return createDateQuery(propertyName, pr);
        case PropertyType.BOOLEAN:
            return createBooleanQuery(propertyName, pr);
        default:
            return createStringQuery(propertyName, pr);
    }
}

private int determinePropertyType(PropertyRestriction pr) {
    PropertyValue value = pr.first != null ? pr.first :
                          (pr.last != null ? pr.last : pr.not);
    if (value == null) {
        return PropertyType.STRING;
    }
    return value.getType().tag();
}

private Query createLongQuery(String propertyName, PropertyRestriction pr) {
    Long first = pr.first != null ? pr.first.getValue(Type.LONG) : null;
    Long last = pr.last != null ? pr.last.getValue(Type.LONG) : null;
    Long not = pr.not != null ? pr.not.getValue(Type.LONG) : null;

    if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
        // Equality: age = 25
        return LongPoint.newExactQuery(propertyName, first);
    } else if (pr.first != null && pr.last != null) {
        // Range with both bounds: age BETWEEN 10 AND 100
        long lowerValue = pr.firstIncluding ? first : Math.addExact(first, 1);
        long upperValue = pr.lastIncluding ? last : Math.addExact(last, -1);
        return LongPoint.newRangeQuery(propertyName, lowerValue, upperValue);
    } else if (pr.first != null) {
        // Lower bound only: age >= 25 or age > 25
        long lowerValue = pr.firstIncluding ? first : Math.addExact(first, 1);
        return LongPoint.newRangeQuery(propertyName, lowerValue, Long.MAX_VALUE);
    } else if (pr.last != null) {
        // Upper bound only: age <= 50 or age < 50
        long upperValue = pr.lastIncluding ? last : Math.addExact(last, -1);
        return LongPoint.newRangeQuery(propertyName, Long.MIN_VALUE, upperValue);
    } else if (pr.list != null) {
        // IN query: age IN (10, 20, 30)
        long[] values = pr.list.stream()
            .map(pv -> pv.getValue(Type.LONG))
            .mapToLong(Long::longValue)
            .toArray();
        return LongPoint.newSetQuery(propertyName, values);
    } else if (pr.isNot && not != null) {
        // NOT equal: age != 25
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new MatchAllDocsQuery(), Occur.MUST);
        bq.add(LongPoint.newExactQuery(propertyName, not), Occur.MUST_NOT);
        return bq.build();
    }

    throw new IllegalArgumentException("Unsupported property restriction: " + pr);
}

private Query createDoubleQuery(String propertyName, PropertyRestriction pr) {
    Double first = pr.first != null ? pr.first.getValue(Type.DOUBLE) : null;
    Double last = pr.last != null ? pr.last.getValue(Type.DOUBLE) : null;
    Double not = pr.not != null ? pr.not.getValue(Type.DOUBLE) : null;

    if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
        return DoublePoint.newExactQuery(propertyName, first);
    } else if (pr.first != null && pr.last != null) {
        double lowerValue = pr.firstIncluding ? first : Math.nextUp(first);
        double upperValue = pr.lastIncluding ? last : Math.nextDown(last);
        return DoublePoint.newRangeQuery(propertyName, lowerValue, upperValue);
    } else if (pr.first != null) {
        double lowerValue = pr.firstIncluding ? first : Math.nextUp(first);
        return DoublePoint.newRangeQuery(propertyName, lowerValue, Double.MAX_VALUE);
    } else if (pr.last != null) {
        double upperValue = pr.lastIncluding ? last : Math.nextDown(last);
        return DoublePoint.newRangeQuery(propertyName, -Double.MAX_VALUE, upperValue);
    } else if (pr.list != null) {
        double[] values = pr.list.stream()
            .map(pv -> pv.getValue(Type.DOUBLE))
            .mapToDouble(Double::doubleValue)
            .toArray();
        return DoublePoint.newSetQuery(propertyName, values);
    } else if (pr.isNot && not != null) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new MatchAllDocsQuery(), Occur.MUST);
        bq.add(DoublePoint.newExactQuery(propertyName, not), Occur.MUST_NOT);
        return bq.build();
    }

    throw new IllegalArgumentException("Unsupported property restriction: " + pr);
}

private Query createDateQuery(String propertyName, PropertyRestriction pr) {
    // Dates are stored as Long (milliseconds since epoch)
    Long first = pr.first != null ? parseDateToMillis(pr.first) : null;
    Long last = pr.last != null ? parseDateToMillis(pr.last) : null;
    Long not = pr.not != null ? parseDateToMillis(pr.not) : null;

    PropertyRestriction longPr = new PropertyRestriction();
    longPr.propertyName = propertyName;
    longPr.first = first != null ? PropertyValues.newLong(first) : null;
    longPr.last = last != null ? PropertyValues.newLong(last) : null;
    longPr.not = not != null ? PropertyValues.newLong(not) : null;
    longPr.firstIncluding = pr.firstIncluding;
    longPr.lastIncluding = pr.lastIncluding;
    longPr.isNot = pr.isNot;
    longPr.list = pr.list != null ?
        pr.list.stream().map(this::parseDateToMillis)
            .map(PropertyValues::newLong).collect(Collectors.toList()) : null;

    return createLongQuery(propertyName, longPr);
}

private Long parseDateToMillis(PropertyValue pv) {
    String dateStr = pv.getValue(Type.DATE);
    try {
        return ISO8601.parse(dateStr).getTimeInMillis();
    } catch (Exception e) {
        LOG.error("Failed to parse date: " + dateStr, e);
        return 0L;
    }
}

private Query createBooleanQuery(String propertyName, PropertyRestriction pr) {
    Boolean first = pr.first != null ? pr.first.getValue(Type.BOOLEAN) : null;
    Boolean not = pr.not != null ? pr.not.getValue(Type.BOOLEAN) : null;

    if (pr.first != null && pr.first.equals(pr.last)) {
        // Equality: isActive = true
        String value = first.toString();
        return new TermQuery(new Term(propertyName, value));
    } else if (pr.isNot && not != null) {
        // NOT equal: isActive != true
        String value = not.toString();
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new MatchAllDocsQuery(), Occur.MUST);
        bq.add(new TermQuery(new Term(propertyName, value)), Occur.MUST_NOT);
        return bq.build();
    }

    throw new IllegalArgumentException("Unsupported boolean restriction: " + pr);
}

private Query createStringQuery(String propertyName, PropertyRestriction pr) {
    String first = pr.first != null ? pr.first.getValue(Type.STRING) : null;
    String last = pr.last != null ? pr.last.getValue(Type.STRING) : null;
    String not = pr.not != null ? pr.not.getValue(Type.STRING) : null;

    if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
        // Equality: title = 'Oak'
        return new TermQuery(new Term(propertyName, first));
    } else if (pr.first != null && pr.last != null) {
        // String range (lexicographic): title BETWEEN 'A' AND 'Z'
        return new TermRangeQuery(propertyName,
            new BytesRef(first), new BytesRef(last),
            pr.firstIncluding, pr.lastIncluding);
    } else if (pr.first != null) {
        // Lower bound: title >= 'M'
        return new TermRangeQuery(propertyName,
            new BytesRef(first), null, pr.firstIncluding, true);
    } else if (pr.last != null) {
        // Upper bound: title <= 'Z'
        return new TermRangeQuery(propertyName,
            null, new BytesRef(last), true, pr.lastIncluding);
    } else if (pr.list != null) {
        // IN query: title IN ('Oak', 'Pine', 'Elm')
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (PropertyValue pv : pr.list) {
            String value = pv.getValue(Type.STRING);
            bq.add(new TermQuery(new Term(propertyName, value)), Occur.SHOULD);
        }
        return bq.build();
    } else if (pr.isNot && not != null) {
        // NOT equal: title != 'Draft'
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new MatchAllDocsQuery(), Occur.MUST);
        bq.add(new TermQuery(new Term(propertyName, not)), Occur.MUST_NOT);
        return bq.build();
    }

    throw new IllegalArgumentException("Unsupported string restriction: " + pr);
}
```

**Step 4: Add required imports**

Add to top of `LuceneNgIndex.java`:

```java
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;

import javax.jcr.PropertyType;
import java.util.stream.Collectors;
```

**Step 5: Update indexing to support numeric fields**

Modify `LuceneNgIndexEditor.indexNode()` to handle numeric types:

```java
private void indexNode(NodeState node) throws IOException {
    Document doc = new Document();

    // Add path as stored field
    doc.add(new StringField("path", path, Field.Store.YES));

    // Index all properties
    for (PropertyState prop : node.getProperties()) {
        String propName = prop.getName();

        // Skip hidden properties (start with ':')
        if (propName.startsWith(":")) {
            continue;
        }

        // Handle different property types
        switch (prop.getType().tag()) {
            case PropertyType.LONG:
                if (!prop.isArray()) {
                    long value = prop.getValue(Type.LONG);
                    doc.add(new LongPoint(propName, value));
                    doc.add(new StoredField(propName, value));
                }
                break;

            case PropertyType.DOUBLE:
                if (!prop.isArray()) {
                    double value = prop.getValue(Type.DOUBLE);
                    doc.add(new DoublePoint(propName, value));
                    doc.add(new StoredField(propName, value));
                }
                break;

            case PropertyType.DATE:
                if (!prop.isArray()) {
                    String dateStr = prop.getValue(Type.DATE);
                    try {
                        long millis = ISO8601.parse(dateStr).getTimeInMillis();
                        doc.add(new LongPoint(propName, millis));
                        doc.add(new StoredField(propName, millis));
                    } catch (Exception e) {
                        LOG.error("Failed to parse date: " + dateStr, e);
                    }
                }
                break;

            case PropertyType.BOOLEAN:
                if (!prop.isArray()) {
                    boolean value = prop.getValue(Type.BOOLEAN);
                    doc.add(new StringField(propName, String.valueOf(value), Field.Store.NO));
                }
                break;

            case PropertyType.STRING:
                String value = prop.getValue(Type.STRING);
                if (value.length() < 32000) {
                    doc.add(new StringField(propName, value, Field.Store.NO));
                }
                doc.add(new TextField(FieldNames.FULLTEXT, value, Field.Store.NO));
                LOG.trace("Indexed property: {} = {}", propName, value);
                break;

            case PropertyType.STRINGS:
                for (String strValue : prop.getValue(Type.STRINGS)) {
                    if (strValue.length() < 32000) {
                        doc.add(new StringField(propName, strValue, Field.Store.NO));
                    }
                    doc.add(new TextField(FieldNames.FULLTEXT, strValue, Field.Store.NO));
                }
                break;
        }
    }

    // Only add document if it has indexed fields
    if (doc.getFields().size() > 1) { // More than just path field
        indexWriter.addDocument(doc);
        LOG.debug("Indexed node at path: {}", path);
    }
}
```

**Step 6: Add imports to LuceneNgIndexEditor**

```java
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.jackrabbit.util.ISO8601;

import javax.jcr.PropertyType;
```

**Step 7: Run test to verify it passes**

Run: `mvn test -Dtest=LuceneNgIndexTest#testNumericRangeQuery`
Expected: PASS

**Step 8: Run all tests to ensure nothing broke**

Run: `mvn test`
Expected: All tests pass

---

## Task 2: Add More Range Query Tests

**Files to modify:**
- `src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexTest.java`

Add comprehensive test coverage for all range query types:

```java
@Test
public void testStringRangeQuery() throws Exception {
    // Test string range: title >= 'M'
    // Index documents with titles: "Apple", "Banana", "Orange", "Zebra"
    // Query: title >= 'M'
    // Should return: "Orange", "Zebra"
}

@Test
public void testDoubleRangeQuery() throws Exception {
    // Test double range: price BETWEEN 10.0 AND 50.0
    // Index documents with prices: 5.99, 25.50, 75.00
    // Should return: 25.50
}

@Test
public void testDateRangeQuery() throws Exception {
    // Test date range: publishDate > '2023-01-01'
    // Index documents with dates: 2022-12-31, 2023-06-15, 2024-01-01
    // Should return: 2023-06-15, 2024-01-01
}

@Test
public void testNotQuery() throws Exception {
    // Test NOT query: status != 'draft'
    // Index documents with status: "draft", "published", "archived"
    // Should return: "published", "archived"
}

@Test
public void testInQuery() throws Exception {
    // Test IN query: category IN ('tech', 'science')
    // Index documents with categories: "tech", "sports", "science", "arts"
    // Should return: "tech", "science"
}

@Test
public void testComplexBooleanQuery() throws Exception {
    // Test: (title CONTAINS 'oak') AND (status = 'published') AND (age > 25)
    // Should combine full-text + property equality + numeric range
}
```

---

## Task 3: Update Cost Estimation

**Files to modify:**
- `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndex.java`

**Step 1: Improve getCost() to favor property indexes**

```java
@Override
public double getCost(Filter filter, NodeState rootState) {
    FullTextExpression ft = filter.getFullTextConstraint();
    List<PropertyRestriction> propRestrictions = filter.getPropertyRestrictions();

    // If we have both full-text and property restrictions, lower cost
    if (ft != null && !propRestrictions.isEmpty()) {
        return 1.5; // Very selective
    }

    // Full-text only
    if (ft != null) {
        return 2.0;
    }

    // Check for property restrictions we can handle
    int supportedRestrictions = 0;
    for (PropertyRestriction pr : propRestrictions) {
        if (canHandleRestriction(pr)) {
            supportedRestrictions++;
        }
    }

    if (supportedRestrictions > 0) {
        // More restrictions = more selective = lower cost
        return 2.0 / Math.sqrt(supportedRestrictions);
    }

    return Double.POSITIVE_INFINITY;
}

private boolean canHandleRestriction(PropertyRestriction pr) {
    // Skip special properties
    if (pr.propertyName.startsWith("rep:") || pr.propertyName.startsWith("oak:")) {
        return false;
    }
    // Can handle equality, range, NOT, and IN queries
    return pr.first != null || pr.last != null || pr.not != null || pr.list != null;
}
```

---

## Verification

**Test execution:**
1. Run all new tests: `mvn test -Dtest=LuceneNgIndexTest`
2. Run all tests in module: `mvn test`
3. Verify all 53+ tests pass

**Manual verification:**
1. Check that range queries work for all types (Long, Double, Date, String)
2. Check that NOT queries exclude correct documents
3. Check that IN queries match multiple values
4. Check that complex boolean combinations work correctly
5. Check that cost estimation favors selective queries

---

## Success Criteria

✅ All range query types working (>, >=, <, <=, BETWEEN)
✅ NOT queries working (!=)
✅ IN queries working (IN list)
✅ Complex boolean combinations (full-text + properties)
✅ All property types supported (String, Long, Double, Date, Boolean)
✅ Cost estimation improved
✅ All tests passing (60+ tests expected)

---

## Notes

- **Lucene 9 Changes:** Legacy Lucene 4.7 used `NumericRangeQuery`, Lucene 9 uses `LongPoint/DoublePoint.newRangeQuery()`
- **NOT Query Pattern:** Use `BooleanQuery.Builder().add(MatchAllDocsQuery(), MUST).add(term, MUST_NOT)`
- **Property Types:** Follow Oak's Type system (Type.LONG, Type.DOUBLE, Type.DATE, etc.)
- **Date Handling:** Dates stored as Long (milliseconds), parsed with ISO8601.parse()
- **String Ranges:** Use TermRangeQuery with BytesRef for lexicographic sorting

Generated-by: Claude Sonnet 4.5 (Anthropic)
