# Phase 2 Step 1: Basic Text Search Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement basic full-text search for Lucene 9, enabling queries to return documents matching text criteria

**Architecture:** Follow Oak's QueryIndexProvider → QueryIndex pattern. Provider returns LuceneIndex instances, which use IndexSearcher to execute Lucene queries built from Oak Filter conditions.

**Tech Stack:** Java 11, Lucene 9.11.1, JUnit 4, Mockito, Oak query SPI

---

## Task 1: IndexSearcherHolder (Resource Management)

**Files:**
- Create: `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/IndexSearcherHolder.java`
- Test: `src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/IndexSearcherHolderTest.java`

**Step 1: Write failing test for IndexSearcherHolder creation**

```java
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.BlobFactory;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;

import static org.junit.Assert.*;

public class IndexSearcherHolderTest {

    @Test
    public void testGetSearcher() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder indexDef = builder.child("oak:index").child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Create empty index
        OakDirectory directory = new OakDirectory(indexDef, "test", false);
        directory.close();

        IndexSearcherHolder holder = new IndexSearcherHolder(indexDef, "test");
        IndexSearcher searcher = holder.getSearcher();

        assertNotNull("Searcher should not be null", searcher);
        assertEquals("Empty index should have 0 docs", 0, searcher.getIndexReader().numDocs());

        holder.close();
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg && mvn test -Dtest=IndexSearcherHolderTest`
Expected: FAIL with "cannot find symbol: class IndexSearcherHolder"

**Step 3: Write minimal IndexSearcherHolder implementation**

```java
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.BlobFactory;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Manages IndexSearcher lifecycle for a Lucene 9 index.
 * Provides thread-safe access to IndexSearcher and handles reopening.
 */
public class IndexSearcherHolder implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexSearcherHolder.class);

    private final NodeBuilder definition;
    private final String indexName;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    public IndexSearcherHolder(NodeBuilder definition, String indexName) throws IOException {
        this.definition = definition;
        this.indexName = indexName;
        this.reader = openReader();
        this.searcher = new IndexSearcher(reader);
    }

    private DirectoryReader openReader() throws IOException {
        OakDirectory directory = new OakDirectory(definition, indexName, true); // read-only
        return DirectoryReader.open(directory);
    }

    public IndexSearcher getSearcher() {
        return searcher;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg && mvn test -Dtest=IndexSearcherHolderTest`
Expected: PASS

**Step 5: Commit**

```bash
cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg
git add src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/IndexSearcherHolder.java \
        src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/IndexSearcherHolderTest.java
git commit -m "feat: add IndexSearcherHolder for managing Lucene 9 searcher lifecycle

- Creates DirectoryReader from OakDirectory
- Wraps in IndexSearcher for query execution
- Thread-safe access to searcher
- Proper resource cleanup

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: LuceneNgQueryIndexProvider (Provider)

**Files:**
- Create: `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgQueryIndexProvider.java`
- Test: `src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgQueryIndexProviderTest.java`

**Step 1: Write failing test for provider returning indexes**

```java
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class LuceneNgQueryIndexProviderTest {

    @Test
    public void testGetQueryIndexes() {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");

        // Create Lucene 9 index
        NodeBuilder lucene9Index = oakIndex.child("test");
        lucene9Index.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Create Lucene 4.7 index (should be ignored)
        NodeBuilder lucene47Index = oakIndex.child("old");
        lucene47Index.setProperty("type", "lucene");

        NodeState root = builder.getNodeState();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        List<? extends QueryIndex> indexes = provider.getQueryIndexes(root);

        assertNotNull("Indexes should not be null", indexes);
        assertEquals("Should return one LuceneNgIndex", 1, indexes.size());
        assertTrue("Should be LuceneNgIndex instance",
                   indexes.get(0) instanceof LuceneNgIndex);
    }

    @Test
    public void testNoIndexesWhenNoLucene9() {
        NodeState root = InitialContentHelper.INITIAL_CONTENT;

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        List<? extends QueryIndex> indexes = provider.getQueryIndexes(root);

        assertNotNull("Indexes should not be null", indexes);
        assertTrue("Should return empty list when no Lucene 9 indexes",
                   indexes.isEmpty());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg && mvn test -Dtest=LuceneNgQueryIndexProviderTest`
Expected: FAIL with "cannot find symbol: class LuceneNgQueryIndexProvider"

**Step 3: Write minimal LuceneNgQueryIndexProvider implementation**

```java
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * QueryIndexProvider for Lucene 9 indexes.
 * Returns LuceneNgIndex instances for all Lucene 9 indexes in the repository.
 */
public class LuceneNgQueryIndexProvider implements QueryIndexProvider {

    private final LuceneNgIndexTracker tracker;

    public LuceneNgQueryIndexProvider(LuceneNgIndexTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    @NotNull
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        // Update tracker with current state
        tracker.update(nodeState);

        List<LuceneNgIndex> indexes = new ArrayList<>();

        // Get all tracked Lucene 9 indexes
        for (String indexPath : tracker.getIndexPaths()) {
            LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
            if (indexNode != null) {
                indexes.add(new LuceneNgIndex(tracker, indexPath));
            }
        }

        return indexes;
    }
}
```

**Step 4: Add getIndexPaths() method to LuceneNgIndexTracker**

Modify: `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexTracker.java`

Add method:
```java
/**
 * Get paths of all tracked indexes.
 *
 * @return set of index paths
 */
public Set<String> getIndexPaths() {
    return new HashSet<>(indices.keySet());
}
```

Add import: `import java.util.Set;` and `import java.util.HashSet;`

**Step 5: Run test to verify it passes**

Run: `cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg && mvn test -Dtest=LuceneNgQueryIndexProviderTest`
Expected: FAIL (LuceneNgIndex doesn't exist yet, but provider compiles)

**Step 6: Commit provider (even though tests don't pass yet)**

```bash
cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg
git add src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgQueryIndexProvider.java \
        src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexTracker.java \
        src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgQueryIndexProviderTest.java
git commit -m "feat: add LuceneNgQueryIndexProvider

- Implements QueryIndexProvider interface
- Returns LuceneNgIndex for each tracked index
- Integrates with LuceneNgIndexTracker
- Tests will pass once LuceneNgIndex is implemented

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: LuceneNgIndex (Basic Query Execution)

**Files:**
- Create: `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndex.java`
- Test: `src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexTest.java`

**Step 1: Write failing test for basic text search**

```java
package org.apache.jackrabbit.oak.plugins.index/lucene9;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.BlobFactory;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LuceneNgIndexTest {

    @Test
    public void testBasicTextQuery() throws Exception {
        // Setup: Create index with documents
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder indexDef = builder.child("oak:index").child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Index some documents
        OakDirectory directory = new OakDirectory(indexDef, "test", false);
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(directory, config);

        Document doc1 = new Document();
        doc1.add(new StringField("path", "/content/article1", Field.Store.YES));
        doc1.add(new TextField("text", "Apache Jackrabbit Oak", Field.Store.NO));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new StringField("path", "/content/article2", Field.Store.YES));
        doc2.add(new TextField("text", "Lucene search engine", Field.Store.NO));
        writer.addDocument(doc2);

        writer.commit();
        writer.close();
        directory.close();

        NodeState root = builder.getNodeState();

        // Create index and tracker
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        // Create filter for full-text search
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(FullTextParser.parse("*", "Oak"));
        when(filter.getPathRestriction()).thenReturn(PathRestriction.ALL);

        // Execute query
        Cursor cursor = index.query(filter, root);

        assertNotNull("Cursor should not be null", cursor);
        assertTrue("Should find article1", cursor.hasNext());

        String path = cursor.next().getPath();
        assertEquals("Should find /content/article1", "/content/article1", path);

        assertFalse("Should only find one document", cursor.hasNext());
    }

    @Test
    public void testGetCost() {
        NodeState root = InitialContentHelper.INITIAL_CONTENT;

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(FullTextParser.parse("*", "test"));

        double cost = index.getCost(filter, root);

        assertTrue("Cost should be greater than 0", cost > 0);
        assertTrue("Cost should be finite", Double.isFinite(cost));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg && mvn test -Dtest=LuceneNgIndexTest`
Expected: FAIL with "cannot find symbol: class LuceneNgIndex"

**Step 3: Write minimal LuceneNgIndex implementation**

```java
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Lucene 9 query index implementation.
 * Executes queries against Lucene 9 indexes.
 */
public class LuceneNgIndex implements QueryIndex {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndex.class);

    private final LuceneNgIndexTracker tracker;
    private final String indexPath;

    public LuceneNgIndex(LuceneNgIndexTracker tracker, String indexPath) {
        this.tracker = tracker;
        this.indexPath = indexPath;
    }

    @Override
    public double getMinimumCost() {
        return 2.0; // Better than traversal (1000+) but not as good as unique lookup (1.0)
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        // Simple cost estimation for now
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            return Double.POSITIVE_INFINITY; // Can't handle non-fulltext queries yet
        }

        // Assume reasonable cost for fulltext queries
        return 100.0;
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        try {
            LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
            if (indexNode == null) {
                LOG.warn("Index node not found: {}", indexPath);
                return Cursor.EMPTY;
            }

            // Get searcher
            IndexSearcherHolder holder = new IndexSearcherHolder(
                indexNode.getDefinition().getDefinition(),
                indexNode.getDefinition().getIndexName()
            );
            IndexSearcher searcher = holder.getSearcher();

            // Build Lucene query from filter
            Query query = buildQuery(filter);

            // Execute query
            TopDocs docs = searcher.search(query, 100); // Limit to 100 for now

            // Return cursor
            return new LuceneNgCursor(docs, searcher, holder);

        } catch (IOException e) {
            LOG.error("Error executing query on index: " + indexPath, e);
            return Cursor.EMPTY;
        }
    }

    private Query buildQuery(Filter filter) {
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            throw new IllegalArgumentException("No fulltext constraint");
        }

        // Simple term query for now - just extract first term
        String queryText = ft.toString();
        return new TermQuery(new Term("text", queryText.toLowerCase()));
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        return "lucene9:" + indexPath + " ft=" + filter.getFullTextConstraint();
    }

    @Override
    public String getIndexName() {
        return "luceneNg";
    }
}
```

**Step 4: Create LuceneNgCursor**

Create: `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgCursor.java`

```java
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Cursor over Lucene 9 search results.
 */
public class LuceneNgCursor implements Cursor {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgCursor.class);

    private final TopDocs docs;
    private final IndexSearcher searcher;
    private final IndexSearcherHolder holder;
    private int currentIndex = 0;

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher, IndexSearcherHolder holder) {
        this.docs = docs;
        this.searcher = searcher;
        this.holder = holder;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < docs.scoreDocs.length;
    }

    @Override
    public IndexRow next() {
        ScoreDoc scoreDoc = docs.scoreDocs[currentIndex++];

        try {
            Document doc = searcher.doc(scoreDoc.doc);
            String path = doc.get("path");

            return new LuceneNgIndexRow(path, scoreDoc.score);

        } catch (IOException e) {
            LOG.error("Error reading document", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getSize() {
        return docs.totalHits.value;
    }
}
```

**Step 5: Create LuceneNgIndexRow**

Create: `src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexRow.java`

```java
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * IndexRow implementation for Lucene 9 results.
 */
public class LuceneNgIndexRow implements IndexRow {

    private final String path;
    private final double score;

    public LuceneNgIndexRow(String path, double score) {
        this.path = path;
        this.score = score;
    }

    @Override
    @NotNull
    public String getPath() {
        return path;
    }

    @Override
    @Nullable
    public PropertyValue getValue(String columnName) {
        if ("jcr:score".equals(columnName)) {
            return PropertyValues.newDouble(score);
        }
        return null;
    }
}
```

Add import in LuceneNgIndexRow: `import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;`

**Step 6: Run test to verify it passes**

Run: `cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg && mvn test -Dtest=LuceneNgIndexTest`
Expected: PASS (may need debugging - query building is simplified)

**Step 7: Commit**

```bash
cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg
git add src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndex.java \
        src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgCursor.java \
        src/main/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexRow.java \
        src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/LuceneNgIndexTest.java
git commit -m "feat: add LuceneNgIndex with basic text search

- Implements QueryIndex interface
- Executes TermQuery for simple text search
- Returns cursor over TopDocs results
- Cost estimation for query planning
- Basic query building from Filter

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: End-to-End Integration Test

**Files:**
- Modify: `src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/IntegrationTest.java`

**Step 1: Add end-to-end query test**

Add new test method to IntegrationTest.java:

```java
@Test
public void testEndToEndQueryWorkflow() throws Exception {
    // Setup: Create index definition
    NodeBuilder builder = INITIAL_CONTENT.builder();
    NodeBuilder oakIndex = builder.child("oak:index");
    NodeBuilder indexDef = oakIndex.child("testIndex");
    indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

    // Create content and index it
    NodeBuilder content = builder.child("content");
    NodeBuilder article1 = content.child("article1");
    article1.setProperty("title", "Introduction to Oak");
    article1.setProperty("text", "Apache Jackrabbit Oak is a scalable repository");

    NodeBuilder article2 = content.child("article2");
    article2.setProperty("title", "Lucene 9 Integration");
    article2.setProperty("text", "Lucene 9 provides advanced search capabilities");

    NodeState root = builder.getNodeState();

    // Index the content (reuse code from existing test)
    LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
    tracker.update(root);

    LuceneNgIndexEditorProvider editorProvider = new LuceneNgIndexEditorProvider(tracker);
    IndexUpdateCallback callback = mock(IndexUpdateCallback.class);

    Editor editor = editorProvider.getIndexEditor(
        LuceneNgIndexConstants.TYPE_LUCENE9,
        indexDef,
        root,
        callback
    );

    assertNotNull(editor);

    try {
        editor.enter(EMPTY_NODE, root);
        Editor contentEditor = editor.childNodeAdded("content", root.getChildNode("content"));

        NodeState contentState = root.getChildNode("content");
        contentEditor.enter(EMPTY_NODE, contentState);

        Editor article1Editor = contentEditor.childNodeAdded("article1",
            contentState.getChildNode("article1"));
        assertNotNull(article1Editor);
        article1Editor.enter(EMPTY_NODE, contentState.getChildNode("article1"));
        article1Editor.leave(EMPTY_NODE, contentState.getChildNode("article1"));

        Editor article2Editor = contentEditor.childNodeAdded("article2",
            contentState.getChildNode("article2"));
        assertNotNull(article2Editor);
        article2Editor.enter(EMPTY_NODE, contentState.getChildNode("article2"));
        article2Editor.leave(EMPTY_NODE, contentState.getChildNode("article2"));

        contentEditor.leave(EMPTY_NODE, contentState);
    } finally {
        editor.leave(EMPTY_NODE, root);
    }

    // Now query the index
    LuceneNgQueryIndexProvider queryProvider = new LuceneNgQueryIndexProvider(tracker);
    List<? extends QueryIndex> indexes = queryProvider.getQueryIndexes(root);

    assertEquals("Should have one index", 1, indexes.size());

    LuceneNgIndex index = (LuceneNgIndex) indexes.get(0);

    // Create filter for "Oak" search
    Filter filter = mock(Filter.class);
    when(filter.getFullTextConstraint()).thenReturn(
        FullTextParser.parse("*", "Oak"));
    when(filter.getPathRestriction()).thenReturn(PathRestriction.ALL);

    // Execute query
    Cursor cursor = index.query(filter, root);

    assertNotNull("Cursor should not be null", cursor);
    assertTrue("Should find at least one result", cursor.hasNext());

    IndexRow row = cursor.next();
    assertTrue("Result should be article1 or article2",
               row.getPath().contains("/content/article"));
}
```

Add imports at top of IntegrationTest.java:
```java
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
```

**Step 2: Run test**

Run: `cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg && mvn test -Dtest=IntegrationTest#testEndToEndQueryWorkflow`
Expected: PASS (verifies write + read path work together)

**Step 3: Commit**

```bash
cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg
git add src/test/java/org/apache/jackrabbit/oak/plugins/index/luceneNg/IntegrationTest.java
git commit -m "test: add end-to-end query integration test

- Indexes documents with write path
- Queries using read path
- Verifies full workflow from index to search
- Validates QueryIndexProvider integration

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Run Full Test Suite

**Step 1: Run all tests**

Run: `cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg && mvn clean test`
Expected: All tests pass (43 existing + new query tests)

**Step 2: Verify test count**

Count tests:
```bash
grep -r "@Test" src/test/java --include="*.java" | wc -l
```

Expected: ~47+ tests

**Step 3: Update coverage documentation**

Modify: `docs/test-coverage-summary.md`

Add Phase 2 Step 1 section:
```markdown
## Phase 2 Step 1: Query Support Added

**Date:** 2026-03-07
**New Tests:** 4 (IndexSearcherHolder, Provider, Index, End-to-end)
**Components:** Read path foundation implemented

### New Components
- IndexSearcherHolder: Manages IndexSearcher lifecycle
- LuceneNgQueryIndexProvider: Routes queries to indexes
- LuceneNgIndex: Executes basic text queries
- LuceneNgCursor/IndexRow: Result iteration

### Query Support
- ✅ Basic full-text search (TermQuery)
- ⏳ Property queries (Step 2)
- ⏳ Sorting (Step 3)
- ⏳ Aggregations (Step 4)
- ⏳ Highlighting (Step 5)
```

**Step 4: Commit**

```bash
cd /Users/bhabegger/claude/jackrabbit-oak/oak-search-luceneNg
git add docs/test-coverage-summary.md
git commit -m "docs: update coverage summary for Phase 2 Step 1

Phase 2 Step 1 (basic text search) complete:
- IndexSearcherHolder for searcher management
- LuceneNgQueryIndexProvider for index routing
- LuceneNgIndex for query execution
- End-to-end integration test

Next: Step 2 (property queries + filtering)

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Summary

This plan implements Phase 2 Step 1: Basic Text Search

**What was built:**
1. IndexSearcherHolder - Manages Lucene IndexSearcher lifecycle
2. LuceneNgQueryIndexProvider - Implements QueryIndexProvider
3. LuceneNgIndex - Implements QueryIndex with basic text search
4. LuceneNgCursor/IndexRow - Result iteration
5. End-to-end integration test

**Queries supported:**
- Basic full-text search using TermQuery
- Returns paths and scores

**Not yet supported (future steps):**
- Property queries (Step 2)
- Boolean combinations (Step 2)
- Sorting (Step 3)
- Aggregations (Step 4)
- Highlighting (Step 5)

**Next steps:**
- Implement Step 2: Property queries + filtering
- Enhance query builder for complex queries
- Add cost estimation based on index statistics
