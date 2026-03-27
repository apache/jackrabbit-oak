/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.BlobFactory;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNode;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import org.apache.jackrabbit.oak.spi.query.QueryIndex;

import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        Document doc1 = new Document();
        doc1.add(new StringField(FieldNames.PATH, "/content/article1", Field.Store.YES));
        doc1.add(new TextField(FieldNames.FULLTEXT, "Apache Jackrabbit Oak", Field.Store.NO));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new StringField(FieldNames.PATH, "/content/article2", Field.Store.YES));
        doc2.add(new TextField(FieldNames.FULLTEXT, "Lucene search engine", Field.Store.NO));
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
        when(filter.getPathRestriction()).thenReturn(PathRestriction.NO_RESTRICTION);
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getQueryLimits()).thenReturn(null);

        // Execute query
        Cursor cursor = index.query(filter, root);

        assertNotNull("Cursor should not be null", cursor);
        assertTrue("Should find article1", cursor.hasNext());

        String path = cursor.next().getPath();
        assertEquals("Should find /content/article1", "/content/article1", path);

        assertFalse("Should only find one document", cursor.hasNext());
    }

    @Test
    public void testGetCost() throws Exception {
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

    @Test
    public void testNumericRangeQuery() throws Exception {
        // Setup: Create index with numeric property
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Index documents with age property
        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        // Document 1: age = 25
        Document doc1 = new Document();
        doc1.add(new StringField(FieldNames.PATH, "/person1", Field.Store.YES));
        doc1.add(new LongPoint("age", 25L));
        doc1.add(new StoredField("age", 25L));
        writer.addDocument(doc1);

        // Document 2: age = 35
        Document doc2 = new Document();
        doc2.add(new StringField(FieldNames.PATH, "/person2", Field.Store.YES));
        doc2.add(new LongPoint("age", 35L));
        doc2.add(new StoredField("age", 35L));
        writer.addDocument(doc2);

        // Document 3: age = 45
        Document doc3 = new Document();
        doc3.add(new StringField(FieldNames.PATH, "/person3", Field.Store.YES));
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

    @Test
    public void testStringRangeQuery() throws Exception {
        // Test string range: title >= 'M'
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        // Add documents with different titles
        String[] titles = {"Apple", "Banana", "Orange", "Zebra"};
        String[] paths = {"/fruit1", "/fruit2", "/fruit3", "/fruit4"};

        for (int i = 0; i < titles.length; i++) {
            Document doc = new Document();
            doc.add(new StringField(FieldNames.PATH, paths[i], Field.Store.YES));
            doc.add(new StringField("title", titles[i], Field.Store.NO));
            writer.addDocument(doc);
        }

        writer.commit();
        writer.close();
        directory.close();

        NodeState root = builder.getNodeState();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        // Create filter for: title >= 'M'
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        PropertyValue pvM = PropertyValues.newString("M");
        PropertyRestriction pr = new PropertyRestriction();
        pr.propertyName = "title";
        pr.first = pvM;
        pr.firstIncluding = true;  // inclusive: >=
        when(filter.getPropertyRestrictions()).thenReturn(Collections.singletonList(pr));
        when(filter.getQueryLimits()).thenReturn(null);

        // Execute query
        Cursor cursor = index.query(filter, root);

        // Should return Orange and Zebra (>= 'M'), not Apple or Banana
        assertTrue("Should find results", cursor.hasNext());
        List<String> resultPaths = new ArrayList<>();
        while (cursor.hasNext()) {
            resultPaths.add(cursor.next().getPath());
        }

        assertEquals("Should find 2 results", 2, resultPaths.size());
        assertTrue("Should contain /fruit3 (Orange)", resultPaths.contains("/fruit3"));
        assertTrue("Should contain /fruit4 (Zebra)", resultPaths.contains("/fruit4"));
    }

    @Test
    public void testDoubleRangeQuery() throws Exception {
        // Test double range: price BETWEEN 10.0 AND 50.0
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        // Add documents with prices: 5.99, 25.50, 75.00
        Document doc1 = new Document();
        doc1.add(new StringField(FieldNames.PATH, "/product1", Field.Store.YES));
        doc1.add(new org.apache.lucene.document.DoublePoint("price", 5.99));
        doc1.add(new org.apache.lucene.document.StoredField("price", 5.99));
        writer.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new StringField(FieldNames.PATH, "/product2", Field.Store.YES));
        doc2.add(new org.apache.lucene.document.DoublePoint("price", 25.50));
        doc2.add(new org.apache.lucene.document.StoredField("price", 25.50));
        writer.addDocument(doc2);

        Document doc3 = new Document();
        doc3.add(new StringField(FieldNames.PATH, "/product3", Field.Store.YES));
        doc3.add(new org.apache.lucene.document.DoublePoint("price", 75.00));
        doc3.add(new org.apache.lucene.document.StoredField("price", 75.00));
        writer.addDocument(doc3);

        writer.commit();
        writer.close();
        directory.close();

        NodeState root = builder.getNodeState();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        // Create filter for: 10.0 <= price <= 50.0
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        PropertyValue pv10 = PropertyValues.newDouble(10.0);
        PropertyValue pv50 = PropertyValues.newDouble(50.0);
        PropertyRestriction pr = new PropertyRestriction();
        pr.propertyName = "price";
        pr.first = pv10;
        pr.last = pv50;
        pr.firstIncluding = true;
        pr.lastIncluding = true;
        when(filter.getPropertyRestrictions()).thenReturn(Collections.singletonList(pr));
        when(filter.getQueryLimits()).thenReturn(null);

        // Execute query
        Cursor cursor = index.query(filter, root);

        // Should return only product2 (25.50)
        assertTrue("Should find results", cursor.hasNext());
        List<String> resultPaths = new ArrayList<>();
        while (cursor.hasNext()) {
            resultPaths.add(cursor.next().getPath());
        }

        assertEquals("Should find 1 result", 1, resultPaths.size());
        assertTrue("Should contain /product2", resultPaths.contains("/product2"));
    }

    @Test
    public void testNotQuery() throws Exception {
        // Test NOT query: status != 'draft'
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        // Add documents with different statuses
        String[] statuses = {"draft", "published", "archived"};
        String[] paths = {"/doc1", "/doc2", "/doc3"};

        for (int i = 0; i < statuses.length; i++) {
            Document doc = new Document();
            doc.add(new StringField(FieldNames.PATH, paths[i], Field.Store.YES));
            doc.add(new StringField("status", statuses[i], Field.Store.NO));
            writer.addDocument(doc);
        }

        writer.commit();
        writer.close();
        directory.close();

        NodeState root = builder.getNodeState();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        // Create filter for: status != 'draft'
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        PropertyValue pvDraft = PropertyValues.newString("draft");
        PropertyRestriction pr = new PropertyRestriction();
        pr.propertyName = "status";
        pr.not = pvDraft;
        pr.isNot = true;
        when(filter.getPropertyRestrictions()).thenReturn(Collections.singletonList(pr));
        when(filter.getQueryLimits()).thenReturn(null);

        // Execute query
        Cursor cursor = index.query(filter, root);

        // Should return published and archived, not draft
        assertTrue("Should find results", cursor.hasNext());
        List<String> resultPaths = new ArrayList<>();
        while (cursor.hasNext()) {
            resultPaths.add(cursor.next().getPath());
        }

        assertEquals("Should find 2 results", 2, resultPaths.size());
        assertTrue("Should contain /doc2 (published)", resultPaths.contains("/doc2"));
        assertTrue("Should contain /doc3 (archived)", resultPaths.contains("/doc3"));
        assertFalse("Should not contain /doc1 (draft)", resultPaths.contains("/doc1"));
    }

    @Test
    public void testInQuery() throws Exception {
        // Test IN query: category IN ('tech', 'science')
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        // Add documents with different categories
        String[] categories = {"tech", "sports", "science", "arts"};
        String[] paths = {"/article1", "/article2", "/article3", "/article4"};

        for (int i = 0; i < categories.length; i++) {
            Document doc = new Document();
            doc.add(new StringField(FieldNames.PATH, paths[i], Field.Store.YES));
            doc.add(new StringField("category", categories[i], Field.Store.NO));
            writer.addDocument(doc);
        }

        writer.commit();
        writer.close();
        directory.close();

        NodeState root = builder.getNodeState();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        // Create filter for: category IN ('tech', 'science')
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        PropertyRestriction pr = new PropertyRestriction();
        pr.propertyName = "category";
        pr.list = new ArrayList<>();
        pr.list.add(PropertyValues.newString("tech"));
        pr.list.add(PropertyValues.newString("science"));
        when(filter.getPropertyRestrictions()).thenReturn(Collections.singletonList(pr));
        when(filter.getQueryLimits()).thenReturn(null);

        // Execute query
        Cursor cursor = index.query(filter, root);

        // Should return tech and science
        assertTrue("Should find results", cursor.hasNext());
        List<String> resultPaths = new ArrayList<>();
        while (cursor.hasNext()) {
            resultPaths.add(cursor.next().getPath());
        }

        assertEquals("Should find 2 results", 2, resultPaths.size());
        assertTrue("Should contain /article1 (tech)", resultPaths.contains("/article1"));
        assertTrue("Should contain /article3 (science)", resultPaths.contains("/article3"));
    }

    @Test
    public void testDirectChildrenPathRestriction() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index").child("testIdx");
        oakIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        // Add index rule so the editor actually indexes these nodes
        oakIndex.child("indexRules").child("nt:unstructured").child("properties")
                .child("title").setProperty("name", "title").setProperty("propertyIndex", true);

        // Write /a, /a/b, /a/b/c, /x using the convenience constructor (definition-backed storage)
        for (String path : new String[]{"/a", "/a/b", "/a/b/c", "/x"}) {
            NodeBuilder nb = builder;
            for (String seg : path.substring(1).split("/")) {
                nb = nb.child(seg);
            }
            nb.setProperty("jcr:primaryType", "nt:unstructured");
            nb.setProperty("title", "node-at-" + path);
            LuceneNgIndexEditor ed = new LuceneNgIndexEditor(path, oakIndex, builder.getNodeState());
            ed.enter(org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE, nb.getNodeState());
            ed.leave(org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE, nb.getNodeState());
        }

        // Read back from definition-backed directory (convenience constructor uses dir name "default")
        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(oakIndex.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            // Direct children of /a should be only /a/b
            // The editor writes the parent path under LuceneNgIndexConstants.FIELD_PARENT_PATH (":parent")
            TopDocs hits = searcher.search(
                    new TermQuery(new Term(LuceneNgIndexConstants.FIELD_PARENT_PATH, "/a")), 10);
            assertEquals("Direct children of /a", 1, hits.totalHits.value);
            assertEquals("/a/b", searcher.storedFields().document(hits.scoreDocs[0].doc).get(FieldNames.PATH));
        }
    }

    @Test
    public void testAllChildrenPathRestriction() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        buildIndexWithPaths(builder, "/a", "/a/b", "/a/b/c", "/x");

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.ALL_CHILDREN);
        when(filter.getPath()).thenReturn("/a");
        when(filter.getQueryLimits()).thenReturn(null);

        Cursor cursor = index.query(filter, builder.getNodeState());
        List<String> paths = new ArrayList<>();
        while (cursor.hasNext()) {
            paths.add(cursor.next().getPath());
        }
        assertTrue("Should contain /a/b",   paths.contains("/a/b"));
        assertTrue("Should contain /a/b/c", paths.contains("/a/b/c"));
        assertFalse("Should not contain /a", paths.contains("/a"));
        assertFalse("Should not contain /x", paths.contains("/x"));
    }

    @Test
    public void testExactPathRestriction() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        buildIndexWithPaths(builder, "/a", "/a/b", "/x");

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.EXACT);
        when(filter.getPath()).thenReturn("/a");
        when(filter.getQueryLimits()).thenReturn(null);

        Cursor cursor = index.query(filter, builder.getNodeState());
        List<String> paths = new ArrayList<>();
        while (cursor.hasNext()) {
            paths.add(cursor.next().getPath());
        }
        assertEquals("Exact restriction should return exactly one result", 1, paths.size());
        assertEquals("/a", paths.get(0));
    }

    @Test
    public void testPrefixFulltextQuery() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index").child("testIdx");
        oakIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory dir = new OakDirectory(
                builder.child("oak:index").child("testIdx").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "testIdx", false);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
                new org.apache.lucene.analysis.standard.StandardAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField(FieldNames.PATH, "/content/page1", Field.Store.YES));
        doc.add(new TextField(FieldNames.FULLTEXT, "Apache Jackrabbit Oak is scalable", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        dir.close();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(
                FullTextParser.parse("*", "jackrab*"));
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.NO_RESTRICTION);
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getQueryLimits()).thenReturn(null);

        Cursor cursor = index.query(filter, builder.getNodeState());
        assertTrue("Prefix query 'jackrab*' should match node", cursor.hasNext());
        assertEquals("/content/page1", cursor.next().getPath());
    }

    @Test
    public void testWildcardFulltextQuery() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index").child("testIdx");
        oakIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory dir = new OakDirectory(
                builder.child("oak:index").child("testIdx").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "testIdx", false);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
                new org.apache.lucene.analysis.standard.StandardAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField(FieldNames.PATH, "/content/page1", Field.Store.YES));
        doc.add(new TextField(FieldNames.FULLTEXT, "jackrabbit scalable", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        dir.close();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(
                FullTextParser.parse("*", "jack*bit"));
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.NO_RESTRICTION);
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getQueryLimits()).thenReturn(null);

        Cursor cursor = index.query(filter, builder.getNodeState());
        assertTrue("Wildcard query 'jack*bit' should match node", cursor.hasNext());
        assertEquals("/content/page1", cursor.next().getPath());
    }

    @Test
    public void exclusiveUpperBoundAtLongMinValueDoesNotThrow() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        // Add a simple document
        Document doc = new Document();
        doc.add(new StringField(FieldNames.PATH, "/test", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        directory.close();

        NodeState root = builder.getNodeState();
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        // Create filter for: score < Long.MIN_VALUE (exclusive upper bound at MIN_VALUE)
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        PropertyValue pvMin = PropertyValues.newLong(Long.MIN_VALUE);
        PropertyRestriction pr = new PropertyRestriction();
        pr.propertyName = "score";
        pr.last = pvMin;
        pr.lastIncluding = false; // exclusive upper bound at MIN_VALUE — triggers nextBelow(MIN_VALUE)
        when(filter.getPropertyRestrictions()).thenReturn(Collections.singletonList(pr));
        when(filter.getQueryLimits()).thenReturn(null);

        // Should not throw ArithmeticException
        Cursor cursor = index.query(filter, root);
        assertNotNull("Cursor should not be null", cursor);
    }

    @Test
    public void exclusiveLowerBoundAtLongMaxValueDoesNotThrow() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        // Add a simple document
        Document doc = new Document();
        doc.add(new StringField(FieldNames.PATH, "/test", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        directory.close();

        NodeState root = builder.getNodeState();
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        // Create filter for: score > Long.MAX_VALUE (exclusive lower bound at MAX_VALUE)
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        PropertyValue pvMax = PropertyValues.newLong(Long.MAX_VALUE);
        PropertyRestriction pr = new PropertyRestriction();
        pr.propertyName = "score";
        pr.first = pvMax;
        pr.firstIncluding = false; // exclusive lower bound at MAX_VALUE — triggers nextAbove(MAX_VALUE)
        when(filter.getPropertyRestrictions()).thenReturn(Collections.singletonList(pr));
        when(filter.getQueryLimits()).thenReturn(null);

        // Should not throw ArithmeticException
        Cursor cursor = index.query(filter, root);
        assertNotNull("Cursor should not be null", cursor);
    }

    /**
     * Builds an index at /oak:index/testIdx/lucene9 with nodes at the given paths.
     * The index definition is at /oak:index/testIdx with type=lucene9.
     * After writing, {@code builder.getNodeState()} will contain both.
     */
    private void buildIndexWithPaths(NodeBuilder builder, String... paths) throws Exception {
        NodeBuilder oakIndex = builder.child("oak:index").child("testIdx");
        oakIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder storageNode = builder.child("oak:index").child("testIdx").child(LuceneNgIndexStorage.STORAGE_NODE_NAME);
        OakDirectory dir = new OakDirectory(storageNode, "testIdx", false);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
                new org.apache.lucene.analysis.standard.StandardAnalyzer()));

        for (String path : paths) {
            int lastSlash = path.lastIndexOf('/');
            String parentPath = lastSlash == 0 ? "/" : path.substring(0, lastSlash);
            Document doc = new Document();
            doc.add(new StringField(FieldNames.PATH, path, Field.Store.YES));
            doc.add(new StringField("parentPath", parentPath, org.apache.lucene.document.Field.Store.NO));
            doc.add(new TextField(FieldNames.FULLTEXT, "node-at-" + path, Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();
        writer.close();
        dir.close();
    }

    @Test
    public void testComplexBooleanQuery() throws Exception {
        // Test: (text CONTAINS 'oak') AND (status = 'published') AND (age > 25)
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("test");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        OakDirectory directory = new OakDirectory(
                builder.child("oak:index").child("test").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "test", false);
        IndexWriterConfig config = new IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        // Document 1: matches all criteria
        Document doc1 = new Document();
        doc1.add(new StringField(FieldNames.PATH, "/match", Field.Store.YES));
        doc1.add(new TextField(FieldNames.FULLTEXT, "Apache Jackrabbit Oak", Field.Store.NO));
        doc1.add(new StringField("status", "published", Field.Store.NO));
        doc1.add(new LongPoint("age", 30L));
        doc1.add(new org.apache.lucene.document.StoredField("age", 30L));
        writer.addDocument(doc1);

        // Document 2: wrong status
        Document doc2 = new Document();
        doc2.add(new StringField(FieldNames.PATH, "/nomatch1", Field.Store.YES));
        doc2.add(new TextField(FieldNames.FULLTEXT, "Apache Jackrabbit Oak", Field.Store.NO));
        doc2.add(new StringField("status", "draft", Field.Store.NO));
        doc2.add(new LongPoint("age", 30L));
        doc2.add(new org.apache.lucene.document.StoredField("age", 30L));
        writer.addDocument(doc2);

        // Document 3: age too low
        Document doc3 = new Document();
        doc3.add(new StringField(FieldNames.PATH, "/nomatch2", Field.Store.YES));
        doc3.add(new TextField(FieldNames.FULLTEXT, "Apache Jackrabbit Oak", Field.Store.NO));
        doc3.add(new StringField("status", "published", Field.Store.NO));
        doc3.add(new LongPoint("age", 20L));
        doc3.add(new org.apache.lucene.document.StoredField("age", 20L));
        writer.addDocument(doc3);

        writer.commit();
        writer.close();
        directory.close();

        NodeState root = builder.getNodeState();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        // First test: just full-text query to verify documents are indexed
        Filter ftFilter = mock(Filter.class);
        when(ftFilter.getFullTextConstraint()).thenReturn(FullTextParser.parse("*", "oak"));
        when(ftFilter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(ftFilter.getQueryLimits()).thenReturn(null);

        Cursor ftCursor = index.query(ftFilter, root);
        int ftCount = 0;
        while (ftCursor.hasNext()) {
            ftCount++;
            ftCursor.next();
        }

        // Second test: property query ONLY (no full-text) - just status
        Filter statusOnlyFilter = mock(Filter.class);
        when(statusOnlyFilter.getFullTextConstraint()).thenReturn(null);

        PropertyRestriction prStatusAlone = new PropertyRestriction();
        prStatusAlone.propertyName = "status";
        prStatusAlone.first = PropertyValues.newString("published");
        prStatusAlone.last = PropertyValues.newString("published");
        prStatusAlone.firstIncluding = true;
        prStatusAlone.lastIncluding = true;

        when(statusOnlyFilter.getPropertyRestrictions()).thenReturn(Collections.singletonList(prStatusAlone));
        when(statusOnlyFilter.getQueryLimits()).thenReturn(null);

        Cursor statusOnlyCursor = index.query(statusOnlyFilter, root);
        int statusOnlyCount = 0;
        while (statusOnlyCursor.hasNext()) {
            statusOnlyCount++;
            statusOnlyCursor.next();
        }

        // Third test: full-text + status restriction
        Filter statusFilter = mock(Filter.class);
        when(statusFilter.getFullTextConstraint()).thenReturn(FullTextParser.parse("*", "oak"));

        PropertyRestriction prStatusOnly = new PropertyRestriction();
        prStatusOnly.propertyName = "status";
        prStatusOnly.first = PropertyValues.newString("published");
        prStatusOnly.last = PropertyValues.newString("published");
        prStatusOnly.firstIncluding = true;
        prStatusOnly.lastIncluding = true;

        when(statusFilter.getPropertyRestrictions()).thenReturn(Collections.singletonList(prStatusOnly));
        when(statusFilter.getQueryLimits()).thenReturn(null);

        Cursor statusCursor = index.query(statusFilter, root);
        int statusCount = 0;
        while (statusCursor.hasNext()) {
            statusCount++;
            statusCursor.next();
        }

        // Create filter for: (text CONTAINS 'oak') AND (status = 'published') AND (age > 25)
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(FullTextParser.parse("*", "oak"));

        PropertyRestriction prStatus = new PropertyRestriction();
        prStatus.propertyName = "status";
        prStatus.first = PropertyValues.newString("published");
        prStatus.last = PropertyValues.newString("published");
        prStatus.firstIncluding = true;
        prStatus.lastIncluding = true;

        PropertyRestriction prAge = new PropertyRestriction();
        prAge.propertyName = "age";
        prAge.first = PropertyValues.newLong(25L);
        prAge.firstIncluding = false;  // exclusive: >

        List<PropertyRestriction> restrictions = new ArrayList<>();
        restrictions.add(prStatus);
        restrictions.add(prAge);

        when(filter.getPropertyRestrictions()).thenReturn(restrictions);
        when(filter.getQueryLimits()).thenReturn(null);

        // Execute query
        Cursor cursor = index.query(filter, root);

        // Should return only /match
        assertTrue("Should find results", cursor.hasNext());
        List<String> resultPaths = new ArrayList<>();
        while (cursor.hasNext()) {
            resultPaths.add(cursor.next().getPath());
        }

        assertEquals("Should find 1 result", 1, resultPaths.size());
        assertTrue("Should contain /match", resultPaths.contains("/match"));
    }

    /**
     * Regression test: getPlans() must offer a plan for a query that has only a
     * node-type restriction and path restriction — no fulltext, no property
     * restrictions, no facets.  This is the pattern of:
     *
     *   SELECT * FROM [dam:Asset] WHERE ISDESCENDANTNODE('/content/dam')
     *
     * Before the fix, the early-exit guard in getPlans() rejected all such queries.
     * The plan must only be offered when the index actually has a rule for the queried
     * type — otherwise AEM's internal queries (cq:Page, cq:Template, etc.) would get
     * hijacked by a wrong index.
     */
    @Test
    public void getPlansOfferedForNodeTypeOnlyQuery() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();

        // Set up index definition with a rule for nt:unstructured.
        // IndexDefinitionBuilder sets type=fulltext by default; override to lucene9.
        NodeBuilder defnBuilder = builder.child("oak:index").child("testIdx");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("title").propertyIndex();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Write some data into the index storage
        NodeBuilder storageNode = builder.child("oak:index").child("testIdx").child(LuceneNgIndexStorage.STORAGE_NODE_NAME);
        OakDirectory dir = new OakDirectory(storageNode, "testIdx", false);
        org.apache.lucene.index.IndexWriter writer = new org.apache.lucene.index.IndexWriter(
                dir, new org.apache.lucene.index.IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField(FieldNames.PATH, "/content/page1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        dir.close();

        NodeState root = builder.getNodeState();
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        // Query for a type covered by the index (nt:unstructured) → must get a plan
        Filter covered = mock(Filter.class);
        when(covered.getFullTextConstraint()).thenReturn(null);
        when(covered.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(covered.matchesAllTypes()).thenReturn(false);
        when(covered.getNodeType()).thenReturn("nt:unstructured");
        when(covered.getPathRestriction()).thenReturn(Filter.PathRestriction.ALL_CHILDREN);
        when(covered.getPath()).thenReturn("/content");
        when(covered.getQueryLimits()).thenReturn(null);

        List<QueryIndex.IndexPlan> plans = index.getPlans(covered, Collections.emptyList(), root);
        assertFalse("getPlans() must offer a plan when the index has a rule for the queried type",
                plans.isEmpty());
        assertFalse("cost must be finite for a covered node-type query",
                Double.isInfinite(index.getCost(covered, root)));
        assertEquals("plan name must equal the index path so Oak's SelectorImpl records the index in query statistics",
                "/oak:index/testIdx", plans.get(0).getPlanName());

        // Query for a type NOT in the index (cq:Page) → must NOT get a plan
        Filter unrelated = mock(Filter.class);
        when(unrelated.getFullTextConstraint()).thenReturn(null);
        when(unrelated.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(unrelated.matchesAllTypes()).thenReturn(false);
        when(unrelated.getNodeType()).thenReturn("cq:Page");
        when(unrelated.getPathRestriction()).thenReturn(Filter.PathRestriction.ALL_CHILDREN);
        when(unrelated.getPath()).thenReturn("/content");
        when(unrelated.getQueryLimits()).thenReturn(null);

        List<QueryIndex.IndexPlan> noPlans = index.getPlans(unrelated, Collections.emptyList(), root);
        assertTrue("getPlans() must NOT offer a plan when the index has no rule for the queried type",
                noPlans.isEmpty());
    }

    /**
     * Regression test for sorting on a multi-valued (array) string property.
     *
     * A multi-valued property's doc-values are written by the index editor as a
     * {@code SortedSetDocValuesField} (SORTED_SET), never as a plain
     * {@code SortedDocValuesField} (SORTED). Lucene requires a {@code SortedSetSortField}
     * to sort against SORTED_SET doc-values -- a plain {@code SortField} with
     * {@code Type.STRING} only works against SORTED doc-values and throws
     * {@code IllegalStateException} otherwise.
     *
     * {@code PropertyDefinition} (the index config class in oak-search) has no static
     * multi-valuedness flag -- whether a property is single- or multi-valued is a
     * per-document, data-level fact, not something declared in the index definition.
     * So {@code createSortField} must derive it from the actual doc-values type recorded
     * in the index (via {@code FieldInfos}), not from config. This test writes a document
     * whose "tags" field is indexed with SORTED_SET doc-values (as the write side does for
     * an array-valued property) and asserts that the private {@code createSortField} method
     * picks a {@code SortedSetSortField} rather than a plain {@code SortField}.
     */
    @Test
    public void sortFieldForMultiValuedPropertyUsesSortedSetSortField() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder defnBuilder = builder.child("oak:index").child("testIdx");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("tags").ordered();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Write a document with "tags" indexed as SortedSetDocValuesField (multi-valued
        // doc-values) -- this is what the write side produces for an array-valued string
        // property.
        NodeBuilder storageNode = builder.child("oak:index").child("testIdx").child(LuceneNgIndexStorage.STORAGE_NODE_NAME);
        OakDirectory dir = new OakDirectory(storageNode, "testIdx", false);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
                new org.apache.lucene.analysis.standard.StandardAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField(FieldNames.PATH, "/content/item1", Field.Store.YES));
        doc.add(new SortedSetDocValuesField("tags", new BytesRef("alpha")));
        doc.add(new SortedSetDocValuesField("tags", new BytesRef("beta")));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        dir.close();

        NodeState root = builder.getNodeState();
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        LuceneNgIndexNode.AcquiredNode indexNode = tracker.acquireIndexNode("/oak:index/testIdx");
        assertNotNull("Index node must be resolvable", indexNode);
        try {
            IndexSearcher searcher = indexNode.getSearcher();
            LuceneNgIndexDefinition definition = indexNode.getDefinition();
            IndexReader reader = searcher.getIndexReader();

            OrderEntry order = new OrderEntry("tags", Type.STRING, OrderEntry.Order.ASCENDING);

            Method createSortField = LuceneNgIndex.class.getDeclaredMethod(
                    "createSortField", OrderEntry.class, LuceneNgIndexDefinition.class, IndexReader.class);
            createSortField.setAccessible(true);
            SortField sf = (SortField) createSortField.invoke(index, order, definition, reader);

            assertTrue("Sorting a multi-valued (SORTED_SET doc-values) property must use "
                            + "SortedSetSortField, not a plain SortField; got: " + sf,
                    sf instanceof SortedSetSortField);
        } finally {
            indexNode.release();
        }
    }
}
