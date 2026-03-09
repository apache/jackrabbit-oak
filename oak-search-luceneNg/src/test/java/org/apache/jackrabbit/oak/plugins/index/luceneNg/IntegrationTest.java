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

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.List;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for Lucene 9 indexing covering end-to-end workflows.
 * Tests verify complete indexing scenarios with tracker, provider, and editor components.
 */
public class IntegrationTest {

    @Test
    public void testCompleteIndexingWorkflow() throws Exception {
        // Setup: Create index definition
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("testIndex");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        indexDef.setProperty("async", "async");

        // Create content tree with 3 articles
        NodeBuilder content = builder.child("content");
        NodeBuilder article1 = content.child("article1");
        article1.setProperty("title", "Introduction to Oak");
        article1.setProperty("text", "Apache Jackrabbit Oak is a scalable repository");

        NodeBuilder article2 = content.child("article2");
        article2.setProperty("title", "Lucene 9 Integration");
        article2.setProperty("text", "Lucene 9 provides advanced search capabilities");

        NodeBuilder article3 = content.child("article3");
        article3.setProperty("title", "Performance Optimization");
        article3.setProperty("text", "Chunked storage improves memory efficiency");

        NodeState root = builder.getNodeState();

        // Index the content
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndexEditorProvider provider = new LuceneNgIndexEditorProvider(tracker);
        IndexUpdateCallback callback = mock(IndexUpdateCallback.class);

        Editor editor = provider.getIndexEditor(
            LuceneNgIndexConstants.TYPE_LUCENE9,
            indexDef,
            root,
            callback
        );

        assertNotNull("Editor should be created", editor);

        // Simulate indexing by traversing tree
        // Use try-finally to ensure IndexWriter is closed even if test fails
        try {
            editor.enter(EMPTY_NODE, root);

            // Index content node
            Editor contentEditor = editor.childNodeAdded("content", content.getNodeState());
            assertNotNull("Content editor should be created", contentEditor);
            contentEditor.enter(EMPTY_NODE, content.getNodeState());

            // Index article1
            Editor article1Editor = contentEditor.childNodeAdded("article1", article1.getNodeState());
            assertNotNull("Article1 editor should be created", article1Editor);
            article1Editor.enter(EMPTY_NODE, article1.getNodeState());
            article1Editor.leave(EMPTY_NODE, article1.getNodeState());

            // Index article2
            Editor article2Editor = contentEditor.childNodeAdded("article2", article2.getNodeState());
            assertNotNull("Article2 editor should be created", article2Editor);
            article2Editor.enter(EMPTY_NODE, article2.getNodeState());
            article2Editor.leave(EMPTY_NODE, article2.getNodeState());

            // Index article3
            Editor article3Editor = contentEditor.childNodeAdded("article3", article3.getNodeState());
            assertNotNull("Article3 editor should be created", article3Editor);
            article3Editor.enter(EMPTY_NODE, article3.getNodeState());
            article3Editor.leave(EMPTY_NODE, article3.getNodeState());

            contentEditor.leave(EMPTY_NODE, content.getNodeState());
        } finally {
            // Ensure cleanup even if test fails
            editor.leave(EMPTY_NODE, root);
        }

        // Verify index was created by checking tracker has the index
        LuceneNgIndexNode indexNode = tracker.acquireIndexNode("/oak:index/testIndex");
        assertNotNull("Index should be tracked", indexNode);
        assertEquals("Index path should match", "/oak:index/testIndex", indexNode.getIndexPath());
    }

    @Test
    public void testChunkedStorageInRealIndex() throws Exception {
        // Setup: Create index definition
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("largeIndex");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        indexDef.setProperty("async", "async");

        // Create 100 nodes with large text (1000x repeated string per node) to force large index
        NodeBuilder content = builder.child("content");
        StringBuilder largeText = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeText.append("This is a test string to create large content for chunked storage testing. ");
        }
        String largeTextValue = largeText.toString();

        for (int i = 0; i < 100; i++) {
            NodeBuilder node = content.child("node" + i);
            node.setProperty("title", "Node " + i);
            node.setProperty("text", largeTextValue);
        }

        NodeState root = builder.getNodeState();

        // Index all 100 nodes
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndexEditorProvider provider = new LuceneNgIndexEditorProvider(tracker);
        IndexUpdateCallback callback = mock(IndexUpdateCallback.class);

        Editor editor = provider.getIndexEditor(
            LuceneNgIndexConstants.TYPE_LUCENE9,
            indexDef,
            root,
            callback
        );

        assertNotNull("Editor should be created", editor);

        // Simulate indexing
        // Use try-finally to ensure IndexWriter is closed even if test fails
        try {
            editor.enter(EMPTY_NODE, root);

            Editor contentEditor = editor.childNodeAdded("content", content.getNodeState());
            assertNotNull("Content editor should be created", contentEditor);
            contentEditor.enter(EMPTY_NODE, content.getNodeState());

            // Index all 100 nodes
            for (int i = 0; i < 100; i++) {
                String nodeName = "node" + i;
                NodeBuilder node = content.child(nodeName);
                Editor nodeEditor = contentEditor.childNodeAdded(nodeName, node.getNodeState());
                assertNotNull("Node editor should be created for " + nodeName, nodeEditor);
                nodeEditor.enter(EMPTY_NODE, node.getNodeState());
                nodeEditor.leave(EMPTY_NODE, node.getNodeState());
            }

            contentEditor.leave(EMPTY_NODE, content.getNodeState());
        } finally {
            // Ensure cleanup even if test fails
            editor.leave(EMPTY_NODE, root);
        }

        // Verify index was created by checking tracker has the index
        LuceneNgIndexNode indexNode = tracker.acquireIndexNode("/oak:index/largeIndex");
        assertNotNull("Index should be tracked", indexNode);
        assertEquals("Index path should match", "/oak:index/largeIndex", indexNode.getIndexPath());
    }

    @Test
    public void testProviderReturnsNullForWrongType() throws Exception {
        // Setup: Create index definition with wrong type
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("wrongTypeIndex");
        indexDef.setProperty("type", "wrong-type");
        indexDef.setProperty("async", "async");

        NodeState root = builder.getNodeState();

        // Create tracker and provider
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgIndexEditorProvider provider = new LuceneNgIndexEditorProvider(tracker);
        IndexUpdateCallback callback = mock(IndexUpdateCallback.class);

        // Verify provider returns null for wrong type
        Editor editor = provider.getIndexEditor(
            "wrong-type",
            indexDef,
            root,
            callback
        );

        assertNull("Editor should be null for wrong type", editor);
    }

    @Test
    public void testTrackerLifecycle() throws Exception {
        // Create index1
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder index1 = oakIndex.child("index1");
        index1.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        index1.setProperty("async", "async");

        NodeState root1 = builder.getNodeState();

        // Update tracker with index1
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root1);

        // Verify acquireIndexNode() returns index1
        LuceneNgIndexNode indexNode1 = tracker.acquireIndexNode("/oak:index/index1");
        assertNotNull("Index1 should be found", indexNode1);

        // Add index2
        NodeBuilder index2 = oakIndex.child("index2");
        index2.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        index2.setProperty("async", "async");

        NodeState root2 = builder.getNodeState();

        // Update tracker with both indexes
        tracker.update(root2);

        // Verify both indexes are found
        LuceneNgIndexNode indexNode1After = tracker.acquireIndexNode("/oak:index/index1");
        assertNotNull("Index1 should still be found", indexNode1After);

        LuceneNgIndexNode indexNode2 = tracker.acquireIndexNode("/oak:index/index2");
        assertNotNull("Index2 should be found", indexNode2);

        // Verify nonexistent index returns null
        LuceneNgIndexNode nonexistent = tracker.acquireIndexNode("/oak:index/nonexistent");
        assertNull("Nonexistent index should return null", nonexistent);
    }

    @Test
    public void testEndToEndQueryWorkflow() throws Exception {
        // Setup: Create index definition
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder indexDef = oakIndex.child("testIndex");
        indexDef.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Create content nodes
        NodeBuilder content = builder.child("content");
        NodeBuilder article1 = content.child("article1");
        article1.setProperty("title", "Introduction to Oak");
        article1.setProperty("text", "Apache Jackrabbit Oak is a scalable repository");

        NodeBuilder article2 = content.child("article2");
        article2.setProperty("title", "Lucene 9 Integration");
        article2.setProperty("text", "Lucene 9 provides advanced search capabilities");

        // Get state with content
        NodeState root = builder.getNodeState();

        // Index the content using OakDirectory directly (simpler than Editor)
        // Use index name "testIndex" to match the index definition
        org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory directory =
            new org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory(indexDef, "testIndex", false);
        org.apache.lucene.index.IndexWriterConfig config = new org.apache.lucene.index.IndexWriterConfig(
            new org.apache.lucene.analysis.standard.StandardAnalyzer());
        org.apache.lucene.index.IndexWriter writer = new org.apache.lucene.index.IndexWriter(directory, config);

        // Index article1
        org.apache.lucene.document.Document doc1 = new org.apache.lucene.document.Document();
        doc1.add(new org.apache.lucene.document.StringField("path", "/content/article1", org.apache.lucene.document.Field.Store.YES));
        doc1.add(new org.apache.lucene.document.TextField(org.apache.jackrabbit.oak.plugins.index.search.FieldNames.FULLTEXT, "Apache Jackrabbit Oak is a scalable repository", org.apache.lucene.document.Field.Store.NO));
        writer.addDocument(doc1);

        // Index article2
        org.apache.lucene.document.Document doc2 = new org.apache.lucene.document.Document();
        doc2.add(new org.apache.lucene.document.StringField("path", "/content/article2", org.apache.lucene.document.Field.Store.YES));
        doc2.add(new org.apache.lucene.document.TextField(org.apache.jackrabbit.oak.plugins.index.search.FieldNames.FULLTEXT, "Lucene 9 provides advanced search capabilities", org.apache.lucene.document.Field.Store.NO));
        writer.addDocument(doc2);

        writer.commit();
        writer.close();
        directory.close();

        // Get fresh root with indexed data
        root = builder.getNodeState();

        // Update tracker with indexed content
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        // Now query the index
        LuceneNgQueryIndexProvider queryProvider = new LuceneNgQueryIndexProvider(tracker);
        List<? extends QueryIndex> indexes = queryProvider.getQueryIndexes(root);

        assertEquals("Should have one index", 1, indexes.size());

        LuceneNgIndex index = (LuceneNgIndex) indexes.get(0);

        // Create filter for "Oak" search
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(
            FullTextParser.parse("*", "Oak"));
        when(filter.getPathRestriction()).thenReturn(PathRestriction.NO_RESTRICTION);
        when(filter.getQueryLimits()).thenReturn(null);

        // Execute query
        Cursor cursor = index.query(filter, root);

        assertNotNull("Cursor should not be null", cursor);
        assertTrue("Should find at least one result", cursor.hasNext());

        IndexRow row = cursor.next();
        assertTrue("Result should be article1 or article2",
                   row.getPath().contains("/content/article"));
    }
}
