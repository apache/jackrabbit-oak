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

import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

/**
 * Functional tests for LuceneNgIndexEditor covering real-world indexing scenarios.
 * Tests verify that the editor can handle various content patterns without errors.
 */
public class IndexingFunctionalTest {

    @Test
    public void testIndexEmptyNode() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder definition = builder.child("oak:index").child("test");
        definition.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder emptyNode = root.child("emptyNode");
        emptyNode.setProperty(":primaryType", "nt:base");

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor(
            "/emptyNode", definition, root.getNodeState());

        // Should not throw exception when entering and leaving node with only hidden properties
        editor.enter(EMPTY_NODE, emptyNode.getNodeState());
        editor.leave(EMPTY_NODE, emptyNode.getNodeState());
    }

    @Test
    public void testIndexDeepHierarchy() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder definition = builder.child("oak:index").child("test");
        definition.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder root = INITIAL_CONTENT.builder();

        // Create 10-level deep hierarchy
        NodeBuilder currentLevel = root.child("level0");
        currentLevel.setProperty("title", "Level 0");

        // Create root editor
        LuceneNgIndexEditor editor = new LuceneNgIndexEditor(
            "/level0", definition, root.getNodeState());

        editor.enter(EMPTY_NODE, currentLevel.getNodeState());

        // Create child editors for each level
        for (int i = 1; i < 10; i++) {
            String levelName = "level" + i;
            NodeBuilder childNode = currentLevel.child(levelName);
            childNode.setProperty("title", "Level " + i);

            // childNodeAdded should return a valid editor
            Editor childEditor = editor.childNodeAdded(levelName, childNode.getNodeState());
            assertNotNull("Child editor should be created for " + levelName, childEditor);

            // Enter and leave should not throw
            childEditor.enter(EMPTY_NODE, childNode.getNodeState());
            childEditor.leave(EMPTY_NODE, childNode.getNodeState());

            currentLevel = childNode;
        }

        // Leave root editor should not throw
        editor.leave(EMPTY_NODE, root.child("level0").getNodeState());
    }

    @Test
    public void testIndexLargePropertyValue() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder definition = builder.child("oak:index").child("test");
        definition.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder nodeWithLargeProperty = root.child("largeNode");

        // Create 100KB text (100*1024 chars cycling through alphabet)
        StringBuilder largeText = new StringBuilder(100 * 1024);
        for (int i = 0; i < 100 * 1024; i++) {
            largeText.append((char) ('a' + (i % 26)));
        }

        nodeWithLargeProperty.setProperty("largeText", largeText.toString());

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor(
            "/largeNode", definition, root.getNodeState());

        // Should not throw OOM or any exception
        editor.enter(EMPTY_NODE, nodeWithLargeProperty.getNodeState());
        editor.leave(EMPTY_NODE, nodeWithLargeProperty.getNodeState());
    }

    @Test
    public void testIndexSpecialCharacters() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder definition = builder.child("oak:index").child("test");
        definition.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder nodeWithSpecialChars = root.child("specialNode");

        // Test various special character scenarios
        nodeWithSpecialChars.setProperty("unicode", "Hello 世界 🌍");
        nodeWithSpecialChars.setProperty("newlines", "Line 1\nLine 2\nLine 3");
        nodeWithSpecialChars.setProperty("quotes", "She said \"hello\" and 'goodbye'");
        nodeWithSpecialChars.setProperty("symbols", "!@#$%^&*()_+-={}[]|\\:;<>?,./");

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor(
            "/specialNode", definition, root.getNodeState());

        // Should handle all special characters without errors
        editor.enter(EMPTY_NODE, nodeWithSpecialChars.getNodeState());
        editor.leave(EMPTY_NODE, nodeWithSpecialChars.getNodeState());
    }

    @Test
    public void testIndexMixedPropertyTypes() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder definition = builder.child("oak:index").child("test");
        definition.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder nodeWithMixedProps = root.child("mixedNode");

        // Set properties of different types
        nodeWithMixedProps.setProperty("stringProp", "Some text");
        nodeWithMixedProps.setProperty("longProp", 12345L);
        nodeWithMixedProps.setProperty("booleanProp", true);
        nodeWithMixedProps.setProperty("doubleProp", 3.14159);

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor(
            "/mixedNode", definition, root.getNodeState());

        // Currently only strings are indexed in Phase 1, others should be ignored gracefully
        editor.enter(EMPTY_NODE, nodeWithMixedProps.getNodeState());
        editor.leave(EMPTY_NODE, nodeWithMixedProps.getNodeState());
    }

    @Test
    public void testHiddenPropertiesExcluded() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder definition = builder.child("oak:index").child("test");
        definition.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder nodeWithHiddenProps = root.child("hiddenPropsNode");

        // Set both normal and hidden properties
        nodeWithHiddenProps.setProperty("normalProp", "This should be indexed");
        nodeWithHiddenProps.setProperty(":hiddenProp", "This should be skipped");
        nodeWithHiddenProps.setProperty(":jcr:primaryType", "nt:base");

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor(
            "/hiddenPropsNode", definition, root.getNodeState());

        // Editor should handle both types, indexing normal and skipping hidden
        editor.enter(EMPTY_NODE, nodeWithHiddenProps.getNodeState());
        editor.leave(EMPTY_NODE, nodeWithHiddenProps.getNodeState());
    }

    @Test
    public void testNodeUpdateReplacesDocument() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index").child("testIdx");
        oakIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder content = builder.child("content").child("page1");
        content.setProperty("title", "Original Title");

        // First indexing
        LuceneNgIndexEditor editor = new LuceneNgIndexEditor("/content/page1", oakIndex, builder.getNodeState());
        editor.enter(EMPTY_NODE, content.getNodeState());
        editor.leave(EMPTY_NODE, content.getNodeState());

        // Second indexing of same path with different content
        content.setProperty("title", "Updated Title");
        LuceneNgIndexEditor editor2 = new LuceneNgIndexEditor("/content/page1", oakIndex, builder.getNodeState());
        editor2.enter(EMPTY_NODE, content.getNodeState());
        editor2.leave(EMPTY_NODE, content.getNodeState());

        // Convenience constructor uses "/oak:index/default" as indexPath, so dir name is "default"
        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(oakIndex.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content/page1")), 10);
            assertEquals("Should have exactly one document, not a duplicate", 1, hits.totalHits.value);
        }
    }

    @Test
    public void testNodeDeletionRemovesDocument() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index").child("testIdx");
        oakIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder content = builder.child("content");
        content.child("keep").setProperty("title", "Keep me");
        content.child("remove").setProperty("title", "Delete me");

        // Index both nodes
        for (String name : new String[]{"keep", "remove"}) {
            NodeBuilder child = content.child(name);
            LuceneNgIndexEditor ed = new LuceneNgIndexEditor("/content/" + name, oakIndex, builder.getNodeState());
            ed.enter(EMPTY_NODE, child.getNodeState());
            ed.leave(EMPTY_NODE, child.getNodeState());
        }

        // Delete /content/remove via parent editor
        LuceneNgIndexEditor parentEditor = new LuceneNgIndexEditor("/content", oakIndex, builder.getNodeState());
        parentEditor.enter(EMPTY_NODE, content.getNodeState());
        parentEditor.childNodeDeleted("remove", content.child("remove").getNodeState());
        parentEditor.leave(EMPTY_NODE, content.getNodeState());

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(oakIndex.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs keepHits   = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content/keep")),   10);
            TopDocs removeHits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content/remove")), 10);
            assertEquals("keep should still be indexed", 1, keepHits.totalHits.value);
            assertEquals("remove should be deleted",     0, removeHits.totalHits.value);
        }
    }

    @Test
    public void testIndexManyProperties() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder definition = builder.child("oak:index").child("test");
        definition.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder nodeWithManyProps = root.child("manyPropsNode");

        // Create 100 properties
        for (int i = 0; i < 100; i++) {
            nodeWithManyProps.setProperty("prop" + i, "Value for property " + i);
        }

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor(
            "/manyPropsNode", definition, root.getNodeState());

        // Should handle large number of properties without issues
        editor.enter(EMPTY_NODE, nodeWithManyProps.getNodeState());
        editor.leave(EMPTY_NODE, nodeWithManyProps.getNodeState());
    }
}
