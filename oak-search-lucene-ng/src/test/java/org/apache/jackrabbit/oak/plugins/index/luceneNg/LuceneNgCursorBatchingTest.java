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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link LuceneNgIndex#query(IndexPlan, NodeState)} fetches results in batches,
 * releases the index node between batches, and returns every row exactly once across batch
 * boundaries and index refreshes.
 */
public class LuceneNgCursorBatchingTest {

    /**
     * Writes {@code count} documents (paths /content/doc0..docN) into a luceneNg index at
     * {@code /oak:index/testIdx}. Each document also carries a stored FULLTEXT field so the
     * same fixture can be queried both by match-all and by a full-text term.
     */
    private static NodeState buildIndexWithDocs(NodeBuilder builder, int count) throws Exception {
        NodeBuilder oakIndex = builder.child("oak:index").child("testIdx");
        oakIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE_NG);

        OakDirectory dir = new OakDirectory(
                builder.child("oak:index").child("testIdx").child(LuceneNgIndexStorage.STORAGE_NODE_NAME),
                "testIdx", false);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()));
        for (int i = 0; i < count; i++) {
            Document doc = new Document();
            doc.add(new StringField(FieldNames.PATH, "/content/doc" + i, Field.Store.YES));
            // Store the fulltext field so UnifiedHighlighter can produce an excerpt.
            doc.add(new TextField(FieldNames.FULLTEXT,
                    "the quick brown fox document number " + i, Field.Store.YES));
            writer.addDocument(doc);
        }
        writer.commit();
        writer.close();
        dir.close();
        return builder.getNodeState();
    }

    @Test
    public void indexRefreshBetweenBatchesNeitherDuplicatesNorSkipsRows() throws Exception {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        List<String> indexedPaths = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            indexedPaths.add("/content/doc" + i);
        }
        NodeState root = rewriteIndex(builder, indexedPaths);
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);
        Cursor cursor = new LuceneNgIndex(tracker, "/oak:index/testIdx").query(plan(matchAllFilter(), root), root);
        List<String> returned = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            returned.add(cursor.next().getPath());
        }

        // A new index version stores the same documents, plus one, in a different order.
        indexedPaths.add("/content/doc60");
        Collections.reverse(indexedPaths);
        tracker.update(rewriteIndex(builder, indexedPaths));
        cursor.forEachRemaining(row -> returned.add(row.getPath()));

        assertEquals(new HashSet<>(indexedPaths), new HashSet<>(returned));
        assertEquals(61, returned.size());
    }

    /**
     * Replaces the content of the luceneNg index at {@code /oak:index/testIdx} with one document
     * per path, in the given order, and marks the index as updated.
     */
    private static NodeState rewriteIndex(NodeBuilder builder, List<String> paths) throws Exception {
        NodeBuilder definition = builder.child("oak:index").child("testIdx");
        definition.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE_NG);
        OakDirectory dir = new OakDirectory(definition.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "testIdx", false);
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig()
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE))) {
            for (String path : paths) {
                Document doc = new Document();
                doc.add(new StringField(FieldNames.PATH, path, Field.Store.YES));
                writer.addDocument(doc);
            }
        }
        dir.close();
        definition.child(":status").setProperty("lastUpdated", System.nanoTime());
        return builder.getNodeState();
    }

    @Test
    public void partiallyConsumedCursorReleasesIndexNodeBetweenBatches() throws Exception {
        // 60 docs > the starting batch size of 50, so the cursor must fetch a second batch.
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeState root = buildIndexWithDocs(builder, 60);

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        // --- Correctness across the batch boundary: drain a full cursor, expect 60 distinct paths.
        Cursor fullCursor = index.query(plan(matchAllFilter(), root), root);
        Set<String> paths = new HashSet<>();
        while (fullCursor.hasNext()) {
            IndexRow row = fullCursor.next();
            assertTrue(paths.add(row.getPath()), "Duplicate path across batch boundary: " + row.getPath());
        }
        assertEquals(60, paths.size(), "All 60 documents must be returned across the two batches");

        // --- Node must be released between batches: drain only the first batch (50 rows), then
        // assert closing the tracker's node completes without blocking. Before the per-batch fix
        // the eager cursor holds the acquired node for its whole life, so close() would block on
        // the reader read-lock and time out.
        //
        // FulltextIndexTracker.close() itself is package-private to oak-search, so it is not
        // reachable from this test; driving update() with an empty root has the same effect
        // through the tracker's public API -- the tracked index is diffed against "removed",
        // which closes its LuceneNgIndexNodeManager (public, inherited IndexNodeManager.close())
        // and, in turn, blocks on exactly the same read/write lock this test is probing.
        Cursor partialCursor = index.query(plan(matchAllFilter(), root), root);
        int drained = 0;
        while (drained < 50 && partialCursor.hasNext()) {
            partialCursor.next();
            drained++;
        }
        assertEquals(50, drained, "Should have drained exactly the first batch");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> closeFuture = executor.submit(() -> tracker.update(EmptyNodeState.EMPTY_NODE));
            // With the per-batch cursor nothing is held between batches, so this returns promptly.
            closeFuture.get(2, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void excerptsAreStillPopulatedAcrossBatches() throws Exception {
        // 55 docs all matching the term "brown" -> spans two batches (50 + 5).
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeState root = buildIndexWithDocs(builder, 55);

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        Cursor cursor = index.query(plan(fulltextFilter("brown"), root), root);
        int rows = 0;
        while (cursor.hasNext()) {
            IndexRow row = cursor.next();
            org.apache.jackrabbit.oak.api.PropertyValue excerpt = row.getValue("rep:excerpt");
            assertNotNull(excerpt, "Excerpt must be present for " + row.getPath() + " (row " + rows + ")");
            String text = excerpt.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
            assertNotNull(text, "Excerpt text must not be null for " + row.getPath());
            assertTrue(!text.isEmpty(), "Excerpt text must not be empty for " + row.getPath());
            rows++;
        }
        assertEquals(55, rows, "All 55 matching documents must be returned across batches");
    }

    // --- helpers ---

    @Test
    public void rowsLoadOnlyTheStoredPath() throws Exception {
        StoredFields fields = mock(StoredFields.class);
        Document document = new Document();
        document.add(new StringField(FieldNames.PATH, "/content/one", Field.Store.YES));
        when(fields.document(0, Set.of(FieldNames.PATH))).thenReturn(document);
        IndexSearcher searcher = mock(IndexSearcher.class);
        when(searcher.storedFields()).thenReturn(fields);
        when(searcher.search(any(MatchAllDocsQuery.class), eq(50))).thenReturn(
                new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {new ScoreDoc(0, 1)}));
        LuceneNgIndexNode node = mock(LuceneNgIndexNode.class);
        when(node.getSearcher()).thenReturn(searcher);
        LuceneNgIndexTracker tracker = mock(LuceneNgIndexTracker.class);
        when(tracker.acquireIndexNode("/oak:index/testIdx")).thenReturn(node);
        Cursor cursor = new LuceneNgIndex(tracker, "/oak:index/testIdx")
                .query(plan(matchAllFilter(), EmptyNodeState.EMPTY_NODE), EmptyNodeState.EMPTY_NODE);
        assertEquals("/content/one", cursor.next().getPath());
        assertFalse(cursor.hasNext());
        verify(fields).document(0, Set.of(FieldNames.PATH));
        verify(fields, never()).document(anyInt());
        verify(node).release();
    }

    private static Filter matchAllFilter() {
        Filter filter = mock(Filter.class);
        when(filter.matchesAllTypes()).thenReturn(true);
        when(filter.getFullTextConstraint()).thenReturn(null);
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.NO_RESTRICTION);
        when(filter.getQueryLimits()).thenReturn(new QueryEngineSettings());
        return filter;
    }

    private static Filter fulltextFilter(String term) throws java.text.ParseException {
        Filter filter = mock(Filter.class);
        when(filter.matchesAllTypes()).thenReturn(true);
        when(filter.getFullTextConstraint()).thenReturn(FullTextParser.parse("*", term));
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.NO_RESTRICTION);
        when(filter.getQueryLimits()).thenReturn(new QueryEngineSettings());
        return filter;
    }

    private static IndexPlan plan(Filter filter, NodeState root) {
        IndexPlan plan = mock(IndexPlan.class);
        when(plan.getFilter()).thenReturn(filter);
        when(plan.getPathPrefix()).thenReturn("");
        String indexPath = "/oak:index/testIdx";
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
                root, root.getChildNode("oak:index").getChildNode("testIdx"), indexPath);
        when(plan.getAttribute(FulltextIndex.ATTR_PLAN_RESULT)).thenReturn(new PlanResult(indexPath, definition, null));
        when(plan.getSortOrder()).thenReturn(Collections.emptyList());
        when(plan.getAttribute("oak.facet.fields")).thenReturn(null);
        return plan;
    }
}
