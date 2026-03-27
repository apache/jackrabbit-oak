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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
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
import org.junit.Test;

/**
 * Verifies that {@link LuceneNgCursor}, when driven by {@link LuceneNgIndex#query(IndexPlan, NodeState)},
 * fetches results in bounded per-batch {@code searchAfter} calls and releases the index node
 * between batches (rather than holding it open for the whole cursor lifetime), while still
 * producing correct results — including full-text excerpts — across the batch boundary.
 */
public class LuceneNgCursorBatchingTest {

    /**
     * Writes {@code count} documents (paths /content/doc0..docN) into a lucene9 index at
     * {@code /oak:index/testIdx}. Each document also carries a stored FULLTEXT field so the
     * same fixture can be queried both by match-all and by a full-text term.
     */
    private static NodeState buildIndexWithDocs(NodeBuilder builder, int count) throws Exception {
        NodeBuilder oakIndex = builder.child("oak:index").child("testIdx");
        oakIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

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
    public void partiallyConsumedCursorReleasesIndexNodeBetweenBatches() throws Exception {
        // 60 docs > the starting batch size of 50, so the cursor must fetch a second batch.
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeState root = buildIndexWithDocs(builder, 60);

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/testIdx");

        // --- Correctness across the batch boundary: drain a full cursor, expect 60 distinct paths.
        Cursor fullCursor = index.query(plan(matchAllFilter()), root);
        Set<String> paths = new HashSet<>();
        while (fullCursor.hasNext()) {
            IndexRow row = fullCursor.next();
            assertTrue("Duplicate path across batch boundary: " + row.getPath(), paths.add(row.getPath()));
        }
        assertEquals("All 60 documents must be returned across the two batches", 60, paths.size());

        // --- Node must be released between batches: drain only the first batch (50 rows), then
        // assert closing the tracker's node (which calls LuceneNgIndexNode.close()) completes
        // without blocking. Before the per-batch fix the eager cursor holds the AcquiredNode for
        // its whole life, so close() would block on the reader read-lock and time out.
        Cursor partialCursor = index.query(plan(matchAllFilter()), root);
        int drained = 0;
        while (drained < 50 && partialCursor.hasNext()) {
            partialCursor.next();
            drained++;
        }
        assertEquals("Should have drained exactly the first batch", 50, drained);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> closeFuture = executor.submit(tracker::close);
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

        Cursor cursor = index.query(plan(fulltextFilter("brown")), root);
        int rows = 0;
        while (cursor.hasNext()) {
            IndexRow row = cursor.next();
            org.apache.jackrabbit.oak.api.PropertyValue excerpt = row.getValue("rep:excerpt");
            assertNotNull("Excerpt must be present for " + row.getPath() + " (row " + rows + ")", excerpt);
            String text = excerpt.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
            assertNotNull("Excerpt text must not be null for " + row.getPath(), text);
            assertTrue("Excerpt text must not be empty for " + row.getPath(), !text.isEmpty());
            rows++;
        }
        assertEquals("All 55 matching documents must be returned across batches", 55, rows);
    }

    // --- helpers ---

    private static Filter matchAllFilter() {
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(null);
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.NO_RESTRICTION);
        when(filter.getQueryLimits()).thenReturn(null);
        return filter;
    }

    private static Filter fulltextFilter(String term) throws java.text.ParseException {
        Filter filter = mock(Filter.class);
        when(filter.getFullTextConstraint()).thenReturn(FullTextParser.parse("*", term));
        when(filter.getPropertyRestrictions()).thenReturn(Collections.emptyList());
        when(filter.getPathRestriction()).thenReturn(Filter.PathRestriction.NO_RESTRICTION);
        when(filter.getQueryLimits()).thenReturn(null);
        return filter;
    }

    private static IndexPlan plan(Filter filter) {
        IndexPlan plan = mock(IndexPlan.class);
        when(plan.getFilter()).thenReturn(filter);
        when(plan.getSortOrder()).thenReturn(Collections.emptyList());
        when(plan.getAttribute("oak.facet.fields")).thenReturn(null);
        return plan;
    }
}
