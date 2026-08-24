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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

/**
 * Tests that the Lucene 9 index editor correctly indexes multi-valued properties that are declared
 * with an explicit type (Long, Double, Date), and that a node losing its matching rule has its stale
 * document deleted (OAK-12244).
 *
 * <p>Task B4 migrated these from driving {@code LuceneNgIndexEditor} directly to driving real
 * commits through {@link LuceneNgIndexEditorProvider} (see {@link LuceneNgEditorCommitUtil}); the
 * range/equality assertions still run against the committed Lucene index via a {@link DirectoryReader}.</p>
 */
public class LuceneNgIndexEditorTest {

    private static final String IDX = "/oak:index/test";

    private static IndexDefinitionBuilder lucene9(NodeBuilder rootBuilder) {
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        return idb;
    }

    @Test
    public void multiValuedLongPropertyWithExplicitTypeIsIndexed() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("score").propertyIndex().type("Long");
        NodeBuilder node = root.child("node");
        node.setProperty("jcr:primaryType", "nt:unstructured");
        node.setProperty("score", List.of(1L, 2L, 3L), Type.LONGS);

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            Query rangeQuery = LongPoint.newRangeQuery("score", 1L, 3L);
            assertEquals("Multi-valued Long property with explicit declared type must be indexed as LongPoint",
                    1, searcher.search(rangeQuery, 10).totalHits.value);
        }
    }

    @Test
    public void multiValuedDoublePropertyWithExplicitTypeIsIndexed() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("price").propertyIndex().type("Double");
        NodeBuilder node = root.child("node");
        node.setProperty("jcr:primaryType", "nt:unstructured");
        node.setProperty("price", List.of(1.5, 2.5, 3.5), Type.DOUBLES);

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            Query rangeQuery = DoublePoint.newRangeQuery("price", 1.5, 3.5);
            assertEquals("Multi-valued Double property with explicit declared type must be indexed as DoublePoint",
                    1, searcher.search(rangeQuery, 10).totalHits.value);
        }
    }

    @Test
    public void multiValuedDatePropertyWithExplicitTypeIsIndexed() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("eventDate").propertyIndex().type("Date");

        // Two well-formed ISO 8601 dates plus one malformed value in between: the malformed value
        // must be silently skipped, the well-formed ones still indexed as LongPoint (epoch millis).
        Calendar cal1 = new GregorianCalendar(2020, Calendar.JANUARY, 1);
        Calendar cal2 = new GregorianCalendar(2021, Calendar.JUNE, 15);
        NodeBuilder node = root.child("node");
        node.setProperty("jcr:primaryType", "nt:unstructured");
        node.setProperty("eventDate", List.of(ISO8601.format(cal1), "not-a-date", ISO8601.format(cal2)), Type.DATES);

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            long minMillis = Math.min(cal1.getTimeInMillis(), cal2.getTimeInMillis());
            long maxMillis = Math.max(cal1.getTimeInMillis(), cal2.getTimeInMillis());
            Query rangeQuery = LongPoint.newRangeQuery("eventDate", minMillis, maxMillis);
            assertEquals("Multi-valued Date property with explicit declared type must index its well-formed "
                            + "values as LongPoint (epoch millis), silently skipping the malformed one",
                    1, searcher.search(rangeQuery, 10).totalHits.value);
        }
    }

    /**
     * OAK-12244: when a node stops matching any indexing rule (e.g. its {@code jcr:primaryType}
     * changes to a type not covered by any {@code indexRule}), the stale document from a prior commit
     * must be deleted.
     */
    @Test
    public void nodeLosingItsMatchingRuleGetsItsDocumentDeleted() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("title").propertyIndex();
        NodeBuilder node = root.child("node");
        node.setProperty("jcr:primaryType", "nt:unstructured");
        node.setProperty("title", "hello");

        // Commit 1: node matches the "nt:unstructured" rule -> gets indexed.
        NodeState afterFirst = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(afterFirst, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals("Node matching the rule must be indexed", 1,
                    searcher.search(new TermQuery(new Term(FieldNames.PATH, "/node")), 10).totalHits.value);
        }

        // Commit 2: primaryType changes to "nt:folder", which no rule covers. Pure rule transition.
        NodeBuilder b2 = afterFirst.builder();
        b2.child("node").setProperty("jcr:primaryType", "nt:folder");
        NodeState afterSecond = LuceneNgEditorCommitUtil.commit(afterFirst, b2.getNodeState());

        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(afterSecond, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals("Stale document must be deleted once the node no longer matches any indexing rule",
                    0, searcher.search(new TermQuery(new Term(FieldNames.PATH, "/node")), 10).totalHits.value);
        }
    }
}
