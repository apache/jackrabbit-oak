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
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

/**
 * Tests that {@link LuceneNgIndexEditor} correctly indexes multi-valued properties
 * that are declared with an explicit type (Long, Double, Date) in the index definition.
 *
 * <p>Prior to the fix under test, {@code indexProperty}'s type-declared switch delegated to
 * {@code readAsLong}/{@code readAsDouble}/{@code readAsDateMillis}, each of which returns
 * {@code null} immediately when {@code prop.isArray()} is {@code true}. This silently skipped
 * indexing for any multi-valued property with an explicit declared type — no field was ever
 * added to the Lucene document, so range/equality queries against such a property returned no
 * results, without any error being raised.</p>
 */
public class LuceneNgIndexEditorTest {

    @Test
    public void multiValuedLongPropertyWithExplicitTypeIsIndexed() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("score").propertyIndex().type("Long");

        NodeBuilder content = INITIAL_CONTENT.builder().child("node");
        content.setProperty("jcr:primaryType", "nt:unstructured");
        content.setProperty("score", List.of(1L, 2L, 3L), Type.LONGS);

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor("/node", defnBuilder, INITIAL_CONTENT);
        editor.enter(EMPTY_NODE, content.getNodeState());
        editor.leave(EMPTY_NODE, content.getNodeState());

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            Query rangeQuery = LongPoint.newRangeQuery("score", 1L, 3L);
            TopDocs hits = searcher.search(rangeQuery, 10);
            assertEquals(
                    "Multi-valued Long property with explicit declared type must be indexed as LongPoint",
                    1, hits.totalHits.value);
        }
    }

    @Test
    public void multiValuedDoublePropertyWithExplicitTypeIsIndexed() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("price").propertyIndex().type("Double");

        NodeBuilder content = INITIAL_CONTENT.builder().child("node");
        content.setProperty("jcr:primaryType", "nt:unstructured");
        content.setProperty("price", List.of(1.5, 2.5, 3.5), Type.DOUBLES);

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor("/node", defnBuilder, INITIAL_CONTENT);
        editor.enter(EMPTY_NODE, content.getNodeState());
        editor.leave(EMPTY_NODE, content.getNodeState());

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            Query rangeQuery = DoublePoint.newRangeQuery("price", 1.5, 3.5);
            TopDocs hits = searcher.search(rangeQuery, 10);
            assertEquals(
                    "Multi-valued Double property with explicit declared type must be indexed as DoublePoint",
                    1, hits.totalHits.value);
        }
    }

    @Test
    public void multiValuedDatePropertyWithExplicitTypeIsIndexed() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("eventDate").propertyIndex().type("Date");

        // Two well-formed ISO 8601 dates plus one malformed value in between. The malformed
        // value must be silently skipped (per-value try/catch in the DATE array branch), while
        // the well-formed values must still be indexed as LongPoint (DATE is stored the same way
        // as a single-value DATE property: epoch millis via ISO8601.parse(...).getTimeInMillis()).
        Calendar cal1 = new GregorianCalendar(2020, Calendar.JANUARY, 1);
        Calendar cal2 = new GregorianCalendar(2021, Calendar.JUNE, 15);
        String validDate1 = ISO8601.format(cal1);
        String validDate2 = ISO8601.format(cal2);
        String malformedDate = "not-a-date";

        NodeBuilder content = INITIAL_CONTENT.builder().child("node");
        content.setProperty("jcr:primaryType", "nt:unstructured");
        content.setProperty("eventDate", List.of(validDate1, malformedDate, validDate2), Type.DATES);

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor("/node", defnBuilder, INITIAL_CONTENT);
        editor.enter(EMPTY_NODE, content.getNodeState());
        editor.leave(EMPTY_NODE, content.getNodeState());

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            long minMillis = Math.min(cal1.getTimeInMillis(), cal2.getTimeInMillis());
            long maxMillis = Math.max(cal1.getTimeInMillis(), cal2.getTimeInMillis());
            Query rangeQuery = LongPoint.newRangeQuery("eventDate", minMillis, maxMillis);
            TopDocs hits = searcher.search(rangeQuery, 10);
            assertEquals(
                    "Multi-valued Date property with explicit declared type must index its well-formed "
                            + "values as LongPoint (epoch millis), silently skipping the malformed one "
                            + "rather than failing the whole property",
                    1, hits.totalHits.value);
        }
    }

    /**
     * Port of OAK-12244 (see {@code FulltextIndexEditor#enter}/{@code #leave}): when a node
     * stops matching any indexing rule (e.g. its {@code jcr:primaryType} changes to a type not
     * covered by any {@code indexRule}), the stale Lucene document from a prior commit must be
     * deleted, even though the current commit's {@code indexNode(after)} call finds no
     * applicable rule and would otherwise return early without touching the index.
     */
    @Test
    public void nodeLosingItsMatchingRuleGetsItsDocumentDeleted() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("title").propertyIndex();

        NodeBuilder content = INITIAL_CONTENT.builder().child("node");
        content.setProperty("jcr:primaryType", "nt:unstructured");
        content.setProperty("title", "hello");
        NodeState afterFirstCommit = content.getNodeState();

        // Commit 1: node matches the "nt:unstructured" rule -> gets indexed.
        LuceneNgIndexEditor editor1 = new LuceneNgIndexEditor("/node", defnBuilder, INITIAL_CONTENT);
        editor1.enter(EMPTY_NODE, afterFirstCommit);
        editor1.leave(EMPTY_NODE, afterFirstCommit);

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/node")), 10);
            assertEquals("Node matching the rule must be indexed", 1, hits.totalHits.value);
        }

        // Commit 2: primaryType changes to "nt:folder", which no rule covers. "title" is
        // untouched, so this is purely a rule-transition case, not a property change.
        content.setProperty("jcr:primaryType", "nt:folder");
        NodeState afterSecondCommit = content.getNodeState();

        LuceneNgIndexEditor editor2 = new LuceneNgIndexEditor("/node", defnBuilder, INITIAL_CONTENT);
        editor2.enter(afterFirstCommit, afterSecondCommit);
        editor2.leave(afterFirstCommit, afterSecondCommit);

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/node")), 10);
            assertEquals(
                    "Stale document must be deleted once the node no longer matches any indexing rule",
                    0, hits.totalHits.value);
        }
    }
}
