/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.test.ISO8601;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class LuceneIndexEditorTest {
    private static final Analyzer analyzer = LuceneIndexConstants.ANALYZER;

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(
                    new LuceneIndexEditorProvider().with(analyzer)));

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    private IndexTracker tracker = new IndexTracker();

    private IndexNode indexNode;

    @Test
    public void testLuceneWithFullText() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene",
                of(TYPENAME_STRING));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        builder.child("test").setProperty("price", 100);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertEquals("/test", query("foo:fox"));
        assertNull("Non string properties not indexed by default",
                getPath(NumericRangeQuery.newLongRange("price", 100L, 100L, true, true)));
    }

    @Test
    public void testLuceneWithNonFullText() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinition(index, "lucene",
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo", "price", "weight", "bool", "creationTime"), STRINGS));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        builder.child("test").setProperty("bar", "kite is flying");
        builder.child("test").setProperty("price", 100);
        builder.child("test").setProperty("weight", 10.0);
        builder.child("test").setProperty("bool", true);
        builder.child("test").setProperty("truth", true);
        builder.child("test").setProperty("creationTime", createCal("05/06/2014"));
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertNull("Fulltext search should not work", query("foo:fox"));
        assertEquals("/test", getPath(new TermQuery(new Term("foo", "fox is jumping"))));
        assertNull("bar must NOT be indexed", getPath(new TermQuery(new Term("bar", "kite is flying"))));

        //Long
        assertEquals("/test", getPath(NumericRangeQuery.newDoubleRange("weight", 8D, 12D, true, true)));

        //Double
        assertEquals("/test", getPath(NumericRangeQuery.newLongRange("price", 100L, 100L, true, true)));

        //Boolean
        assertEquals("/test", getPath(new TermQuery(new Term("bool", "true"))));
        assertNull("truth must NOT be indexed", getPath(new TermQuery(new Term("truth", "true"))));

        //Date
        assertEquals("/test", getPath(NumericRangeQuery.newLongRange("creationTime",
                dateToTime("05/05/2014"), dateToTime("05/07/2014"), true, true)));
    }

    @Test
    public void noOfDocsIndexedNonFullText() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinition(index, "lucene",
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo"), STRINGS));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        builder.child("test2").setProperty("bar", "kite is flying");
        builder.child("test3").setProperty("foo", "wind is blowing");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);

        assertEquals(2, getSearcher().getIndexReader().numDocs());
    }

    //@Test
    public void checkLuceneIndexFileUpdates() throws Exception{
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder nb = newLuceneIndexDefinition(index, "lucene",
                of(TYPENAME_STRING));
        nb.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        nb.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "bar", "baz"), STRINGS));

        NodeState before = builder.getNodeState();
        builder.child("test").setProperty("foo", "fox is jumping");
        before = commitAndDump(before);

        builder.child("test2").setProperty("bar", "ship is sinking");
        before = commitAndDump(before);

        builder.child("test3").setProperty("baz", "horn is blowing");
        before = commitAndDump(before);

        builder.child("test2").remove();
        before = commitAndDump(before);

        builder.child("test2").setProperty("bar", "ship is back again");
        before = commitAndDump(before);
    }

    @After
    public void releaseIndexNode(){
        if(indexNode != null){
            indexNode.release();
        }
        indexNode = null;
    }

    private String query(String query) throws IOException, ParseException {
        QueryParser queryParser = new QueryParser(VERSION, "", analyzer);
        return getPath(queryParser.parse(query));
    }

    private String getPath(Query query) throws IOException {
        TopDocs td = getSearcher().search(query, 100);
        if (td.totalHits > 0){
            if(td.totalHits > 1){
                fail("More than 1 result found for query " + query);
            }
            return getSearcher().getIndexReader().document(td.scoreDocs[0].doc).get(PATH);
        }
        return null;
    }

    private IndexSearcher getSearcher(){
        if(indexNode == null){
            indexNode = tracker.acquireIndexNode("/oak:index/lucene");
        }
        return indexNode.getSearcher();
    }

    private NodeState commitAndDump(NodeState before) throws CommitFailedException, IOException {
        NodeState after = builder.getNodeState();
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        tracker.update(indexed);
        dumpIndexDir();
        return after;
    }

    private void dumpIndexDir() throws IOException {
        Directory dir = ((DirectoryReader)getSearcher().getIndexReader()).directory();

        System.out.println("================");
        for (String file : dir.listAll()){
            System.out.printf("%s - %d %n", file, dir.fileLength(file));
        }
        releaseIndexNode();
    }

    static Calendar createCal(String dt) throws java.text.ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(dt));
        return cal;
    }

    static long dateToTime(String dt) throws java.text.ParseException {
        return FieldFactory.dateToLong(ISO8601.format(createCal(dt)));
    }
}
