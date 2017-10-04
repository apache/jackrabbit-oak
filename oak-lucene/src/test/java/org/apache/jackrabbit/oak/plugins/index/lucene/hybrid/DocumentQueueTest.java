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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.IndexingMode;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.LocalIndexObserverTest.NOOP_EXECUTOR;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DocumentQueueTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();
    private EditorHook asyncHook;
    private EditorHook syncHook;
    private CommitInfo info;

    private IndexTracker tracker = new IndexTracker();
    private NRTIndexFactory indexFactory;
    private Clock clock = new Clock.Virtual();
    private long refreshDelta = TimeUnit.SECONDS.toMillis(1);

    @Before
    public void setUp() throws IOException {
        IndexEditorProvider editorProvider = new LuceneIndexEditorProvider(
                null,
                null,
                null,
                defaultMountInfoProvider()
        );

        syncHook = new EditorHook(new IndexUpdateProvider(editorProvider));
        asyncHook = new EditorHook(new IndexUpdateProvider(editorProvider, "async", false));
    }

    @Test
    public void dropDocOnLimit() throws Exception{
        DocumentQueue queue = new DocumentQueue(2, tracker, NOOP_EXECUTOR);
        assertTrue(queue.add(LuceneDoc.forDelete("foo", "bar")));
        assertTrue(queue.add(LuceneDoc.forDelete("foo", "bar")));

        //3rd one would be dropped as queue size is 2
        assertFalse(queue.add(LuceneDoc.forDelete("foo", "bar")));
    }

    @Test
    public void noIssueIfNoIndex() throws Exception{
        DocumentQueue queue = new DocumentQueue(2, tracker, sameThreadExecutor());
        assertTrue(queue.add(LuceneDoc.forDelete("foo", "bar")));
        assertTrue(queue.getQueuedDocs().isEmpty());
    }

    @Test
    public void closeQueue() throws Exception{
        DocumentQueue queue = new DocumentQueue(2, tracker, sameThreadExecutor());
        queue.close();

        try {
            queue.add(LuceneDoc.forDelete("foo", "bar"));
            fail();
        } catch(IllegalStateException ignore){

        }
    }

    @Test
    public void noIssueIfNoWriter() throws Exception{
        NodeState indexed = createAndPopulateAsyncIndex(IndexingMode.NRT);
        DocumentQueue queue = new DocumentQueue(2, tracker, sameThreadExecutor());

        tracker.update(indexed);
        assertTrue(queue.add(LuceneDoc.forDelete("/oak:index/fooIndex", "bar")));
    }

    @Test
    public void updateDocument() throws Exception{
        IndexTracker tracker = createTracker();
        NodeState indexed = createAndPopulateAsyncIndex(IndexingMode.NRT);
        tracker.update(indexed);
        DocumentQueue queue = new DocumentQueue(2, tracker, sameThreadExecutor());

        Document d1 = new Document();
        d1.add(newPathField("/a/b"));
        d1.add(new StringField("foo", "a", Field.Store.NO));
        queue.add(LuceneDoc.forUpdate("/oak:index/fooIndex", "/a/b", d1));

        List<NRTIndex> indexes = indexFactory.getIndexes("/oak:index/fooIndex");
        NRTIndex index = indexes.get(indexes.size() - 1);
        assertEquals(1, index.getPrimaryReaderForTest().numDocs());
    }

    @Test
    public void indexRefresh() throws Exception{
        tracker = createTracker();
        NodeState indexed = createAndPopulateAsyncIndex(IndexingMode.NRT);
        tracker.update(indexed);

        clock.waitUntil(refreshDelta);

        DocumentQueue queue = new DocumentQueue(2, tracker, sameThreadExecutor());

        TopDocs td = doSearch("bar");
        assertEquals(1, td.totalHits);

        addDoc(queue, "/a/b", "bar");

        //First update would be picked as base time was zero which would now
        //get initialized
        td = doSearch("bar");
        assertEquals(2, td.totalHits);

        addDoc(queue, "/a/c", "bar");

        //Now it would not update as refresh interval has not exceeded
        td = doSearch("bar");
        assertEquals(2, td.totalHits);

        addDoc(queue, "/a/d", "bar");

        //Get past the delta time
        clock.waitUntil(clock.getTime() + refreshDelta + 1);

        //Now it should show updated result
        td = doSearch("bar");
        assertEquals(4, td.totalHits);

        //Phase 2 - Check affect of async index update cycle
        //With that there should only be 2 copies of NRTIndex kept
        indexed = doAsyncIndex(indexed, "a2", "bar");

        tracker.update(indexed);

        //Now result would be latest from async + last local
        td = doSearch("bar");
        assertEquals(5, td.totalHits);

        //Now there would be to NRTIndex - previous and current
        //so add to current and query again
        addDoc(queue, "/a/e", "bar");
        td = doSearch("bar");
        assertEquals(6, td.totalHits);

        //Now do another async update
        indexed = doAsyncIndex(indexed, "a3", "bar");

        tracker.update(indexed);

        //Now total count would be 4
        //3 from async and 1 from current
        td = doSearch("bar");
        assertEquals(4, td.totalHits);
    }

    @Test
    public void addAllSync() throws Exception{
        ListMultimap<String, LuceneDoc> docs = ArrayListMultimap.create();
        tracker = createTracker();
        NodeState indexed = createAndPopulateAsyncIndex(IndexingMode.SYNC);
        tracker.update(indexed);

        DocumentQueue queue = new DocumentQueue(2, tracker, sameThreadExecutor());

        TopDocs td = doSearch("bar");
        assertEquals(1, td.totalHits);

        docs.get("/oak:index/fooIndex").add(createDoc("/a/c", "bar"));
        queue.addAllSynchronously(docs.asMap());

        td = doSearch("bar");
        assertEquals(2, td.totalHits);

        docs.clear();

        docs.get("/oak:index/fooIndex").add(createDoc("/a/d", "bar"));
        queue.addAllSynchronously(docs.asMap());

        td = doSearch("bar");
        assertEquals(3, td.totalHits);
    }

    //@Test
    public void benchMarkIndexWriter() throws Exception{
        Executor executor = Executors.newFixedThreadPool(5);
        IndexCopier indexCopier = new IndexCopier(executor, temporaryFolder.getRoot());
        indexFactory = new NRTIndexFactory(indexCopier, clock, TimeUnit.MILLISECONDS.toSeconds(refreshDelta), StatisticsProvider.NOOP);
        tracker = new IndexTracker(
                new DefaultIndexReaderFactory(defaultMountInfoProvider(), indexCopier),
                indexFactory
        );
        NodeState indexed = createAndPopulateAsyncIndex(IndexingMode.NRT);
        tracker.update(indexed);

        DocumentQueue queue = new DocumentQueue(1000, tracker, executor);

        /*
            Sample output
            [nrt] Time taken for 10000 is 639.3 ms with waits 1
            [sync] Time taken for 10000 is 30.34 s

            Refreshing reader after every commit would slow down things
         */

        LuceneDoc doc = createDoc("/a/b", "a");
        int numDocs = 10000;
        Stopwatch w = Stopwatch.createStarted();
        int waitCount = 0;
        for (int i = 0; i < numDocs; i++) {
            while(!queue.add(doc)){
                waitCount++;
            }
        }

        System.out.printf("%n[nrt] Time taken for %d is %s with waits %d%n", numDocs, w, waitCount);

        indexed = createAndPopulateAsyncIndex(IndexingMode.SYNC);
        tracker.update(indexed);
        queue = new DocumentQueue(1000, tracker, executor);

        w = Stopwatch.createStarted();
        for (int i = 0; i < numDocs; i++) {
            ListMultimap<String, LuceneDoc> docs = ArrayListMultimap.create();
            docs.get("/oak:index/fooIndex").add(doc);
            queue.addAllSynchronously(docs.asMap());
        }
        System.out.printf("%n[sync] Time taken for %d is %s%n", numDocs, w);

    }

    private NodeState doAsyncIndex(NodeState current, String childName, String fooValue) throws CommitFailedException {
        //Have some stuff to be indexed
        NodeBuilder builder = current.builder();
        builder.child(childName).setProperty("foo", fooValue);
        NodeState after = builder.getNodeState();
        return asyncHook.processCommit(current, after, newCommitInfo());
    }

    private TopDocs doSearch(String fooValue) throws IOException {
        IndexNode indexNode = tracker.acquireIndexNode("/oak:index/fooIndex");
        try {
            return indexNode.getSearcher().search(new TermQuery(new Term("foo", fooValue)), 10);
        } finally {
            indexNode.release();
        }
    }

    private void addDoc(DocumentQueue queue, String docPath, String fooValue) {
        LuceneDoc doc = createDoc(docPath, fooValue);
        queue.add(doc);
    }

    private static LuceneDoc createDoc(String docPath, String fooValue) {
        Document d1 = new Document();
        d1.add(newPathField(docPath));
        d1.add(new StringField("foo", fooValue, Field.Store.NO));
        return LuceneDoc.forUpdate("/oak:index/fooIndex", docPath, d1);
    }

    private IndexTracker createTracker() throws IOException {
        IndexCopier indexCopier = new IndexCopier(sameThreadExecutor(), temporaryFolder.getRoot());
        indexFactory = new NRTIndexFactory(indexCopier, clock, TimeUnit.MILLISECONDS.toSeconds(refreshDelta), StatisticsProvider.NOOP);
        return new IndexTracker(
                new DefaultIndexReaderFactory(defaultMountInfoProvider(), indexCopier),
                indexFactory
        );
    }

    private NodeState createAndPopulateAsyncIndex(IndexingMode indexingMode) throws CommitFailedException {
        createIndexDefinition("fooIndex", indexingMode);

        //Have some stuff to be indexed
        builder.child("a").setProperty("foo", "bar");
        NodeState after = builder.getNodeState();
        return asyncHook.processCommit(EMPTY_NODE, after, newCommitInfo());
    }

    private CommitInfo newCommitInfo(){
        info = new CommitInfo("admin", "s1",
                ImmutableMap.<String, Object>of(CommitContext.NAME, new SimpleCommitContext()));
        return info;
    }

    private void createIndexDefinition(String idxName, IndexingMode indexingMode) {
        NodeBuilder idx = newLucenePropertyIndexDefinition(builder.child("oak:index"),
                idxName, ImmutableSet.of("foo"), "async");
        //Disable compression
        //idx.setProperty("codec", "oakCodec");
        TestUtil.enableIndexingMode(idx, indexingMode);
    }

}