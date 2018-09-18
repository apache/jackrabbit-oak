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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;
import static org.junit.Assert.fail;

public class ReaderRefCountIT {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;
    private IndexCopier indexCopier;
    private int runTimeInSecs = 25;
    private int noOfThread = 5;

    @Before
    public void setUp() throws IOException {
        indexCopier = new IndexCopier(sameThreadExecutor(), temporaryFolder.getRoot());
    }

    @Test
    public void syncIndex() throws Exception{
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.indexRule("nt:base").property("foo").propertyIndex();
        idx.async("async", "sync");

        NRTIndexFactory nrtFactory = new NRTIndexFactory(indexCopier, StatisticsProvider.NOOP);
        runMultiReaderScenario(idx, nrtFactory, false);
    }

    @Test
    public void nrtIndex() throws Exception{
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.indexRule("nt:base").property("foo").propertyIndex();
        idx.async("async", "nrt");

        NRTIndexFactory nrtFactory = new NRTIndexFactory(indexCopier, Clock.SIMPLE,
                0 , StatisticsProvider.NOOP);
        runMultiReaderScenario(idx, nrtFactory, false);
    }

    /**
     * This test enables 1 more thread which updates the IndexTracker
     * This causes the IndexNodeManager to switch to newer indexes
     * and hence lead to creation and closing of older NRTIndexes
     */
    @Test
    public void indexTrackerUpdatesAndNRT() throws Exception{
        IndexDefinitionBuilder idx = new IndexDefinitionBuilder();
        idx.indexRule("nt:base").property("foo").propertyIndex();
        idx.async("async", "nrt");

        NRTIndexFactory nrtFactory = new NRTIndexFactory(indexCopier, Clock.SIMPLE,
                0 , StatisticsProvider.NOOP);
        runMultiReaderScenario(idx, nrtFactory, true);
    }

    private void runMultiReaderScenario(IndexDefinitionBuilder defnb,
                                       NRTIndexFactory nrtFactory, boolean updateIndex) throws Exception{
        NodeBuilder builder = root.builder();
        builder.child("oak:index").setChildNode("fooIndex", defnb.build());
        LuceneIndexEditorContext.configureUniqueId(builder.child("oak:index").child("fooIndex"));
        NodeState repoState = builder.getNodeState();

        String indexPath = "/oak:index/fooIndex";

        AtomicBoolean stop = new AtomicBoolean();
        List<Throwable> exceptionList = new CopyOnWriteArrayList<>();

        IndexTracker tracker = new IndexTracker(new DefaultIndexReaderFactory(defaultMountInfoProvider(), indexCopier), nrtFactory);
        tracker.update(repoState);

        CountDownLatch errorLatch = new CountDownLatch(1);
        UncaughtExceptionHandler uh = new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace();
                exceptionList.add(e);
                errorLatch.countDown();
            }
        };

        DocumentQueue queue = new DocumentQueue(100, tracker, sameThreadExecutor());
        queue.setExceptionHandler(uh);


        //Writer should try to refresh same IndexNode within same lock
        //i.e. simulate a scenario where DocumentQueue pushes multiple
        //sync index docs in same commit
        Runnable writer = new Runnable() {
            @Override
            public void run() {
                while(!stop.get()) {
                    Document d1 = new Document();
                    d1.add(newPathField("/a/b"));
                    LuceneDoc lcDoc = LuceneDoc.forUpdate(indexPath, "/a", d1);
                    queue.addAllSynchronously(of(indexPath, singletonList(lcDoc)));
                }
            }
        };

        //Reader would try perform query
        Runnable reader = new Runnable() {
            @Override
            public void run() {
                while(!stop.get()) {
                    IndexNode indexNode = tracker.acquireIndexNode(indexPath);
                    if (indexNode != null) {
                        try {
                            indexNode.getSearcher().search(new MatchAllDocsQuery(), 5);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            indexNode.release();
                        }
                    }
                }
            }
        };

        Runnable indexUpdater = new Runnable() {
            @Override
            public void run() {
                int count = 0;
                while(!stop.get()) {
                    NodeBuilder b = repoState.builder();
                    b.getChildNode("oak:index").getChildNode("fooIndex").setProperty("count", count++);
                    tracker.update(b.getNodeState());
                }
            }
        };

        Thread wt = new Thread(writer);
        List<Thread> threads = new ArrayList<>();
        threads.add(wt);
        for (int i = 0; i < noOfThread; i++) {
            Thread t = new Thread(reader);
            threads.add(t);
            t.setUncaughtExceptionHandler(uh);
        }

        if (updateIndex) {
            threads.add(new Thread(indexUpdater));
        }

        for (Thread t : threads) {
            t.start();
        }

        errorLatch.await(runTimeInSecs, TimeUnit.SECONDS);

        stop.set(true);

        for (Thread t : threads) {
            t.join();
        }

        nrtFactory.close();

        if (!exceptionList.isEmpty()) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            for (Throwable t : exceptionList) {
                t.printStackTrace(pw);
            }
            pw.flush();
            fail(sw.toString());
        }
    }
}
