/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.guava.common.util.concurrent.Monitor;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.document.PausableDocumentStore.PauseCallback;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests DocumentNodeStore on various DocumentStore back-ends.
 */
public class DocumentNodeStoreIT extends AbstractDocumentStoreTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    public DocumentNodeStoreIT(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @After
    public void tearDown() {
        Revision.resetClockToDefault();
        markDocumentsForCleanup();
    }

    private void markDocumentsForCleanup() {
        for (NodeDocument doc : Utils.getAllDocuments(ds)) {
            removeMe.add(doc.getId());
        }
    }

    @Test
    public void modifiedResetWithDiff() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        DocumentStore docStore = new NonDisposingDocumentStore(ds);
        // use a builder with a no-op diff cache to simulate a
        // cache miss when the diff is made later in the test
        DocumentNodeStore ns1 = new TestBuilder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).clock(clock)
                .build();
        removeMeClusterNodes.add("1");
        NodeBuilder builder1 = ns1.getRoot().builder();
        builder1.child("node");
        removeMe.add(getIdFromPath("/node"));
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD; i++) {
            builder1.child("node-" + i);
            removeMe.add(getIdFromPath("/node/node-" + i));
        }
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // make sure commit is visible to other node store instance
        ns1.runBackgroundOperations();

        DocumentNodeStore ns2 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        removeMeClusterNodes.add("2");

        NodeBuilder builder2 = ns2.getRoot().builder();
        builder2.child("node").child("child-a");
        removeMe.add(getIdFromPath("/node/child-a"));
        ns2.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // wait at least _modified resolution. in reality the wait may
        // not be necessary. e.g. when the clock passes the resolution boundary
        // exactly at this time
        clock.waitUntil(System.currentTimeMillis() +
                SECONDS.toMillis(MODIFIED_IN_SECS_RESOLUTION + 1));

        builder1 = ns1.getRoot().builder();
        builder1.child("node").child("child-b");
        removeMe.add(getIdFromPath("/node/child-b"));
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // remember root for diff
        DocumentNodeState root1 = ns1.getRoot();

        builder1 = root1.builder();
        builder1.child("node").child("child-c");
        removeMe.add(getIdFromPath("/node/child-c"));
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // remember root for diff
        DocumentNodeState root2 = ns1.getRoot();

        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        JsopDiff diff = new JsopDiff("", 0);
        ns1.compare(root2, root1, diff);
        // must report /node as changed
        assertEquals("^\"node\":{}", diff.toString());

        ns1.dispose();
        ns2.dispose();
    }

    @Test
    public void blockingBlob() throws Exception {
        ExecutorService updateExecutor = newSingleThreadExecutor();
        ExecutorService commitExecutor = newSingleThreadExecutor();
        DocumentStore docStore = new NonDisposingDocumentStore(ds);
        DocumentNodeStore store = builderProvider.newBuilder()
                .setDocumentStore(docStore).build();
        removeMeClusterNodes.add("" + store.getClusterId());
        try {

            // A blob whose stream blocks on read
            BlockingBlob blockingBlob = new BlockingBlob();

            // Use a background thread to add the blocking blob to a property
            updateExecutor.submit((Callable<?>) () -> {
                DocumentNodeState root = store.getRoot();
                NodeBuilder builder = root.builder();
                builder.setProperty("blockingBlob", blockingBlob);
                merge(store, builder);
                return null;
            });

            // Wait for reading on the blob to block
            assertTrue(blockingBlob.waitForRead(1, SECONDS));

            // Commit something else in another background thread
            Future<Void> committed = commitExecutor.submit(() -> {
                DocumentNodeState root = store.getRoot();
                NodeBuilder builder = root.builder();
                builder.child("foo");
                merge(store, builder);
                return null;
            });

            // Commit should not get blocked by the blob blocked on reading
            try {
                committed.get(5, SECONDS);
            } catch (TimeoutException e) {
                fail("Commit must not block");
            } finally {
                blockingBlob.unblock();
            }
        } finally {
            new ExecutorCloser(commitExecutor).close();
            new ExecutorCloser(updateExecutor).close();
            store.dispose();
        }
    }

    /**
     * OAK-11184 : a cluster node A is merging a change on root. That involves two
     * updates : the first one writing the changes. The second one updating the
     * commit root (with a revisions=c). If a cluster node B reads the root while
     * the commit root was not yet updated, it has to read previous documents as
     * part resolving the value of a property in getNodeAtRevision. Only to find
     * that the new property does not exist in any previous document and is thus
     * non-existent. Cluster node B will have to repeat going through
     * previous documents whenever reading root - until B is
     * able to do a backgroundRead. The backgroundRead however could be blocked by a
     * number of merge operations - as those merge operations acquire the
     * backgroundOperationLock - and backgroundRead wants that lock exclusively.
     *
     * The test reproduces only part one of the above (expensiveness of reading
     * not-yet-visible property of a document with many previous documents).
     */
    @Ignore
    @Test
    public void unmergedCommitOnRoot() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);

        FailingDocumentStore fs1 = new FailingDocumentStore(ds);
        PausableDocumentStore store1 = new PausableDocumentStore(fs1);
        DocumentNodeStore ns1 = builderProvider.newBuilder().setClusterId(1).setAsyncDelay(0).clock(clock)
                .setDocumentStore(store1).build();

        NodeBuilder b1 = ns1.getRoot().builder();
        b1.setProperty("prop", -1);
        ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // create MANY previous docs
        System.out.println(new Date() + " - creating MANY previous docs...");
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
                b1 = ns1.getRoot().builder();
                b1.setProperty("prop", i);
                ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
            ns1.runBackgroundOperations();
        }
        System.out.println(new Date() + " - done creating MANY previous docs.");

        // create /a as some initial content
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("a1").child("b1");
        builder.child("a2").child("b2");
        merge(ns1, builder);

        // update a root property (but not via a branch commit)
        builder = ns1.getRoot().builder();
        builder.setProperty("rootprop", "v");
        builder.child("a1").setProperty("nonrootprop", "v");
        final NodeBuilder finalBuilder = builder;
        final Semaphore breakpointReachedSemaphore = new Semaphore(0);
        final Semaphore continueSemaphore = new Semaphore(0);
        final Thread mergeThread = new Thread(() -> {
            try {
                merge(ns1, finalBuilder);
            } catch (CommitFailedException e) {
                throw new RuntimeException(e);
            }
        });
        PauseCallback pauseCallback = new PauseCallback() {
            @Override
            public PauseCallback handlePause(List<UpdateOp> remainingOps) {
                breakpointReachedSemaphore.release(1);
                try {
                    if (!continueSemaphore.tryAcquire(5, TimeUnit.SECONDS)) {
                        System.err.println("timeout");
                        throw new RuntimeException("timeout");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        };
        store1.pauseWith(pauseCallback).on(NODES).on("0:/").after(2).eternally();
        mergeThread.start();
        boolean breakpointReached = breakpointReachedSemaphore.tryAcquire(5, TimeUnit.SECONDS);
        assertTrue(breakpointReached);

        try {
            // start B
            CountingDocumentStore cds = new CountingDocumentStore(ds);
            DocumentNodeStore ns2 = builderProvider.newBuilder().setClusterId(2).setAsyncDelay(0).clock(clock)
                    .setDocumentStore(cds).build();

            // now simulate any write and count how many times
            // find() had to be invoked (basically to read a previous doc)
            // if there's more than 495, then that's considered expensive
            {
                int c = getNodesFindCountOfAnUpdate(cds, ns2, "v0");
                assertNoPreviousDocsRead(c, 95);
                // prior to the fix it would have been:
                // assertPreviousDocsRead(c, 95);
            }

            // while the merge in the other thread isn't done, we can repeat this
            // and it's still slow
            {
                int c = getNodesFindCountOfAnUpdate(cds, ns2, "v1");
                assertNoPreviousDocsRead(c, 95);
                // prior to the fix it would have been:
                // assertPreviousDocsRead(c, 95);
            }

            // and again, you get the point
            {
                int c = getNodesFindCountOfAnUpdate(cds, ns2, "v2");
                assertNoPreviousDocsRead(c, 95);
                // prior to the fix it would have been:
                // assertPreviousDocsRead(c, 95);
            }

            // but as soon as we release the other thread and let it finish
            // its commit, then our updates here are no longer expensive
            continueSemaphore.release();
            // let's make sure that other thread is really done
            mergeThread.join(5000);
            assertFalse(mergeThread.isAlive());

            // doing just another update without any bg work won't fix anything yet
            {
                int c = getNodesFindCountOfAnUpdate(cds, ns2, "v3");
                assertNoPreviousDocsRead(c, 95);
                // prior to the fix it would have been:
                // assertPreviousDocsRead(c, 95);
            }
            // neither does doing just an update on ns2
            ns2.runBackgroundOperations();
            {
                int c = getNodesFindCountOfAnUpdate(cds, ns2, "v4");
                assertNoPreviousDocsRead(c, 95);
                // prior to the fix it would have been:
                // assertPreviousDocsRead(c, 95);
            }
            // neither does just doing an update on ns1
            ns1.runBackgroundOperations();
            {
                int c = getNodesFindCountOfAnUpdate(cds, ns2, "v5");
                assertNoPreviousDocsRead(c, 95);
                // prior to the fix it would have been:
                // assertPreviousDocsRead(c, 95);
            }
            // just doing ns1 THEN ns2 will do the trick
            ns2.runBackgroundOperations();
            {
                int c = getNodesFindCountOfAnUpdate(cds, ns2, "v6");
                // this worked irrespective of the fix:
                assertNoPreviousDocsRead(c, 95);
            }
        } finally {
            // in case anyone is still waiting (eg in a test failure case) :
            continueSemaphore.release();
        }

        // a bit simplistic, but that's one way to reproduce the bug
        System.out.println(new Date() + " - success.");
    }

    private void assertNoPreviousDocsRead(int c, int threshold) {
        assertTrue("c expected smaller than " + threshold + ", is: " + c, c < threshold);
    }

    // unused with fix for OAK-11184 - reproducing it requires this though (ie leaving FTR)
    @SuppressWarnings("unused")
    private void assertPreviousDocsRead(int c, int threshold) {
        assertTrue("c expected larger than " + threshold + ", is: " + c, c > threshold);
    }

    private int getNodesFindCountOfAnUpdate(CountingDocumentStore cds, DocumentNodeStore ns2, String newValue)
            throws CommitFailedException {
        cds.resetCounters();
        int nodesFindCount;
        NodeBuilder b2 = ns2.getRoot().builder();
        b2.setProperty("p", newValue);
        merge(ns2, b2);
        nodesFindCount = cds.getNumFindCalls(NODES);
        cds.resetCounters();
        return nodesFindCount;
    }

    /**
     *  A blob that blocks on read until unblocked
     */
    class BlockingBlob extends AbstractBlob {
        private final AtomicBoolean blocking = new AtomicBoolean(true);
        private final Monitor readMonitor = new Monitor();
        private boolean reading = false;

        boolean waitForRead(int time, TimeUnit unit) throws InterruptedException {
            readMonitor.enter();
            try {
                return readMonitor.waitFor(new Monitor.Guard(readMonitor) {
                    @Override
                    public boolean isSatisfied() {
                        return reading;
                    }
                }, time, unit);
            } finally {
                readMonitor.leave();
            }
        }

        void unblock() {
            blocking.set(false);
        }

        @NotNull
        @Override
        public InputStream getNewStream() {
            return new InputStream() {

                @Override
                public int read() {
                    while (blocking.get()) {
                        if (!reading) {
                            readMonitor.enter();
                            try {
                                reading = true;
                            } finally {
                                readMonitor.leave();
                            }
                        }
                    }
                    return -1;
                }
            };
        }

        @Override
        public long length() {
            return -1;
        }
    }

    private static class NonDisposingDocumentStore
            extends TimingDocumentStoreWrapper {

        NonDisposingDocumentStore(DocumentStore base) {
            super(base);
        }

        @Override
        public void dispose() {
            // do not dispose yet
        }
    }

    private class TestBuilder extends DocumentNodeStoreBuilder<TestBuilder> {

        @Override
        public DiffCache getDiffCache(int clusterId) {
            return AmnesiaDiffCache.INSTANCE;
        }
    }
}
