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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Monitor;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
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
