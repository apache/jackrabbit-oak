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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.google.common.collect.Iterables.size;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NUM_REVS_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PREV_SPLIT_FACTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs a DocumentMK revision GC interleaved with a document split.
 * Test for OAK-1791.
 */
@RunWith(Parameterized.class)
public class VersionGCWithSplitTest {

    private DocumentStoreFixture fixture;

    private Clock clock;

    private Map<Thread, Semaphore> updateLocks = Maps.newIdentityHashMap();

    private DocumentNodeStore store;

    private VersionGarbageCollector gc;

    public VersionGCWithSplitTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters
    public static java.util.Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = Lists.newArrayList();
        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if(mongo.isAvailable()){
            fixtures.add(new Object[] {mongo});
        }
        return fixtures;
    }

    @Before
    public void setUp() throws InterruptedException {
        final DocumentStore docStore = fixture.createDocumentStore();
        DocumentStore testStore = new TestStore(docStore);

        clock = new Clock.Virtual();
        store = new DocumentMK.Builder()
                .clock(clock)
                .setDocumentStore(testStore)
                .setAsyncDelay(0)
                .getNodeStore();
        gc = store.getVersionGarbageCollector();

        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());
    }

    @After
    public void tearDown() throws Exception {
        store.dispose();
        fixture.dispose();
        Revision.resetClockToDefault();
    }

    @Test
    public void gcWithConcurrentSplit() throws Exception {
        Revision.setClock(clock);

        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").setProperty("prop", -1);
        merge(store, builder);

        final String id = Utils.getIdFromPath("/test");

        DocumentStore docStore = store.getDocumentStore();
        int count = 0;
        while (docStore.find(NODES, id).getPreviousRanges().size() < PREV_SPLIT_FACTOR) {
            builder = store.getRoot().builder();
            builder.child("test").setProperty("prop", count);
            merge(store, builder);
            if (count++ % NUM_REVS_THRESHOLD == 0) {
                store.runBackgroundOperations();
            }
        }
        // wait one hour
        clock.waitUntil(Revision.getCurrentTimestamp() +
                HOURS.toMillis(1) +
                2 * SECONDS.toMillis(MODIFIED_IN_SECS_RESOLUTION));

        final AtomicReference<VersionGarbageCollector.VersionGCStats> stats = new AtomicReference<VersionGarbageCollector.VersionGCStats>();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    stats.set(gc.gc(1, HOURS));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // block gc thread when it attempts to write
        Semaphore gcLock = new Semaphore(0);
        updateLocks.put(t, gcLock);
        t.start();
        while (!gcLock.hasQueuedThreads()) {
            Thread.sleep(1);
        }

        // perform more changes until intermediate docs are created
        while (docStore.find(NODES, id).getPreviousRanges().size() >= PREV_SPLIT_FACTOR) {
            builder = store.getRoot().builder();
            builder.child("test").setProperty("prop", count);
            merge(store, builder);
            if (count++ % NUM_REVS_THRESHOLD == 0) {
                store.runBackgroundOperations();
            }
        }

        // let gc thread continue
        gcLock.release();
        // the first split doc disconnect will be based on the main document
        // with 10 previous entries (before the intermediate doc was created).
        // the next 9 disconnect calls will see the updated main document
        // pointing to the intermediate doc. those 9 split docs will be
        // disconnected from there.
        t.join();

        assertEquals(10, stats.get().splitDocGCCount);

        NodeDocument doc = docStore.find(NODES, id);
        // there must only be one stale entry because 9 other split docs were
        // disconnected from the new intermediate split doc
        assertEquals(1, doc.getStalePrev().size());

        store.addSplitCandidate(id);
        store.runBackgroundOperations();
        // now there must not be any stale prev entries
        doc = docStore.find(NODES, id);
        assertTrue(doc.getStalePrev().isEmpty());

        Map<Revision, String> valueMap = doc.getValueMap("prop");
        // there must be 101 revisions left. one in the main document and one
        // split document with 100 revisions created after the GC was triggered
        assertEquals(NUM_REVS_THRESHOLD + 1, valueMap.size());
        // also count them individually
        assertEquals(NUM_REVS_THRESHOLD + 1, size(valueMap.entrySet()));
    }

    private void merge(DocumentNodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private final class TestStore extends TimingDocumentStoreWrapper {

        private final DocumentStore docStore;

        public TestStore(DocumentStore base) {
            super(base);
            this.docStore = base;
        }

        @Nonnull
        @Override
        public <T extends Document> T createOrUpdate(final Collection<T> collection,
                                                     final UpdateOp update) {
            final AtomicReference<T> ref = new AtomicReference<T>();
            runLocked(new Runnable() {
                public void run() {
                    ref.set(docStore.createOrUpdate(collection, update));
                }
            });
            return ref.get();
        }

        @Override
        public <T extends Document> T findAndUpdate(final Collection<T> collection,
                                                    final UpdateOp update) {
            final AtomicReference<T> ref = new AtomicReference<T>();
            runLocked(new Runnable() {
                public void run() {
                    ref.set(docStore.findAndUpdate(collection, update));
                }
            });
            return ref.get();
        }

        private void runLocked(Runnable run) {
            Thread t = Thread.currentThread();
            Semaphore s = updateLocks.get(t);
            if (s != null) {
                s.acquireUninterruptibly();
            }
            try {
                run.run();
            } finally {
                if (s != null) {
                    s.release();
                }
            }
        }
    }
}
