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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.slf4j.helpers.MessageFormatter.arrayFormat;

public class VersionGCTest {

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private ExecutorService execService;

    private TestStore store = new TestStore();

    private DocumentNodeStore ns;

    private VersionGarbageCollector gc;

    @Before
    public void setUp() throws Exception {
        execService = Executors.newCachedThreadPool();
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ns = builderProvider.newBuilder()
                .clock(clock)
                .setLeaseCheck(false)
                .setDocumentStore(store)
                .setAsyncDelay(0)
                .getNodeStore();
        // create test content
        createNode("foo");
        removeNode("foo");

        // wait one hour
        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        gc = ns.getVersionGarbageCollector();
    }

    @After
    public void tearDown() throws Exception {
        execService.shutdown();
        execService.awaitTermination(1, MINUTES);
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
    }

    @Test
    public void failParallelGC() throws Exception {
        // block gc call
        store.semaphore.acquireUninterruptibly();
        Future<VersionGCStats> stats = gc();
        boolean gcBlocked = false;
        for (int i = 0; i < 10; i ++) {
            if (store.semaphore.hasQueuedThreads()) {
                gcBlocked = true;
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(gcBlocked);
        // now try to trigger another GC
        try {
            gc.gc(30, TimeUnit.MINUTES);
            fail("must throw an IOException");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("already running"));
        } finally {
            store.semaphore.release();
            stats.get();
        }
    }

    @Test
    public void cancel() throws Exception {
        // block gc call
        store.semaphore.acquireUninterruptibly();
        Future<VersionGCStats> stats = gc();
        boolean gcBlocked = false;
        for (int i = 0; i < 10; i ++) {
            if (store.semaphore.hasQueuedThreads()) {
                gcBlocked = true;
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(gcBlocked);
        // now cancel the GC
        gc.cancel();
        store.semaphore.release();
        assertTrue(stats.get().canceled);
    }

    @Test
    public void cancelMustNotUpdateLastOldestTimeStamp() throws Exception {
        // get previous entry from SETTINGS
        String versionGCId = "versionGC";
        String lastOldestTimeStampProp = "lastOldestTimeStamp";
        Document statusBefore = store.find(Collection.SETTINGS, versionGCId);
        // block gc call
        store.semaphore.acquireUninterruptibly();
        Future<VersionGCStats> stats = gc();
        boolean gcBlocked = false;
        for (int i = 0; i < 10; i ++) {
            if (store.semaphore.hasQueuedThreads()) {
                gcBlocked = true;
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(gcBlocked);
        // now cancel the GC
        gc.cancel();
        store.semaphore.release();
        assertTrue(stats.get().canceled);

        // ensure a canceled GC doesn't update that versionGC SETTINGS entry
        Document statusAfter = store.find(Collection.SETTINGS, "versionGC");
        if (statusBefore == null) {
            assertNull(statusAfter);
        } else {
            assertNotNull(statusAfter);
            assertEquals(
                    "canceled GC shouldn't change the " + lastOldestTimeStampProp + " property on " + versionGCId
                            + " settings entry",
                    statusBefore.get(lastOldestTimeStampProp), statusAfter.get(lastOldestTimeStampProp));
        }
    }

    @Test
    public void getInfo() throws Exception {
        gc.gc(1, TimeUnit.HOURS);

        gc.getInfo(1, TimeUnit.HOURS);
    }

    @Test
    public void gcMonitorStatusUpdates() throws Exception {
        final List<String> statusMessages = Lists.newArrayList();
        GCMonitor monitor = new GCMonitor.Empty() {
            @Override
            public void updateStatus(String status) {
                statusMessages.add(status);
            }
        };
        gc.setGCMonitor(monitor);

        gc.gc(30, TimeUnit.MINUTES);

        List<String> expected = Lists.newArrayList("INITIALIZING",
                "COLLECTING", "CHECKING", "COLLECTING", "DELETING", "SORTING",
                "DELETING", "UPDATING", "SPLITS_CLEANUP", "IDLE");
        assertEquals(expected, statusMessages);
    }

    @Test
    public void gcMonitorInfoMessages() throws Exception {
        final List<String> infoMessages = Lists.newArrayList();
        GCMonitor monitor = new GCMonitor.Empty() {
            @Override
            public void info(String message, Object[] arguments) {
                infoMessages.add(arrayFormat(message, arguments).getMessage());
            }
        };
        gc.setGCMonitor(monitor);

        gc.gc(1, TimeUnit.HOURS);

        assertEquals(3, infoMessages.size());
        assertTrue(infoMessages.get(0).startsWith("Start "));
        assertTrue(infoMessages.get(1).startsWith("Looking at revisions"));
        assertTrue(infoMessages.get(2).startsWith("Revision garbage collection finished"));
    }

    @Test
    public void findVersionGC() throws Exception {
        store.findVersionGC.set(0);
        gc.gc(1, TimeUnit.HOURS);
        // must only read once
        assertEquals(1, store.findVersionGC.get());
    }

    private Future<VersionGCStats> gc() {
        // run gc in a separate thread
        return execService.submit(new Callable<VersionGCStats>() {
            @Override
            public VersionGCStats call() throws Exception {
                return gc.gc(30, TimeUnit.MINUTES);
            }
        });
    }

    private void removeNode(String name) throws CommitFailedException {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child(name).remove();
        merge(ns, builder);
    }

    private void createNode(String name) throws CommitFailedException {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child(name);
        merge(ns, builder);
    }

    private void merge(DocumentNodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private class TestStore extends MemoryDocumentStore {

        Semaphore semaphore = new Semaphore(1);

        AtomicLong findVersionGC = new AtomicLong();

        @Nonnull
        @Override
        public <T extends Document> List<T> query(Collection<T> collection,
                                                  String fromKey,
                                                  String toKey,
                                                  String indexedProperty,
                                                  long startValue,
                                                  int limit) {
            semaphore.acquireUninterruptibly();
            try {
                return super.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
            } finally {
                semaphore.release();
            }
        }

        @Override
        public <T extends Document> T find(Collection<T> collection,
                                           String key) {
            if (collection == Collection.SETTINGS
                    && key.equals("versionGC")) {
                findVersionGC.incrementAndGet();
            }
            return super.find(collection, key);
        }
    }

}
