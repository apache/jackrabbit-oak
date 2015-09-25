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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class VersionGCDeletionTest {
    private Clock clock;

    private DocumentNodeStore store;

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
    }

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.dispose();
        }
        Revision.resetClockToDefault();
    }

    @Test
    public void deleteParentLast() throws Exception{
        TestDocumentStore ts = new TestDocumentStore();
        store = new DocumentMK.Builder()
                .clock(clock)
                .setDocumentStore(ts)
                .setAsyncDelay(0)
                .getNodeStore();

        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());

        NodeBuilder b1 = store.getRoot().builder();
        b1.child("x").child("y");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        long maxAge = 1; //hours
        long delta = TimeUnit.MINUTES.toMillis(10);

        //Remove x/y
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x").remove();
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store.runBackgroundOperations();

        //3. Check that deleted doc does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        VersionGarbageCollector gc = store.getVersionGarbageCollector();

        //4. Ensure that while GC is being run /x gets removed but failure occurs
        //for /x/y. At least attempt that! Once issue is fixed the list would be
        //sorted again by VersionGC and then /x would always come after /x/y
        try {
            ts.throwException = true;
            gc.gc(maxAge * 2, HOURS);
            fail("Exception should be thrown");
        } catch (AssertionError ignore) {

        }

        ts.throwException = false;
        gc.gc(maxAge * 2, HOURS);
        assertNull(ts.find(Collection.NODES, "2:/x/y"));
        assertNull(ts.find(Collection.NODES, "1:/x"));
    }

    @Test
    public void deleteLargeNumber() throws Exception{
        int noOfDocsToDelete = 10000;
        DocumentStore ts = new MemoryDocumentStore();
        store = new DocumentMK.Builder()
                .clock(clock)
                .setDocumentStore(new MemoryDocumentStore())
                .setAsyncDelay(0)
                .getNodeStore();

        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());

        NodeBuilder b1 = store.getRoot().builder();
        NodeBuilder xb = b1.child("x");
        for (int i = 0; i < noOfDocsToDelete; i++){
            xb.child("a"+i).child("b"+i);
        }
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        long maxAge = 1; //hours
        long delta = TimeUnit.MINUTES.toMillis(10);

        //Remove x/y
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x").remove();
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store.runBackgroundOperations();

        //3. Check that deleted doc does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        VersionGarbageCollector gc = store.getVersionGarbageCollector();
        gc.setOverflowToDiskThreshold(100);

        VersionGCStats stats = gc.gc(maxAge * 2, HOURS);
        assertEquals(noOfDocsToDelete * 2 + 1, stats.deletedDocGCCount);


        assertNull(ts.find(Collection.NODES, "1:/x"));

        for (int i = 0; i < noOfDocsToDelete; i++){
            assertNull(ts.find(Collection.NODES, "2:/a"+i+"/b"+i));
            assertNull(ts.find(Collection.NODES, "1:/a"+i));
        }
    }

    @Test
    public void gcWithPathsHavingNewLine() throws Exception{
        int noOfDocsToDelete = 200;
        DocumentStore ts = new MemoryDocumentStore();
        store = new DocumentMK.Builder()
                .clock(clock)
                .setDocumentStore(new MemoryDocumentStore())
                .setAsyncDelay(0)
                .getNodeStore();

        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());

        NodeBuilder b1 = store.getRoot().builder();
        NodeBuilder xb = b1.child("x");
        for (int i = 0; i < noOfDocsToDelete - 1; i++){
            xb.child("a"+i).child("b"+i);
        }
        xb.child("a-1").child("b\r");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        long maxAge = 1; //hours
        long delta = TimeUnit.MINUTES.toMillis(10);

        //Remove x/y
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x").remove();
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store.runBackgroundOperations();

        //3. Check that deleted doc does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        VersionGarbageCollector gc = store.getVersionGarbageCollector();
        gc.setOverflowToDiskThreshold(100);

        VersionGCStats stats = gc.gc(maxAge * 2, HOURS);
        assertEquals(noOfDocsToDelete * 2 + 1, stats.deletedDocGCCount);
    }

    // OAK-2420
    @Test
    public void queryWhileDocsAreRemoved() throws Exception {
        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());

        final Thread currentThread = Thread.currentThread();
        final Semaphore queries = new Semaphore(0);
        final CountDownLatch ready = new CountDownLatch(1);
        MemoryDocumentStore ms = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                if (Thread.currentThread() != currentThread) {
                    ready.countDown();
                    queries.acquireUninterruptibly();
                }
                return super.find(collection, key);
            }
        };
        store = new DocumentMK.Builder().clock(clock)
                .setDocumentStore(ms).setAsyncDelay(0).getNodeStore();

        // create nodes
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder node = builder.child("node");
        for (int i = 0; i < 200; i++) {
            node.child("c-" + i);
        }
        merge(store, builder);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        // remove nodes
        builder = store.getRoot().builder();
        node = builder.child("node");
        for (int i = 0; i < 90; i++) {
            node.getChildNode("c-" + i).remove();
        }
        merge(store, builder);

        store.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        List<String> expected = Lists.newArrayList();
        // fill caches
        NodeState n = store.getRoot().getChildNode("node");
        for (ChildNodeEntry entry : n.getChildNodeEntries()) {
            expected.add(entry.getName());
        }
        assertEquals(110, expected.size());

        // invalidate the nodeChildren cache only
        store.invalidateNodeChildrenCache();

        Future<List<String>> f = newSingleThreadExecutor().submit(
                new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                List<String> names = Lists.newArrayList();
                NodeState n = store.getRoot().getChildNode("node");
                for (ChildNodeEntry entry : n.getChildNodeEntries()) {
                    names.add(entry.getName());
                }
                return names;
            }
        });

        // run GC once the reader thread is collecting documents
        ready.await();
        VersionGarbageCollector gc = store.getVersionGarbageCollector();
        VersionGCStats stats = gc.gc(30, MINUTES);
        assertEquals(90, stats.deletedDocGCCount);

        queries.release(200);

        List<String> names = f.get();
        assertEquals(expected, names);
    }

    private void merge(DocumentNodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static class TestDocumentStore extends MemoryDocumentStore {
        boolean throwException;
        @Override
        public <T extends Document> void remove(Collection<T> collection, String key) {
            if (throwException && "2:/x/y".equals(key)){
                throw new AssertionError();
            }
            super.remove(collection, key);
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        @Override
        public <T extends Document> List<T> query(Collection<T> collection, String fromKey,
                                                  String toKey, String indexedProperty, long startValue, int limit) {
            List<T> result = super.query(collection, fromKey, toKey, indexedProperty, startValue, limit);

            //Ensure that /x comes before /x/y
            if (NodeDocument.DELETED_ONCE.equals(indexedProperty)){
                Collections.sort((List<NodeDocument>)result, new NodeDocComparator());
            }
            return result;
        }
    }

    /**
     * Ensures that NodeDocument with path  /x/y /x/y/z /x get sorted to
     * /x /x/y /x/y/z
     */
    private static class NodeDocComparator implements Comparator<NodeDocument> {
        private static Comparator<String> reverse = Collections.reverseOrder(PathComparator.INSTANCE);

        @Override
        public int compare(NodeDocument o1, NodeDocument o2) {
            return reverse.compare(o1.getPath(), o2.getPath());
        }
    }
}
