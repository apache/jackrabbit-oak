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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore.REMEMBER_REVISION_ORDER_MILLIS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NUM_REVS_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PREV_SPLIT_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentNodeStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStoreTest.class);

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @After
    public void tearDown() {
        Revision.resetClockToDefault();
    }

    // OAK-1254
    @Test
    public void backgroundRead() throws Exception {
        final Semaphore semaphore = new Semaphore(1);
        DocumentStore docStore = new MemoryDocumentStore();
        DocumentStore testStore = new TimingDocumentStoreWrapper(docStore) {
            @Override
            public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
                super.invalidateCache(keys);
                semaphore.acquireUninterruptibly();
                semaphore.release();
                return null;
            }
        };
        final DocumentNodeStore store1 = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(testStore).setClusterId(1).getNodeStore();
        DocumentNodeStore store2 = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(docStore).setClusterId(2).getNodeStore();

        NodeBuilder builder = store2.getRoot().builder();
        builder.child("node2");
        store2.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // force update of _lastRevs
        store2.runBackgroundOperations();

        // at this point only node2 must not be visible
        assertFalse(store1.getRoot().hasChildNode("node2"));

        builder = store1.getRoot().builder();
        builder.child("node1");
        NodeState root =
                store1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        semaphore.acquireUninterruptibly();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                store1.runBackgroundOperations();
            }
        });
        t.start();
        // sleep until 'background thread' invalidated cache
        // and is waiting for semaphore
        while (!semaphore.hasQueuedThreads()) {
            Thread.sleep(10);
        }

        // must still not be visible at this state
        try {
            assertFalse(root.hasChildNode("node2"));
        } finally {
            semaphore.release();
        }
        t.join();
        // background operations completed
        root = store1.getRoot();
        // now node2 is visible
        assertTrue(root.hasChildNode("node2"));
    }

    @Test
    public void childNodeCache() throws Exception {
        DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        int max = (int) (100 * 1.5);
        SortedSet<String> children = new TreeSet<String>();
        for (int i = 0; i < max; i++) {
            String name = "c" + i;
            children.add(name);
            builder.child(name);
        }
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        builder = store.getRoot().builder();
        String name = new ArrayList<String>(children).get(
                100 / 2);
        builder.child(name).remove();
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        int numEntries = Iterables.size(store.getRoot().getChildNodeEntries());
        assertEquals(max - 1, numEntries);
    }

    @Test
    public void childNodeEntries() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        DocumentStore docStore = new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      int limit) {
                counter.incrementAndGet();
                return super.query(collection, fromKey, toKey, limit);
            }
        };
        DocumentNodeStore store = builderProvider.newBuilder()
                .setDocumentStore(docStore).getNodeStore();
        NodeBuilder root = store.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            root.child("node-" + i);
        }
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        counter.set(0);
        // the following should just make one call to DocumentStore.query()
        for (ChildNodeEntry e : store.getRoot().getChildNodeEntries()) {
            e.getNodeState();
        }
        assertEquals(1, counter.get());

        counter.set(0);
        // now the child node entries are cached and no call should happen
        for (ChildNodeEntry e : store.getRoot().getChildNodeEntries()) {
            e.getNodeState();
        }
        assertEquals(0, counter.get());
    }

    @Test
    public void rollback() throws Exception {
        final Map<Thread, Semaphore> locks = Collections.synchronizedMap(
                new HashMap<Thread, Semaphore>());
        final Semaphore created = new Semaphore(0);
        DocumentStore docStore = new MemoryDocumentStore() {
            @Override
            public <T extends Document> boolean create(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
                Semaphore semaphore = locks.get(Thread.currentThread());
                boolean result = super.create(collection, updateOps);
                if (semaphore != null) {
                    created.release();
                    semaphore.acquireUninterruptibly();
                }
                return result;
            }
        };
        final List<Exception> exceptions = new ArrayList<Exception>();
        final DocumentMK mk = builderProvider.newBuilder()
                .setDocumentStore(docStore).setAsyncDelay(0).open();
        final DocumentNodeStore store = mk.getNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("deletedNode");
        builder.child("updateNode").setProperty("foo", "bar");
        merge(store, builder);

        builder = store.getRoot().builder();
        builder.child("deletedNode").remove();
        merge(store, builder);

        final Revision head = store.getHeadRevision();

        Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Revision r = store.newRevision();
                    Commit c = new Commit(store, r, head, null);
                    c.addNode(new DocumentNodeState(store, "/newConflictingNode", r));
                    c.addNode(new DocumentNodeState(store, "/deletedNode", r));
                    c.updateProperty("/updateNode", "foo", "baz");
                    c.apply();
                } catch (DocumentStoreException e) {
                    exceptions.add(e);
                }
            }
        });
        final Semaphore s = new Semaphore(0);
        locks.put(writer, s);
        // will block in DocumentStore.create()
        writer.start();
        // wait for writer to create nodes
        created.acquireUninterruptibly();
        // commit will succeed and add collision marker to writer commit
        Revision r = store.newRevision();
        Commit c = new Commit(store, r, head, null);
        c.addNode(new DocumentNodeState(store, "/newConflictingNode", r));
        c.addNode(new DocumentNodeState(store, "/newNonConflictingNode", r));
        c.apply();
        // allow writer to continue
        s.release();
        writer.join();
        assertEquals("expected exception", 1, exceptions.size());

        String id = Utils.getIdFromPath("/newConflictingNode");
        NodeDocument doc = docStore.find(NODES, id);
        assertNotNull("document with id " + id + " does not exist", doc);
        assertTrue("document with id " + id + " should get _deletedOnce marked due to rollback",
                doc.wasDeletedOnce());

        id = Utils.getIdFromPath("/newNonConflictingNode");
        doc = docStore.find(NODES, id);
        assertNull("document with id " + id + " must not have _deletedOnce",
                doc.get(NodeDocument.DELETED_ONCE));

        id = Utils.getIdFromPath("/deletedNode");
        doc = docStore.find(NODES, id);
        assertTrue("document with id " + id + " should get _deletedOnce marked due to rollback",
                doc.wasDeletedOnce());

        id = Utils.getIdFromPath("/updateNode");
        doc = docStore.find(NODES, id);
        assertNull("document with id " + id + " must not have _deletedOnce despite rollback",
                doc.get(NodeDocument.DELETED_ONCE));
    }

    // OAK-1662
    @Test
    public void getNewestRevision() throws Exception {
        DocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setAsyncDelay(0)
                .setClusterId(1).getNodeStore();
        ns1.getRoot();
        ns1.runBackgroundOperations();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setAsyncDelay(0)
                .setClusterId(2).getNodeStore();
        ns2.getRoot();

        NodeBuilder b1 = ns1.getRoot().builder();
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            b1.setProperty("p", String.valueOf(i));
            ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        ns1.runBackgroundOperations();

        NodeBuilder b2 = ns2.getRoot().builder();
        b2.setProperty("q", "value");
        ns2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void commitHookChangesOnBranch() throws Exception {
        final int NUM_NODES = DocumentRootBuilder.UPDATE_LIMIT / 2;
        final int NUM_PROPS = 10;
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < NUM_NODES; i++) {
            NodeBuilder c = builder.child("n" + i);
            for (int j = 0; j < NUM_PROPS; j++) {
                c.setProperty("q" + j, "value");
                c.setProperty("p" + j, "value");
            }
        }
        try {
            ns.merge(builder, CompositeHook.compose(
                    Arrays.asList(new TestHook("p"), new TestHook("q"), FAILING_HOOK)),
                    CommitInfo.EMPTY);
            fail("merge must fail and reset changes done by commit hooks");
        } catch (CommitFailedException e) {
            // expected
        }
        for (int i = 0; i < NUM_NODES; i++) {
            NodeBuilder c = builder.getChildNode("n" + i);
            assertTrue(c.exists());
            for (int j = 0; j < NUM_PROPS; j++) {
                PropertyState p = c.getProperty("p" + j);
                assertNotNull(p);
                // must still see initial values before failed merge
                assertEquals("value", p.getValue(Type.STRING));
                // same for property 'qX'
                p = c.getProperty("q" + j);
                assertNotNull(p);
                // must still see initial values before failed merge
                assertEquals("value", p.getValue(Type.STRING));
            }
        }
        ns.merge(builder, CompositeHook.compose(
                Arrays.<CommitHook>asList(new TestHook("p"), new TestHook("q"))),
                CommitInfo.EMPTY);

        builder = ns.getRoot().builder();
        // must see properties changed by commit hook
        for (int i = 0; i < NUM_NODES; i++) {
            NodeBuilder c = builder.getChildNode("n" + i);
            assertTrue(c.exists());
            for (int j = 0; j < NUM_PROPS; j++) {
                PropertyState p = c.getProperty("p" + j);
                assertNotNull(p);
                assertEquals("test", p.getValue(Type.STRING));
                p = c.getProperty("q" + j);
                assertNotNull(p);
                assertEquals("test", p.getValue(Type.STRING));
            }
        }
    }

    // OAK-1814
    @Test
    public void visibilityAfterRevisionComparatorPurge() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore nodeStore1 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        nodeStore1.runBackgroundOperations();
        DocumentNodeStore nodeStore2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        DocumentNodeStore nodeStore3 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(3)
                .setAsyncDelay(0).clock(clock).getNodeStore();

        NodeDocument doc = docStore.find(NODES, Utils.getIdFromPath("/"));
        assertNotNull(doc);
        Revision created = doc.getLocalDeleted().firstKey();
        assertEquals(1, created.getClusterId());

        clock.waitUntil(System.currentTimeMillis() +
                REMEMBER_REVISION_ORDER_MILLIS / 2);

        NodeBuilder builder = nodeStore2.getRoot().builder();
        builder.setProperty("prop", "value");
        nodeStore2.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        nodeStore2.runBackgroundOperations();

        clock.waitUntil(System.currentTimeMillis() +
                REMEMBER_REVISION_ORDER_MILLIS + 1000);
        nodeStore3.runBackgroundOperations();

        doc = docStore.find(NODES, Utils.getIdFromPath("/"));
        assertNotNull(doc);
        NodeState state = doc.getNodeAtRevision(nodeStore3,
                nodeStore3.getHeadRevision(), null);
        assertNotNull(state);
    }

    @Test
    public void modifiedReset() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        NodeBuilder builder1 = ns1.getRoot().builder();
        builder1.child("node");
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns1.runBackgroundOperations();

        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();

        NodeBuilder builder2 = ns2.getRoot().builder();
        builder2.child("node").child("child-2");
        ns2.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // wait at least _modified resolution. in reality the wait may
        // not be necessary. e.g. when the clock passes the resolution boundary
        // exactly at this time
        clock.waitUntil(System.currentTimeMillis() +
                SECONDS.toMillis(MODIFIED_IN_SECS_RESOLUTION + 1));

        builder1 = ns1.getRoot().builder();
        builder1.child("node").child("child-1");
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ns1.runBackgroundOperations();

        // get current _modified timestamp on /node
        NodeDocument doc = docStore.find(NODES, Utils.getIdFromPath("/node"));
        Long mod1 = (Long) doc.get(MODIFIED_IN_SECS);
        assertNotNull(mod1);

        ns2.runBackgroundOperations();

        doc = docStore.find(NODES, Utils.getIdFromPath("/node"));
        Long mod2 = (Long) doc.get(MODIFIED_IN_SECS);
        assertTrue("" + mod2 + " < " + mod1, mod2 >= mod1);
    }

    // OAK-1861
    @Test
    public void readChildrenWithDeletedSiblings() throws Exception {
        final AtomicInteger maxLimit = new AtomicInteger(0);
        DocumentStore docStore = new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      int limit) {
                if (collection == NODES) {
                    maxLimit.set(Math.max(limit, maxLimit.get()));
                }
                return super.query(collection, fromKey, toKey, limit);
            }
        };
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(docStore)
                .setAsyncDelay(0).getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 1000; i++) {
            builder.child("node-" + i);
        }
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // now remove all except the last one
        for (int i = 0; i < 999; i++) {
            builder = ns.getRoot().builder();
            builder.getChildNode("node-" + i).remove();
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        for (ChildNodeEntry entry : ns.getRoot().getChildNodeEntries()) {
            entry.getName();
        }
        // must not read more than DocumentNodeState.INITIAL_FETCH_SIZE + 1
        assertTrue(maxLimit.get() + " > " + (DocumentNodeState.INITIAL_FETCH_SIZE + 1),
                maxLimit.get() <= DocumentNodeState.INITIAL_FETCH_SIZE + 1);
    }

    // OAK-1972
    @Test
    public void readFromPreviousDoc() throws CommitFailedException {
        DocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(docStore).getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test").setProperty("prop", "initial");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.dispose();

        ns = builderProvider.newBuilder().setClusterId(2).setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();
        builder = ns.getRoot().builder();
        builder.child("test").setProperty("prop", "value");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Revision rev = ns.getHeadRevision();
        NodeDocument doc = docStore.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);
        DocumentNodeState state = doc.getNodeAtRevision(ns, rev, null);
        assertNotNull(state);
        assertTrue(state.hasProperty("prop"));
        assertEquals("value", state.getProperty("prop").getValue(Type.STRING));

        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            builder = ns.getRoot().builder();
            builder.child("test").setProperty("prop", "v-" + i);
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        ns.runBackgroundOperations();

        // must still return the same value as before the split
        doc = docStore.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);
        state = doc.getNodeAtRevision(ns, rev, null);
        assertNotNull(state);
        assertTrue(state.hasProperty("prop"));
        assertEquals("value", state.getProperty("prop").getValue(Type.STRING));
    }

    // OAK-2232
    @Test
    public void diffExternalChanges() throws Exception {
        long modifiedResMillis = SECONDS.toMillis(MODIFIED_IN_SECS_RESOLUTION);
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);

        DocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder().setAsyncDelay(0)
                .clock(clock).setDocumentStore(docStore).setClusterId(1)
                .getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder().setAsyncDelay(0)
                .clock(clock).setDocumentStore(docStore).setClusterId(2)
                .getNodeStore();

        NodeBuilder builder = ns1.getRoot().builder();
        NodeBuilder test = builder.child("test");
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            test.child("node-" + i);
        }
        ns1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        // make sure next change has a different _modified value
        clock.waitUntil(clock.getTime() + modifiedResMillis * 2);

        builder = ns2.getRoot().builder();
        builder.child("test").child("foo");
        ns2.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // 'wait' again for a different _modified value
        clock.waitUntil(clock.getTime() + modifiedResMillis * 2);

        builder = ns1.getRoot().builder();
        builder.child("test").child("bar");
        ns1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // remember current root for diff
        NodeState r1 = ns1.getRoot();

        ns2.runBackgroundOperations();
        ns1.runBackgroundOperations();

        NodeState r2 = ns1.getRoot();

        // are we able to see foo?
        boolean found = false;
        for (ChildNodeEntry entry : r2.getChildNode("test").getChildNodeEntries()) {
            if (entry.getName().equals("foo")) {
                found = true;
                break;
            }
        }
        assertTrue(found);

        // diff must report '/test' modified and '/test/foo' added
        TrackingDiff diff = new TrackingDiff();
        r2.compareAgainstBaseState(r1, diff);
        assertEquals(1, diff.modified.size());
        assertTrue(diff.modified.contains("/test"));
        assertEquals(1, diff.added.size());
        assertTrue(diff.added.contains("/test/foo"));
    }

    @Test
    public void updateClusterState() {
        DocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();

        ns1.updateClusterState();
        ns2.updateClusterState();

        assertEquals(0, ns1.getInactiveClusterNodes().size());
        assertEquals(0, ns2.getInactiveClusterNodes().size());
        assertEquals(2, ns1.getActiveClusterNodes().size());
        assertEquals(2, ns2.getActiveClusterNodes().size());

        ns1.dispose();

        ns2.updateClusterState();

        Map<Integer, Long> inactive = ns2.getInactiveClusterNodes();
        Map<Integer, Long> active = ns2.getActiveClusterNodes();
        assertEquals(1, inactive.size());
        assertEquals(1, (int) inactive.keySet().iterator().next());
        assertEquals(1, active.size());
        assertEquals(2, (int) active.keySet().iterator().next());
    }

    // OAK-2288
    @Test
    public void mergedBranchVisibility() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder()
                .setAsyncDelay(0).getNodeStore();
        DocumentStore docStore = store.getDocumentStore();

        NodeBuilder builder1 = store.getRoot().builder();
        builder1.child("test");
        merge(store, builder1);

        builder1 = store.getRoot().builder();
        NodeBuilder node = builder1.getChildNode("test").child("node");
        String id = Utils.getIdFromPath("/test/node");
        int i = 0;
        // force creation of a branch
        while (docStore.find(NODES, id) == null) {
            node.setProperty("foo", i++);
        }

        NodeDocument doc = docStore.find(NODES, id);
        assertNotNull(doc);
        Revision rev = doc.getLocalDeleted().firstKey();

        merge(store, builder1);

        // must not be visible at the revision of the branch commit
        assertFalse(store.getRoot(rev).getChildNode("test").getChildNode("node").exists());

        // must be visible at the revision of the merged branch
        assertTrue(store.getRoot().getChildNode("test").getChildNode("node").exists());
    }

    // OAK-2308
    @Test
    public void recoverBranchCommit() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());

        MemoryDocumentStore docStore = new MemoryDocumentStore();

        DocumentNodeStore store1 = builderProvider.newBuilder()
                .setDocumentStore(docStore)
                .setAsyncDelay(0).clock(clock).getNodeStore();

        NodeBuilder builder = store1.getRoot().builder();
        builder.child("test");
        merge(store1, builder);
        // make sure all _lastRevs are written back
        store1.runBackgroundOperations();

        builder = store1.getRoot().builder();
        NodeBuilder node = builder.getChildNode("test").child("node");
        String id = Utils.getIdFromPath("/test/node");
        int i = 0;
        // force creation of a branch
        while (docStore.find(NODES, id) == null) {
            node.setProperty("foo", i++);
        }
        merge(store1, builder);

        // wait until lease expires
        clock.waitUntil(clock.getTime() + store1.getClusterInfo().getLeaseTime() + 1000);
        // run recovery for this store
        LastRevRecoveryAgent agent = store1.getLastRevRecoveryAgent();
        assertTrue(agent.isRecoveryNeeded());
        agent.recover(store1.getClusterId());

        // start a second store
        DocumentNodeStore store2 = builderProvider.newBuilder()
                .setDocumentStore(docStore)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        // must see /test/node
        assertTrue(store2.getRoot().getChildNode("test").getChildNode("node").exists());
    }

    // OAK-2336
    @Test
    public void readBranchCommit() throws Exception {
        final Set<String> readSet = Sets.newHashSet();
        DocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                readSet.add(key);
                return super.find(collection, key);
            }
        };
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        String testId = Utils.getIdFromPath("/test");
        NodeBuilder test = builder.child("test");
        test.setProperty("p", "value");
        // force creation of branch
        int q = 0;
        while (store.find(NODES, testId) == null) {
            test.setProperty("q", q++);
        }
        merge(ns, builder);

        // commit enough changes for a previous doc
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            builder = ns.getRoot().builder();
            builder.child("test").setProperty("q", i);
            merge(ns, builder);
        }
        // trigger split
        ns.runBackgroundOperations();

        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);

        readSet.clear();

        // must not access previous document of /test
        doc.getNodeAtRevision(ns, ns.getHeadRevision(), null);
        for (String id : Sets.newHashSet(readSet)) {
            doc = store.find(NODES, id);
            assertNotNull(doc);
            if (doc.isSplitDocument() && !doc.getMainPath().equals("/")) {
                fail("must not access previous document: " + id);
            }
        }
    }

    // OAK-2345
    @Test
    public void inactiveClusterId() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("test");
        merge(ns1, builder);
        Revision r = ns1.getHeadRevision();
        ns1.dispose();

        // start other cluster node
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        assertNotNull(ns2.getRevisionComparator().getRevisionSeen(r));
        ns2.dispose();

        // wait until revision is old
        clock.waitUntil(System.currentTimeMillis()
                + REMEMBER_REVISION_ORDER_MILLIS + 1000);

        // start cluster 2 again
        ns2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        // now r is considered old and revisionSeen is null
        assertNull(ns2.getRevisionComparator().getRevisionSeen(r));
    }

    // OAK-1782
    @Test
    public void diffOnce() throws Exception {
        final AtomicInteger numQueries = new AtomicInteger();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      String indexedProperty,
                                                      long startValue,
                                                      int limit) {
                numQueries.getAndIncrement();
                return super.query(collection, fromKey, toKey,
                        indexedProperty, startValue, limit);
            }
        };
        final DocumentMK mk = builderProvider.newBuilder()
                .setDocumentStore(store).open();
        final DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        // make sure we have enough children to trigger diffManyChildren
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            builder.child("node-" + i);
        }
        merge(ns, builder);

        final Revision head = ns.getHeadRevision();
        final Revision to = new Revision(
                head.getTimestamp() + 1000, 0, head.getClusterId());
        int numReaders = 10;
        final CountDownLatch ready = new CountDownLatch(numReaders);
        final CountDownLatch go = new CountDownLatch(1);
        List<Thread> readers = Lists.newArrayList();
        for (int i = 0; i < numReaders; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        ready.countDown();
                        go.await();
                        mk.diff(head.toString(), to.toString(), "/", 0);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            });
            readers.add(t);
            t.start();
        }

        ready.await();
        numQueries.set(0);
        go.countDown();

        for (Thread t : readers) {
            t.join();
        }

        // must not perform more than two queries
        // 1) query the first 50 children to find out there are many
        // 2) query for the changed children between the two revisions
        assertTrue(numQueries.get() <= 2);
    }

    // OAK-2359
    @Test
    public void readNullEntry() throws CommitFailedException {
        final Set<String> reads = Sets.newHashSet();
        MemoryDocumentStore docStore = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                reads.add(key);
                return super.find(collection, key);
            }
        };
        DocumentNodeStore store = builderProvider.newBuilder()
                .setClusterId(1).setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").setProperty("foo", "bar");
        merge(store, builder);

        builder = store.getRoot().builder();
        builder.child("test").remove();
        merge(store, builder);

        Revision removedAt = store.getHeadRevision();

        String id = Utils.getIdFromPath("/test");
        int count = 0;
        // update node until we have at least two levels of split documents
        while (docStore.find(NODES, id).getPreviousRanges().size() <= PREV_SPLIT_FACTOR) {
            builder = store.getRoot().builder();
            builder.child("test").setProperty("count", count++);
            merge(store, builder);
            store.runBackgroundOperations();
        }

        NodeDocument doc = docStore.find(NODES, id);
        assertNotNull(doc);
        reads.clear();
        doc.getNodeAtRevision(store, store.getHeadRevision(), null);
        assertNoPreviousDocs(reads);

        reads.clear();
        doc.getValueMap("foo").get(removedAt);
        assertNoPreviousDocs(reads);
    }

    // OAK-2464
    @Test
    public void useDocChildCacheForFindingNodes() throws CommitFailedException {
        final Set<String> reads = Sets.newHashSet();
        MemoryDocumentStore docStore = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                reads.add(key);
                return super.find(collection, key);
            }
        };
        DocumentNodeStore store = builderProvider.newBuilder()
                .setClusterId(1).setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("a");
        builder.child("b").child("c");
        merge(store, builder);

        NodeState parentState = store.getRoot().getChildNode("b");
        reads.clear();
        NodeState nonExistingChild = parentState.getChildNode("non-existing-node-1");
        assertEquals("Should not go to DocStore::find for a known non-existent child", 0, reads.size());
        assertFalse("Non existing children should be reported as such", nonExistingChild.exists());

        builder = store.getRoot().builder();
        NodeBuilder childPropBuilder = builder.child("a");
        childPropBuilder.setProperty("foo", "bar");
        merge(store, builder);

        parentState = store.getRoot().getChildNode("b");
        reads.clear();
        nonExistingChild = parentState.getChildNode("non-existing-node-2");
        assertEquals("Should not go to DocStore::find for a known non-existent child," +
                " even if another merge has happened (on another sub-tree)", 0, reads.size());
        assertFalse("Non existing children should be reported as such", nonExistingChild.exists());

        store.invalidateNodeChildrenCache();

        //force filling up doc child cache
        parentState = store.getRoot().getChildNode("b");
        Iterables.size(parentState.getChildNodeEntries());

        reads.clear();
        nonExistingChild = parentState.getChildNode("non-existing-node-3");
        assertEquals("Should not go to DocStore::find when doc child cache is filled by reading",
                0, reads.size());
        assertFalse("Non existing children should be reported as such", nonExistingChild.exists());
    }

    @Test
    public void ignoreDocChildCacheForIncompleteEntry() throws CommitFailedException {
        final Set<String> reads = Sets.newHashSet();
        MemoryDocumentStore docStore = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                reads.add(key);
                return super.find(collection, key);
            }
        };
        DocumentNodeStore store = builderProvider.newBuilder()
                .setUseSimpleRevision(true)
                .setClusterId(1).setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder parentBuilder = builder.child("a");

        //create > INITIAL_FETCH_SIZE children to have incomplete child cache entries
        int numChildren = DocumentNodeState.INITIAL_FETCH_SIZE + 2;
        for (int i = 0; i < numChildren; i++) {
            parentBuilder.child("child" + i);
        }

        merge(store, builder);

        store.invalidateNodeChildrenCache();

        //force filling up doc child cache
        NodeState parentNodeState = store.getRoot().getChildNode("a");
        Iterables.size(parentNodeState.getChildNodeEntries());

        reads.clear();
        NodeState nonExistingChild = parentNodeState.getChildNode("non-existing-child-1");
        assertTrue("DocStore should be queried when no doc child cache entry has all children",
                reads.size() > 0);
        assertFalse("Non existing children should be reported as such", nonExistingChild.exists());
    }

    @Test
    public void docChildCacheWithIncompatiblDocStoreSort() throws CommitFailedException {
        final Set<String> reads = Sets.newHashSet();
        final ConcurrentSkipListMap<String, NodeDocument> nodes = new ConcurrentSkipListMap<String, NodeDocument>(
                new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        int ret = o1.compareTo(o2);
                        if ( o1.indexOf("child") > 0 && o2.indexOf("child") > 0 ) {
                            ret = (-ret);
                        }
                        return ret;
                    }
                }
        );
        MemoryDocumentStore docStore = new MemoryDocumentStore() {
            @Override
            @SuppressWarnings("unchecked")
            protected <T extends Document> ConcurrentSkipListMap<String, T> getMap(Collection<T> collection) {
                if (collection == Collection.NODES) {
                    return (ConcurrentSkipListMap<String, T>) nodes;
                } else {
                    return super.getMap(collection);
                }
            }

            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                reads.add(key);
                return super.find(collection, key);
            }

        };
        DocumentNodeStore store = builderProvider.newBuilder()
                .setUseSimpleRevision(true)
                .setClusterId(1).setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();

        NodeBuilder builder = store.getRoot().builder();

        //create < INITIAL_FETCH_SIZE children to have complete child cache entries
        NodeBuilder parentBuilder = builder.child("parent");
        int numChildren = DocumentNodeState.INITIAL_FETCH_SIZE - 2;
        for (int i = 0; i < numChildren; i++) {
            parentBuilder.child("child" + (i + 1));
        }
        merge(store, builder);

        store.invalidateNodeChildrenCache();

        //Force fill child node cache
        NodeState parentNodeState = store.getRoot().getChildNode("parent");
        Iterables.size(parentNodeState.getChildNodeEntries());

        reads.clear();
        NodeState nonExistingChild = parentNodeState.getChildNode("child501-non-existing-child");
        assertEquals("Fully cached entry in doc child cache should be able to find non existing children" +
                " even if doc store sort order is incompatible to that of Java", 0, reads.size());
        assertFalse("Non existing children should be reported as such", nonExistingChild.exists());

        store.invalidateNodeCache("/parent/child25", store.getHeadRevision());

        reads.clear();
        NodeState existingChild = parentNodeState.getChildNode("child25");
        assertTrue("Fully cached entry in doc child cache should be able to find existing children" +
                " even if doc store sort order is incompatible to that of Java", reads.size() > 0);
        assertTrue("Existing children should be reported as such", existingChild.exists());
    }

    // OAK-2929
    @Test
    public void conflictDetectionWithClockDifference() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        long now = System.currentTimeMillis();
        Clock c1 = new Clock.Virtual();
        c1.waitUntil(now);
        Revision.setClock(c1);
        DocumentNodeStore ns1 = builderProvider.newBuilder().clock(c1)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("node");
        merge(ns1, b1);
        // make /node visible
        ns1.runBackgroundOperations();

        Revision.resetClockToDefault();
        Clock c2 = new Clock.Virtual();
        // c2 is five seconds ahead
        c2.waitUntil(now + 5000);
        Revision.setClock(c2);

        DocumentNodeStore ns2 = builderProvider.newBuilder().clock(c2)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        // ns2 sees /node
        assertTrue(ns2.getRoot().hasChildNode("node"));

        // add a child /node/foo
        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("node").child("foo");
        merge(ns2, b2);
        // make /node/foo visible
        ns2.runBackgroundOperations();

        Revision.resetClockToDefault();
        Revision.setClock(c1);
        ns1.runBackgroundOperations();
        b1 = ns1.getRoot().builder();
        // ns1 sees /node/foo as well
        assertTrue(b1.getChildNode("node").hasChildNode("foo"));
        // remove both /node and /node/foo
        b1.child("node").remove();
        merge(ns1, b1);

        Revision.resetClockToDefault();
        Revision.setClock(c2);
        b2 = ns2.getRoot().builder();
        b2.child("node").child("bar");
        try {
            merge(ns2, b2);
            // must not be able to add another child node
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }
    }

    // OAK-2929
    @Test
    public void parentWithUnseenChildrenMustNotBeDeleted() throws Exception {
        final MemoryDocumentStore docStore = new MemoryDocumentStore();
        final DocumentNodeStore store1 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setAsyncDelay(0)
                .setClusterId(1)
                .getNodeStore();
        store1.setEnableConcurrentAddRemove(true);
        final DocumentNodeStore store2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setAsyncDelay(0)
                .setClusterId(2)
                .getNodeStore();
        store2.setEnableConcurrentAddRemove(true);

        NodeBuilder builder = store1.getRoot().builder();
        builder.child(":hidden");
        merge(store1, builder);

        store1.runBackgroundOperations();
        store2.runBackgroundOperations();

        builder = store1.getRoot().builder();
        builder.child(":hidden").child("parent").child("node1");
        merge(store1, builder);

        builder = store2.getRoot().builder();
        builder.child(":hidden").child("parent").child("node2");
        merge(store2, builder);

        //Test 1 - parent shouldn't be removable if order of operation is:
        //# N1 and N2 know about /:hidden
        //# N1->create(/:hidden/parent/node1)
        //# N2->create(/:hidden/parent/node2)
        //# N1->remove(/:hidden/parent)
        builder = store1.getRoot().builder();
        builder.child(":hidden").child("parent").remove();
        try {
            merge(store1, builder);
            fail("parent node of unseen children must not get deleted");
        } catch (CommitFailedException cfe) {
            //this merge should fail -- but our real check is done by asserting that parent remains intact
        }

        String parentPath = "/:hidden/parent";
        NodeDocument parentDoc = docStore.find(Collection.NODES, Utils.getIdFromPath(parentPath));
        assertFalse("parent node of unseen children must not get deleted",
                isDocDeleted(parentDoc, store1.getRevisionComparator()));

        //Test 2 - parent shouldn't be removable if order of operation is:
        //# N1 and N2 know about /:hidden
        //# N1->create(/:hidden/parent/node1)
        //# N2->create(/:hidden/parent/node2)
        //# N2->remove(/:hidden/parent)
        builder = store2.getRoot().builder();
        builder.child(":hidden").child("parent").remove();
        try {
            merge(store2, builder);
            fail("parent node of unseen children must not get deleted");
        } catch (CommitFailedException cfe) {
            //this merge should fail -- but our real check is done by asserting that parent remains intact
        }

        parentDoc = docStore.find(Collection.NODES, Utils.getIdFromPath(parentPath));
        assertFalse("parent node of unseen children must not get deleted",
                isDocDeleted(parentDoc, store2.getRevisionComparator()));

        store1.runBackgroundOperations();
        store2.runBackgroundOperations();
        builder = store1.getRoot().builder();
        builder.child(":hidden").child("parent").remove();
        builder.child(":hidden").child("parent1");
        store1.runBackgroundOperations();
        store2.runBackgroundOperations();

        builder = store1.getRoot().builder();
        builder.child(":hidden").child("parent1").child("node1");
        merge(store1, builder);

        builder = store2.getRoot().builder();
        builder.child(":hidden").child("parent1").child("node2");
        merge(store2, builder);

        //Test 3 - parent shouldn't be removable if order of operation is:
        //# N1 and N2 know about /:hidden/parent1
        //# N1->create(/:hidden/parent1/node1)
        //# N2->create(/:hidden/parent1/node2)
        //# N1->remove(/:hidden/parent1)
        builder = store1.getRoot().builder();
        builder.child(":hidden").child("parent1").remove();
        try {
            merge(store1, builder);
        } catch (CommitFailedException cfe) {
            //this merge should fail -- but our real check is done by asserting that parent remains intact
        }

        parentPath = "/:hidden/parent1";
        parentDoc = docStore.find(Collection.NODES, Utils.getIdFromPath(parentPath));
        assertFalse("parent node of unseen children must not get deleted",
                isDocDeleted(parentDoc, store1.getRevisionComparator()));

        //Test 4 - parent shouldn't be removable if order of operation is:
        //# N1 and N2 know about /:hidden/parent1
        //# N1->create(/:hidden/parent1/node1)
        //# N2->create(/:hidden/parent1/node2)
        //# N2->remove(/:hidden/parent1)
        builder = store2.getRoot().builder();
        builder.child(":hidden").child("parent1").remove();
        try {
            merge(store2, builder);
        } catch (CommitFailedException cfe) {
            //this merge should fail -- but our real check is done by asserting that parent remains intact
        }

        parentDoc = docStore.find(Collection.NODES, Utils.getIdFromPath(parentPath));
        assertFalse("parent node of unseen children must not get deleted",
                isDocDeleted(parentDoc, store2.getRevisionComparator()));
    }

    @Test
    public void mergeInternalDocAcrossCluster() throws Exception {
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        final DocumentNodeStore store1 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setAsyncDelay(0)
                .setClusterId(1)
                .getNodeStore();
        store1.setEnableConcurrentAddRemove(true);
        final DocumentNodeStore store2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setAsyncDelay(0)
                .setClusterId(2)
                .getNodeStore();
        store2.setEnableConcurrentAddRemove(true);

        NodeState root;
        NodeBuilder builder;

        //Prepare repo
        root = store1.getRoot();
        builder = root.builder();
        builder.child(":hidden").child("deleteDeleted");
        builder.child(":hidden").child("deleteChanged");
        builder.child(":hidden").child("changeDeleted");
        merge(store1, builder);
        store1.runBackgroundOperations();
        store2.runBackgroundOperations();

        //Changes in store1
        root = store1.getRoot();
        builder = root.builder();
        builder.child("visible");
        builder.child(":hidden").child("b");
        builder.child(":hidden").child("deleteDeleted").remove();
        builder.child(":hidden").child("changeDeleted").remove();
        builder.child(":hidden").child("deleteChanged").setProperty("foo", "bar");
        builder.child(":dynHidden").child("c");
        builder.child(":dynHidden").child("childWithProp").setProperty("foo", "bar");
        merge(store1, builder);

        //Changes in store2

        //root would hold reference to store2 root state after initial repo initialization
        root = store2.getRoot();

        //The hidden node and children should be creatable across cluster concurrently
        builder = root.builder();
        builder.child(":hidden").child("b");
        builder.child(":dynHidden").child("c");
        merge(store2, builder);

        //Deleted deleted conflict of internal node should work across cluster concurrently
        builder = root.builder();
        builder.child(":hidden").child("deleteDeleted").remove();
        merge(store2, builder);

        //Avoid repeated merge tries ... fail early
        store2.setMaxBackOffMillis(0);

        boolean commitFailed = false;
        try {
            builder = root.builder();
            builder.child("visible");
            merge(store2, builder);
        } catch (CommitFailedException cfe) {
            commitFailed = true;
        }
        assertTrue("Concurrent creation of visible node across cluster must fail", commitFailed);

        commitFailed = false;
        try {
            builder = root.builder();
            builder.child(":dynHidden").child("childWithProp").setProperty("foo", "bar");
            merge(store2, builder);
        } catch (CommitFailedException cfe) {
            commitFailed = true;
        }
        assertTrue("Concurrent creation of hidden node with properties across cluster must fail", commitFailed);

        commitFailed = false;
        try {
            builder = root.builder();
            builder.child(":hidden").child("deleteChanged").remove();
            merge(store2, builder);
        } catch (CommitFailedException cfe) {
            commitFailed = true;
        }
        assertTrue("Delete changed merge across cluster must fail even under hidden tree", commitFailed);

        commitFailed = false;
        try {
            builder = root.builder();
            builder.child(":hidden").child("changeDeleted").setProperty("foo", "bar");
            merge(store2, builder);
        } catch (CommitFailedException cfe) {
            commitFailed = true;
        }
        assertTrue("Change deleted merge across cluster must fail even under hidden tree", commitFailed);
    }

    @Test
    public void mergeDeleteDeleteEmptyInternalDoc() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        store.setEnableConcurrentAddRemove(true);
        NodeBuilder builder = store.getRoot().builder();
        builder.child(":a");
        builder.child(":b");
        merge(store, builder);
        SingleInstanceConflictUtility.generateConflict(store,
                new String[]{":1"}, new String[]{":a"},
                new String[]{":2"}, new String[]{":b"},
                new String[]{":3"}, new String[]{":a", ":b"},
                true, "Delete-delete merge conflicts for internal docs should be resolved");
    }

    @Test
    public void mergeDeleteDeleteNonEmptyInternalDocShouldFail() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        store.setEnableConcurrentAddRemove(true);
        NodeBuilder builder = store.getRoot().builder();
        builder.child(":a").setProperty("foo", "bar");
        builder.child(":b");
        merge(store, builder);
        SingleInstanceConflictUtility.generateConflict(store,
                new String[]{":1"}, new String[]{":a"},
                new String[]{":2"}, new String[]{":b"},
                new String[]{":3"}, new String[]{":a", ":b"},
                false, "Delete-delete merge conflicts for non-empty internal docs should fail");
    }

    @Test
    public void mergeDeleteDeleteNormalDocShouldFail() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        store.setEnableConcurrentAddRemove(true);
        NodeBuilder builder = store.getRoot().builder();
        builder.child("a");
        builder.child("b");
        merge(store, builder);
        SingleInstanceConflictUtility.generateConflict(store,
                new String[]{":1"}, new String[]{"a"},
                new String[]{":2"}, new String[]{"b"},
                new String[]{":3"}, new String[]{"a", "b"},
                false, "Delete-delete merge conflicts for normal docs should fail");
    }

    @Test
    public void mergeAddAddEmptyInternalDoc() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        store.setEnableConcurrentAddRemove(true);
        SingleInstanceConflictUtility.generateConflict(store,
                new String[]{":1", ":a"}, new String[]{},
                new String[]{":2", ":b"}, new String[]{},
                new String[]{":3", ":a", ":b"}, new String[]{},
                true, "Add-add merge conflicts for internal docs should be resolvable");
    }

    @Test
    public void mergeAddAddNonEmptyInternalDocShouldFail() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        store.setEnableConcurrentAddRemove(true);
        SingleInstanceConflictUtility.generateConflict(store,
                new String[]{":1", ":a"}, new String[]{}, true,
                new String[]{":2", ":b"}, new String[]{}, true,
                new String[]{":3", ":a", ":b"}, new String[]{}, false,
                false, "Add-add merge conflicts for non empty internal docs should fail");
    }

    @Test
    public void mergeAddAddNormalDocShouldFail() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        store.setEnableConcurrentAddRemove(true);
        SingleInstanceConflictUtility.generateConflict(store,
                new String[]{":1", "a"}, new String[]{},
                new String[]{":2", "b"}, new String[]{},
                new String[]{":3", "a", "b"}, new String[]{},
                false, "Add-add merge conflicts for normal docs should fail");
    }

    @Test
    public void mergeDeleteChangedInternalDocShouldFail() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        store.setEnableConcurrentAddRemove(true);
        NodeBuilder builder = store.getRoot().builder();
        builder.child(":a");
        builder.child(":b");
        merge(store, builder);
        SingleInstanceConflictUtility.generateConflict(store,
                new String[]{":1", ":a"}, new String[]{}, true,
                new String[]{":2", ":b"}, new String[]{}, true,
                new String[]{":3"}, new String[]{":a", ":b"}, false,
                false, "Delete changed merge conflicts for internal docs should fail");
    }

    @Test
    public void mergeChangeDeletedInternalDocShouldFail() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        store.setEnableConcurrentAddRemove(true);
        NodeBuilder builder = store.getRoot().builder();
        builder.child(":a");
        builder.child(":b");
        merge(store, builder);
        SingleInstanceConflictUtility.generateConflict(store,
                new String[]{":1"}, new String[]{":a"}, false,
                new String[]{":2"}, new String[]{":b"}, false,
                new String[]{":3", ":a", ":b"}, new String[]{}, true,
                false, "Change deleted merge conflicts for internal docs should fail");
    }

    @Test
    public void retrieve() throws Exception {
        DocumentNodeStore store = new DocumentMK.Builder().getNodeStore();
        String ref = store.checkpoint(60000);
        assertNotNull(store.retrieve(ref));
        ref = Revision.newRevision(1).toString();
        assertNull(store.retrieve(ref));
        ref = UUID.randomUUID().toString();
        assertNull(store.retrieve(ref));
        store.dispose();
    }

    // OAK-3388
    @Test
    public void clusterWithClockDifferences() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        long now = System.currentTimeMillis();
        Clock c1 = new Clock.Virtual();
        c1.waitUntil(now);
        Revision.setClock(c1);
        DocumentNodeStore ns1 = builderProvider.newBuilder().clock(c1)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("node");
        merge(ns1, b1);
        // make /node visible
        ns1.runBackgroundOperations();

        Revision.resetClockToDefault();
        Clock c2 = new Clock.Virtual();
        // c2 is five seconds ahead
        c2.waitUntil(now + 5000);
        Revision.setClock(c2);

        DocumentNodeStore ns2 = builderProvider.newBuilder().clock(c2)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        // ns2 sees /node
        assertTrue(ns2.getRoot().hasChildNode("node"));

        // remove /node on ns2
        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("node").remove();
        merge(ns2, b2);
        ns2.runBackgroundOperations();

        // add /node again on ns1
        Revision.resetClockToDefault();
        Revision.setClock(c1);
        ns1.runBackgroundOperations();
        b1 = ns1.getRoot().builder();
        assertFalse(b1.hasChildNode("node"));
        b1.child("node");
        merge(ns1, b1);
        ns1.runBackgroundOperations();

        // check if /node is visible on ns2
        Revision.resetClockToDefault();
        Revision.setClock(c2);
        ns2.runBackgroundOperations();
        b2 = ns2.getRoot().builder();
        assertTrue(b2.hasChildNode("node"));
    }

    // OAK-3388
    @Test
    public void clusterWithClockDifferences2() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        long now = System.currentTimeMillis();
        Clock c1 = new Clock.Virtual();
        c1.waitUntil(now);
        Revision.setClock(c1);
        DocumentNodeStore ns1 = builderProvider.newBuilder().clock(c1)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("node").setProperty("p", 1);
        merge(ns1, b1);
        // make /node visible
        ns1.runBackgroundOperations();

        Revision.resetClockToDefault();
        Clock c2 = new Clock.Virtual();
        // c2 is five seconds ahead
        c2.waitUntil(now + 5000);
        Revision.setClock(c2);

        DocumentNodeStore ns2 = builderProvider.newBuilder().clock(c2)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        // ns2 sees /node
        assertTrue(ns2.getRoot().hasChildNode("node"));
        assertEquals(1, ns2.getRoot().getChildNode("node").getProperty("p").getValue(Type.LONG).longValue());

        // increment /node/p ns2
        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("node").setProperty("p", 2);
        merge(ns2, b2);
        ns2.runBackgroundOperations();

        // increment /node/p2 on ns1
        Revision.resetClockToDefault();
        Revision.setClock(c1);
        ns1.runBackgroundOperations();
        b1 = ns1.getRoot().builder();
        assertEquals(2, b1.getChildNode("node").getProperty("p").getValue(Type.LONG).longValue());
        b1.child("node").setProperty("p", 3);
        merge(ns1, b1);
        ns1.runBackgroundOperations();

        // check if /node/p=3 is visible on ns2
        Revision.resetClockToDefault();
        Revision.setClock(c2);
        ns2.runBackgroundOperations();
        b2 = ns2.getRoot().builder();
        assertEquals(3, b2.getChildNode("node").getProperty("p").getValue(Type.LONG).longValue());
    }

    // OAK-3455
    @Test
    public void notYetVisibleExceptionMessage() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        ns2.setMaxBackOffMillis(0);

        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("test").setProperty("p", "v");
        merge(ns1, b1);

        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("test").setProperty("q", "v");
        try {
            merge(ns2, b2);
            fail("Must throw CommitFailedException");
        } catch (CommitFailedException e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause().getMessage().contains("not yet visible"));
        }

    }

    // OAK-3579
    @Test
    public void backgroundLeaseUpdateThread() throws Exception {
        int clusterId = -1;
        Random random = new Random();
        // pick a random clusterId between 1000 and 2000
        // and make sure it is not in use (give up after 10 tries)
        for (int i = 0; i < 10; i++) {
            int id = random.nextInt(1000) + 1000;
            if (!backgroundLeaseUpdateThreadRunning(id)) {
                clusterId = id;
                break;
            }
        }
        assertNotEquals(-1, clusterId);
        DocumentNodeStore ns = builderProvider.newBuilder().setAsyncDelay(0)
                .setClusterId(clusterId).getNodeStore();
        for (int i = 0; i < 10; i++) {
            if (!backgroundLeaseUpdateThreadRunning(clusterId)) {
                Thread.sleep(100);
            }
        }
        assertTrue(backgroundLeaseUpdateThreadRunning(clusterId));
        // access DocumentNodeStore to make sure it is not
        // garbage collected prematurely
        assertEquals(clusterId, ns.getClusterId());
    }

    // OAK-3646
    @Ignore("OAK-3646")
    @Test
    public void concurrentChildOperations() throws Exception {
        Clock clock = new Clock.Virtual();
        Revision.setClock(clock);
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setAsyncDelay(0).clock(clock)
                .setDocumentStore(store).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setAsyncDelay(0).clock(clock)
                .setDocumentStore(store).getNodeStore();

        // create some children under /foo/bar
        NodeBuilder b1 = ns1.getRoot().builder();
        NodeBuilder node = b1.child("foo").child("bar");
        node.child("child-0");
        node.child("child-1");
        node.child("child-2");
        merge(ns1, b1);

        // make changes visible on both cluster nodes
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        // remove child-0 on cluster node 1
        b1 = ns1.getRoot().builder();
        b1.child("foo").child("bar").getChildNode("child-0").remove();
        merge(ns1, b1);

        // push _lastRev updates to DocumentStore
        ns1.runBackgroundOperations();

        // remove child-1 on cluster node 2
        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("foo").child("bar").getChildNode("child-1").remove();
        merge(ns2, b2);

        // on cluster node 2, remove of child-0 is not yet visible
        List<ChildNodeEntry> children = Lists.newArrayList(ns2.getRoot().getChildNode("foo").getChildNode("bar").getChildNodeEntries());
        assertEquals(2, Iterables.size(children));
        Revision invalidate = null;
        for (ChildNodeEntry entry : children) {
            if (entry.getName().equals("child-0")) {
                invalidate = asDocumentNodeState(entry.getNodeState()).getRevision();
            }
        }
        assertNotNull(invalidate);

        // this will make changes from cluster node 1 visible
        ns2.runBackgroundOperations();

        // wait twice the time we remember revision order
        clock.waitUntil(clock.getTime() + 2 * REMEMBER_REVISION_ORDER_MILLIS);
        // collect everything older than one hour (time revision order is remembered)
        // this will remove child-0 and child-1 doc
        ns1.getVersionGarbageCollector().gc(REMEMBER_REVISION_ORDER_MILLIS, TimeUnit.MILLISECONDS);

        // trigger purge of revisions older than one hour in RevisionComparator
        // this is usually done by the background read operation, but we
        // do it explicitly here to make sure it really happens in this test
        ns2.getRevisionComparator().purge(clock.getTime() - REMEMBER_REVISION_ORDER_MILLIS);
        // forget cache entry for deleted node
        ns2.invalidateNodeCache("/foo/bar/child-0", invalidate);

        children = Lists.newArrayList(ns2.getRoot().getChildNode("foo").getChildNode("bar").getChildNodeEntries());
        assertEquals(1, Iterables.size(children));
    }

    // OAK-3646
    // similar to previous test but both cluster nodes add a child node
    @Ignore("OAK-3646")
    @Test
    public void concurrentChildOperations2() throws Exception {
        Clock clock = new Clock.Virtual();
        Revision.setClock(clock);
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setAsyncDelay(0).clock(clock)
                .setDocumentStore(store).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setAsyncDelay(0).clock(clock)
                .setDocumentStore(store).getNodeStore();

        // create initial /foo
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("foo");
        merge(ns1, b1);

        // make changes visible on both cluster nodes
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        // add child-1 on cluster node 1
        b1 = ns1.getRoot().builder();
        b1.child("foo").child("child-1");
        merge(ns1, b1);

        // push _lastRev updates to DocumentStore
        ns1.runBackgroundOperations();

        // remove child-2 on cluster node 2
        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("foo").child("child-2");
        merge(ns2, b2);

        // on cluster node 2, add of child-1 is not yet visible
        List<ChildNodeEntry> children = Lists.newArrayList(ns2.getRoot().getChildNode("foo").getChildNodeEntries());
        assertEquals(1, Iterables.size(children));

        // this will make changes from cluster node 1 visible
        ns2.runBackgroundOperations();

        // wait twice the time we remember revision order
        clock.waitUntil(clock.getTime() + 2 * REMEMBER_REVISION_ORDER_MILLIS);

        // trigger purge of revisions older than one hour in RevisionComparator
        // this is usually done by the background read operation, but we
        // do it explicitly here to make sure it really happens in this test
        ns2.getRevisionComparator().purge(clock.getTime() - REMEMBER_REVISION_ORDER_MILLIS);

        children = Lists.newArrayList(ns2.getRoot().getChildNode("foo").getChildNodeEntries());
        assertEquals(2, Iterables.size(children));
    }

    private static boolean backgroundLeaseUpdateThreadRunning(int clusterId) {
        String threadName = "DocumentNodeStore lease update thread (" + clusterId + ")";
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        for (ThreadInfo ti : threadBean.getThreadInfo(threadBean.getAllThreadIds())) {
            if (ti != null) {
                if (threadName.equals(ti.getThreadName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Utility class that eases creating single cluster id merge conflicts. The two methods:
     * <ul>
     *     <li>{@link #generateConflict(DocumentNodeStore, String[], String[], String[], String[], String[], String[], boolean, String)}</li>
     *     <li>{@link #generateConflict(DocumentNodeStore, String[], String[], boolean, String[], String[], boolean, String[], String[], boolean, boolean, String)}</li>
     * </ul>
     * can be passed descriptions of modifications required to create conflict. These methods would also take
     * expectation of successful/failure of resolution of merge conflict. In case of failure of that assertion, these
     * methods would mark the test to fail.
     */
    private static class SingleInstanceConflictUtility {
        /**
         * Wrapper of {@link #generateConflict(DocumentNodeStore, String[], String[], boolean, String[], String[], boolean, String[], String[], boolean, boolean, String)}
         * with value of {@code change1, change2, and change3} as {@code false}
         */
        public static void generateConflict(final DocumentNodeStore store,
                                            String [] normalAddChildren1, String [] normalRemoveChildren1,
                                            String [] normalAddChildren2, String [] normalRemoveChildren2,
                                            String [] conflictingAddChildren3, String [] conflictingRemoveChildren3,
                                            boolean shouldMerge, String assertMessage)
                throws CommitFailedException, InterruptedException {
            generateConflict(store,
                    normalAddChildren1, normalRemoveChildren1, false,
                    normalAddChildren2, normalRemoveChildren2, false,
                    conflictingAddChildren3, conflictingRemoveChildren3, false,
                    shouldMerge, assertMessage
                    );
        }

        /**
         * This method takes 3 descriptions of changes for conflict to happen. Each description has a set of
         * {@code AddChildren}, {@code RemoveChildren}, and {@code change} parameters. {@code AddChidren} is an
         * array of children to be added, {@code RemoveChildren} is an array of children to be removed, and
         * {@code change} controls if a property (hard-coded to {@code @foo=bar}) needs to be set on children
         * that are part of {@code AddChildren} array.
         * The changes should be such that set1 changes and set2 changes should be safe. The conflict should be
         * represented by changes in set3 -- and the conflict should exist against both set1 and set2.
         * These 3 description are then used to create changes on 3 threads in such a way that by the time thread3
         * gets around to persist its changes, there are more revisions which get committed. In case the conflict
         * couldn't be resolved, thread3 would report an exception which is tested
         * against {@code mergeable}.
         * @throws InterruptedException
         */
        public static void generateConflict(final DocumentNodeStore store,
                                            String [] normalAddChildren1, String [] normalRemoveChildren1, boolean change1,
                                            String [] normalAddChildren2, String [] normalRemoveChildren2, boolean change2,
                                            String [] conflictingAddChildren3, String [] conflictingRemoveChildren3, boolean change3,
                                            boolean mergeable, String assertMessage)
                throws InterruptedException {
            //This would result in 0 retries... 1 rebase would happen and we'd control it :D
            store.setMaxBackOffMillis(0);

            SingleInstanceConflictUtility thread1 = new SingleInstanceConflictUtility();
            SingleInstanceConflictUtility thread3 = new SingleInstanceConflictUtility();
            SingleInstanceConflictUtility thread2 = new SingleInstanceConflictUtility();

            thread1.startMerge(store, normalAddChildren1, normalRemoveChildren1, change1);
            thread2.startMerge(store, conflictingAddChildren3, conflictingRemoveChildren3, change3);

            thread1.join();
            thread2.waitForNextMerge();

            thread3.startMerge(store, normalAddChildren2, normalRemoveChildren2, change2);
            thread3.join();

            thread2.join();

            assertNull("There shouldn't be any exception for thread1", thread1.getException());
            assertNull("There shouldn't be any exception for thread3", thread3.getException());

            CommitFailedException cfe = thread2.getException();
            if (mergeable != (cfe == null)) {
                StringBuffer message = new StringBuffer(assertMessage);
                if (cfe != null) {
                    message.append("\n");
                    message.append(Throwables.getStackTraceAsString(cfe));
                }
                fail(message.toString());
            }
        }

        private Thread merger;
        private CommitFailedException mergeException = null;

        private boolean dontBlock;
        private final Semaphore controller = new Semaphore(0);
        private final Semaphore controllee = new Semaphore(0);

        private void startMerge(final NodeStore store,
                                @Nonnull String [] addChildren, @Nonnull String [] removeChildren, boolean change) {
            startMerge(store, null, addChildren, removeChildren, change);
        }

        private void startMerge(final NodeStore store, final CommitHook hook,
                                @Nonnull String [] addChildren, @Nonnull String [] removeChildren, boolean change) {
            setDontBlock(false);

            //our controller is controllee for merge thread (and vice versa)
            merger = createMergeThread(store, hook, controllee, controller, addChildren, removeChildren, change);
            merger.start();
            controllee.acquireUninterruptibly();//wait for merge thread to get to blocking hook
        }

        private void waitForNextMerge() throws InterruptedException{
            controller.release();
            controllee.tryAcquire(2, TimeUnit.SECONDS);
        }
        private void unblock() {
            setDontBlock(true);
            controller.release();
        }
        private void join() throws InterruptedException {
            unblock();
            merger.join();
        }

        private synchronized void setDontBlock(boolean dontBlock) {
            this.dontBlock = dontBlock;
        }
        private synchronized boolean getDontBlock() {
            return dontBlock;
        }
        private CommitFailedException getException() {
            return mergeException;
        }

        private Thread createMergeThread(final NodeStore store, final CommitHook hook,
                                         final Semaphore controller, final Semaphore controllee,
                                         @Nonnull final String [] addChildren, @Nonnull final String [] removeChildren,
                                         final boolean change) {
            return new Thread(new Runnable() {
                @Override
                public void run() {
                    final CommitHook blockingHook = new CommitHook() {
                        @Nonnull
                        @Override
                        public NodeState processCommit(NodeState before, NodeState after, CommitInfo info)
                                throws CommitFailedException {
                            controller.release();
                            if(!getDontBlock()) {
                                controllee.acquireUninterruptibly();
                            }
                            return after;
                        }
                    };

                    try {
                        NodeBuilder builder = store.getRoot().builder();
                        for (String child : addChildren) {
                            if (change) {
                                builder.child(child).setProperty("foo", "bar");
                            } else {
                                builder.child(child);
                            }
                        }
                        for (String child : removeChildren) {
                            builder.child(child).remove();
                        }

                        List<CommitHook> hookList = new ArrayList<CommitHook>();
                        if(hook != null) {
                            hookList.add(hook);
                        }
                        hookList.add(blockingHook);
                        hookList.add(new ConflictHook(new AnnotatingConflictHandler()));
                        hookList.add(new EditorHook(new ConflictValidatorProvider()));
                        store.merge(builder, CompositeHook.compose(hookList), CommitInfo.EMPTY);
                    } catch (CommitFailedException cfe) {
                        mergeException = cfe;
                    }
                }
            });
        }
    }

    @Test
    public void slowRebase() throws Exception {
        final int NUM_NODES = DocumentRootBuilder.UPDATE_LIMIT / 2;
        final int NUM_PROPS = 10;
        final int REBASE_COUNT = 5;
        final DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();

        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < NUM_NODES / 2; i++) {
            NodeBuilder c = deepTree(builder.child("n" + i), 5);
            for (int j = 0; j < NUM_PROPS; j++) {
                c.setProperty("p" + j, "value");
            }
        }

        //1. Prepare a large tree
        merge(ns, builder);

        builder = ns.getRoot().builder();
        int[] rebaseCounts = {2,3,1,8,3};
        for (int r = 0; r < REBASE_COUNT; r++){
            for (int i = 0; i < NUM_NODES / 2; i++) {
                NodeBuilder c = deepTree(builder.child("n" + i), 5);
                for (int j = 0; j < NUM_PROPS; j++) {
                    c.setProperty("q"+ r + "" + j, "value");
                }
            }

            //Do multiple rebase for each round of branch commit phase
            for (int k = 0; k < rebaseCounts[r]; k++){
                doSomeChange(ns);
                ns.rebase(builder);
            }
        }

        LOG.info("Starting the final merge {}", new Date());
        merge(ns, builder);
    }

    // OAK-2642
    @Test
    public void dispose() throws CommitFailedException, InterruptedException {
        final BlockingQueue<String> updates = new ArrayBlockingQueue<String>(1);
        // when disposing of the DocumentNodeStore instances the updates queue
        // becomes full due to the pending operations being flushed.
        // This flag ensures that after the main test is completed all
        // updates are processed without being blocked
        final AtomicBoolean throttleUpdates = new AtomicBoolean(true);
        MemoryDocumentStore docStore = new MemoryDocumentStore() {
            @Override
            public <T extends Document> void update(Collection<T> collection,
                                                    List<String> keys,
                                                    UpdateOp updateOp) {
                
                if ( throttleUpdates.get() ) {
                    for (String k : keys) {
                        try {
                            updates.put(k);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                super.update(collection, keys, updateOp);
            }
        };
        final DocumentNodeStore store = builderProvider.newBuilder()
                .setClusterId(1).setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();
        updates.clear();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child("node");
        merge(store, builder);

        builder = store.getRoot().builder();
        builder.child("test").child("node").child("child-1");
        merge(store, builder);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                store.dispose();
            }
        });
        t.start();

        String p = updates.take();
        assertEquals("2:/test/node", p);
        // background ops in dispose is still in progress
        assertTrue(t.isAlive());
        // wait until next update comes in
        for (;;) {
            if (updates.peek() != null) {
                break;
            }
        }

        // add child-2 while dispose is in progress
        try {
            builder = store.getRoot().builder();
            builder.child("test").child("node").child("child-2");
            merge(store, builder);
            fail("Merge must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }

        // drain updates until dispose finished
        while (t.isAlive()) {
            updates.poll(10, TimeUnit.MILLISECONDS);
        }
        updates.clear();
        throttleUpdates.set(false);

        // start new store with clusterId 2
        DocumentNodeStore store2 = builderProvider.newBuilder()
                .setClusterId(2).setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();

        // perform recovery if needed
        LastRevRecoveryAgent agent = new LastRevRecoveryAgent(store2);
        if (agent.isRecoveryNeeded()) {
            agent.recover(1);
        }

        builder = store2.getRoot().builder();
        NodeBuilder test = builder.getChildNode("test");
        assertTrue(test.exists());
        NodeBuilder node = test.getChildNode("node");
        assertTrue(node.exists());
        if (!node.hasChildNode("child-2")) {
            node.child("child-2");
            merge(store2, builder);
        }
    }

    // OAK-2695
    @Test
    public void dispatch() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();

        Revision from = ns.getHeadRevision();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        merge(ns, builder);
        Revision to = ns.getHeadRevision();

        DiffCache.Entry entry = ns.getDiffCache().newEntry(from, to, true);
        entry.append("/", "-\"foo\"");
        entry.done();

        ns.compare(ns.getRoot(), ns.getRoot(from), new DefaultNodeStateDiff() {
            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                assertNotNull(before);
                return true;
            }
        });
    }

    @Test
    public void rootRevision() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder().getNodeStore();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").child("child");
        builder.child("bar").child("child");
        merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("foo").child("child").child("node");
        merge(ns, builder);

        Revision head = ns.getHeadRevision();
        NodeState child = ns.getRoot().getChildNode("bar").getChildNode("child");
        assertTrue(child instanceof DocumentNodeState);
        DocumentNodeState state = (DocumentNodeState) child;
        assertEquals(head, state.getRootRevision());
    }

    @Test
    public void diffCache() throws Exception {
        final AtomicInteger numQueries = new AtomicInteger();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      int limit) {
                numQueries.incrementAndGet();
                return super.query(collection, fromKey, toKey, limit);
            }
        };
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").child("child");
        merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("bar");
        merge(ns, builder);

        DocumentNodeState before = ns.getRoot();

        builder = ns.getRoot().builder();
        builder.child("foo").child("child").child("node");
        merge(ns, builder);

        DocumentNodeState after = ns.getRoot();

        numQueries.set(0);
        final List<String> added = Lists.newArrayList();
        ns.compare(asDocumentNodeState(after.getChildNode("foo").getChildNode("child")),
                asDocumentNodeState(before.getChildNode("foo").getChildNode("child")),
                new DefaultNodeStateDiff() {
                    @Override
                    public boolean childNodeAdded(String name,
                                                  NodeState after) {
                        added.add(name);
                        return super.childNodeAdded(name, after);
                    }
                });


        assertEquals(1, added.size());
        assertEquals("node", added.get(0));
        assertEquals("must not run queries", 0, numQueries.get());
    }

    // OAK-1970
    @Test
    public void diffMany() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        final List<Long> startValues = Lists.newArrayList();
        MemoryDocumentStore ds = new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      String indexedProperty,
                                                      long startValue,
                                                      int limit) {
                if (indexedProperty != null) {
                    startValues.add(startValue);
                }
                return super.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
            }
        };
        DocumentNodeStore ns = builderProvider.newBuilder().clock(clock)
                .setDocumentStore(ds).setAsyncDelay(0).getNodeStore();

        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder test = builder.child("test");
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            test.child("node-" + i);
        }
        merge(ns, builder);

        // 'wait one hour'
        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(1));

        // perform a change and use the resulting root as before state
        builder = ns.getRoot().builder();
        builder.child("foo");
        DocumentNodeState before = asDocumentNodeState(merge(ns, builder));
        NodeState beforeTest = before.getChildNode("test");

        // perform another change to span the diff across multiple revisions
        // this will prevent diff calls served from the local cache
        builder = ns.getRoot().builder();
        builder.child("bar");
        merge(ns, builder);

        // add a child node
        builder = ns.getRoot().builder();
        builder.child("test").child("bar");
        NodeState after = merge(ns, builder);
        NodeState afterTest = after.getChildNode("test");

        startValues.clear();
        afterTest.compareAgainstBaseState(beforeTest, new DefaultNodeStateDiff());

        assertEquals(1, startValues.size());
        long beforeModified = getModifiedInSecs(before.getRevision().getTimestamp());
        // startValue must be based on the revision of the before state
        // and not when '/test' was last modified
        assertEquals(beforeModified, (long) startValues.get(0));
    }

    // OAK-2620
    @Test
    public void nonBlockingReset() throws Exception {
        final List<String> failure = Lists.newArrayList();
        final AtomicReference<ReentrantReadWriteLock> mergeLock
                = new AtomicReference<ReentrantReadWriteLock>();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                        UpdateOp update) {
                for (Map.Entry<Key, Operation> entry : update.getChanges().entrySet()) {
                    if (entry.getKey().getName().equals(NodeDocument.COLLISIONS)) {
                        ReentrantReadWriteLock rwLock = mergeLock.get();
                        if (rwLock.getReadHoldCount() > 0
                                || rwLock.getWriteHoldCount() > 0) {
                            failure.add("Branch reset still holds merge lock");
                            break;
                        }
                    }
                }
                return super.findAndUpdate(collection, update);
            }
        };
        DocumentNodeStore ds = builderProvider.newBuilder()
                .setDocumentStore(store)
                .setAsyncDelay(0).getNodeStore();
        ds.setMaxBackOffMillis(0); // do not retry merges

        DocumentNodeState root = ds.getRoot();
        final DocumentNodeStoreBranch b = ds.createBranch(root);
        // branch state is now Unmodified

        assertTrue(b.getMergeLock() instanceof ReentrantReadWriteLock);
        mergeLock.set((ReentrantReadWriteLock) b.getMergeLock());

        NodeBuilder builder = root.builder();
        builder.child("foo");
        b.setRoot(builder.getNodeState());
        // branch state is now InMemory
        builder.child("bar");
        b.setRoot(builder.getNodeState());
        // branch state is now Persisted

        try {
            b.merge(new CommitHook() {
                @Nonnull
                @Override
                public NodeState processCommit(NodeState before,
                                               NodeState after,
                                               CommitInfo info)
                        throws CommitFailedException {
                    NodeBuilder foo = after.builder().child("foo");
                    for (int i = 0; i <= DocumentRootBuilder.UPDATE_LIMIT; i++) {
                        foo.setProperty("prop", i);
                    }
                    throw new CommitFailedException("Fail", 0, "");
                }
            }, CommitInfo.EMPTY);
        } catch (CommitFailedException e) {
            // expected
        }

        for (String s : failure) {
            fail(s);
        }
    }

    @Test
    public void failFastOnBranchConflict() throws Exception {
        final AtomicInteger mergeAttempts = new AtomicInteger();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                        UpdateOp update) {
                for (Key k : update.getConditions().keySet()) {
                    if (k.getName().equals(NodeDocument.COLLISIONS)) {
                        mergeAttempts.incrementAndGet();
                        break;
                    }
                }
                return super.findAndUpdate(collection, update);
            }
        };
        DocumentNodeStore ds = builderProvider.newBuilder()
                .setDocumentStore(store)
                .setAsyncDelay(0).getNodeStore();

        DocumentNodeState root = ds.getRoot();
        DocumentNodeStoreBranch b = ds.createBranch(root);
        // branch state is now Unmodified
        NodeBuilder builder = root.builder();
        builder.child("foo");
        b.setRoot(builder.getNodeState());
        // branch state is now InMemory
        builder.child("bar").setProperty("p", "foo");
        b.setRoot(builder.getNodeState());
        // branch state is now Persisted

        // create conflict with persisted branch
        NodeBuilder nb = ds.getRoot().builder();
        nb.child("bar").setProperty("p", "bar");
        merge(ds, nb);

        mergeAttempts.set(0);
        try {
            b.merge(EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }

        assertTrue("too many merge attempts: " + mergeAttempts.get(),
                mergeAttempts.get() <= 1);
    }

    // OAK-3586
    @Test
    public void resolveMultipleConflictedRevisions() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        final DocumentNodeStore ds = builderProvider.newBuilder()
                .setDocumentStore(store)
                .setAsyncDelay(0).getNodeStore();

        DocumentNodeState root = ds.getRoot();
        final DocumentNodeStoreBranch b = ds.createBranch(root);

        NodeBuilder builder = root.builder();
        builder.child("foo");
        b.setRoot(builder.getNodeState());

        final Set<Revision> revisions = new HashSet<Revision>();
        final List<Commit> commits = new ArrayList<Commit>();
        for (int i = 0; i < 10; i++) {
            Revision revision = ds.newRevision();
            Commit commit = ds.newCommit(revision, ds.createBranch(root));
            commits.add(commit);
            revisions.add(revision);
        }

        final AtomicBoolean merged = new AtomicBoolean();
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    CommitFailedException exception = new ConflictException("Can't merge", revisions).asCommitFailedException();
                    b.merge(new HookFailingOnce(exception), CommitInfo.EMPTY);
                    merged.set(true);
                } catch (CommitFailedException e) {
                    LOG.error("Can't commit", e);
                }
            }
        });
        t.start();

        // 6 x done()
        for (int i = 0; i < 6; i++) {
            assertFalse("The branch can't be merged yet", merged.get());
            ds.done(commits.get(i), false, CommitInfo.EMPTY);
        }

        // 2 x cancel()
        for (int i = 6; i < 8; i++) {
            assertFalse("The branch can't be merged yet", merged.get());
            ds.canceled(commits.get(i));
        }

        // 2 x branch done()
        for (int i = 8; i < 10; i++) {
            assertFalse("The branch can't be merged yet", merged.get());
            ds.done(commits.get(i), true, CommitInfo.EMPTY);
        }

        for (int i = 0; i < 100; i++) {
            if (merged.get()) {
                break;
            }
            Thread.sleep(10);
        }
        assertTrue("The branch should be merged by now", merged.get());

        t.join();
    }

    // OAK-3411
    @Test
    public void sameSeenAtRevision() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("test");
        merge(ns2, b2);
        ns2.runBackgroundOperations();
        ns1.runBackgroundOperations();

        NodeBuilder b1 = ns1.getRoot().builder();
        assertTrue(b1.hasChildNode("test"));
        b1.child("test").remove();
        merge(ns1, b1);
        ns1.runBackgroundOperations();

        DocumentNodeStore ns3 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        ns3.setMaxBackOffMillis(0);
        NodeBuilder b3 = ns3.getRoot().builder();
        assertFalse(b3.hasChildNode("test"));
        b3.child("test");
        merge(ns3, b3);
    }

    // OAK-3411
    @Test
    public void sameSeenAtRevision2() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("test");
        merge(ns2, b2);
        b2 = ns2.getRoot().builder();
        b2.child("test").remove();
        merge(ns2, b2);
        ns2.runBackgroundOperations();
        ns1.runBackgroundOperations();

        NodeBuilder b1 = ns1.getRoot().builder();
        assertFalse(b1.hasChildNode("test"));
        b1.child("test");
        merge(ns1, b1);
        ns1.runBackgroundOperations();

        DocumentNodeStore ns3 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        ns3.setMaxBackOffMillis(0);
        NodeBuilder b3 = ns3.getRoot().builder();
        assertTrue(b3.hasChildNode("test"));
        b3.child("test").remove();
        merge(ns3, b3);
    }

    // OAK-3474
    @Test
    public void ignoreUncommitted() throws Exception {
        final AtomicLong numPreviousFinds = new AtomicLong();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                if (Utils.getPathFromId(key).startsWith("p")) {
                    numPreviousFinds.incrementAndGet();
                }
                return super.find(collection, key);
            }
        };
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        String id = Utils.getIdFromPath("/test");
        NodeBuilder b = ns.getRoot().builder();
        b.child("test").setProperty("p", "a");
        merge(ns, b);
        NodeDocument doc;
        int i = 0;
        do {
            b = ns.getRoot().builder();
            b.child("test").setProperty("q", i++);
            merge(ns, b);
            doc = store.find(NODES, id);
            assertNotNull(doc);
            if (i % 100 == 0) {
                ns.runBackgroundOperations();
            }
        } while (doc.getPreviousRanges().isEmpty());

        Revision r = ns.newRevision();
        UpdateOp op = new UpdateOp(id, false);
        NodeDocument.setCommitRoot(op, r, 0);
        op.setMapEntry("p", r, "b");
        assertNotNull(store.findAndUpdate(NODES, op));

        doc = store.find(NODES, id);
        numPreviousFinds.set(0);
        doc.getNodeAtRevision(ns, ns.getHeadRevision(), null);
        assertEquals(0, numPreviousFinds.get());
    }

    // OAK-3608
    @Test
    public void compareOnBranch() throws Exception {
        long modifiedResMillis = SECONDS.toMillis(MODIFIED_IN_SECS_RESOLUTION);
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock)
                .setAsyncDelay(0).getNodeStore();
        // initial state
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder p = builder.child("parent");
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            p.child("node-" + i);
        }
        p.child("node-x").child("child");
        merge(ns, builder);
        ns.runBackgroundOperations();

        // wait until modified timestamp changes
        clock.waitUntil(clock.getTime() + modifiedResMillis * 2);
        // force new head revision with this different modified timestamp
        builder = ns.getRoot().builder();
        builder.child("a");
        merge(ns, builder);

        DocumentNodeState root = ns.getRoot();
        final DocumentNodeStoreBranch b = ns.createBranch(root);
        // branch state is now Unmodified
        builder = root.builder();
        builder.child("parent").child("node-x").child("child").child("x");
        b.setRoot(builder.getNodeState());
        // branch state is now InMemory
        builder.child("b");
        b.setRoot(builder.getNodeState());
        // branch state is now Persisted
        builder.child("c");
        b.setRoot(builder.getNodeState());
        // branch state is Persisted

        // create a diff between base and head state of branch
        DocumentNodeState head = asDocumentNodeState(b.getHead());
        TrackingDiff diff = new TrackingDiff();
        head.compareAgainstBaseState(root, diff);
        assertTrue(diff.modified.contains("/parent/node-x/child"));
    }

    private static DocumentNodeState asDocumentNodeState(NodeState state) {
        if (!(state instanceof DocumentNodeState)) {
            throw new IllegalArgumentException("Not a DocumentNodeState");
        }
        return (DocumentNodeState) state;
    }

    private void doSomeChange(NodeStore ns) throws CommitFailedException {
        NodeBuilder b = ns.getRoot().builder();
        b.setProperty("count", System.currentTimeMillis());
        merge(ns, b);
    }

    private NodeBuilder deepTree(NodeBuilder parent, int depth){
        NodeBuilder nb = parent;
        for (int i = depth ; i >= 0; i--){
            nb = nb.child("c"+i);
        }
        return nb;
    }

    private static void assertNoPreviousDocs(Set<String> ids) {
        for (String id : ids) {
            assertFalse("must not read previous document: " +
                            id + " (all: " + ids + ")",
                    Utils.getPathFromId(id).startsWith("p"));
        }
    }

    private static NodeState merge(NodeStore store, NodeBuilder root)
            throws CommitFailedException {
        return store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static class TestHook extends EditorHook {

        TestHook(final String prefix) {
            super(new EditorProvider() {
                @CheckForNull
                @Override
                public Editor getRootEditor(NodeState before,
                                            NodeState after,
                                            NodeBuilder builder,
                                            CommitInfo info)
                        throws CommitFailedException {
                    return new TestEditor(builder, prefix);
                }
            });
        }
    }

    private static final CommitHook FAILING_HOOK = new CommitHook() {
        @Nonnull
        @Override
        public NodeState processCommit(NodeState before,
                                       NodeState after,
                                       CommitInfo info)
                throws CommitFailedException {
            throw new CommitFailedException(CONSTRAINT, 0, "fail");
        }
    };

    private static class HookFailingOnce implements CommitHook {

        private final AtomicBoolean failedAlready = new AtomicBoolean();

        private final CommitFailedException exception;

        private HookFailingOnce(CommitFailedException exception) {
            this.exception = exception;
        }

        @Override
        public NodeState processCommit(NodeState before, NodeState after, CommitInfo info)
                throws CommitFailedException {
            if (failedAlready.getAndSet(true)) {
                return after;
            } else {
                throw exception;
            }
        }

    }

    private static class TestEditor extends DefaultEditor {

        private final NodeBuilder builder;
        private final String prefix;

        TestEditor(NodeBuilder builder, String prefix) {
            this.builder = builder;
            this.prefix = prefix;
        }

        @Override
        public Editor childNodeAdded(String name, NodeState after)
                throws CommitFailedException {
            return new TestEditor(builder.child(name), prefix);
        }

        @Override
        public void propertyAdded(PropertyState after)
                throws CommitFailedException {
            if (after.getName().startsWith(prefix)) {
                builder.setProperty(after.getName(), "test");
            }
        }
    }

    /**
     * @param doc the document to be tested
     * @return latest committed value of _deleted map
     */
    private boolean isDocDeleted(NodeDocument doc,
                                 Comparator<Revision> comparator) {
        boolean latestDeleted = false;
        SortedMap<Revision, String> localDeleted = Maps.newTreeMap(
                Collections.reverseOrder(comparator));
        localDeleted.putAll(doc.getLocalDeleted());

        for (Map.Entry<Revision, String> entry : localDeleted.entrySet()) {
            if (doc.isCommitted(entry.getKey())) {
                latestDeleted = Boolean.parseBoolean(entry.getValue());
                break;
            }
        }
        return latestDeleted;
    }
}
