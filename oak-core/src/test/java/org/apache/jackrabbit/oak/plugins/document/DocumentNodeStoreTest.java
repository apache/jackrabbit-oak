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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Test;

public class DocumentNodeStoreTest {

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
            public CacheInvalidationStats invalidateCache() {
                super.invalidateCache();
                semaphore.acquireUninterruptibly();
                semaphore.release();
                return null;
            }
        };
        final DocumentNodeStore store1 = new DocumentMK.Builder().setAsyncDelay(0)
                .setDocumentStore(testStore).setClusterId(1).getNodeStore();
        DocumentNodeStore store2 = new DocumentMK.Builder().setAsyncDelay(0)
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

        store1.dispose();
        store2.dispose();
    }

    @Test
    public void childNodeCache() throws Exception {
        DocumentNodeStore store = new DocumentMK.Builder().getNodeStore();
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
        store.dispose();
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
        DocumentNodeStore store = new DocumentMK.Builder()
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

        store.dispose();
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
        final DocumentMK mk = new DocumentMK.Builder()
                .setDocumentStore(docStore).setAsyncDelay(0).open();
        final DocumentNodeStore store = mk.getNodeStore();
        final String head = mk.commit("/", "+\"foo\":{}+\"bar\":{}", null, null);
        Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Revision r = store.newRevision();
                    Commit c = new Commit(store, r, Revision.fromString(head), null);
                    c.addNode(new DocumentNodeState(store, "/foo/node", r));
                    c.addNode(new DocumentNodeState(store, "/bar/node", r));
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
        Commit c = new Commit(store, r, Revision.fromString(head), null);
        c.addNode(new DocumentNodeState(store, "/foo/node", r));
        c.addNode(new DocumentNodeState(store, "/bar/node", r));
        c.apply();
        // allow writer to continue
        s.release();
        writer.join();
        assertEquals("expected exception", 1, exceptions.size());

        String id = Utils.getIdFromPath("/foo/node");
        NodeDocument doc = docStore.find(NODES, id);
        assertNotNull("document with id " + id + " does not exist", doc);
        id = Utils.getIdFromPath("/bar/node");
        doc = docStore.find(NODES, id);
        assertNotNull("document with id " + id + " does not exist", doc);

        mk.dispose();
    }

    // OAK-1662
    @Test
    public void getNewestRevision() throws Exception {
        DocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setAsyncDelay(0)
                .setClusterId(1).getNodeStore();
        ns1.getRoot();
        ns1.runBackgroundOperations();
        DocumentNodeStore ns2 = new DocumentMK.Builder()
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

        ns1.dispose();
        ns2.dispose();
    }

    @Test
    public void commitHookChangesOnBranch() throws Exception {
        final int NUM_NODES = DocumentRootBuilder.UPDATE_LIMIT / 2;
        final int NUM_PROPS = 10;
        DocumentNodeStore ns = new DocumentMK.Builder().getNodeStore();
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

        ns.dispose();
    }

    // OAK-1814
    @Test
    public void visibilityAfterRevisionComparatorPurge() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore nodeStore1 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        nodeStore1.runBackgroundOperations();
        DocumentNodeStore nodeStore2 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        DocumentNodeStore nodeStore3 = new DocumentMK.Builder()
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

        nodeStore1.dispose();
        nodeStore2.dispose();
        nodeStore3.dispose();
    }

    @Test
    public void modifiedReset() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        NodeBuilder builder1 = ns1.getRoot().builder();
        builder1.child("node");
        ns1.merge(builder1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns1.runBackgroundOperations();

        DocumentNodeStore ns2 = new DocumentMK.Builder()
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

        ns1.dispose();
        ns2.dispose();
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
        DocumentNodeStore ns = new DocumentMK.Builder()
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
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(docStore).getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test").setProperty("prop", "initial");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.dispose();

        ns = new DocumentMK.Builder().setClusterId(2).setAsyncDelay(0)
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
        DocumentNodeStore ns1 = new DocumentMK.Builder().setAsyncDelay(0)
                .clock(clock).setDocumentStore(docStore).setClusterId(1)
                .getNodeStore();
        DocumentNodeStore ns2 = new DocumentMK.Builder().setAsyncDelay(0)
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
        ClusterTest.TrackingDiff diff = new ClusterTest.TrackingDiff();
        r2.compareAgainstBaseState(r1, diff);
        assertEquals(1, diff.modified.size());
        assertTrue(diff.modified.contains("/test"));
        assertEquals(1, diff.added.size());
        assertTrue(diff.added.contains("/test/foo"));

        ns1.dispose();
        ns2.dispose();
    }

    @Test
    public void updateClusterState() {
        DocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = new DocumentMK.Builder().setAsyncDelay(0)
                .setDocumentStore(docStore).getNodeStore();
        DocumentNodeStore ns2 = new DocumentMK.Builder().setAsyncDelay(0)
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

        ns2.dispose();
    }

    // OAK-2288
    @Test
    public void mergedBranchVisibility() throws Exception {
        final DocumentNodeStore store = new DocumentMK.Builder()
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

        store.dispose();
    }

    // OAK-2308
    @Test
    public void recoverBranchCommit() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());

        MemoryDocumentStore docStore = new MemoryDocumentStore();

        DocumentNodeStore store1 = new DocumentMK.Builder()
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
        DocumentNodeStore store2 = new DocumentMK.Builder()
                .setDocumentStore(docStore)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        // must see /test/node
        assertTrue(store2.getRoot().getChildNode("test").getChildNode("node").exists());

        store2.dispose();
        store1.dispose();
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
        DocumentNodeStore ns = new DocumentMK.Builder()
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

        ns.dispose();
    }

    // OAK-2345
    @Test
    public void inactiveClusterId() throws Exception {
        Clock clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("test");
        merge(ns1, builder);
        Revision r = ns1.getHeadRevision();
        ns1.dispose();

        // start other cluster node
        DocumentNodeStore ns2 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        assertNotNull(ns2.getRevisionComparator().getRevisionSeen(r));
        ns2.dispose();

        // wait until revision is old
        clock.waitUntil(System.currentTimeMillis()
                + REMEMBER_REVISION_ORDER_MILLIS + 1000);

        // start cluster 2 again
        ns2 = new DocumentMK.Builder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).clock(clock).getNodeStore();
        // now r is considered old and revisionSeen is null
        assertNull(ns2.getRevisionComparator().getRevisionSeen(r));
        ns2.dispose();
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
        final DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(store).getNodeStore();
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
                        ns.diff(head.toString(), to.toString(), "/");
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

        store.dispose();
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
        DocumentNodeStore store = new DocumentMK.Builder()
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

        store.dispose();
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
        DocumentNodeStore store = new DocumentMK.Builder()
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

        store.dispose();
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
        DocumentNodeStore store = new DocumentMK.Builder()
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
        store.dispose();
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
        DocumentNodeStore store = new DocumentMK.Builder()
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

        store.dispose();
    }

    @Test
    public void slowRebase() throws Exception {
        final int NUM_NODES = DocumentRootBuilder.UPDATE_LIMIT / 2;
        final int NUM_PROPS = 10;
        final int REBASE_COUNT = 5;
        final DocumentNodeStore ns = new DocumentMK.Builder().getNodeStore();

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

        System.out.println("Starting the final merge "+ new Date());
        merge(ns, builder);

        ns.dispose();
    }

    // OAK-2642
    @Test
    public void dispose() throws CommitFailedException, InterruptedException {
        final BlockingQueue<String> updates = new ArrayBlockingQueue<String>(1);
        MemoryDocumentStore docStore = new MemoryDocumentStore() {
            @Override
            public <T extends Document> void update(Collection<T> collection,
                                                    List<String> keys,
                                                    UpdateOp updateOp) {
                for (String k : keys) {
                    try {
                        updates.put(k);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                super.update(collection, keys, updateOp);
            }
        };
        final DocumentNodeStore store = new DocumentMK.Builder()
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

        // start new store with clusterId 2
        DocumentNodeStore store2 = new DocumentMK.Builder()
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

    private static void merge(NodeStore store, NodeBuilder root)
            throws CommitFailedException {
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
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
}
