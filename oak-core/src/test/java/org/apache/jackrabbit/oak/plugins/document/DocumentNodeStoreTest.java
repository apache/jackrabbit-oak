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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.kernel.KernelNodeState;
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
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.Iterables;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
            public void invalidateCache() {
                super.invalidateCache();
                semaphore.acquireUninterruptibly();
                semaphore.release();
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
        int max = (int) (KernelNodeState.MAX_CHILD_NAMES * 1.5);
        SortedSet<String> children = new TreeSet<String>();
        for (int i = 0; i < max; i++) {
            String name = "c" + i;
            children.add(name);
            builder.child(name);
        }
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        builder = store.getRoot().builder();
        String name = new ArrayList<String>(children).get(
                KernelNodeState.MAX_CHILD_NAMES / 2);
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
                    Commit c = new Commit(store, Revision.fromString(head), r);
                    c.addNode(new DocumentNodeState(store, "/foo/node", r));
                    c.addNode(new DocumentNodeState(store, "/bar/node", r));
                    c.apply();
                } catch (MicroKernelException e) {
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
        Commit c = new Commit(store, Revision.fromString(head), r);
        c.addNode(new DocumentNodeState(store, "/foo/node", r));
        c.addNode(new DocumentNodeState(store, "/bar/node", r));
        c.apply();
        // allow writer to continue
        s.release();
        writer.join();
        assertEquals("expected exception", 1, exceptions.size());

        String id = Utils.getIdFromPath("/foo/node");
        NodeDocument doc = docStore.find(Collection.NODES, id);
        assertNotNull("document with id " + id + " does not exist", doc);
        assertTrue(!doc.getLastRev().isEmpty());
        id = Utils.getIdFromPath("/bar/node");
        doc = docStore.find(Collection.NODES, id);
        assertNotNull("document with id " + id + " does not exist", doc);
        assertTrue(!doc.getLastRev().isEmpty());

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
                DocumentNodeStore.REMEMBER_REVISION_ORDER_MILLIS / 2);

        NodeBuilder builder = nodeStore2.getRoot().builder();
        builder.setProperty("prop", "value");
        nodeStore2.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        nodeStore2.runBackgroundOperations();

        clock.waitUntil(System.currentTimeMillis() +
                DocumentNodeStore.REMEMBER_REVISION_ORDER_MILLIS + 1000);
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
                if (collection == Collection.NODES) {
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
