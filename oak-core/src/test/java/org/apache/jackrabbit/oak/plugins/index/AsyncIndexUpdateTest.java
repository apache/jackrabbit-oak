/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.AsyncIndexStats;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.IndexTaskSpliter;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;
import org.junit.Test;

import ch.qos.logback.classic.Level;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class AsyncIndexUpdateTest {

    // TODO test index config deletes

    private static Set<String> find(PropertyIndexLookup lookup, String name,
            String value) {
        return Sets.newHashSet(lookup.query(new FilterImpl(), name,
                PropertyValues.newString(value)));
    }

    private static NodeState checkPathExists(NodeState state, String... verify) {
        NodeState c = state;
        for (String p : verify) {
            c = c.getChildNode(p);
            assertTrue(c.exists());
        }
        return c;
    }

    /**
     * Async Index Test
     * <ul>
     * <li>Add an index definition</li>
     * <li>Add some content</li>
     * <li>Search & verify</li>
     * </ul>
     * 
     */
    @Test
    public void testAsync() throws Exception {
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        NodeState root = store.getRoot();

        // first check that the index content nodes exist
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        assertFalse(root.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(
                ":conflict"));

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    /**
     * Async Index Test with 2 index defs at the same location
     * <ul>
     * <li>Add an index definition</li>
     * <li>Add some content</li>
     * <li>Search & verify</li>
     * </ul>
     * 
     */
    @Test
    public void testAsyncDouble() throws Exception {
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndexSecond", true, false, ImmutableSet.of("bar"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");

        builder.child("testRoot").setProperty("foo", "abc")
                .setProperty("bar", "def");
        builder.child("testSecond").setProperty("bar", "ghi");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        NodeState root = store.getRoot();

        // first check that the index content nodes exist
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndexSecond",
                INDEX_CONTENT_NODE_NAME);

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
        assertEquals(ImmutableSet.<String> of(), find(lookup, "foo", "def"));
        assertEquals(ImmutableSet.<String> of(), find(lookup, "foo", "ghi"));

        assertEquals(ImmutableSet.<String> of(), find(lookup, "bar", "abc"));
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "bar", "def"));
        assertEquals(ImmutableSet.of("testSecond"), find(lookup, "bar", "ghi"));

    }

    /**
     * Async Index Test with 2 index defs at different tree locations
     * <ul>
     * <li>Add an index definition</li>
     * <li>Add some content</li>
     * <li>Search & verify</li>
     * </ul>
     * 
     */
    @Test
    public void testAsyncDoubleSubtree() throws Exception {
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        createIndexDefinition(
                builder.child("newchild").child("other")
                        .child(INDEX_DEFINITIONS_NAME), "subIndex", true,
                false, ImmutableSet.of("foo"), null).setProperty(
                ASYNC_PROPERTY_NAME, "async");

        builder.child("testRoot").setProperty("foo", "abc");
        builder.child("newchild").child("other").child("testChild")
                .setProperty("foo", "xyz");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        NodeState root = store.getRoot();

        // first check that the index content nodes exist
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        checkPathExists(root, "newchild", "other", INDEX_DEFINITIONS_NAME,
                "subIndex", INDEX_CONTENT_NODE_NAME);

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));

        PropertyIndexLookup lookupChild = new PropertyIndexLookup(root
                .getChildNode("newchild").getChildNode("other"));
        assertEquals(ImmutableSet.of("testChild"),
                find(lookupChild, "foo", "xyz"));
        assertEquals(ImmutableSet.<String> of(),
                find(lookupChild, "foo", "abc"));
    }

    @Test
    public void testAsyncPause() throws Exception {
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.getIndexStats().pause();
        async.run();
        assertFalse(store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME)
                .getChildNode("rootIndex")
                .hasChildNode(INDEX_CONTENT_NODE_NAME));

        async.getIndexStats().resume();
        async.run();
        NodeState root = store.getRoot();

        // first check that the index content nodes exist
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        assertFalse(root.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(
                ":conflict"));

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    // OAK-1749
    @Test
    public void branchBaseOnCheckpoint() throws Exception {
        final Semaphore retrieve = new Semaphore(1);
        final Semaphore checkpoint = new Semaphore(0);
        NodeStore store = new MemoryNodeStore() {
            @CheckForNull
            @Override
            public NodeState retrieve(@Nonnull String checkpoint) {
                retrieve.acquireUninterruptibly();
                try {
                    return super.retrieve(checkpoint);
                } finally {
                    retrieve.release();
                }
            }

            @Nonnull
            @Override
            public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
                try {
                    return super.checkpoint(lifetime, properties);
                } finally {
                    checkpoint.release();
                }
            }
        };
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                false, ImmutableSet.of("foo"), null, TYPE,
                Collections.singletonMap(ASYNC_PROPERTY_NAME, "async"));

        builder.child("test").setProperty("foo", "a");
        builder.child("child");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final AsyncIndexUpdate async = new AsyncIndexUpdate("async", store,
                provider);
        async.run();

        builder = store.getRoot().builder();
        builder.child("test").setProperty("foo", "b");
        builder.child("child").setProperty("prop", "value");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                async.run();
            }
        });

        // drain checkpoint permits
        checkpoint.acquireUninterruptibly(checkpoint.availablePermits());
        // block NodeStore.retrieve()
        retrieve.acquireUninterruptibly();
        t.start();

        // wait until async update called checkpoint
        retrieve.release();
        checkpoint.acquireUninterruptibly();
        builder = store.getRoot().builder();
        builder.child("child").remove();
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // allow async update to proceed with NodeStore.retrieve()
        retrieve.release();
        t.join();

        assertFalse(store.getRoot().hasChildNode("child"));
    }

    // OAK-1784
    @Test
    public void failOnConflict() throws Exception {
        final Map<Thread, Semaphore> locks = Maps.newIdentityHashMap();
        NodeStore store = new MemoryNodeStore() {
            @Nonnull
            @Override
            public NodeState merge(@Nonnull NodeBuilder builder,
                    @Nonnull CommitHook commitHook, @Nullable CommitInfo info)
                    throws CommitFailedException {
                Semaphore s = locks.get(Thread.currentThread());
                if (s != null) {
                    s.acquireUninterruptibly();
                }
                return super.merge(builder, commitHook, info);
            }
        };
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                false, ImmutableSet.of("foo"), null, TYPE,
                Collections.singletonMap(ASYNC_PROPERTY_NAME, "async"));

        builder.child("test").setProperty("foo", "a");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final AsyncIndexUpdate async = new AsyncIndexUpdate("async", store,
                provider);
        async.run();

        builder = store.getRoot().builder();
        builder.child("test").setProperty("foo", "b");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                async.run();
            }
        });
        Semaphore s = new Semaphore(0);
        locks.put(t, s);
        t.start();

        // make some unrelated changes to trigger indexing
        builder = store.getRoot().builder();
        builder.setChildNode("dummy").setProperty("foo", "bar");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        while (!s.hasQueuedThreads()) {
            Thread.yield();
        }

        // introduce a conflict
        builder = store.getRoot().builder();
        builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("foo")
                .getChildNode(":index").child("a").remove();
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        s.release(100);
        t.join();

        builder = store.getRoot().builder();
        assertNoConflictMarker(builder);
    }

    private void assertNoConflictMarker(NodeBuilder builder) {
        for (String name : builder.getChildNodeNames()) {
            if (name.equals(ConflictAnnotatingRebaseDiff.CONFLICT)) {
                fail("conflict marker detected");
            }
            assertNoConflictMarker(builder.getChildNode(name));
        }
    }

    /**
     * OAK-1959, stale ref to checkpoint thorws the indexer into a reindexing
     * loop
     */
    @Test
    public void recoverFromMissingCpRef() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        new AsyncIndexUpdate("async", store, provider).run();
        checkPathExists(store.getRoot(), INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME, "abc", "testRoot");

        builder = store.getRoot().builder();
        // change cp ref to point to a non-existing one
        builder.child(AsyncIndexUpdate.ASYNC).setProperty("async", "faulty");
        builder.child("testAnother").setProperty("foo", "def");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();
        checkPathExists(store.getRoot(), INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME, "def", "testAnother");
    }

    @Test
    public void cpCleanupNoChanges() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);

        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        // no changes on diff, no checkpoints left behind
        async.run();
        assertTrue(async.isFinished());
        Set<String> checkpoints = newHashSet(store.listCheckpoints());
        assertTrue("Expecting the initial checkpoint",
                checkpoints.size() == 1);
        assertEquals(store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                .getString("async"), checkpoints.iterator().next());

        async.run();
        assertEquals("Expecting no checkpoint changes",
                checkpoints, store.listCheckpoints());
    }

    @Test
    public void cpCleanupWChanges() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String firstCp = store.listCheckpoints().iterator().next();

        builder = store.getRoot().builder();
        builder.child("testRoot").setProperty("foo", "def");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        async.run();
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String secondCp = store.listCheckpoints().iterator().next();
        assertFalse("Store should keep only second checkpoint",
                secondCp.equals(firstCp));
        assertEquals(
                secondCp,
                store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                        .getString("async"));
    }

    @Test
    public void cpCleanupWUnrelatedChanges() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String firstCp = store.listCheckpoints().iterator().next();

        // add content that's hidden from indexing
        builder = store.getRoot().builder();
        builder.child("testRoot").child(":hidden");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        async.run();

        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String secondCp = store.listCheckpoints().iterator().next();
        assertFalse("Store should keep only second checkpoint",
                secondCp.equals(firstCp));
        assertEquals(
                secondCp,
                store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                        .getString("async"));
    }

    @Test
    public void cpCleanupWErrors() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        FaultyIndexEditorProvder provider = new FaultyIndexEditorProvder();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        assertTrue("Error should have been triggered by the commit",
                provider.isFailed());
        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        // OAK-3054 failure reports
        AsyncIndexStats stats = async.getIndexStats();
        String since = stats.getFailingSince();
        assertTrue(stats.isFailing());
        assertEquals(1, stats.getConsecutiveFailedExecutions());
        assertEquals(since, stats.getLatestErrorTime());

        TimeUnit.MILLISECONDS.sleep(100);
        async.run();
        assertTrue(stats.isFailing());
        assertEquals(2, stats.getConsecutiveFailedExecutions());
        assertEquals(since, stats.getFailingSince());
        assertNotEquals(since, stats.getLatestErrorTime());

        stats.fixed();
        assertFalse(stats.isFailing());
        assertEquals(0, stats.getConsecutiveFailedExecutions());
        assertEquals("", stats.getFailingSince());
    }

    @Test
    public void cpCleanupNoRelease() throws Exception {
        final MemoryNodeStore mns = new MemoryNodeStore();
        final AtomicBoolean canRelease = new AtomicBoolean(false);

        ProxyNodeStore store = new ProxyNodeStore() {

            @Override
            protected NodeStore getNodeStore() {
                return mns;
            }

            @Override
            public boolean release(String checkpoint) {
                if (canRelease.get()) {
                    return super.release(checkpoint);
                }
                return false;
            }
        };

        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Expecting no checkpoints",
                mns.listCheckpoints().size() == 0);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        assertTrue("Expecting one checkpoint",
                mns.listCheckpoints().size() == 1);
        assertTrue(
                "Expecting one temp checkpoint",
                newHashSet(
                        store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                                .getStrings("async-temp")).size() == 1);

        builder = store.getRoot().builder();
        builder.child("testRoot").setProperty("foo", "def");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        async.run();
        assertTrue("Expecting two checkpoints",
                mns.listCheckpoints().size() == 2);
        assertTrue(
                "Expecting two temp checkpoints",
                newHashSet(
                        store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                                .getStrings("async-temp")).size() == 2);

        canRelease.set(true);

        builder = store.getRoot().builder();
        builder.child("testRoot").setProperty("foo", "ghi");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        async.run();

        assertTrue("Expecting one checkpoint",
                mns.listCheckpoints().size() == 1);
        String secondCp = mns.listCheckpoints().iterator().next();
        assertEquals(
                secondCp,
                store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                        .getString("async"));
        // the temp cps size is 2 now but the unreferenced checkpoints have been
        // cleared from the store already
        for (String cp : store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                .getStrings("async-temp")) {
            if (cp.equals(secondCp)) {
                continue;
            }
            assertNull("Temp checkpoint was already cleared from store",
                    store.retrieve(cp));
        }
    }

    /**
     * OAK-2203 Test reindex behavior on an async index when the index provider is missing
     * for a given type
     */
    @Test
    public void testReindexMissingProvider() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "asyncMissing");

        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("asyncMissing", store,
                provider);
        //first run, creates a checkpoint and a ref to it as the last indexed state
        async.run();
        assertFalse(async.isFailing());
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String firstCp = store.listCheckpoints().iterator().next();
        assertEquals(
                firstCp,
                store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                        .getString("asyncMissing"));

        // second run, simulate an index going away
        provider = CompositeIndexEditorProvider
                .compose(new ArrayList<IndexEditorProvider>());
        async = new AsyncIndexUpdate("asyncMissing", store, provider);
        async.run();
        assertTrue(async.isFailing());
        // don't set reindex=true but skip the update
        PropertyState reindex = store.getRoot()
                .getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("rootIndex")
                .getProperty(REINDEX_PROPERTY_NAME);
        assertTrue(reindex == null || !reindex.getValue(Type.BOOLEAN));

        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String secondCp = store.listCheckpoints().iterator().next();
        assertTrue("Store should not create a new checkpoint",
                secondCp.equals(firstCp));
        assertEquals(
                firstCp,
                store.getRoot().getChildNode(AsyncIndexUpdate.ASYNC)
                        .getString("asyncMissing"));
    }

    private static class FaultyIndexEditorProvder implements
            IndexEditorProvider {

        private final FaultyIndexEditor faulty = new FaultyIndexEditor();

        @Override
        public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
                throws CommitFailedException {
            return faulty;
        }

        public boolean isFailed() {
            return faulty.failed;
        }

    }

    private static class FaultyIndexEditor implements IndexEditor {

        private boolean failed = false;

        @Override
        public void enter(NodeState before, NodeState after)
                throws CommitFailedException {
            failed = true;
            throw new CommitFailedException("test", -1, "Testing failures");
        }

        @Override
        public void leave(NodeState before, NodeState after)
                throws CommitFailedException {
        }

        @Override
        public void propertyAdded(PropertyState after)
                throws CommitFailedException {
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after)
                throws CommitFailedException {
        }

        @Override
        public void propertyDeleted(PropertyState before)
                throws CommitFailedException {
        }

        @Override
        public Editor childNodeAdded(String name, NodeState after)
                throws CommitFailedException {
            return null;
        }

        @Override
        public Editor childNodeChanged(String name, NodeState before,
                NodeState after) throws CommitFailedException {
            return null;
        }

        @Override
        public Editor childNodeDeleted(String name, NodeState before)
                throws CommitFailedException {
            return null;
        }

    }

    @Test
    public void taskSplit() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "changedIndex", true, false, ImmutableSet.of("bar"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "ignored1", true, false, ImmutableSet.of("baz"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async-ignored");
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "ignored2", true, false, ImmutableSet.of("etc"), null);

        builder.child("testRoot").setProperty("foo", "abc");
        builder.child("testRoot").setProperty("bar", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String firstCp = store.listCheckpoints().iterator().next();

        builder = store.getRoot().builder();
        builder.child("testRoot").setProperty("foo", "def");
        builder.child("testRoot").setProperty("bar", "def");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexTaskSpliter splitter = async.getTaskSplitter();
        splitter.registerSplit(newHashSet("/oak:index/changedIndex"), "async-slow");

        async.run();

        Set<String> checkpoints = newHashSet(store.listCheckpoints());

        assertTrue("Expecting two checkpoints",
                checkpoints.size() == 2);
        assertTrue(checkpoints.remove(firstCp));
        String secondCp = checkpoints.iterator().next();

        NodeState asyncNode = store.getRoot().getChildNode(
                AsyncIndexUpdate.ASYNC);
        assertEquals(firstCp, asyncNode.getString("async-slow"));
        assertEquals(secondCp, asyncNode.getString("async"));
        assertFalse(newHashSet(asyncNode.getStrings("async-temp")).contains(
                firstCp));

        NodeState indexNode = store.getRoot().getChildNode(
                INDEX_DEFINITIONS_NAME);
        assertEquals("async",
                indexNode.getChildNode("rootIndex").getString("async"));
        assertEquals("async-ignored", indexNode.getChildNode("ignored1")
                .getString("async"));
        assertNull(indexNode.getChildNode("ignored2").getString("async"));

        assertEquals("async-slow", indexNode.getChildNode("changedIndex")
                .getString("async"));
        assertEquals(false,
                indexNode.getChildNode("changedIndex").getBoolean("reindex"));

        // new index task is on previous checkpoint
        PropertyIndexLookup lookup = new PropertyIndexLookup(store.getRoot());
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "bar", "abc"));
        assertEquals(ImmutableSet.of(), find(lookup, "bar", "def"));
    }

    @Test
    public void taskSplitNoMatch() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "ignored", true, false, ImmutableSet.of("baz"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async-ignored");

        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String firstCp = store.listCheckpoints().iterator().next();

        builder = store.getRoot().builder();
        builder.child("testRoot").setProperty("foo", "def");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexTaskSpliter splitter = async.getTaskSplitter();
        // no match on the provided path
        splitter.registerSplit(newHashSet("/oak:index/ignored"), "async-slow");
        async.run();

        Set<String> checkpoints = newHashSet(store.listCheckpoints());

        assertTrue("Expecting a single checkpoint",
                checkpoints.size() == 1);
        String secondCp = checkpoints.iterator().next();

        NodeState asyncNode = store.getRoot().getChildNode(
                AsyncIndexUpdate.ASYNC);
        assertEquals(secondCp, asyncNode.getString("async"));
        assertNull(firstCp, asyncNode.getString("async-slow"));

    }

    @Test
    public void testAsyncExecutionStats() throws Exception {
        final Set<String> knownCheckpoints = Sets.newHashSet();
        MemoryNodeStore store = new MemoryNodeStore(){
            @Override
            public synchronized NodeState retrieve(@Nonnull String checkpoint) {
                if (!knownCheckpoints.isEmpty() && !knownCheckpoints.contains(checkpoint)){
                    return null;
                }
                return super.retrieve(checkpoint);
            }
        };
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        runOneCycle(async);
        assertEquals(1, lastExecutionStats(async.getIndexStats().getExecutionCount()));

        //Run a cycle so that change of reindex flag gets indexed
        runOneCycle(async);
        assertEquals(1, lastExecutionStats(async.getIndexStats().getExecutionCount()));

        //Now run so that it results in an empty cycle
        runOneCycle(async);
        assertEquals(1, lastExecutionStats(async.getIndexStats().getExecutionCount()));

        //Do some updates and counter should increase
        builder = store.getRoot().builder();
        builder.child("testRoot2").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        runOneCycle(async);
        assertEquals(1, lastExecutionStats(async.getIndexStats().getExecutionCount()));

        //Do some updates but disable checkpoints. Counter should not increase
        builder = store.getRoot().builder();
        builder.child("testRoot3").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Disable new checkpoint retrieval
        knownCheckpoints.addAll(store.listCheckpoints());
        runOneCycle(async);
        assertEquals(0, lastExecutionStats(async.getIndexStats().getExecutionCount()));
    }


    private static long lastExecutionStats(CompositeData cd){
        //Last stat is the last entry in the array
        return ((long[]) cd.get("per second"))[59];
    }

    private static void runOneCycle(AsyncIndexUpdate async){
        async.run();
        async.getIndexStats().run();
    }

    @Test
    public void checkpointStability() throws Exception{
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);

        //Initial indexing
        async.run();
        //Now checkpoints = [checkpoints0]

        //Index again so as to get change in reindex flag done
        async.run();
        //Now checkpoints = [checkpoints1]. checkpoints0 released

        //Now make some changes to
        builder = store.getRoot().builder();
        builder.child("testRoot2").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        async.run();
        //Now checkpoints = [checkpoints1]. Note that size is 1 so new checkpoint name remains same

        LogCustomizer customLogs = LogCustomizer.forLogger(AsyncIndexUpdate.class.getName())
                .filter(Level.WARN)
                .create();

        builder = store.getRoot().builder();
        builder.child("testRoot3").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        customLogs.starting();

        async.run();

        assertEquals(Collections.emptyList(), customLogs.getLogs());
        customLogs.finished();
    }

}
