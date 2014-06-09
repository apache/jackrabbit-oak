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

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

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
            public String checkpoint(long lifetime) {
                try {
                    return super.checkpoint(lifetime);
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

    @Test
    public void cpCleanupNoChanges() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        // no changes on diff, no checkpoints left behind
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);
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
    }

    private static class FaultyIndexEditorProvder implements
            IndexEditorProvider {

        private final FaultyIndexEditor faulty = new FaultyIndexEditor();

        @Override
        public Editor getIndexEditor(String type, NodeBuilder definition,
                NodeState root, IndexUpdateCallback callback)
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

}
