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
import static org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.ASYNC;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.SUPERSEDED_INDEX_PATHS;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.openmbean.CompositeData;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.AsyncIndexStats;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.IndexTaskSpliter;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler.CorruptIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.After;
import org.junit.Test;

import ch.qos.logback.classic.Level;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class AsyncIndexUpdateTest {
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private MetricStatisticsProvider statsProvider =
            new MetricStatisticsProvider(ManagementFactory.getPlatformMBeanServer(),executor);

    @After
    public void shutDown(){
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    // TODO test index config deletes

    private static Set<String> find(PropertyIndexLookup lookup, String name,
            String value) {
        return Sets.newHashSet(lookup.query(FilterImpl.newTestInstance(), name,
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
                    @Nonnull CommitHook commitHook, @Nonnull CommitInfo info)
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
        builder.child(ASYNC).setProperty("async", "faulty");
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
        assertEquals(store.getRoot().getChildNode(ASYNC)
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
                store.getRoot().getChildNode(ASYNC)
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
                store.getRoot().getChildNode(ASYNC)
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
                        store.getRoot().getChildNode(ASYNC)
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
                        store.getRoot().getChildNode(ASYNC)
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
                store.getRoot().getChildNode(ASYNC)
                        .getString("async"));
        // the temp cps size is 2 now but the unreferenced checkpoints have been
        // cleared from the store already
        for (String cp : store.getRoot().getChildNode(ASYNC)
                .getStrings("async-temp")) {
            if (cp.equals(secondCp)) {
                continue;
            }
            assertNull("Temp checkpoint was already cleared from store",
                    store.retrieve(cp));
        }
    }

    // OAK-4826
    @Test
    public void cpCleanupOrphaned() throws Exception {
        Clock clock = Clock.SIMPLE;
        MemoryNodeStore store = new MemoryNodeStore();
        // prepare index and initial content
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Expecting no checkpoints",
                store.listCheckpoints().size() == 0);

        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String cp = store.listCheckpoints().iterator().next();
        Map<String, String> info = store.checkpointInfo(cp);

        builder = store.getRoot().builder();
        builder.child("testRoot").setProperty("foo", "def");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // wait until currentTimeMillis() changes. this ensures
        // the created value for the checkpoint is different
        // from the previous checkpoint.
        clock.waitUntil(clock.getTime() + 1);
        async.run();
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        cp = store.listCheckpoints().iterator().next();

        // create a new checkpoint with the info from the first checkpoint
        // this simulates an orphaned checkpoint that should be cleaned up.
        // the created timestamp is set back in time because cleanup preserves
        // checkpoints within the lease time frame.
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(clock.getTime() - 2 * async.getLeaseTimeOut());
        info.put("created", ISO8601.format(c));
        assertNotNull(store.checkpoint(TimeUnit.HOURS.toMillis(1), info));
        assertTrue("Expecting two checkpoints",
                store.listCheckpoints().size() == 2);

        async.cleanUpCheckpoints();
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        assertEquals(cp, store.listCheckpoints().iterator().next());
    }

    @Test
    public void disableCheckpointCleanup() throws Exception {
        String propertyName = "oak.async.checkpointCleanupIntervalMinutes";
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        try {
            System.setProperty(propertyName, "-1");
            final AtomicBoolean cleaned = new AtomicBoolean();
            AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider) {
                @Override
                void cleanUpCheckpoints() {
                    cleaned.set(true);
                    super.cleanUpCheckpoints();
                }
            };
            async.run();
            assertFalse(cleaned.get());
        } finally {
            System.clearProperty(propertyName);
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
        String missingAsync = "missing-async";
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, missingAsync);

        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate(missingAsync, store,
                provider);
        //first run, creates a checkpoint and a ref to it as the last indexed state
        async.run();
        assertFalse(async.isFailing());
        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String firstCp = store.listCheckpoints().iterator().next();
        assertEquals(
                firstCp,
                store.getRoot().getChildNode(ASYNC)
                        .getString(missingAsync));

        // second run, simulate an index going away
        provider = CompositeIndexEditorProvider
                .compose(new ArrayList<IndexEditorProvider>());
        async = new AsyncIndexUpdate(missingAsync, store, provider);
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
                store.getRoot().getChildNode(ASYNC)
                        .getString(missingAsync));
    }

    @Test
    public void testReindexMissingProvider_NonRoot() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        String missingAsyncName = "missing-async";
        createIndexDefinition(builder.child("subNodeIndex").child(INDEX_DEFINITIONS_NAME),
                "rootIndex2", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, missingAsyncName);

        builder.child("subNodeIndex").child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate(missingAsyncName, store,  provider);
        //first run, creates a checkpoint and a ref to it as the last indexed state
        async.run();
        assertFalse(async.isFailing());

        assertTrue("Expecting one checkpoint",
                store.listCheckpoints().size() == 1);
        String firstCp = store.listCheckpoints().iterator().next();
        assertEquals(firstCp, store.getRoot().getChildNode(ASYNC).getString(missingAsyncName));

        builder = store.getRoot().builder();
        builder.child("subNodeIndex").child("testRoot2").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // second run, simulate an index going away
        provider = CompositeIndexEditorProvider.compose(new ArrayList<IndexEditorProvider>());
        async = new AsyncIndexUpdate(missingAsyncName, store, provider);
        async.run();
        assertTrue(async.isFailing());
        // don't set reindex=true but skip the update
        NodeState rootIndex2 = NodeStateUtils.getNode(store.getRoot(), "/subNodeIndex/oak:index/rootIndex2");
        assertTrue(rootIndex2.exists());

        PropertyState reindex2 = rootIndex2.getProperty(REINDEX_PROPERTY_NAME);
        assertTrue(reindex2 == null || !reindex2.getValue(Type.BOOLEAN));

        assertTrue("Expecting one checkpoint",store.listCheckpoints().size() == 1);
        String secondCp = store.listCheckpoints().iterator().next();
        assertTrue("Store should not create a new checkpoint",  secondCp.equals(firstCp));
        assertEquals(firstCp, store.getRoot().getChildNode(ASYNC).getString(missingAsyncName));
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
                ASYNC);
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
                ASYNC);
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

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider, statsProvider, false);
        runOneCycle(async);
        assertEquals(1, async.getIndexStats().getExecutionStats().getExecutionCounter().getCount());

        //Run a cycle so that change of reindex flag gets indexed
        runOneCycle(async);
        assertEquals(2, async.getIndexStats().getExecutionStats().getExecutionCounter().getCount());

        long indexedNodeCount = async.getIndexStats().getExecutionStats().getIndexedNodeCount().getCount();
        //Now run so that it results in an empty cycle
        runOneCycle(async);
        assertEquals(indexedNodeCount, async.getIndexStats().getExecutionStats().getIndexedNodeCount().getCount());

        //Do some updates and counter should increase
        builder = store.getRoot().builder();
        builder.child("testRoot2").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        runOneCycle(async);
        assertEquals(4, async.getIndexStats().getExecutionStats().getExecutionCounter().getCount());

        //Do some updates but disable checkpoints. Counter should not increase
        builder = store.getRoot().builder();
        builder.child("testRoot3").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Disable new checkpoint retrieval
        knownCheckpoints.addAll(store.listCheckpoints());
        runOneCycle(async);
        assertEquals(0, lastExecutionStats(async.getIndexStats().getExecutionCount()));
    }

    @Test
    public void executionCountUpdatesOnRunWithoutAnyChangeInRepo() throws Exception {
        AsyncIndexUpdate async = new AsyncIndexUpdate("async",
                new MemoryNodeStore(),
                new PropertyIndexEditorProvider(),
                statsProvider, false);

        long execCnt1 = async.getIndexStats().getTotalExecutionCount();
        runOneCycle(async);
        long execCnt2 = async.getIndexStats().getTotalExecutionCount();
        runOneCycle(async);
        long execCnt3 = async.getIndexStats().getTotalExecutionCount();

        assertNotEquals("execCnt1 " + execCnt1 + " and execCnt2 " + execCnt2 + " are same", execCnt1, execCnt2);
        assertNotEquals("execCnt2 " + execCnt2 + " and execCnt3 " + execCnt3 + " are same", execCnt2, execCnt3);
    }


    private static long lastExecutionStats(CompositeData cd){
        //Last stat is the last entry in the array
        return ((long[]) cd.get("per second"))[59];
    }

    private static void runOneCycle(AsyncIndexUpdate async){
        async.run();
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

    @Test
    public void noRunWhenClosed() throws Exception{
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();

        async.close();
        LogCustomizer lc = createLogCustomizer(Level.WARN);
        async.run();
        assertEquals(1, lc.getLogs().size());
        assertThat(lc.getLogs().get(0), containsString("Could not acquire run permit"));

        lc.finished();

        async.close();
    }

    @Test
    public void closeWithSoftLimit() throws Exception{
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final Semaphore asyncLock = new Semaphore(1);
        final AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider) {
            @Override
            protected AsyncUpdateCallback newAsyncUpdateCallback(NodeStore store, String name, long leaseTimeOut,
                                                                 String beforeCheckpoint,
                                                                 AsyncIndexStats indexStats, AtomicBoolean stopFlag) {
                try {
                    asyncLock.acquire();
                } catch (InterruptedException ignore) {
                }
                return super.newAsyncUpdateCallback(store, name, leaseTimeOut, beforeCheckpoint,
                        indexStats, stopFlag);
            }
        };

        async.setCloseTimeOut(1000);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                async.run();
            }
        });

        Thread closer = new Thread(new Runnable() {
            @Override
            public void run() {
                async.close();
            }
        });

        asyncLock.acquire();
        t.start();

        //Wait till async gets to wait state i.e. inside run
        while(!asyncLock.hasQueuedThreads());

        LogCustomizer lc = createLogCustomizer(Level.DEBUG);
        closer.start();

        //Wait till closer is in waiting state
        while(!async.isClosing());

        //For softLimit case the flag should not be set
        assertFalse(async.isClosed());
        assertLogPhrase(lc.getLogs(), "[WAITING]");

        //Let indexing run complete now
        asyncLock.release();

        //Wait for both threads
        t.join();
        closer.join();

        //Close call should complete
        assertLogPhrase(lc.getLogs(), "[CLOSED OK]");
    }

    @Test
    public void closeWithHardLimit() throws Exception{
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final Semaphore asyncLock = new Semaphore(1);
        final AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider) {
            @Override
            protected AsyncUpdateCallback newAsyncUpdateCallback(NodeStore store, String name, long leaseTimeOut,
                                                                 String beforeCheckpoint,
                                                                 AsyncIndexStats indexStats, AtomicBoolean stopFlag) {
                try {
                    asyncLock.acquire();
                } catch (InterruptedException ignore) {
                }
                return super.newAsyncUpdateCallback(store, name, leaseTimeOut, beforeCheckpoint,
                        indexStats, stopFlag);
            }
        };

        //Set a 1 sec close timeout
        async.setCloseTimeOut(1);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                async.run();
            }
        });

        Thread closer = new Thread(new Runnable() {
            @Override
            public void run() {
                async.close();
            }
        });

        //Lock to ensure that AsyncIndexUpdate waits
        asyncLock.acquire();

        t.start();

        //Wait till async gets to wait state i.e. inside run
        while(!asyncLock.hasQueuedThreads());

        LogCustomizer lc = createLogCustomizer(Level.DEBUG);
        closer.start();

        //Wait till stopFlag is set
        while(!async.isClosed());

        assertLogPhrase(lc.getLogs(), "[SOFT LIMIT HIT]");

        //Let indexing run complete now
        asyncLock.release();

        //Wait for both threads
        t.join();

        //Async run would have exited with log message logged
        assertLogPhrase(lc.getLogs(), "The index update interrupted");

        //Wait for close call to complete
        closer.join();
        lc.finished();
    }

    @Test
    public void abortedRun() throws Exception{
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final Semaphore asyncLock = new Semaphore(1);
        final AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider) {
            @Override
            protected AsyncUpdateCallback newAsyncUpdateCallback(NodeStore store, String name, long leaseTimeOut,
                                                                 String beforeCheckpoint,
                                                                 AsyncIndexStats indexStats, AtomicBoolean stopFlag) {
                return new AsyncUpdateCallback(store, name, leaseTimeOut, beforeCheckpoint,
                        indexStats, stopFlag){

                    @Override
                    public void indexUpdate() throws CommitFailedException {
                        try {
                            asyncLock.acquire();
                        } catch (InterruptedException ignore) {
                        }
                        try {
                            super.indexUpdate();
                        }finally {
                            asyncLock.release();
                        }
                    }
                };
            }
        };

        runOneCycle(async);
        assertEquals(IndexStatsMBean.STATUS_DONE, async.getIndexStats().getStatus());

        //Below we ensure that we interrupt while the indexing is in progress
        //hence the use of asyncLock which ensures the abort is called at right time

        //Now make some changes to
        builder = store.getRoot().builder();
        builder.child("testRoot2").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Thread t = new Thread(async);
        //Lock to ensure that AsyncIndexUpdate waits
        asyncLock.acquire();

        t.start();

        //Wait till async gets to wait state i.e. inside run
        while(!asyncLock.hasQueuedThreads());

        assertEquals(IndexStatsMBean.STATUS_RUNNING, async.getIndexStats().getStatus());
        assertThat(async.getIndexStats().abortAndPause(), containsString("Abort request placed"));

        asyncLock.release();

        retry(5, 5, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return IndexStatsMBean.STATUS_INTERRUPTED.equals(async.getIndexStats().getStatus());
            }
        });

        //Post abort indexing should be fine
        runOneCycle(async);
        assertTrue(async.getIndexStats().isPaused());

        //Now resume indexing
        async.getIndexStats().resume();

        runOneCycle(async);
        assertEquals(IndexStatsMBean.STATUS_DONE, async.getIndexStats().getStatus());
        assertFalse(async.isClosed());
    }



    private void assertLogPhrase(List<String> logs, String logPhrase){
        assertThat(logs.toString(), containsString(logPhrase));
    }

    private static LogCustomizer createLogCustomizer(Level level){
        LogCustomizer lc = LogCustomizer.forLogger(AsyncIndexUpdate.class.getName())
                .filter(level)
                .enable(level)
                .create();
        lc.starting();
        return lc;
    }

    private static void retry(int timeoutSeconds, int intervalBetweenTriesMsec, Callable<Boolean> c) {
        long timeout = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < timeout) {
            try {
                if (c.call()) {
                    return;
                }
            } catch (Exception ignore) {
            }

            try {
                Thread.sleep(intervalBetweenTriesMsec);
            } catch (InterruptedException ignore) {
            }
        }

        fail("RetryLoop failed, condition is false after " + timeoutSeconds + " seconds: ");
    }

    @Test
    public void greedyLeaseReindex() throws Exception {

        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate pre = new AsyncIndexUpdate("async", store, provider);
        pre.run();
        pre.close();

        // rm all cps to simulate 'missing cp scenario'
        for (String cp : store.listCheckpoints()) {
            store.release(cp);
        }

        final AtomicBoolean greedyLease = new AtomicBoolean(false);
        final AsyncIndexUpdate async = new AsyncIndexUpdate("async", store,
                provider) {
            @Override
            protected AsyncUpdateCallback newAsyncUpdateCallback(
                    NodeStore store, String name, long leaseTimeOut,
                    String beforeCheckpoint, AsyncIndexStats indexStats,
                    AtomicBoolean stopFlag) {
                return new AsyncUpdateCallback(store, name, leaseTimeOut,
                        beforeCheckpoint, indexStats, stopFlag) {

                    @Override
                    protected void initLease() throws CommitFailedException {
                        greedyLease.set(true);
                        super.initLease();
                    }

                    @Override
                    protected void prepare(String afterCheckpoint)
                            throws CommitFailedException {
                        assertTrue(greedyLease.get());
                        super.prepare(afterCheckpoint);
                    }
                };
            }
        };
        async.run();
        async.close();
        assertTrue(greedyLease.get());
    }

    @Test
    public void checkpointLostEventualConsistent() throws Exception {

        MemoryNodeStore store = new MemoryNodeStore();
        final List<NodeState> rootStates = Lists.newArrayList();
        store.addObserver(new Observer() {
            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                rootStates.add(root);
            }
        });

        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate pre = new AsyncIndexUpdate("async", store, provider);
        pre.run();

        //Create another commit so that we have two checkpoints
        builder = store.getRoot().builder();
        builder.child("testRoot2").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        pre.run();

        pre.close();

        //Look for the nodestate just before the final merge in AsyncIndexUpdate
        //i.e. where older checkpoint was still referred and which has been "released"
        //post last run
        Collections.reverse(rootStates);
        final AtomicReference<NodeState> oldRootState = new AtomicReference<NodeState>();
        for (NodeState ns : rootStates) {
            NodeState async = ns.getChildNode(ASYNC);
            String checkpointName = async.getString("async");
            if (store.retrieve(checkpointName) == null &&
                    async.getProperty(AsyncIndexUpdate.leasify("async")) == null){
                oldRootState.set(ns);
                break;
            }
        }

        assertNotNull(oldRootState.get());

        final AtomicBoolean intiLeaseCalled = new AtomicBoolean(false);
        //Here for the call to read existing NodeState we would return the old
        //"stale" state where we have a stale checkpoint
        store = new MemoryNodeStore(store.getRoot()) {
            @Override
            public NodeState getRoot() {
                //Keep returning stale view untill initlease is not invoked
                if (!intiLeaseCalled.get()) {
                    return oldRootState.get();
                }
                return super.getRoot();
            }
        };

        final AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider){
            @Override
            protected AsyncUpdateCallback newAsyncUpdateCallback(
                    NodeStore store, String name, long leaseTimeOut,
                    String beforeCheckpoint, AsyncIndexStats indexStats,
                    AtomicBoolean stopFlag) {
                return new AsyncUpdateCallback(store, name, leaseTimeOut,
                        beforeCheckpoint, indexStats, stopFlag) {

                    @Override
                    protected void initLease() throws CommitFailedException {
                        intiLeaseCalled.set(true);
                        super.initLease();
                    }
                };
            }
        };
        async.run();

        //This run should fail
        assertTrue(async.getIndexStats().isFailing());
        async.close();
    }

    @Test
    public void commitContext() throws Exception{
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        CommitInfoCollector infoCollector = new CommitInfoCollector();
        store.addObserver(infoCollector);

        async.run();

        assertFalse(infoCollector.infos.isEmpty());
        assertNotNull(infoCollector.infos.get(0).getInfo().get(CommitContext.NAME));
    }

    @Test
    public void validatorProviderInvocation() throws Exception{
        MemoryNodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        CollectingValidatorProvider v = new CollectingValidatorProvider();

        async.setValidatorProviders(ImmutableList.<ValidatorProvider>of(v));
        async.run();

        assertFalse(v.visitedPaths.isEmpty());
        assertThat(v.visitedPaths, hasItem("/:async"));
        assertThat(v.visitedPaths, hasItem("/oak:index/rootIndex"));

    }

    @Test
    public void longTimeFailingIndexMarkedAsCorrupt() throws Exception{
        MemoryNodeStore store = new MemoryNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "barIndex", true, false, ImmutableSet.of("bar"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot1").setProperty("foo", "abc");
        builder.child("testRoot2").setProperty("bar", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        TestIndexEditorProvider provider = new TestIndexEditorProvider();

        Clock clock = new Clock.Virtual();
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.getCorruptIndexHandler().setClock(clock);
        async.run();

        //1. Basic sanity check. Indexing works
        PropertyIndexLookup lookup = new PropertyIndexLookup(store.getRoot());
        assertEquals(ImmutableSet.of("testRoot1"), find(lookup, "foo", "abc"));
        assertEquals(ImmutableSet.of("testRoot2"), find(lookup, "bar", "abc"));

        //2. Add some new content
        builder = store.getRoot().builder();
        builder.child("testRoot3").setProperty("foo", "xyz");
        builder.child("testRoot4").setProperty("bar", "xyz");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //3. Now fail the indexing for 'bar'
        provider.enableFailureMode("/oak:index/barIndex");

        async.run();
        assertTrue(async.getIndexStats().isFailing());
        //barIndex is failing but not yet considered corrupted
        assertTrue(async.getCorruptIndexHandler().getFailingIndexData("async").containsKey("/oak:index/barIndex"));
        assertFalse(async.getCorruptIndexHandler().getCorruptIndexData("async").containsKey("/oak:index/barIndex"));

        CorruptIndexInfo barIndexInfo = async.getCorruptIndexHandler().getFailingIndexData("async").get("/oak:index/barIndex");

        //fooIndex is fine
        assertFalse(async.getCorruptIndexHandler().getFailingIndexData("async").containsKey("/oak:index/fooIndex"));

        //lookup should also fail as indexing failed
        lookup = new PropertyIndexLookup(store.getRoot());
        assertTrue(find(lookup, "foo", "xyz").isEmpty());
        assertTrue(find(lookup, "bar", "xyz").isEmpty());

        //4.Now move the clock forward and let the failing index marked as corrupt
        clock.waitUntil(clock.getTime() + async.getCorruptIndexHandler().getCorruptIntervalMillis() + 1);

        //5. Let async run again
        async.run();

        //Indexing would be considered as failing
        assertTrue(async.getIndexStats().isFailing());
        assertEquals(IndexStatsMBean.STATUS_FAILING, async.getIndexStats().getStatus());

        //barIndex should be considered corrupt now
        assertTrue(async.getCorruptIndexHandler().getCorruptIndexData("async").containsKey("/oak:index/barIndex"));

        lookup = new PropertyIndexLookup(store.getRoot());

        //fooIndex should now report updated result. barIndex would fail
        assertEquals(ImmutableSet.of("testRoot3"), find(lookup, "foo", "xyz"));
        assertTrue(find(lookup, "bar", "xyz").isEmpty());
        assertEquals(1, barIndexInfo.getSkippedCount());

        //6. Index some stuff
        builder = store.getRoot().builder();
        builder.child("testRoot5").setProperty("foo", "pqr");
        builder.child("testRoot6").setProperty("bar", "pqr");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        async.run();
        assertTrue(async.getIndexStats().isFailing());

        //barIndex should be skipped
        assertEquals(2, barIndexInfo.getSkippedCount());

        //7. Lets reindex barIndex and ensure index is not misbehaving
        provider.disableFailureMode();

        builder = store.getRoot().builder();
        builder.child("oak:index").child("barIndex").setProperty("reindex", true);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        async.run();

        //now barIndex should not be part of failing index
        assertFalse(async.getCorruptIndexHandler().getFailingIndexData("async").containsKey("/oak:index/barIndex"));
    }

    @Test
    public void validName() throws Exception{
        assertNotNull(AsyncIndexUpdate.checkValidName("async"));
        assertNotNull(AsyncIndexUpdate.checkValidName("foo-async"));
        assertNotNull(AsyncIndexUpdate.checkValidName(IndexConstants.ASYNC_REINDEX_VALUE));

        try{
            AsyncIndexUpdate.checkValidName(null);
            fail();
        } catch (Exception ignore){

        }

        try{
            AsyncIndexUpdate.checkValidName("foo");
            fail();
        } catch (Exception ignore){

        }
    }

    @Test
    public void traversalCount() throws Exception{
        MemoryNodeStore store = new MemoryNodeStore();
        PropertyIndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();

        //Get rid of changes in index nodes i.e. /oak:index/rootIndex
        async.run();

        //Do a run without any index property change
        builder = store.getRoot().builder();
        builder.child("a").child("b");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        async.run();

        AsyncIndexStats stats = async.getIndexStats();
        assertEquals(3, stats.getNodesReadCount());
        assertEquals(0, stats.getUpdates());

        //Do a run with a index property change
        builder = store.getRoot().builder();
        builder.child("a").child("b").setProperty("foo", "bar");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        async.run();

        stats = async.getIndexStats();
        assertEquals(3, stats.getNodesReadCount());
        assertEquals(1, stats.getUpdates());
    }

    @Test
    public void startTimePresentInCommitInfo() throws Exception{
        MemoryNodeStore store = new MemoryNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot1").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexingContextCapturingProvider provider = new IndexingContextCapturingProvider();
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();

        assertNotNull(provider.lastIndexingContext);
        CommitInfo info = provider.lastIndexingContext.getCommitInfo();
        String indexStartTime = (String) info.getInfo().get(IndexConstants.CHECKPOINT_CREATION_TIME);
        assertNotNull(indexStartTime);
    }

    // OAK-6864
    @Test
    public void disableSupersededIndex() throws Exception {
        IndexEditorProvider propIdxEditorProvider = new PropertyIndexEditorProvider();
        EditorHook propIdxHook = new EditorHook(new IndexUpdateProvider(propIdxEditorProvider));
        MemoryNodeStore store = new MemoryNodeStore();

        String supersededIndexName = "supersededIndex";
        String supersedingIndexName = "supersedingIndex";

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, propIdxEditorProvider);

        // Create superseded index def and merge it
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder oakIndex = builder.child(INDEX_DEFINITIONS_NAME);
        createIndexDefinition(oakIndex, supersededIndexName, true, false, ImmutableSet.of("foo"), null);
        store.merge(builder, propIdxHook, CommitInfo.EMPTY);

        // Create superseding index def and merge it
        builder = store.getRoot().builder();
        oakIndex = builder.child(INDEX_DEFINITIONS_NAME);
        createIndexDefinition(oakIndex, supersedingIndexName, true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, ImmutableSet.of("async", "nrt"), Type.STRINGS)
                .setProperty(SUPERSEDED_INDEX_PATHS, INDEX_DEFINITIONS_NAME + "/" + supersededIndexName)
        ;
        store.merge(builder, propIdxHook, CommitInfo.EMPTY);

        // add a change and index it thought superseded index
        builder = store.getRoot().builder();
        builder.child("testNode1").setProperty("foo", "bar");
        store.merge(builder, propIdxHook, CommitInfo.EMPTY);

        // verify state
        NodeState supersededIndex = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(supersededIndexName);
        assertEquals("Index disabled too early", "property", supersededIndex.getString("type"));
        assertFalse("Don't set :disableIndexesOnNextCycle on superseded index",
                supersededIndex.hasProperty(DISABLE_INDEXES_ON_NEXT_CYCLE));
        NodeState supersedingIndex = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(supersedingIndexName);
        assertFalse("Don't set :disableIndexesOnNextCycle on superseding index just yet",
                supersedingIndex.hasProperty(DISABLE_INDEXES_ON_NEXT_CYCLE));

        // do an async run - this should reindex the superseding index
        async.run();

        // verify state
        supersededIndex = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(supersededIndexName);
        assertEquals("Index disabled too early", "property", supersededIndex.getString("type"));
        assertFalse("Don't set :disableIndexesOnNextCycle on superseded index",
                supersededIndex.hasProperty(DISABLE_INDEXES_ON_NEXT_CYCLE));
        supersedingIndex = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(supersedingIndexName);
        assertTrue(":disableIndexesOnNextCycle not set on superseding index after reindexing run",
                supersedingIndex.hasProperty(DISABLE_INDEXES_ON_NEXT_CYCLE));

        // add another change and index it thought superseded index
        builder = store.getRoot().builder();
        store.getRoot().builder().child("testNode2").setProperty("foo", "bar");
        store.merge(builder, propIdxHook, CommitInfo.EMPTY);

        // verify state
        supersededIndex = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(supersededIndexName);
        assertEquals("Index disabled too early", "property", supersededIndex.getString("type"));
        assertFalse("Don't set :disableIndexesOnNextCycle on superseded index",
                supersededIndex.hasProperty(DISABLE_INDEXES_ON_NEXT_CYCLE));
        supersedingIndex = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(supersedingIndexName);
        assertTrue(":disableIndexesOnNextCycle not set on superseding index after reindexing run",
                supersedingIndex.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));

        // do another async run - indexes should get disabled now
        async.run();

        // verify state
        supersededIndex = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(supersededIndexName);
        assertEquals("Index yet not disabled", "disabled", supersededIndex.getString("type"));
        assertFalse("Don't set :disableIndexesOnNextCycle on superseded index",
                supersededIndex.hasProperty(DISABLE_INDEXES_ON_NEXT_CYCLE));
        supersedingIndex = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(supersedingIndexName);
        assertFalse("Don't keep :disableIndexesOnNextCycle on superseding index after disabling",
                supersedingIndex.hasProperty(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    private static class TestIndexEditorProvider extends PropertyIndexEditorProvider {
        private String indexPathToFail;
        @Override
        public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root,
                                     @Nonnull IndexUpdateCallback callback) {
            IndexingContext context = ((ContextAwareCallback)callback).getIndexingContext();
            if (indexPathToFail != null && indexPathToFail.equals(context.getIndexPath())){
                RuntimeException e = new RuntimeException();
                context.indexUpdateFailed(e);
                throw e;
            }
            return super.getIndexEditor(type, definition, root, callback);
        }

        public void enableFailureMode(String indexPathToFail){
            this.indexPathToFail = indexPathToFail;
        }

        public void disableFailureMode(){
            indexPathToFail = null;
        }
    }

    private static class IndexingContextCapturingProvider extends PropertyIndexEditorProvider {
        IndexingContext lastIndexingContext;
        @Override
        public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root,
                                     @Nonnull IndexUpdateCallback callback) {
            lastIndexingContext = ((ContextAwareCallback)callback).getIndexingContext();
            return super.getIndexEditor(type, definition, root, callback);
        }
    }

    private static class CollectingValidatorProvider extends ValidatorProvider {
        final Set<String> visitedPaths = Sets.newHashSet();

        @Override
        protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
            return new CollectingValidator("/");
        }

        public void reset(){
            visitedPaths.clear();
        }

        private class CollectingValidator extends DefaultValidator {
            private final String path;

            public CollectingValidator(String path){
                this.path = path;
            }

            @Override
            public void enter(NodeState before, NodeState after) throws CommitFailedException {
                visitedPaths.add(path);
                super.enter(before, after);
            }

            @Override
            public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
                return new CollectingValidator(PathUtils.concat(path, name));
            }

            @Override
            public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
                return new CollectingValidator(PathUtils.concat(path, name));
            }

            @Override
            public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
                return new CollectingValidator(PathUtils.concat(path, name));
            }
        }
    }

    static class CommitInfoCollector implements Observer {
        List<CommitInfo> infos = Lists.newArrayList();

        @Override
        public void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
            if (info != CommitInfo.EMPTY_EXTERNAL){
                infos.add(info);
            }
        }

        void reset(){
            infos.clear();
        }
    }

}
