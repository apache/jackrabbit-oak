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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_REINDEX_VALUE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_NRT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate.MissingIndexProviderStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class IndexUpdateTest {

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new PropertyIndexEditorProvider()));

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    /**
     * Simple Test
     * <ul>
     * <li>Add an index definition</li>
     * <li>Add some content</li>
     * <li>Search & verify</li>
     * </ul>
     *
     */
    @Test
    public void test() throws Exception {
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        createIndexDefinition(
                builder.child("newchild").child("other")
                        .child(INDEX_DEFINITIONS_NAME), "subIndex", true,
                false, ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();

        // Add nodes
        builder.child("testRoot").setProperty("foo", "abc");
        builder.child("newchild").child("other").child("testChild")
                .setProperty("foo", "xyz");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        checkPathExists(indexed, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        checkPathExists(indexed, "newchild", "other", INDEX_DEFINITIONS_NAME,
                "subIndex", INDEX_CONTENT_NODE_NAME);

        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));

        PropertyIndexLookup lookupChild = new PropertyIndexLookup(indexed
                .getChildNode("newchild").getChildNode("other"));
        assertEquals(ImmutableSet.of("testChild"),
                find(lookupChild, "foo", "xyz"));
        assertEquals(ImmutableSet.of(), find(lookupChild, "foo", "abc"));

    }

    /**
     * Reindex Test
     * <ul>
     * <li>Add some content</li>
     * <li>Add an index definition with the reindex flag set</li>
     * <li>Search & verify</li>
     * </ul>
     */
    @Test
    public void testReindex() throws Exception {
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");
        checkPathExists(ns, INDEX_CONTENT_NODE_NAME);
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN));

        // next, lookup
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    /**
     * Reindex Test
     * <ul>
     * <li>Add some content & an index definition</li>
     * <li>Update the index def by setting the reindex flag to true</li>
     * <li>Search & verify</li>
     * </ul>
     */
    @Test
    public void testReindex2() throws Exception {
        builder.child("testRoot").setProperty("foo", "abc");

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .removeProperty("reindex");

        NodeState before = builder.getNodeState();
        builder.child(INDEX_DEFINITIONS_NAME).child("rootIndex")
                .setProperty(REINDEX_PROPERTY_NAME, true);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");
        checkPathExists(ns, INDEX_CONTENT_NODE_NAME);
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN));

        // next, lookup
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    /**
     * Auto Reindex Test
     * <ul>
     * <li>Add some content</li>
     * <li>Add an index definition without a reindex flag (see OAK-1874)</li>
     * <li>Search & verify</li>
     * </ul>
     */
    @Test
    public void testReindexAuto() throws Exception {
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", false, false, ImmutableSet.of("foo"), null);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");
        checkPathExists(ns, INDEX_CONTENT_NODE_NAME);
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN));

        // next, lookup
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    @Test
    public void testReindexAuto_ImportCase() throws Exception{
        NodeState before = builder.getNodeState();

        NodeBuilder idx = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", false, false, ImmutableSet.of("foo"), null);
        idx.child(":index");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");

        assertEquals(0, ns.getLong("reindexCount"));
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN));
    }

    @Test
    public void testReindexHidden() throws Exception {
        NodeState before = EmptyNodeState.EMPTY_NODE;
        NodeBuilder builder = before.builder();
        builder.child(":testRoot").setProperty("foo", "abc");
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", false, false, ImmutableSet.of("foo"), null);
        NodeState after = builder.getNodeState();
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");
        NodeState index = checkPathExists(ns, INDEX_CONTENT_NODE_NAME);
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN));
        assertFalse(index.getChildNodeCount(1) > 0);

        before = indexed;
        builder = before.builder();
        builder.child(INDEX_DEFINITIONS_NAME).child("rootIndex")
                .setProperty("reindex", true);
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        index = checkPathExists(ns, INDEX_CONTENT_NODE_NAME);
        ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN));
        assertFalse(index.getChildNodeCount(1) > 0);
    }

    @Test
    public void testIndexDefinitions() throws Exception {
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "existing", true, false, ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        NodeBuilder other = builder.child("test").child("other");
        // Add index definition
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        createIndexDefinition(
                other.child(INDEX_DEFINITIONS_NAME), "index2", true, false,
                ImmutableSet.of("foo"), null);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // check that the index content nodes exist
        checkPathExists(indexed, INDEX_DEFINITIONS_NAME, "existing",
                INDEX_CONTENT_NODE_NAME);
        checkPathExists(indexed, "test", "other", INDEX_DEFINITIONS_NAME,
                "index2", INDEX_CONTENT_NODE_NAME);
    }

    @Test
    public void reindexAndIndexDefnChildRemoval_OAK_2117() throws Exception{
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();

        NodeBuilder nb = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", false, false, ImmutableSet.of("foo"), null);
        nb.child("prop1").setProperty("foo", "bar");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");

        //Check index defn child node exist
        checkPathExists(ns, "prop1");
        checkPathExists(ns, INDEX_CONTENT_NODE_NAME);

        // next, lookup
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));

    }

    /**
     * Tests that with explicit reindex i.e. reindex=true those hidden nodes
     * which have IndexConstants.REINDEX_RETAIN set to true are not removed
     */
    @Test
    public void reindexSkipRemovalOfRetainedNodes() throws Exception{
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();

        NodeBuilder nb = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        nb.child(":hidden-node-1").setProperty("foo", "bar");
        nb.child(":hidden-node-2").setProperty(IndexConstants.REINDEX_RETAIN, true);
        nb.child("visible-node");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME, "rootIndex");
        checkPathExists(ns, "visible-node");
        checkPathExists(ns, ":hidden-node-2");
        assertFalse(ns.getChildNode(":hidden-node-1").exists());
        assertEquals(1, ns.getLong(REINDEX_COUNT));
    }

    /**
     * Test that an index is still reindexed if it has hidden nodes but with all such
     * hidden nodes having IndexConstants.REINDEX_RETAIN set to true i.e. this index
     * does not yet have any hidden nodes corresponding to persisted index like lucene
     */
    @Test
    public void reindexSkipRemovalOfRetainedNodes_FreshIndex() throws Exception{
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();

        NodeBuilder nb = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", false, false, ImmutableSet.of("foo"), null);
        nb.child(":hidden-node-2").setProperty(IndexConstants.REINDEX_RETAIN, true);
        nb.child("visible-node");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME, "rootIndex");
        checkPathExists(ns, "visible-node");
        checkPathExists(ns, ":hidden-node-2");
        assertEquals(1, ns.getLong(REINDEX_COUNT));
    }


    /**
     * Async Reindex Test (OAK-2174)
     * <ul>
     * <li>Add some content</li>
     * <li>Add an index definition with the reindex flag and the reindex-async flag set</li>
     * <li>Run the background async job manually</li>
     * <li>Search & verify</li>
     * </ul>
     */
    @Test
    public void testReindexAsync() throws Exception {
        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        EditorHook hook = new EditorHook(new IndexUpdateProvider(provider));

        NodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(REINDEX_ASYNC_PROPERTY_NAME, true);
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, hook, CommitInfo.EMPTY);

        // first check that the async flag exist
        NodeState ns1 = checkPathExists(store.getRoot(),
                INDEX_DEFINITIONS_NAME, "rootIndex");
        assertTrue(ns1.getProperty(REINDEX_PROPERTY_NAME)
                .getValue(Type.BOOLEAN));
        assertTrue(ns1.getProperty(REINDEX_ASYNC_PROPERTY_NAME).getValue(
                Type.BOOLEAN));
        assertEquals(ASYNC_REINDEX_VALUE, ns1.getString(ASYNC_PROPERTY_NAME));

        AsyncIndexUpdate async = new AsyncIndexUpdate(ASYNC_REINDEX_VALUE,
                store, provider, true);
        int max = 5;
        // same behaviour as PropertyIndexAsyncReindex mbean
        boolean done = false;
        int count = 0;
        while (!done || count >= max) {
            async.run();
            done = async.isFinished();
            count++;
        }

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(store.getRoot(), INDEX_DEFINITIONS_NAME,
                "rootIndex");
        checkPathExists(ns, INDEX_CONTENT_NODE_NAME);
        assertFalse(ns.getProperty(REINDEX_PROPERTY_NAME)
                .getValue(Type.BOOLEAN));
        assertNull(ns.getProperty(ASYNC_PROPERTY_NAME));

        // next, lookup
        PropertyIndexLookup lookup = new PropertyIndexLookup(store.getRoot());
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo",
        "abc"));
    }

    /**
     * OAK-2203 Test reindex behavior on a sync index when the index provider is missing
     * for a given type
     */
    @Test
    public void testReindexSyncMissingProvider() throws Exception {
        EditorHook hook = new EditorHook(new IndexUpdateProvider(
                emptyProvider()));
        NodeState before = builder.getNodeState();

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        builder.child(INDEX_DEFINITIONS_NAME).child("azerty");
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        NodeState rootIndex = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");
        PropertyState ps = rootIndex.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertTrue(ps.getValue(Type.BOOLEAN));

        NodeState azerty = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "azerty");
        assertNull("Node should be ignored by reindexer",
                azerty.getProperty(REINDEX_PROPERTY_NAME));
    }

    /**
     * OAK-3505 Provide an optionally stricter policy for missing synchronous
     * index editor providers
     */
    @Test
    public void testMissingProviderFailsCommit() throws Exception {

        final IndexUpdateCallback noop = new IndexUpdateCallback() {
            @Override
            public void indexUpdate() {
            }
        };
        final MissingIndexProviderStrategy mips = new MissingIndexProviderStrategy();
        mips.setFailOnMissingIndexProvider(true);

        EditorHook hook = new EditorHook(new EditorProvider() {
            @Override
            public Editor getRootEditor(NodeState before, NodeState after,
                    NodeBuilder builder, CommitInfo info)
                    throws CommitFailedException {
                return new IndexUpdate(emptyProvider(), null, after, builder,
                        noop).withMissingProviderStrategy(mips);
            }
        });

        NodeState before = builder.getNodeState();

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        builder.child(INDEX_DEFINITIONS_NAME).child("azerty");
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        try {
            hook.processCommit(before, after, CommitInfo.EMPTY);
            fail("commit should fail on missing index provider");
        } catch (CommitFailedException ex) {
            // expected
        }
    }

    @Test
    public void testReindexCount() throws Exception{
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", false, false, ImmutableSet.of("foo"), null);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        long t1 = getReindexCount(indexed);

        NodeBuilder b2 = indexed.builder();
        b2.child(INDEX_DEFINITIONS_NAME).child("rootIndex").setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        indexed = HOOK.processCommit(indexed, b2.getNodeState(), CommitInfo.EMPTY);
        long t2 = getReindexCount(indexed);

        assertTrue(t2 > t1);
    }

    @Test
    public void contextAwareCallback() throws Exception{
        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);

        NodeState after = builder.getNodeState();

        CallbackCapturingProvider provider = new CallbackCapturingProvider();
        EditorHook hook = new EditorHook(new IndexUpdateProvider(provider));

        CommitInfo info = new CommitInfo("foo", "bar");
        NodeState indexed = hook.processCommit(before, after, info);

        assertNotNull(provider.callback);
        assertThat(provider.callback, instanceOf(ContextAwareCallback.class));
        ContextAwareCallback contextualCallback = (ContextAwareCallback) provider.callback;
        IndexingContext context = contextualCallback.getIndexingContext();

        assertNotNull(context);
        assertEquals("/oak:index/rootIndex", context.getIndexPath());
        assertTrue(context.isReindexing());
        assertFalse(context.isAsync());
        assertSame(info, context.getCommitInfo());

        before = indexed;
        builder = indexed.builder();
        builder.child("a").setProperty("foo", "bar");
        after = builder.getNodeState();

        hook.processCommit(before, after, info);
        assertFalse(((ContextAwareCallback)provider.callback).getIndexingContext().isReindexing());
    }

    @Test
    public void contextAwareCallback_async() throws Exception{
        NodeState before = builder.getNodeState();
        NodeBuilder idx = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        idx.setProperty("async", asList("sync", "async"), Type.STRINGS);

        NodeState after = builder.getNodeState();

        CallbackCapturingProvider provider = new CallbackCapturingProvider();
        EditorHook hook = new EditorHook(new IndexUpdateProvider(provider, "async", false));

        hook.processCommit(before, after, CommitInfo.EMPTY);

        assertTrue(((ContextAwareCallback)provider.callback).getIndexingContext().isAsync());
    }

    private static class CallbackCapturingProvider extends PropertyIndexEditorProvider {
        private Map<String, IndexingContext> callbacks = Maps.newHashMap();
        IndexUpdateCallback callback;

        @Override
        public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                                     @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback) {
            Editor editor = super.getIndexEditor(type, definition, root, callback);
            if (editor != null){
                this.callback = callback;
                if (callback instanceof ContextAwareCallback){
                    IndexingContext context = ((ContextAwareCallback) callback).getIndexingContext();
                    callbacks.put(context.getIndexPath(), context);
                }
            }
            return editor;
        }

        public void reset(){
            callback = null;
            callbacks.clear();
        }

        public IndexingContext getContext(String indexPath){
            return callbacks.get(indexPath);
        }
    }


    long getReindexCount(NodeState indexed) {
        return indexed.getChildNode(INDEX_DEFINITIONS_NAME)
                .getChildNode("rootIndex")
                .getProperty(IndexConstants.REINDEX_COUNT).getValue(Type.LONG);
    }

    private static IndexEditorProvider emptyProvider() {
        return new IndexEditorProvider() {
            @Override
            public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                    @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
                    throws CommitFailedException {
                return null;
            }
        };
    }

    private Set<String> find(PropertyIndexLookup lookup, String name,
            String value) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(NT_BASE);        
        SelectorImpl selector = new SelectorImpl(type, NT_BASE);
        Filter filter = new FilterImpl(selector, "SELECT * FROM [nt:base]", new QueryEngineSettings());
        return Sets.newHashSet(lookup.query(filter, name,
                PropertyValues.newString(value)));
    }

    static NodeState checkPathExists(NodeState state, String... verify) {
        NodeState c = state;
        for (String p : verify) {
            c = c.getChildNode(p);
            assertTrue(c.exists());
        }
        return c;
    }

    @Test
    public void testAsyncMVPDefinition() throws Exception {
        NodeBuilder base;

        // async null
        base = EmptyNodeState.EMPTY_NODE.builder();
        assertTrue(IndexUpdate.isIncluded(null, base));
        assertFalse(IndexUpdate.isIncluded("async", base));

        // async single value
        base = EmptyNodeState.EMPTY_NODE.builder().setProperty(
                ASYNC_PROPERTY_NAME, "async");
        assertFalse(IndexUpdate.isIncluded(null, base));
        assertTrue(IndexUpdate.isIncluded("async", base));

        // async multiple values: "" for sync
        base = EmptyNodeState.EMPTY_NODE.builder()
                .setProperty(ASYNC_PROPERTY_NAME, Sets.newHashSet(INDEXING_MODE_NRT, "async"),
                        Type.STRINGS);
        assertTrue(IndexUpdate.isIncluded(null, base));
        assertTrue(IndexUpdate.isIncluded("async", base));
        assertFalse(IndexUpdate.isIncluded("async-other", base));

        // async multiple values: "sync" for sync
        base = EmptyNodeState.EMPTY_NODE.builder().setProperty(
                ASYNC_PROPERTY_NAME, Sets.newHashSet("sync", "async"),
                Type.STRINGS);
        assertTrue(IndexUpdate.isIncluded(null, base));
        assertTrue(IndexUpdate.isIncluded("async", base));
        assertFalse(IndexUpdate.isIncluded("async-other", base));

        // async multiple values: no sync present
        base = EmptyNodeState.EMPTY_NODE.builder().setProperty(
                ASYNC_PROPERTY_NAME, Sets.newHashSet("async", "async-other"),
                Type.STRINGS);
        assertFalse(IndexUpdate.isIncluded(null, base));
        assertTrue(IndexUpdate.isIncluded("async", base));
        assertTrue(IndexUpdate.isIncluded("async-other", base));
    }

    @Test
    public void corruptIndexSkipped() throws Exception{
        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);

        NodeState after = builder.getNodeState();

        CallbackCapturingProvider provider = new CallbackCapturingProvider();
        EditorHook hook = new EditorHook(new IndexUpdateProvider(provider));

        //1. Basic sanity - provider gets invoked
        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        String indexPath = "/oak:index/rootIndex";
        assertNotNull(provider.getContext(indexPath));


        //2. Mark as corrupt and assert that editor is not invoked
        builder = indexed.builder();
        before = indexed;
        builder.child("testRoot").setProperty("foo", "abc");
        markCorrupt(builder, "rootIndex");
        after = builder.getNodeState();

        provider.reset();
        indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        assertNull(provider.getContext(indexPath));

        //3. Now reindex and that should reset corrupt flag
        builder = indexed.builder();
        before = indexed;
        child(builder, indexPath).setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        after = builder.getNodeState();
        provider.reset();
        indexed = hook.processCommit(before, after, CommitInfo.EMPTY);

        assertFalse(NodeStateUtils.getNode(indexed, indexPath).hasProperty(IndexConstants.CORRUPT_PROPERTY_NAME));
        assertNotNull(provider.getContext(indexPath));
    }

    @Test
    public void ignoreReindexingFlag() throws Exception{
        String indexPath = "/oak:index/rootIndex";
        CallbackCapturingProvider provider = new CallbackCapturingProvider();

        IndexUpdateProvider indexUpdate = new IndexUpdateProvider(provider);
        EditorHook hook = new EditorHook(indexUpdate);

        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);

        builder.child("a").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        assertTrue(provider.getContext(indexPath).isReindexing());

        before = indexed;
        builder = before.builder();
        builder.child("b").setProperty("foo", "xyz");
        child(builder, indexPath).setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        after = builder.getNodeState();

        provider.reset();
        indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        assertTrue(provider.getContext(indexPath).isReindexing());

        //Now set IndexUpdate to ignore the reindex flag
        indexUpdate.setIgnoreReindexFlags(true);
        indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        assertFalse(provider.getContext(indexPath).isReindexing());

        //Despite reindex flag set to true and reindexing not done new
        //content should still get picked up
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertFalse(find(lookup, "foo", "xyz").isEmpty());
    }

    @Test
    public void shouldNotReindexAsyncIndexInSyncMode() throws Exception{
        String indexPath = "/oak:index/rootIndex";
        CallbackCapturingProvider provider = new CallbackCapturingProvider();

        IndexUpdateProvider indexUpdate = new IndexUpdateProvider(provider);
        EditorHook hook = new EditorHook(indexUpdate);

        NodeState before = builder.getNodeState();
        NodeBuilder idx = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        idx.setProperty("async", asList("async", "sync"), Type.STRINGS);

        builder.child("a").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        assertFalse(provider.getContext(indexPath).isReindexing());
    }

    @Test
    public void indexUpdateToleratesMalignCommitProgressCallback() throws Exception {
        final IndexUpdateCallback noop = new IndexUpdateCallback() {
            @Override
            public void indexUpdate() {
            }
        };

        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        NodeState after = builder.getNodeState();

        CallbackCapturingProvider provider = new CallbackCapturingProvider();
        IndexUpdate indexUpdate = new IndexUpdate(provider, null, after, builder,
                noop);
        indexUpdate.enter(before, after);

        ContextAwareCallback contextualCallback = (ContextAwareCallback) provider.callback;
        IndexingContext context = contextualCallback.getIndexingContext();

        context.registerIndexCommitCallback(new IndexCommitCallback() {
            @Override
            public void commitProgress(IndexProgress indexProgress) {
                throw new NullPointerException("Malign callback");
            }
        });

        indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_SUCCEDED);
    }

    @Test
    public void commitProgressCallback() throws Exception {
        final IndexUpdateCallback noop = new IndexUpdateCallback() {
            @Override
            public void indexUpdate() {
            }
        };

        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        NodeState after = builder.getNodeState();

        CallbackCapturingProvider provider = new CallbackCapturingProvider();
        IndexUpdate indexUpdate = new IndexUpdate(provider, null, after, builder,
                noop);
        indexUpdate.enter(before, after);

        ContextAwareCallback contextualCallback = (ContextAwareCallback) provider.callback;
        IndexingContext context = contextualCallback.getIndexingContext();

        final AtomicInteger numCallbacks = new AtomicInteger();
        IndexCommitCallback callback1 = new IndexCommitCallback() {
            @Override
            public void commitProgress(IndexProgress indexProgress) {
                numCallbacks.incrementAndGet();
            }
        };
        IndexCommitCallback callback2 = new IndexCommitCallback() {
            @Override
            public void commitProgress(IndexProgress indexProgress) {
                numCallbacks.incrementAndGet();
            }
        };

        context.registerIndexCommitCallback(callback1);
        context.registerIndexCommitCallback(callback2);
        context.registerIndexCommitCallback(callback1);//intentionally adding same one twice

        for (IndexCommitCallback.IndexProgress progress : IndexCommitCallback.IndexProgress.values()) {
            numCallbacks.set(0);
            indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_SUCCEDED);
            assertEquals("Either not all callbacks are called OR same callback got called twice for " + progress,
                    2, numCallbacks.get());
        }
    }

    @Test
    public void indexesDisabled() throws Exception{
        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), null);
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "barIndex", true, false, ImmutableSet.of("bar"), null);
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        before = indexed;
        builder = indexed.builder();
        NodeBuilder newIndex = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "newIndex", true, false, ImmutableSet.of("bar"), null);
        newIndex.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS, asList("/oak:index/fooIndex"), Type.STRINGS);

        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        //Post reindex also index should not be disabled
        assertEquals("property", indexed.getChildNode("oak:index").getChildNode("fooIndex").getString(TYPE_PROPERTY_NAME));
        assertTrue(indexed.getChildNode("oak:index").getChildNode("newIndex").getBoolean(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE));

        before = indexed;
        builder = indexed.builder();
        builder.child("testRoot2").setProperty("foo", "abc");
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        //Index only disabled after next cycle
        assertEquals(IndexConstants.TYPE_DISABLED, indexed.getChildNode("oak:index").getChildNode("fooIndex").getString(TYPE_PROPERTY_NAME));
        assertFalse(indexed.getChildNode("oak:index").getChildNode("newIndex").getBoolean(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    @Test
    public void reindexForDisabledIndexes() throws Exception{
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new CompositeIndexEditorProvider(
                        new PropertyIndexEditorProvider(),
                        new ReferenceEditorProvider()
                )));

        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), null);
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);

        before = indexed;
        builder = before.builder();
        builder.getChildNode("oak:index").getChildNode("fooIndex").setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED);
        builder.getChildNode("oak:index").getChildNode("fooIndex").setProperty(REINDEX_PROPERTY_NAME, true);
        after = builder.getNodeState();

        LogCustomizer customLogs = LogCustomizer.forLogger(IndexUpdate.class.getName()).filter(Level.INFO).create();
        customLogs.starting();

        before = after;
        builder = before.builder();
        builder.child("testRoot2").setProperty("foo", "abc");
        after = builder.getNodeState();
        indexed = hook.processCommit(before, after, CommitInfo.EMPTY);

        assertTrue(customLogs.getLogs().isEmpty());
        customLogs.finished();

    }

    private static void markCorrupt(NodeBuilder builder, String indexName) {
        builder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexName)
                .setProperty(IndexConstants.CORRUPT_PROPERTY_NAME, ISO8601.format(Calendar.getInstance()));
    }

    private static NodeBuilder child(NodeBuilder nb, String path){
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

}
