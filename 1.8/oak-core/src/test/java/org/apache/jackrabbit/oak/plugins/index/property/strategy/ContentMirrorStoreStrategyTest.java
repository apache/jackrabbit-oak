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
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.KEY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditor.COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditor.DEFAULT_RESOLUTION;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.index.counter.ApproximateCounter.COUNT_PROPERTY_PREFIX;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Set;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the content mirror strategy
 */
public class ContentMirrorStoreStrategyTest {

    private static final Set<String> EMPTY = newHashSet();

    private static final Set<String> KEY = newHashSet("key");

    /**
     * <p>
     * Tests the index pruning mechanism
     * </p>
     * <ul>
     * <li>
     * adds a few levels of nodes, nodes with an even index will have the
     * 'match' property set</li>
     *
     * <li>
     * pruning in this case means that whatever path that doesn't have a 'match'
     * property is considered dead weight and should be removed from the index</li>
     * </ul>
     */
    @Test
    public void testIndexPruning() throws CommitFailedException {
        IndexStoreStrategy store = new ContentMirrorStoreStrategy();

        NodeState root = EMPTY_NODE;
        NodeBuilder builder = root.builder();
        Supplier<NodeBuilder> index = () -> builder;

        for (String path : asList("/", "a/b/c", "a/b/d", "b", "d/e", "d/e/f")) {
            store.update(index, path, null, null, EMPTY, KEY);
        }
        checkPath(builder, "key", "", true);
        checkPath(builder, "key", "a/b/c", true);
        checkPath(builder, "key", "a/b/d", true);
        checkPath(builder, "key", "b", true);
        checkPath(builder, "key", "d/e", true);
        checkPath(builder, "key", "d/e/f", true);

        // remove the root key, removes just the "match" property, when the
        // index is not empty
        store.update(index, "/", null, null, KEY, EMPTY);
        checkPath(builder, "key", "d/e/f", true);

        // removing intermediary path doesn't remove the entire subtree
        store.update(index, "d/e", null, null, KEY, EMPTY);
        checkPath(builder, "key", "d/e/f", true);

        // removing intermediary path doesn't remove the entire subtree
        store.update(index, "d/e/f", null, null, KEY, EMPTY);
        checkNotPath(builder, "key", "d");

        // brother segment removed
        store.update(index, "a/b/d", null, null, KEY, EMPTY);
        store.update(index, "a/b", null, null, KEY, EMPTY);
        checkPath(builder, "key", "a/b/c", true);

        // reinsert root and remove everything else
        store.update(index, "", null, null, EMPTY, KEY);
        store.update(index, "d/e/f", null, null, KEY, EMPTY);
        store.update(index, "b", null, null, KEY, EMPTY);
        store.update(index, "a/b/c", null, null, KEY, EMPTY);

        // remove the root key when the index is empty
        store.update(index, "", null, null, KEY, EMPTY);
        Assert.assertEquals(0, builder.getChildNodeCount(1));
    }

    private static void checkPath(NodeBuilder node, String key, String path,
            boolean checkMatch) {
        path = PathUtils.concat(key, path);
        NodeBuilder check = node;
        for (String p : PathUtils.elements(path)) {
            Assert.assertTrue("Missing child node " + p + " on path " + path,
                    check.hasChildNode(p));
            check = check.child(p);
        }
        if (checkMatch) {
            Assert.assertTrue(check.hasProperty("match"));
        }
    }

    private static void checkNotPath(NodeBuilder node, String key, String path) {
        path = PathUtils.concat(key, path);
        String parentPath = PathUtils.getParentPath(path);
        String name = PathUtils.getName(path);
        NodeBuilder check = node;
        for (String p : PathUtils.elements(parentPath)) {
            Assert.assertTrue("Missing child node " + p + " on path " + path,
                    check.hasChildNode(p));
            check = check.child(p);
        }
        Assert.assertFalse(check.hasChildNode(name));
    }

    @Test
    public void testUnique() throws CommitFailedException {
        IndexStoreStrategy store = new ContentMirrorStoreStrategy();
        NodeState root = EMPTY_NODE;
        NodeBuilder indexMeta = root.builder();
        Supplier<NodeBuilder> index = memoize(() -> indexMeta.child(INDEX_CONTENT_NODE_NAME));
        store.update(index, "a", null, null, EMPTY, KEY);
        store.update(index, "b", null, null, EMPTY, KEY);
        Assert.assertTrue(
                "ContentMirrorStoreStrategy should guarantee uniqueness on insert",
                store.count(root, indexMeta.getNodeState(), Collections.singleton("key"), 2) > 1);
    }
    
    @Test
    public void testIndexCountersUsageWithoutPathRestriction() {
        final long approxNodeCount = 50;
        final long approxKeyCount = 25;
        final long entryCount = 30 * DEFAULT_RESOLUTION;
        final long keyCount = 75;
        final int maxTraversal = 200;
        final String keyValue = KEY.iterator().next();
        final String approxPropName = COUNT_PROPERTY_PREFIX + "gen_uuid";

        IndexStoreStrategy store = new ContentMirrorStoreStrategy();
        NodeState root = EMPTY_NODE;
        NodeBuilder indexMeta = root.builder();
        NodeBuilder index = indexMeta.child(INDEX_CONTENT_NODE_NAME);
        NodeBuilder key = index.child(keyValue);

        // is-not-null query without entryCount
        index.setProperty(approxPropName, approxNodeCount, Type.LONG);
        Assert.assertEquals("Approximate count not used for is-not-null query",
                approxNodeCount, store.count(root, indexMeta.getNodeState(),
                        null, maxTraversal));

        // prop=value query without entryCount
        key.setProperty(approxPropName, approxKeyCount, Type.LONG);
        Assert.assertEquals("Approximate count not used for key=value query",
                approxKeyCount, store.count(root, indexMeta.getNodeState(),
                        KEY, maxTraversal));

        // is-not-null query with entryCount
        indexMeta
                .setProperty(ENTRY_COUNT_PROPERTY_NAME, entryCount, Type.LONG);
        Assert.assertEquals(
                "Entry count not used even when present for is-not-null query",
                entryCount, store.count(root, indexMeta.getNodeState(), null,
                        maxTraversal));

        // prop=value query with entryCount but without keyCount
        Assert.assertTrue("Rough key count not considered for key=value query",
                entryCount > store.count(root, indexMeta.getNodeState(), KEY,
                        maxTraversal));

        // prop=value query with entryCount and keyCount
        indexMeta.setProperty(KEY_COUNT_PROPERTY_NAME, keyCount, Type.LONG);
        Assert.assertTrue("Key count not considered for key=value query",
                entryCount > store.count(root, indexMeta.getNodeState(), KEY,
                        maxTraversal));

        // is-not-null query with entryCount=-1 (this should lead to traversal
        // and hence should result in '0'
        indexMeta.setProperty(ENTRY_COUNT_PROPERTY_NAME, (long) -1, Type.LONG);
        Assert.assertEquals(
                "Entry count not used even when present for is-not-null query",
                0, store.count(root, indexMeta.getNodeState(), null,
                        maxTraversal));
    }

    @Test
    public void testIndexCountersUsageWithPathRestriction() {
        final String subPathName = "sub-path";
        final int filteredNodeFactor = 2;
        final long repoTreeApproxNodeCount = 50000;
        final long repoSubPathApproxNodeCount = repoTreeApproxNodeCount /
                filteredNodeFactor;
        final FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictPath("/" + subPathName,
                Filter.PathRestriction.ALL_CHILDREN);

        final long approxNodeCount = 100;
        final long approxKeyCount = 50;
        final long entryCount = 60 * DEFAULT_RESOLUTION;
        final long keyCount = 150;
        final int maxTraversal = 200;
        final String keyValue = KEY.iterator().next();
        final String approxPropName = COUNT_PROPERTY_PREFIX + "gen_uuid";

        IndexStoreStrategy store = new ContentMirrorStoreStrategy();
        NodeBuilder rootBuilder = EMPTY_NODE.builder();

        // setup tree for NodeCounter to work
        rootBuilder.setProperty(COUNT_PROPERTY_NAME,
                repoTreeApproxNodeCount, Type.LONG);
        NodeBuilder subPath = rootBuilder.child(subPathName);
        subPath.setProperty(COUNT_PROPERTY_NAME,
                repoSubPathApproxNodeCount, Type.LONG);

        NodeState root = rootBuilder.getNodeState();

        NodeBuilder indexMeta = rootBuilder.child("propIndex");
        NodeBuilder index = indexMeta.child(INDEX_CONTENT_NODE_NAME);
        NodeBuilder key = index.child(keyValue);

        // is-not-null query without entryCount
        index.setProperty(approxPropName, approxNodeCount, Type.LONG);
        assertInRange(
                "Approximate count not used for is-not-null query",
                approxNodeCount,
                store.count(filter, root, indexMeta.getNodeState(),
                            null, maxTraversal));

        // prop=value query without entryCount
        key.setProperty(approxPropName, approxKeyCount, Type.LONG);
        assertInRange(
                "Approximate count not used for key=value query",
                approxKeyCount,
                store.count(filter, root, indexMeta.getNodeState(),
                            KEY, maxTraversal));

        // is-not-null query with entryCount
        indexMeta
                .setProperty(ENTRY_COUNT_PROPERTY_NAME, entryCount, Type.LONG);
        assertInRange(
                "Entry count not used even when present for is-not-null query",
                entryCount,
                filteredNodeFactor *
                        store.count(filter, root, indexMeta.getNodeState(),
                                null, maxTraversal));

        // prop=value query with entryCount but without keyCount
        Assert.assertTrue(
                "Rough key count not considered for key=value query",
                entryCount > filteredNodeFactor *
                        store.count(filter, root, indexMeta.getNodeState(),
                                KEY, maxTraversal));

        // prop=value query with entryCount and keyCount
        indexMeta.setProperty(KEY_COUNT_PROPERTY_NAME, keyCount, Type.LONG);
        Assert.assertTrue(
                "Key count not considered for key=value query",
                entryCount > filteredNodeFactor *
                        store.count(filter, root, indexMeta.getNodeState(),
                                KEY, maxTraversal));
    }

    @Test
    public void nonRootStorage() throws Exception{
        IndexStoreStrategy store = new ContentMirrorStoreStrategy(INDEX_CONTENT_NODE_NAME, "/content", false);

        NodeState root = EMPTY_NODE;
        NodeBuilder builder = root.builder();
        Supplier<NodeBuilder> index = () -> builder;

        for (String path : asList("a", "a/c", "b")) {
            store.update(index, path, null, null, EMPTY, KEY);
        }

        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictPath("/content", Filter.PathRestriction.ALL_CHILDREN);

        NodeBuilder indexMeta = EMPTY_NODE.builder();
        indexMeta.setChildNode(INDEX_CONTENT_NODE_NAME, builder.getNodeState());

        Iterable<String> paths = store.query(filter, null, indexMeta.getNodeState(), KEY);
        assertThat(copyOf(paths), containsInAnyOrder("a", "a/c", "b"));

        FilterImpl filter2 = FilterImpl.newTestInstance();
        filter2.restrictPath("/content/a", Filter.PathRestriction.ALL_CHILDREN);

        paths = store.query(filter2, null, indexMeta.getNodeState(), KEY);
        assertThat(copyOf(paths), containsInAnyOrder("a", "a/c"));

        store = new ContentMirrorStoreStrategy(INDEX_CONTENT_NODE_NAME, "/content", true);

        paths = store.query(filter, null, indexMeta.getNodeState(), KEY);
        assertThat(copyOf(paths), containsInAnyOrder("/content/a", "/content/a/c", "/content/b"));

        paths = store.query(filter2, null, indexMeta.getNodeState(), KEY);
        assertThat(copyOf(paths), containsInAnyOrder("/content/a", "/content/a/c"));
    }

    private static void assertInRange(String msg, double expected, double actual) {
        double allowedError = 0.1;
        double diff = Math.abs(expected - actual);
        Assert.assertTrue(msg + "; expected about " + expected + ", got " + actual, 
                diff < expected * allowedError);
    }

}
