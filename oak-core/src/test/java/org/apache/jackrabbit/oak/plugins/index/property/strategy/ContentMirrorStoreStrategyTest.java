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

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.util.Collections;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
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
    public void testIndexPruning() {
        IndexStoreStrategy store = new ContentMirrorStoreStrategy();

        NodeState root = EMPTY_NODE;
        NodeBuilder index = root.builder();

        for (String path : asList("/", "a/b/c", "a/b/d", "b", "d/e", "d/e/f")) {
            store.update(index, path, EMPTY, KEY);
        }
        checkPath(index, "key", "", true);
        checkPath(index, "key", "a/b/c", true);
        checkPath(index, "key", "a/b/d", true);
        checkPath(index, "key", "b", true);
        checkPath(index, "key", "d/e", true);
        checkPath(index, "key", "d/e/f", true);

        // remove the root key, removes just the "match" property, when the
        // index is not empty
        store.update(index, "/", KEY, EMPTY);
        checkPath(index, "key", "d/e/f", true);

        // removing intermediary path doesn't remove the entire subtree
        store.update(index, "d/e", KEY, EMPTY);
        checkPath(index, "key", "d/e/f", true);

        // removing intermediary path doesn't remove the entire subtree
        store.update(index, "d/e/f", KEY, EMPTY);
        checkNotPath(index, "key", "d");

        // brother segment removed
        store.update(index, "a/b/d", KEY, EMPTY);
        store.update(index, "a/b", KEY, EMPTY);
        checkPath(index, "key", "a/b/c", true);

        // reinsert root and remove everything else
        store.update(index, "", EMPTY, KEY);
        store.update(index, "d/e/f", KEY, EMPTY);
        store.update(index, "b", KEY, EMPTY);
        store.update(index, "a/b/c", KEY, EMPTY);

        // remove the root key when the index is empty
        store.update(index, "", KEY, EMPTY);
        Assert.assertEquals(0, index.getChildNodeCount(1));
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
        NodeBuilder index = indexMeta.child(INDEX_CONTENT_NODE_NAME);
        store.update(index, "a", EMPTY, KEY);
        store.update(index, "b", EMPTY, KEY);
        Assert.assertTrue(
                "ContentMirrorStoreStrategy should guarantee uniqueness on insert",
                store.count(indexMeta.getNodeState(), Collections.singleton("key"), 2) > 1);
    }

}
