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
package org.apache.jackrabbit.oak.plugins.index.p2.strategy;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class ContentMirrorStoreStrategyTest {

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
    public void testIndexPruning() throws Exception {
        IndexStoreStrategy store = new ContentMirrorStoreStrategy();

        NodeState root = MemoryNodeState.EMPTY_NODE;
        NodeBuilder index = root.builder();

        store.insert(index, "key", false,
                Sets.newHashSet("/", "a/b/c", "a/b/d", "b", "d/e", "d/e/f"));
        checkPath(index, "key", "", true);
        checkPath(index, "key", "a/b/c", true);
        checkPath(index, "key", "a/b/d", true);
        checkPath(index, "key", "b", true);
        checkPath(index, "key", "d/e", true);
        checkPath(index, "key", "d/e/f", true);

        // remove the root key, removes just the "match" property, when the
        // index is not empty
        store.remove(index, "key", Sets.newHashSet("/"));
        checkPath(index, "key", "d/e/f", true);

        // removing intermediary path doesn't remove the entire subtree
        store.remove(index, "key", Sets.newHashSet("d/e"));
        checkPath(index, "key", "d/e/f", true);

        // removing intermediary path doesn't remove the entire subtree
        store.remove(index, "key", Sets.newHashSet("d/e/f"));
        checkNotPath(index, "key", "d");

        // brother segment removed
        store.remove(index, "key", Sets.newHashSet("a/b/d", "a/b"));
        checkPath(index, "key", "a/b/c", true);

        // reinsert root and remove everything else
        store.insert(index, "key", false, Sets.newHashSet("/"));
        store.remove(index, "key", Sets.newHashSet("d/e/f", "b", "a/b/c"));

        // remove the root key when the index is empty
        store.remove(index, "key", Sets.newHashSet("/"));
        Assert.assertEquals(0, index.getChildNodeCount());
    }

    private void checkPath(NodeBuilder node, String key, String path,
            boolean checkMatch) {
        path = PathUtils.concat(key, path);
        NodeBuilder check = node;
        for (String p : PathUtils.elements(path)) {
            Assert.assertTrue("Missing child node " + p + " on path " + path,
                    check.hasChildNode(p));
            check = check.child(p);
        }
        if (checkMatch) {
            Assert.assertNotNull(check.getProperty("match"));
        }
    }

    private void checkNotPath(NodeBuilder node, String key, String path) {
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

}
