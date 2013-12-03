/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Predicates.alwaysTrue;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class SelectorTest {
    public static final Predicate<Tree> ALL = alwaysTrue();

    private final NodeState root; {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("a").setChildNode("b").setChildNode("c");
        root = builder.getNodeState();
    }

    private final ImmutableTree rootTree = new ImmutableTree(root);

    @Test
    public void selectDescendant() {
        String path = "/a/b/c";
        Selector selector = new RelativePathSelector(path, Selectors.PARENT);
        UniversalFilter filter = new UniversalFilter(rootTree, rootTree, selector, ALL);
        assertEquals(path, selector.select(filter, null, null).getPath());
    }

    @Test
    public void selectThis() {
        String path = ".";
        Selector selector = new RelativePathSelector(path, Selectors.PARENT);
        UniversalFilter filter = new UniversalFilter(rootTree, rootTree, selector, ALL);
        assertEquals(rootTree.getPath(), selector.select(filter, null, null).getPath());
    }

    @Test
    public void selectAncestor() {
        String path = "../..";
        Selector selector = new RelativePathSelector(path, Selectors.PARENT);
        UniversalFilter filter = new UniversalFilter(
                rootTree.getChild("a").getChild("b").getChild("c"), rootTree, selector, ALL);
        assertEquals("/a", selector.select(filter, null, null).getPath());
    }

    @Test
    public void selectAncestorOfRoot() {
        String path = "../..";
        Selector selector = new RelativePathSelector(path, Selectors.PARENT);
        UniversalFilter filter = new UniversalFilter(rootTree, rootTree, selector, ALL);
        assertFalse("/a", selector.select(filter, null, null).exists());
    }

}
