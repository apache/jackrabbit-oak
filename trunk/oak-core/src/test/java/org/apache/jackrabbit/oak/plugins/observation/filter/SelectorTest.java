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

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class SelectorTest {
    public static final Predicate<NodeState> ALL = alwaysTrue();

    private final NodeState root = createRoot();

    private final NodeState nodeA = root.getChildNode("a");
    private final NodeState nodeB = nodeA.getChildNode("b");
    private final NodeState nodeC = nodeB.getChildNode("c");

    private static NodeState createRoot() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("a").setChildNode("b").setChildNode("c");
        return builder.getNodeState();
    }

    @Test
    public void selectDescendant() {
        Selector selector = new RelativePathSelector("/a/b/c", Selectors.PARENT);
        UniversalFilter filter = new UniversalFilter(root, root, selector, ALL);
        assertEquals(nodeC, selector.select(filter, null, null));
    }

}
