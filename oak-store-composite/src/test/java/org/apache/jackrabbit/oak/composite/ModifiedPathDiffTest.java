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
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;

public class ModifiedPathDiffTest {

    private final NodeState base; {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setChildNode("a");
        builder.setChildNode("b");
        builder.setChildNode("c").child("d");
        builder.setProperty("x", 1);
        builder.setProperty("y", 1);
        builder.setProperty("z", 1);
        base = builder.getNodeState();
    }

    @Test
    public void testAddedNodesAreModified() {
        NodeBuilder builder = base.builder();
        builder.getChildNode("a").child("xyz");
        builder.getChildNode("c").getChildNode("d").child("abc");
        Set<String> paths = ModifiedPathDiff.getModifiedPaths(base, builder.getNodeState());
        assertEquals(newHashSet("/a/xyz", "/c/d/abc"), paths);
    }

    @Test
    public void testRemovedNodesAreModified() {
        NodeBuilder builder = base.builder();
        builder.getChildNode("a").remove();
        builder.getChildNode("c").getChildNode("d").remove();
        Set<String> paths = ModifiedPathDiff.getModifiedPaths(base, builder.getNodeState());
        assertEquals(newHashSet("/a", "/c/d"), paths);
    }

    @Test
    public void testModifiedNodesAreModified() {
        NodeBuilder builder = base.builder();
        builder.getChildNode("a").setProperty("x", 1l, Type.LONG);
        builder.getChildNode("c").getChildNode("d").setProperty("x", 1l, Type.LONG);
        Set<String> paths = ModifiedPathDiff.getModifiedPaths(base, builder.getNodeState());
        assertEquals(newHashSet("/a", "/c/d"), paths);
    }
}
