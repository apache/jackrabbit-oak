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
 *
 */
package org.apache.jackrabbit.oak.segment.tool.iotrace;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class BreadthFirstTraceTest {

    @NotNull
    private static NodeState createTree(int depth) {
        NodeBuilder root = EMPTY_NODE.builder();
        NodeBuilder child = root;
        for (int k = 0 ; k < depth; k++) {
            child = child.setChildNode("node-" + k);
        }
        return root.getNodeState();
    }

    @Test
    public void testTraverseEmptyTree() {
        List<List<String>> trace = new ArrayList<>();
        new BreadthFirstTrace(4, "/", trace::add).run(createTree(0));
        assertEquals(1, trace.size());
        assertEquals(ImmutableList.of("0", "1"), trace.get(0));
    }

    @Test
    public void testTraverseDepth1Tree() {
        List<List<String>> trace = new ArrayList<>();
        new BreadthFirstTrace(4, "/", trace::add).run(createTree(1));
        assertEquals(2, trace.size());
        assertEquals(ImmutableList.of("0", "1"), trace.get(0));
        assertEquals(ImmutableList.of("1", "2"), trace.get(1));
    }

    @Test
    public void testTraverseDepth2Tree() {
        List<List<String>> trace = new ArrayList<>();
        new BreadthFirstTrace(4, "/", trace::add).run(createTree(2));
        assertEquals(3, trace.size());
        assertEquals(ImmutableList.of("0", "1"), trace.get(0));
        assertEquals(ImmutableList.of("1", "2"), trace.get(1));
        assertEquals(ImmutableList.of("2", "3"), trace.get(2));
    }

    @Test
    public void testTraverseDepth3TreeWithLimit2() {
        List<List<String>> trace = new ArrayList<>();
        new BreadthFirstTrace(2, "/", trace::add).run(createTree(3));
        assertEquals(3, trace.size());
        assertEquals(ImmutableList.of("0", "1"), trace.get(0));
        assertEquals(ImmutableList.of("1", "2"), trace.get(1));
        assertEquals(ImmutableList.of("2", "3"), trace.get(2));
    }

}
