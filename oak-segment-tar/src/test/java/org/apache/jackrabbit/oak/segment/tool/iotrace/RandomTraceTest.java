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

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class RandomTraceTest {

    @NotNull
    private static NodeState createTree(@NotNull List<String> paths) {
        NodeBuilder root = EMPTY_NODE.builder();
        for (String path : paths) {
            createNode(root, path);
        }
        return root.getNodeState();
    }

    private static void createNode(@NotNull NodeBuilder root, @NotNull String path) {
        NodeBuilder child = root;
        for (String name : elements(path)) {
            child = child.child(name);
        }
    }

    @Test
    public void testTraverseEmptyTree() {
        List<List<String>> trace = new ArrayList<>();
        new RandomAccessTrace(emptyList(), 0,10, trace::add)
            .run(createTree(emptyList()));
        assertEquals(0, trace.size());
    }

    @Test
    public void testTraverseNonExistingPath() {
        List<List<String>> trace = new ArrayList<>();
        new RandomAccessTrace(ImmutableList.of("/not/here"), 0, 1, trace::add)
            .run(createTree(emptyList()));
        assertEquals(1, trace.size());
        assertEquals(ImmutableList.of("/not/here"), trace.get(0));
    }

    @Test
    public void testTraverse() {
        List<List<String>> trace = new ArrayList<>();
        ImmutableList<String> paths = ImmutableList.of("/a/b/c", "/d/e/f");
        new RandomAccessTrace(paths, 0, 2, trace::add)
                .run(createTree(paths));
        assertEquals(2, trace.size());
        assertTrue(paths.contains(trace.get(0).get(0)));
        assertTrue(paths.contains(trace.get(1).get(0)));
    }

}
