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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class PropertyIndexTest {

    private static final int MANY = 100;

    @Test
    public void testPropertyLookup() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.getBuilder();
        builder.getChildBuilder("oak:index").getChildBuilder("foo");
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.getBuilder();
        builder.getChildBuilder("a").set("foo", "abc");
        builder.getChildBuilder("b").set("foo", "abc", "def");
        // plus lots of dummy content to highlight the benefit of indexing
        for (int i = 0; i < MANY; i++) {
            builder.getChildBuilder("n" + i).set("foo", "xyz");
        }
        NodeState after = builder.getNodeState();

        // First check lookups without an index
        PropertyIndexLookup lookup = new PropertyIndexLookup(after);
        long withoutIndex = System.nanoTime();
        assertEquals(ImmutableSet.of("a", "b"), lookup.find("foo", "abc"));
        assertEquals(ImmutableSet.of("b"), lookup.find("foo", "def"));
        assertEquals(ImmutableSet.of(), lookup.find("foo", "ghi"));
        assertEquals(MANY, lookup.find("foo", "xyz").size());
        withoutIndex = System.nanoTime() - withoutIndex;

        // ... then see how adding an index affects the code
        lookup = new PropertyIndexLookup(
                new PropertyIndexHook().processCommit(before, after));
        long withIndex = System.nanoTime();
        assertEquals(ImmutableSet.of("a", "b"), lookup.find("foo", "abc"));
        assertEquals(ImmutableSet.of("b"), lookup.find("foo", "def"));
        assertEquals(ImmutableSet.of(), lookup.find("foo", "ghi"));
        assertEquals(MANY, lookup.find("foo", "xyz").size());
        withIndex = System.nanoTime() - withIndex;

        // System.out.println("Index performance ratio: " + withoutIndex/withIndex);
        // assertTrue(withoutIndex > withIndex);
    }

}
