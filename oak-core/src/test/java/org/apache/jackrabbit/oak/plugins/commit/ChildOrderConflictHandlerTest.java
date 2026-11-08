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
package org.apache.jackrabbit.oak.plugins.commit;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.PartialConflictHandler.Resolution;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

public class ChildOrderConflictHandlerTest {

    private final ChildOrderConflictHandler handler = new ChildOrderConflictHandler();

    @Test
    public void mergedOrderKeepsTheirOrderAndAppendsOurNewChildren() {
        NodeBuilder parent = parentWithChildren("a", "b", "theirNew", "ourNew");

        Resolution resolution = handler.changeChangedProperty(parent,
                childOrder("a", "b", "ourNew"), childOrder("a", "b", "theirNew"));

        assertEquals(Resolution.MERGED, resolution);
        assertEquals(List.of("a", "b", "theirNew", "ourNew"), mergedOrder(parent));
    }

    @Test
    public void childDeletedByThemIsRemovedFromMergedOrder() {
        NodeBuilder parent = parentWithChildren("a", "theirNew");

        handler.changeChangedProperty(parent,
                childOrder("a", "deletedByThem"), childOrder("a", "theirNew"));

        assertEquals(List.of("a", "theirNew"), mergedOrder(parent));
    }

    @Test
    public void childDeletedByUsIsRemovedFromMergedOrder() {
        NodeBuilder parent = parentWithChildren("a", "ourNew");

        handler.changeChangedProperty(parent,
                childOrder("a", "ourNew"), childOrder("a", "deletedByUs"));

        assertEquals(List.of("a", "ourNew"), mergedOrder(parent));
    }

    @Test
    public void onlyChildrenListedByOneSideAreLookedUp() {
        List<String> shared = names(10_000);
        List<String> children = new ArrayList<>(shared);
        children.add("theirNew");
        children.add("ourNew");
        NodeBuilder parent = spy(parentWithChildren(children.toArray(new String[0])));
        List<String> ours = new ArrayList<>(shared);
        ours.add("ourNew");
        List<String> theirs = new ArrayList<>(shared);
        theirs.add("theirNew");

        handler.changeChangedProperty(parent, childOrder(ours), childOrder(theirs));

        assertEquals(children.size(), mergedOrder(parent).size());
        verify(parent, atMost(2)).hasChildNode(anyString());
    }

    private static NodeBuilder parentWithChildren(String... names) {
        NodeBuilder parent = EMPTY_NODE.builder();
        for (String name : names) {
            parent.child(name);
        }
        return parent;
    }

    private static PropertyState childOrder(String... names) {
        return childOrder(List.of(names));
    }

    private static PropertyState childOrder(List<String> names) {
        return PropertyStates.createProperty(OAK_CHILD_ORDER, names, Type.NAMES);
    }

    private static List<String> mergedOrder(NodeBuilder parent) {
        List<String> order = new ArrayList<>();
        parent.getProperty(OAK_CHILD_ORDER).getValue(Type.NAMES).forEach(order::add);
        return order;
    }

    private static List<String> names(int count) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add("child" + i);
        }
        return names;
    }
}
