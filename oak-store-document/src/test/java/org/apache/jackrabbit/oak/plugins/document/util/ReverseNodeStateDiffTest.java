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
package org.apache.jackrabbit.oak.plugins.document.util;

import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReverseNodeStateDiffTest {

    private NodeState base = EmptyNodeState.EMPTY_NODE;

    @Before
    public void initializeBase() {
        NodeBuilder builder = new MemoryNodeBuilder(base);
        builder.child("a");
        builder.setProperty("p", "v");
        base = builder.getNodeState();
    }

    @Test
    public void compare() {
        // diff must report the reverse
        compareAgainstBase(b -> b.setProperty("q", "v"), "^\"/q\":null");
        compareAgainstBase(b -> b.removeProperty("p"), "^\"/p\":\"v\"");
        compareAgainstBase(b -> b.setProperty("p", "w"), "^\"/p\":\"v\"");
        compareAgainstBase(b -> b.child("a").setProperty("p", "v"), "^\"/a/p\":null");
        compareAgainstBase(b -> b.child("a").remove(), "+\"/a\":{}");
        compareAgainstBase(b -> b.child("a").child("b"), "-\"/a/b\"");
    }

    private void compareAgainstBase(Modification modification,
                                    String jsopDiff) {
        NodeBuilder builder = new MemoryNodeBuilder(base);
        modification.apply(builder);
        NodeState modified = builder.getNodeState();

        JsopDiff diff = new JsopDiff();
        modified.compareAgainstBaseState(base, new ReverseNodeStateDiff(diff));
        assertEquals(jsopDiff, diff.toString());
    }

    private interface Modification {

        void apply(NodeBuilder builder);
    }
}
