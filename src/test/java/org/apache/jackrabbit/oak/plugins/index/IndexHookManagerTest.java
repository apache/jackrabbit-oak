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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager.IndexDefDiff;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class IndexHookManagerTest {

    @Test
    public void test() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        NodeBuilder builder = root.builder();
        // this index is on the current update branch, it should be seen by the
        // diff
        builder.child("oak:index")
                .child("existing")
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE,
                        Type.NAME);
        // this index is NOT the current update branch, it should NOT be seen by
        // the diff
        builder.child("newchild")
                .child("other")
                .child("oak:index")
                .child("existing2")
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE,
                        Type.NAME);

        NodeState before = builder.getNodeState();
        // Add index definition
        builder.child("oak:index")
                .child("foo")
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE,
                        Type.NAME);
        builder.child("test")
                .child("other")
                .child("oak:index")
                .child("index2")
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE,
                        Type.NAME);
        NodeState after = builder.getNodeState();

        // <path>, <state>
        Map<String, NodeBuilder> changedDefs = new HashMap<String, NodeBuilder>();
        // <path>, <state>
        Map<String, NodeBuilder> existingDefs = new HashMap<String, NodeBuilder>();

        IndexDefDiff diff = new IndexDefDiff(builder, changedDefs, existingDefs);
        after.compareAgainstBaseState(before, diff);

        Set<String> updates = changedDefs.keySet();
        String[] expected = new String[] { "/oak:index/foo",
                "/test/other/oak:index/index2" };
        for (String def : expected) {
            assertTrue("Expected to find " + def, updates.remove(def));
        }
        assertTrue(updates.isEmpty());

        Set<String> existing = existingDefs.keySet();
        String[] expectedE = new String[] { "/oak:index/existing", };
        for (String def : expectedE) {
            assertTrue("Expected to find " + def, existing.remove(def));
        }
        assertTrue(existing.isEmpty());
    }

    @Test
    public void testReindex() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        NodeBuilder builder = root.builder();
        builder.child("oak:index")
                .child("reindexed")
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE,
                        Type.NAME)
                .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        NodeState state = builder.getNodeState();

        // <path>, <state>
        Map<String, NodeBuilder> changedDefs = new HashMap<String, NodeBuilder>();
        // <path>, <state>
        Map<String, NodeBuilder> existingDefs = new HashMap<String, NodeBuilder>();

        IndexDefDiff diff = new IndexDefDiff(builder, changedDefs, existingDefs);
        state.compareAgainstBaseState(state, diff);

        Set<String> updates = changedDefs.keySet();
        String[] expected = new String[] { "/oak:index/reindexed" };
        for (String def : expected) {
            assertTrue("Expected to find " + def, updates.remove(def));
        }
        assertTrue(updates.isEmpty());
        assertTrue(existingDefs.isEmpty());
    }
}
