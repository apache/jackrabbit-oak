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

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Test;

public class IndexHookManagerDiffTest {

    @Test
    public void testIndexDefinitions() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        NodeBuilder builder = root.builder();
        // this index is on the current update branch, it should be seen by the
        // diff
        builder.child("oak:index")
                .child("existing")
                .setProperty("type", "property")
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE,
                        Type.NAME);
        // this index is NOT the current update branch, it should NOT be seen by
        // the diff
        builder.child("newchild")
                .child("other")
                .child("oak:index")
                .child("existing2")
                .setProperty("type", "property")
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
                .setProperty("type", "property")
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE,
                        Type.NAME);
        NodeState after = builder.getNodeState();

        IndexHookProvider provider = new CompositeIndexHookProvider(
                new PropertyIndexHookProvider());

        // <type, <path, indexhook>>
        Map<String, Map<String, List<IndexHook>>> updates = new HashMap<String, Map<String, List<IndexHook>>>();
        NodeStateDiff diff = new IndexHookManagerDiff(provider, builder,
                updates);
        after.compareAgainstBaseState(before, diff);

        for (String type : updates.keySet()) {
            for (List<IndexHook> hooks : updates.get(type).values()) {
                for (IndexHook hook : hooks) {
                    hook.apply();
                }
            }
        }

        Set<String> expected = newHashSet("/", "/test/other");
        Set<String> found = updates.remove("property").keySet();
        assertTrue("Expecting " + expected + " got " + found,
                difference(found, expected).isEmpty());
        assertTrue(updates.isEmpty());

        NodeState indexed = builder.getNodeState();

        // check that the index content nodes exist
        checkPathExists(indexed, "oak:index", "existing", ":index");
        checkPathExists(indexed, "test", "other", "oak:index", "index2",
                ":index");
        NodeState ignoredIndex = checkPathExists(indexed, "newchild", "other",
                "oak:index", "existing2");
        assertFalse(ignoredIndex.hasChildNode(":index"));
    }

    private static NodeState checkPathExists(NodeState state, String... verify) {
        NodeState c = state;
        for (String p : verify) {
            assertTrue(c.hasChildNode(p));
            c = c.getChildNode(p);
        }
        return c;
    }
}
