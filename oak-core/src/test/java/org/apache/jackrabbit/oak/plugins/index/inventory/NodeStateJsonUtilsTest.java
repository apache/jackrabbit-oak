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

package org.apache.jackrabbit.oak.plugins.index.inventory;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.inventory.NodeStateJsonUtils;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class NodeStateJsonUtilsTest {

    @Test
    public void toJson() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").setProperty("foo", 10);
        builder.child("a").setProperty("foo2", "bar");
        builder.child("a").setProperty("foo3", true);
        builder.child("a").setProperty("foo4", new ArrayBasedBlob("foo".getBytes()));
        builder.child("a").child("b").setProperty("foo", Lists.newArrayList(1L,2L,3L), Type.LONGS);
        builder.child("a").child("b").setProperty("foo2", Lists.newArrayList("x", "y", "z"), Type.STRINGS);
        builder.child("a").child("b").setProperty("foo3", Lists.newArrayList(true, false), Type.BOOLEANS);
        builder.child("a").child(":c").setProperty("foo", "bar");

        String jsop = NodeStateJsonUtils.toJson(builder.getNodeState(), false);
        JSONObject json = (JSONObject) JSONValue.parseWithException(jsop);

        assertTrue(json.containsKey("a"));
        JSONObject a = (JSONObject) json.get("a");
        JSONObject b = (JSONObject) a.get("b");
        assertEquals(Lists.newArrayList(1L,2L,3L), b.get("foo"));
        assertEquals(Lists.newArrayList("x", "y", "z"), b.get("foo2"));
        assertEquals(Lists.newArrayList(true, false), b.get("foo3"));
    }

    @Test
    public void hiddenContent() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").setProperty(":foo", "bar");
        builder.child("a");
        builder.child(":b");
        NodeState state = builder.getNodeState();

        String jsop = NodeStateJsonUtils.toJson(state, false);
        JSONObject json = (JSONObject) JSONValue.parseWithException(jsop);
        assertFalse(json.containsKey(":b")); //Hidden content should not be found

        JSONObject a = (JSONObject) json.get("a");
        assertTrue(a.containsKey("foo"));
        assertFalse(a.containsKey(":foo"));

        String jsop2 = NodeStateJsonUtils.toJson(builder.getNodeState(), true);
        JSONObject json2 = (JSONObject) JSONValue.parseWithException(jsop2);
        assertTrue(json2.containsKey(":b"));

        JSONObject a2 = (JSONObject) json2.get("a");
        assertTrue(a2.containsKey("foo"));
        assertTrue(a2.containsKey(":foo"));
    }

}