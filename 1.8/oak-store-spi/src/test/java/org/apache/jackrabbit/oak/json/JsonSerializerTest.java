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

package org.apache.jackrabbit.oak.json;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

public class JsonSerializerTest {

    @Test
    public void childOrder() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a");
        builder.child("b");
        builder.child("c");
        List<String> expectedOrder = Arrays.asList("a", "c", "b");
        builder.setProperty(":childOrder", expectedOrder, Type.NAMES);

        NodeState state = builder.getNodeState();
        String json = serialize(state);

        JsopReader reader = new JsopTokenizer(json);
        List<String> childNames = Lists.newArrayList();
        reader.read('{');
        do {
            String key = reader.readString();
            reader.read(':');
            if (reader.matches('{')) {
                childNames.add(key);
                reader.matches('}');
            }

        } while (reader.matches(','));

        assertEquals(expectedOrder, childNames);
    }

    private String serialize(NodeState nodeState){
        JsopBuilder json = new JsopBuilder();
        new JsonSerializer(json, "{\"properties\":[\"*\", \"-:*\"]}", new BlobSerializer()).serialize(nodeState);
        return json.toString();
    }
}