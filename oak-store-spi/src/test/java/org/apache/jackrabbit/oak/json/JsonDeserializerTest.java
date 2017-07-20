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

import java.util.Collections;
import java.util.Random;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class JsonDeserializerTest {
    private Base64BlobSerializer blobHandler = new Base64BlobSerializer();
    private Random rnd = new Random();

    @Test
    public void basicStuff() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").setProperty("foo", "bar");
        builder.child("b").setProperty("foo", 1);
        assertDeserialization(builder);
    }

    @Test
    public void variousPropertyTypes() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").setProperty("foo", 10);
        builder.child("a").setProperty("foo2", "bar");
        builder.child("a").setProperty("foo3", true);
        builder.child("a").setProperty("foo4", false);
        builder.child("a").setProperty("foo5", 1.1);
        builder.child("a").setProperty("foo6", "nt:base", Type.NAME);
        builder.child("a").child("b").setProperty("foo", Lists.newArrayList(1L,2L,3L), Type.LONGS);
        builder.child("a").child("b").setProperty("foo2", Lists.newArrayList("x", "y", "z"), Type.STRINGS);
        builder.child("a").child("b").setProperty("foo3", Lists.newArrayList(true, false), Type.BOOLEANS);
        builder.child("a").child("b").setProperty("foo4", Lists.newArrayList(1.1, 1.2), Type.DOUBLES);
        builder.child("a").child(":c").setProperty("foo", "bar");

        assertDeserialization(builder);
    }
    
    @Test
    public void emptyProperty() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").setProperty("foo", Collections.emptyList(), Type.NAMES);
        assertDeserialization(builder);
    }

    @Test
    public void binaryProperty() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").setProperty("foo", createBlob(100));
        builder.child("b").setProperty("foo", Lists.newArrayList(createBlob(200), createBlob(300)), Type.BINARIES);
        assertDeserialization(builder);
    }

    @Test
    public void singleProperty() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").setProperty("foo3", 1.1);
        //builder.child("a").setProperty("foo3", Lists.newArrayList(true, false), Type.BOOLEANS);

        assertDeserialization(builder);

    }

    private Blob createBlob(int length) {
        return new ArrayBasedBlob(randomBytes(length));
    }

    private byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }

    private void assertDeserialization(NodeBuilder builder) {
        NodeState nodeState = builder.getNodeState();
        String json = serialize(nodeState);
        NodeState nodeState2 = deserialize(json);
        assertTrue(EqualsDiff.equals(nodeState, nodeState2));
    }

    private NodeState deserialize(String json) {
        JsonDeserializer deserializer = new JsonDeserializer(blobHandler);
        return deserializer.deserialize(json);
    }

    private String serialize(NodeState nodeState){
        JsopBuilder json = new JsopBuilder();
        new JsonSerializer(json, blobHandler).serialize(nodeState);
        return json.toString();
    }

}