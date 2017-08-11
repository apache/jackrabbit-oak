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

package org.apache.jackrabbit.oak.exporter;

import java.io.File;

import com.google.common.io.Files;
import org.apache.jackrabbit.oak.json.BlobDeserializer;
import org.apache.jackrabbit.oak.json.JsonDeserializer;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class NodeStateSerializerTest {
    private NodeBuilder builder = EMPTY_NODE.builder();
    private BlobDeserializer blobHandler = mock(BlobDeserializer.class);

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void basics() throws Exception{
        builder.child("a").setProperty("foo", "bar");

        NodeStateSerializer serializer = new NodeStateSerializer(builder.getNodeState());
        String json = serializer.serialize();
        NodeState nodeState2 = deserialize(json);
        assertTrue(EqualsDiff.equals(builder.getNodeState(), nodeState2));
    }

    @Test
    public void serializeToFile() throws Exception{
        builder.child("a").setProperty("foo", "bar");

        NodeStateSerializer serializer = new NodeStateSerializer(builder.getNodeState());
        serializer.serialize(folder.getRoot());

        File json = new File(folder.getRoot(), serializer.getFileName());
        assertTrue(json.exists());

        String text = Files.toString(json, UTF_8);
        NodeState nodeState2 = deserialize(text);
        assertTrue(EqualsDiff.equals(builder.getNodeState(), nodeState2));

    }

    private NodeState deserialize(String json) {
        JsonDeserializer deserializer = new JsonDeserializer(blobHandler);
        return deserializer.deserialize(json);
    }

}