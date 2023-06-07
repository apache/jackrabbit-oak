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


import ch.qos.logback.classic.Level;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

public class JsonSerializerTest {
    @Rule
    public TemporarySystemProperty temporarySystemProperty = new TemporarySystemProperty();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    LogCustomizer customLogs;

    @Before
    public void setup() {
        customLogs = LogCustomizer.forLogger(JsonSerializer.class.getName())
                .filter(Level.WARN)
                .create();
        customLogs.starting();
    }

    @After
    public void cleanup() {
        customLogs.finished();
    }


    @Test
    public void childOrder() throws Exception {
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

    @Test
    public void jsonNotFlushedInChunks() throws Exception {
        System.setProperty(JsonSerializer.WRITER_FLUSH_THRESHOLD_KEY, "" + 10);
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
        Assert.assertTrue(customLogs.getLogs().stream().filter(n -> n.contains("Json is not getting flushed in chunks:")).findFirst().isPresent());
        assertEquals(expectedOrder, childNames);
    }

    @Test
    public void jsonFlushedInChunks() throws Exception {
        System.setProperty(JsonSerializer.WRITER_FLUSH_THRESHOLD_KEY, "" + 10);
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a");
        builder.child("b");
        builder.child("c");
        builder.child("d");

        List<String> expectedOrder = Arrays.asList("a", "c", "b", "d");
        builder.setProperty(":childOrder", expectedOrder, Type.NAMES);

        NodeState state = builder.getNodeState();
        String json = serializeJsonFlushedInChunks(state);

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
        Assert.assertFalse(customLogs.getLogs().stream().filter(n -> n.contains("Json is not getting flushed in chunks:")).findFirst().isPresent());
        assertEquals(expectedOrder, childNames);
    }

    private String serialize(NodeState nodeState) {
        JsopBuilder json = new JsopBuilder();
        new JsonSerializer(json, "{\"properties\":[\"*\", \"-:*\"]}", new BlobSerializer()).serialize(nodeState);
        return json.toString();
    }

    private String serializeJsonFlushedInChunks(NodeState nodeState) throws IOException {
        PrintWriter pw;
        OutputStream writerStream;
        File outFile = temporaryFolder.newFile();
        OutputStream os = new BufferedOutputStream(FileUtils.openOutputStream(outFile));
        writerStream = os;
        pw = new PrintWriter(writerStream);

        JsopBuilder json = new JsopBuilder();
        new JsonSerializer(json, "{\"properties\":[\"*\", \"-:*\"]}", new BlobSerializer(), pw).serialize(nodeState);
        pw.print(json);
        pw.flush();
        return Files.readAllLines(Paths.get(outFile.toURI())).get(0);
    }
}