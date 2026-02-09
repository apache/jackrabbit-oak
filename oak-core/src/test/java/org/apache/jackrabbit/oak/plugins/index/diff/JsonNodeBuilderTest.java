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
package org.apache.jackrabbit.oak.plugins.index.diff;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.json.JsonUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

public class JsonNodeBuilderTest {

    @Test
    public void addNodeTypeAndUUID() throws CommitFailedException, IOException {
        MemoryNodeStore ns = new MemoryNodeStore();
        JsonObject json = JsonObject.fromJson(
                "{\n"
                + "                  \"includedPaths\": \"/same\",\n"
                + "                  \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "                  \"queryPaths\": \"/same\",\n"
                + "                  \"type\": \"lucene\",\n"
                + "                  \"diff.json\": {\n"
                + "                    \"jcr:primaryType\": \"nt:file\",\n"
                + "                    \"jcr:content\": {\n"
                + "                      \"jcr:data\": \":blobId:dGVzdA==\",\n"
                + "                      \"jcr:mimeType\": \"application/json\",\n"
                + "                      \"jcr:primaryType\": \"nt:resource\"\n"
                + "                    }\n"
                + "                  }\n"
                + "                }", true);
        NodeBuilder builder = ns.getRoot().builder();
        JsonNodeUpdater.addOrReplace(builder, ns, "/test", "nt:test", json.toString());
        ns.merge(builder, new EmptyHook(), CommitInfo.EMPTY);
        String json2 = JsonUtils.nodeStateToJson(ns.getRoot(), 5);
        json2 = json2.replaceAll("jcr:uuid\" : \".*\"", "jcr:uuid\" : \"...\"");
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"queryPaths\" : \"/same\",\n"
                + "    \"includedPaths\" : \"/same\",\n"
                + "    \"jcr:primaryType\" : \"nt:unstructured\",\n"
                + "    \"type\" : \"lucene\",\n"
                + "    \":childOrder\" : [ \"diff.json\" ],\n"
                + "    \"diff.json\" : {\n"
                + "      \"jcr:primaryType\" : \"nt:file\",\n"
                + "      \":childOrder\" : [ \"jcr:content\" ],\n"
                + "      \"jcr:content\" : {\n"
                + "        \"jcr:mimeType\" : \"application/json\",\n"
                + "        \"jcr:data\" : \"test\",\n"
                + "        \"jcr:primaryType\" : \"nt:resource\",\n"
                + "        \"jcr:uuid\" : \"...\",\n"
                + "        \":childOrder\" : [ ]\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", json2);

        json = JsonObject.fromJson(
                "{\"number\":1," +
                "\"double2\":1.0," +
                "\"child2\":{\"y\":2}}", true);
        builder = ns.getRoot().builder();
        JsonNodeUpdater.addOrReplace(builder, ns, "/test", "nt:test", json.toString());
        ns.merge(builder, new EmptyHook(), CommitInfo.EMPTY);
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"number\" : 1,\n"
                + "    \"double2\" : 1.0,\n"
                + "    \"jcr:primaryType\" : \"nt:test\",\n"
                + "    \":childOrder\" : [ \"child2\" ],\n"
                + "    \"child2\" : {\n"
                + "      \"y\" : 2,\n"
                + "      \"jcr:primaryType\" : \"nt:test\",\n"
                + "      \":childOrder\" : [ ]\n"
                + "    }\n"
                + "  }\n"
                + "}", JsonUtils.nodeStateToJson(ns.getRoot(), 5));
    }

    @Test
    public void store() throws CommitFailedException, IOException {
        MemoryNodeStore ns = new MemoryNodeStore();
        JsonObject json = JsonObject.fromJson(
                "{\"number\":1," +
                "\"double\":1.0," +
                "\"string\":\"hello\"," +
                "\"array\":[\"a\",\"b\"]," +
                "\"child\":{\"x\":1}," +
                "\"blob\":\":blobId:dGVzdA==\"}", true);
        NodeBuilder builder = ns.getRoot().builder();
        JsonNodeUpdater.addOrReplace(builder, ns, "/test", "nt:test", json.toString());
        ns.merge(builder, new EmptyHook(), CommitInfo.EMPTY);
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"number\" : 1,\n"
                + "    \"blob\" : \"test\",\n"
                + "    \"string\" : \"hello\",\n"
                + "    \"array\" : [ \"a\", \"b\" ],\n"
                + "    \"double\" : 1.0,\n"
                + "    \"jcr:primaryType\" : \"nt:test\",\n"
                + "    \":childOrder\" : [ \"child\" ],\n"
                + "    \"child\" : {\n"
                + "      \"x\" : 1,\n"
                + "      \"jcr:primaryType\" : \"nt:test\",\n"
                + "      \":childOrder\" : [ ]\n"
                + "    }\n"
                + "  }\n"
                + "}", JsonUtils.nodeStateToJson(ns.getRoot(), 5));

        json = JsonObject.fromJson(
                "{\"number\":1," +
                "\"double2\":1.0," +
                "\"child2\":{\"y\":2}}", true);
        builder = ns.getRoot().builder();
        JsonNodeUpdater.addOrReplace(builder, ns, "/test", "nt:test", json.toString());
        ns.merge(builder, new EmptyHook(), CommitInfo.EMPTY);
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"number\" : 1,\n"
                + "    \"double2\" : 1.0,\n"
                + "    \"jcr:primaryType\" : \"nt:test\",\n"
                + "    \":childOrder\" : [ \"child2\" ],\n"
                + "    \"child2\" : {\n"
                + "      \"y\" : 2,\n"
                + "      \"jcr:primaryType\" : \"nt:test\",\n"
                + "      \":childOrder\" : [ ]\n"
                + "    }\n"
                + "  }\n"
                + "}", JsonUtils.nodeStateToJson(ns.getRoot(), 5));
    }

    @Test
    public void oakStringValue() {
        assertEquals("123", JsonNodeUpdater.oakStringValue("123"));
        assertEquals("45.67", JsonNodeUpdater.oakStringValue("45.67"));
        assertEquals("-10", JsonNodeUpdater.oakStringValue("-10"));

        String helloBase64 = Base64.getEncoder().encodeToString("hello".getBytes(StandardCharsets.UTF_8));
        assertEquals("hello", JsonNodeUpdater.oakStringValue("\":blobId:" + helloBase64 + "\""));

        assertEquals("hello", JsonNodeUpdater.oakStringValue("\"str:hello\""));
        assertEquals("acme:Test", JsonNodeUpdater.oakStringValue("\"nam:acme:Test\""));
        assertEquals("2024-01-19", JsonNodeUpdater.oakStringValue("\"dat:2024-01-19\""));
    }

    @Test
    public void getStringSet() {
        assertNull(JsonNodeUpdater.getStringSet(null));
        assertEquals(new TreeSet<>(Arrays.asList("hello")), JsonNodeUpdater.getStringSet("\"hello\""));
        assertEquals(null, JsonNodeUpdater.getStringSet("123"));
        assertEquals(new TreeSet<>(Arrays.asList("content/abc")), JsonNodeUpdater.getStringSet("\"content\\/abc\""));
        assertTrue(JsonNodeUpdater.getStringSet("[]").isEmpty());
        assertEquals(new TreeSet<>(Arrays.asList("a")), JsonNodeUpdater.getStringSet("[\"a\"]"));
        assertEquals(new TreeSet<>(Arrays.asList("content/abc")), JsonNodeUpdater.getStringSet("[\"content\\/abc\"]"));
        assertEquals(new TreeSet<>(Arrays.asList("a")), JsonNodeUpdater.getStringSet("[\"a\",\"a\"]"));
        assertEquals(new TreeSet<>(Arrays.asList("a", "z")), JsonNodeUpdater.getStringSet("[\"z\",\"a\"]"));
    }

    @Test
    public void oakStringArrayValue() throws IOException {
        assertNull(JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{}", true), "p"));
        assertArrayEquals(new String[]{"hello"}, JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{\"p\":\"hello\"}", true), "p"));
        assertNull(JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{\"p\":123}", true), "p"));
        assertArrayEquals(new String[]{"content/abc"}, JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{\"p\":\"content\\/abc\"}", true), "p"));
        assertArrayEquals(new String[]{}, JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{\"p\":[]}", true), "p"));
        assertArrayEquals(new String[]{"a"}, JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{\"p\":[\"a\"]}", true), "p"));
        assertArrayEquals(new String[]{"content/abc"}, JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{\"p\":[\"content\\/abc\"]}", true), "p"));
        assertArrayEquals(new String[]{"a"}, JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{\"p\":[\"a\",\"a\"]}", true), "p"));
        assertArrayEquals(new String[]{"a", "z"}, JsonNodeUpdater.oakStringArrayValue(JsonObject.fromJson("{\"p\":[\"z\",\"a\"]}", true), "p"));
    }

    @Test
    public void addOrReplacePrefixesBooleansAndEscapes() throws CommitFailedException, IOException {
        MemoryNodeStore ns = new MemoryNodeStore();
        JsonObject json = JsonObject.fromJson(
                "{\"strValue\":\"str:hello\"," +
                "\"namValue\":\"nam:acme:Test\"," +
                "\"datValue\":\"dat:2024-01-19\"," +
                "\"boolTrue\":true," +
                "\"boolFalse\":false," +
                "\"escapedArray\":[\"\\/content\\/path\"]}", true);
        NodeBuilder builder = ns.getRoot().builder();
        JsonNodeUpdater.addOrReplace(builder, ns, "/test", "nt:test", json.toString());
        ns.merge(builder, new EmptyHook(), CommitInfo.EMPTY);
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"namValue\" : \"acme:Test\",\n"
                + "    \"boolTrue\" : true,\n"
                + "    \"boolFalse\" : false,\n"
                + "    \"datValue\" : \"2024-01-19\",\n"
                + "    \"escapedArray\" : [ \"/content/path\" ],\n"
                + "    \"jcr:primaryType\" : \"nt:test\",\n"
                + "    \"strValue\" : \"hello\",\n"
                + "    \":childOrder\" : [ ]\n"
                + "  }\n"
                + "}", JsonUtils.nodeStateToJson(ns.getRoot(), 5));
    }

}
