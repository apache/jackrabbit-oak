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
package org.apache.jackrabbit.oak.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.junit.Before;
import org.junit.Test;

public class JsonNodeBuilderTest {

    private MemoryNodeStore ns;

    @Before
    public void before() {
        ns = new MemoryNodeStore();
    }

    @Test
    public void addNodeTypeAndUUID() throws CommitFailedException, IOException {
        JsonObject json = JsonObject.fromJson("{\n"
                + "  \"includedPaths\": \"/same\",\n"
                + "  \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "  \"queryPaths\": \"/same\",\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"diff.json\": {\n"
                + "    \"jcr:primaryType\": \"nt:file\",\n"
                + "    \"jcr:content\": {\n"
                + "      \"jcr:data\": \":blobId:dGVzdA==\",\n"
                + "      \"jcr:mimeType\": \"application/json\",\n"
                + "      \"jcr:primaryType\": \"nt:resource\"\n"
                + "    }\n"
                + "  }\n"
                + "}" , true);
        JsonNodeBuilder.addOrReplace(ns, "/test", "nt:test", json.toString());
        String json2 = JsonUtils.nodeStateToJson(ns.getRoot(), 5);
        json2 = json2.replaceAll("jcr:uuid\" : \".*\"", "jcr:uuid\" : \"...\"");
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"queryPaths\" : \"/same\",\n"
                + "    \"includedPaths\" : \"/same\",\n"
                + "    \"jcr:primaryType\" : \"nt:unstructured\",\n"
                + "    \"type\" : \"lucene\",\n"
                + "    \"diff.json\" : {\n"
                + "      \"jcr:primaryType\" : \"nt:file\",\n"
                + "      \"jcr:content\" : {\n"
                + "        \"jcr:mimeType\" : \"application/json\",\n"
                + "        \"jcr:data\" : \"test\",\n"
                + "        \"jcr:primaryType\" : \"nt:resource\",\n"
                + "        \"jcr:uuid\" : \"...\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", json2);

        json = JsonObject.fromJson(
                "{\"number\":1," +
                        "\"double2\":1.0," +
                        "\"child2\":{\"y\":2}}", true);
        JsonNodeBuilder.addOrReplace(ns, "/test", "nt:test", json.toString());
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"number\" : 1,\n"
                + "    \"double2\" : 1.0,\n"
                + "    \"jcr:primaryType\" : \"nt:test\",\n"
                + "    \"child2\" : {\n"
                + "      \"y\" : 2,\n"
                + "      \"jcr:primaryType\" : \"nt:test\"\n"
                + "    }\n"
                + "  }\n"
                + "}", JsonUtils.nodeStateToJson(ns.getRoot(), 5));
    }

    @Test
    public void storeDifferentDataTypes() throws CommitFailedException, IOException {
        JsonObject json = JsonObject.fromJson(
                "{\"number\":1," +
                        "\"double\":1.0," +
                        "\"string\":\"hello\"," +
                        "\"array\":[\"a\",\"b\"]," +
                        "\"child\":{\"x\":1}," +
                        "\"blob\":\":blobId:dGVzdA==\"}", true);
        JsonNodeBuilder.addOrReplace(ns, "/test", "nt:test", json.toString());
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"number\" : 1,\n"
                + "    \"blob\" : \"test\",\n"
                + "    \"string\" : \"hello\",\n"
                + "    \"array\" : [ \"a\", \"b\" ],\n"
                + "    \"double\" : 1.0,\n"
                + "    \"jcr:primaryType\" : \"nt:test\",\n"
                + "    \"child\" : {\n"
                + "      \"x\" : 1,\n"
                + "      \"jcr:primaryType\" : \"nt:test\"\n"
                + "    }\n"
                + "  }\n"
                + "}", JsonUtils.nodeStateToJson(ns.getRoot(), 5));

        json = JsonObject.fromJson(
                "{\"number\":1," +
                        "\"boolTrue\":true," +
                        "\"boolFalse\":false," +
                        "\"double2\":1.0," +
                        "\"child2\":{\"y\":2}}", true);
        JsonNodeBuilder.addOrReplace(ns, "/test", "nt:test", json.toString());
        assertEquals("{\n"
                + "  \"test\" : {\n"
                + "    \"number\" : 1,\n"
                + "    \"boolTrue\" : true,\n"
                + "    \"boolFalse\" : false,\n"
                + "    \"double2\" : 1.0,\n"
                + "    \"jcr:primaryType\" : \"nt:test\",\n"
                + "    \"child2\" : {\n"
                + "      \"y\" : 2,\n"
                + "      \"jcr:primaryType\" : \"nt:test\"\n"
                + "    }\n"
                + "  }\n"
                + "}", JsonUtils.nodeStateToJson(ns.getRoot(), 5));
    }

    @Test
    public void illegalNodeTypesAreProhibited() {
        String simpleJson = "{\"property\": \"value\"}";
        
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> JsonNodeBuilder.addOrReplace(ns, "/test", "invalid/nodetype", simpleJson)
        );
        assertEquals("Illegal node type: invalid/nodetype", exception.getMessage());
    }

    @Test
    public void removingEntriesIsProhibited() {
        String jsonWithNull = "{\"nullProperty\": null}";
        
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> JsonNodeBuilder.addOrReplace(ns, "/test", "nt:unstructured", jsonWithNull)
        );
        assertEquals("Removing entries is not supported", exception.getMessage());
    }
}
