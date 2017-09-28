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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.util.Collections;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonDeserializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertTrue;

public class JsonDeserializationTest {

    @Test
    public void deserialize() throws Exception{
        String json = "{\n" +
                "    \"evaluatePathRestrictions\": true,\n" +
                "    \"compatVersion\": 2,\n" +
                "    \"type\": \"lucene\",\n" +
                "    \"includedPaths\": [\"/content\"],\n" +
                "    \"excludedPaths\": [\"/jcr:system\"],\n" +
                "    \"async\": \"async\",\n" +
                "    \"jcr:primaryType\": \"oak:QueryIndexDefinition\",\n" +
                "    \"indexRules\": {\n" +
                "        \"jcr:primaryType\": \"nt:unstructured\",\n" +
                "        \"dam:Asset\": {\n" +
                "            \"jcr:primaryType\": \"nt:unstructured\",\n" +
                "            \"properties\": {\n" +
                "                \"jcr:primaryType\": \"nt:unstructured\",\n" +
                "                \"valid\": {\n" +
                "                    \"name\": \"valid\",\n" +
                "                    \"propertyIndex\": true,\n" +
                "                    \"jcr:primaryType\": \"nt:unstructured\",\n" +
                "                    \"notNullCheckEnabled\": true\n" +
                "                },\n" +
                "                \"mimetype\": {\n" +
                "                    \"name\": \"mimetype\",\n" +
                "                    \"analyzed\": true,\n" +
                "                    \"jcr:primaryType\": \"nt:unstructured\"\n" +
                "                },\n" +
                "                \"lastModified\": {\n" +
                "                    \"ordered\": true,\n" +
                "                    \"name\": \"jcr:content/metadata/jcr:lastModified\",\n" +
                "                    \"jcr:primaryType\": \"nt:unstructured\"\n" +
                "                },\n" +
                "                \"status\": {\n" +
                "                    \"name\": \"jcr:content/metadata/status\",\n" +
                "                    \"propertyIndex\": true,\n" +
                "                    \"jcr:primaryType\": \"nt:unstructured\"\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        NodeBuilder builder = EMPTY_NODE.builder();
        Tree tree = TreeFactory.createTree(builder);
        tree.setProperty("evaluatePathRestrictions", true);
        tree.setProperty("compatVersion", 2);
        tree.setProperty("type", "lucene");
        tree.setProperty("includedPaths", Collections.singletonList("/content"), Type.STRINGS);
        tree.setProperty("excludedPaths", Collections.singletonList("/jcr:system"), Type.STRINGS);
        tree.setProperty("async", "async");
        tree.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);

        Tree indexRules = addUnstructuredChild(tree, "indexRules");
        Tree asset = addUnstructuredChild(indexRules, "dam:Asset");
        Tree properties = addUnstructuredChild(asset, "properties");

        Tree valid = addUnstructuredChild(properties, "valid");
        valid.setProperty("name", "valid");
        valid.setProperty("propertyIndex", true);
        valid.setProperty("notNullCheckEnabled", true);

        Tree mimetype = addUnstructuredChild(properties, "mimetype");
        mimetype.setProperty("name", "mimetype");
        mimetype.setProperty("analyzed", true);

        Tree lastModified = addUnstructuredChild(properties, "lastModified");
        lastModified.setProperty("name", "jcr:content/metadata/jcr:lastModified");
        lastModified.setProperty("ordered", true);

        Tree status = addUnstructuredChild(properties, "status");
        status.setProperty("name", "jcr:content/metadata/status");
        status.setProperty("propertyIndex", true);

        NodeState idx = builder.getNodeState();
        NodeState idx2 = deserialize(json);

        //System.out.println(JsopDiff.diffToJsop(idx, idx2));

        assertTrue(EqualsDiff.equals(idx, idx2));

        String json2 = serialize(idx2);
        NodeState idx3 = deserialize(json2);
        assertTrue(EqualsDiff.equals(idx3, idx2));
    }

    private NodeState deserialize(String json) {
        JsonDeserializer deserializer = new JsonDeserializer(new Base64BlobSerializer());
        return deserializer.deserialize(json);
    }

    private String serialize(NodeState nodeState){
        JsopBuilder json = new JsopBuilder();
        new JsonSerializer(json, "{\"properties\":[\"*\", \"-:*\"]}", new BlobSerializer()).serialize(nodeState);
        return json.toString();
    }

    private Tree addUnstructuredChild(Tree tree, String name){
        Tree child = tree.addChild(name);
        child.setOrderableChildren(true);
        child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        return child;
    }
}
