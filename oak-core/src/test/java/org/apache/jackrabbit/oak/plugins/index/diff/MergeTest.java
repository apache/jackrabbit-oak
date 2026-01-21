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

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class MergeTest {

    // test that we can extract the file from the diff.json node (just that)
    @Test
    public void extractFile() {
            JsonObject indexDiff = JsonObject.fromJson("{\n"
                    + "                    \"damAssetLucene\": {\n"
                    + "                        \"indexRules\": {\n"
                    + "                            \"dam:Asset\": {\n"
                    + "                                \"properties\": {\n"
                    + "                                    \"y\": {\n"
                    + "                                        \"name\": \"y\",\n"
                    + "                                        \"propertyIndex\": true\n"
                    + "                                    }\n"
                    + "                                }\n"
                    + "                            }\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }", true);
            String indexDiffString = indexDiff.toString();
            String base64Prop =
                    "\":blobId:" + Base64.getEncoder().encodeToString(indexDiffString.getBytes(StandardCharsets.UTF_8)) + "\"";
            JsonObject repositoryDefinitions = JsonObject.fromJson("{\n"
                    + "                    \"/oak:index/damAssetLucene-12\": {\n"
                    + "                        \"jcr:primaryType\": \"oak:IndexDefinition\",\n"
                    + "                        \"type\": \"lucene\",\n"
                    + "                        \"async\": [\"async\", \"nrt\"],\n"
                    + "                        \"tags\": [\"abc\"],\n"
                    + "                        \"includedPaths\": \"/content/dam\",\n"
                    + "                        \"indexRules\": {\n"
                    + "                            \"dam:Asset\": {\n"
                    + "                                \"properties\": {\n"
                    + "                                    \"x\": {\n"
                    + "                                        \"name\": \"x\",\n"
                    + "                                        \"propertyIndex\": true\n"
                    + "                                    }\n"
                    + "                                }\n"
                    + "                            }\n"
                    + "                        }\n"
                    + "                    },\n"
                    + "                    \"/oak:index/diff.index\": {\n"
                    + "                        \"jcr:primaryType\": \"nt:unstructured\",\n"
                    + "                        \"type\": \"lucene\", \"includedPaths\": \"/same\", \"queryPaths\": \"/same\",\n"
                    + "                        \"diff.json\": {\n"
                    + "                            \"jcr:primaryType\": \"nam:nt:file\",\n"
                    + "                            \"jcr:content\": {\n"
                    + "                                \"jcr:primaryType\": \"nam:nt:resource\",\n"
                    + "                                \"jcr:mimeType\": \"application/json\",\n"
                    + "                                \"jcr:data\":\n"
                    + "                " + base64Prop + "\n"
                    + "                            }\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }", true);

            HashMap<String, JsonObject> target = new HashMap<>();
            DiffIndexMerger.tryExtractDiffIndex(repositoryDefinitions, "/oak:index/diff.index", target);
            assertEquals("{damAssetLucene={\n"
                    + "  \"indexRules\": {\n"
                    + "    \"dam:Asset\": {\n"
                    + "      \"properties\": {\n"
                    + "        \"y\": {\n"
                    + "          \"name\": \"y\",\n"
                    + "          \"propertyIndex\": true\n"
                    + "        }\n"
                    + "      }\n"
                    + "    }\n"
                    + "  }\n"
                    + "}}", target.toString());
    }

    @Test
    public void renamedProperty() {
        // A property might be indexed twice, by adding two children to the "properties" node
        // that both have the same "name" value.
        // Alternatively, they could have the same "function" value.
        String merged = DiffIndexMerger.instance().processMerge(JsonObject.fromJson("{\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"acme:Test\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"abc\": {\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "            \"name\": \"test\",\n"
                + "            \"boost\": 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "", true), JsonObject.fromJson("{\n"
                        + "    \"indexRules\": {\n"
                        + "      \"acme:Test\": {\n"
                        + "        \"properties\": {\n"
                        + "          \"def\": {\n"
                        + "            \"name\": \"test\",\n"
                        + "            \"boost\": 1.2\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }", true)).toString();
        assertEquals("{\n"
                + "  \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"indexRules\": {\n"
                + "    \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "    \"acme:Test\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"properties\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"abc\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"name\": \"test\",\n"
                + "          \"boost\": 1.2\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", merged);
    }

    @Test
    public void renamedFunction() {
        // A function might be indexed twice, by adding two children to the "properties" node
        // that both have the same "function" value.
        String merged = DiffIndexMerger.instance().processMerge(JsonObject.fromJson("{\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"acme:Test\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"abc\": {\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "            \"function\": \"upper(test)\",\n"
                + "            \"boost\": 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "", true), JsonObject.fromJson("{\n"
                        + "    \"indexRules\": {\n"
                        + "      \"acme:Test\": {\n"
                        + "        \"properties\": {\n"
                        + "          \"def\": {\n"
                        + "            \"function\": \"upper(test)\",\n"
                        + "            \"boost\": 1.2\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }", true)).toString();
        assertEquals("{\n"
                + "  \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"indexRules\": {\n"
                + "    \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "    \"acme:Test\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"properties\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"abc\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"function\": \"upper(test)\",\n"
                + "          \"boost\": 1.2\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", merged);
    }

    @Test
    public void createDummy() {
        // when enabling "deleteCreatesDummyIndex", then a dummy index is created
        // (that indexes /dummy, which doesn't exist)
        String merged = new DiffIndexMerger(new String[0], true, true, false).processMerge(JsonObject.fromJson("{}"
                + "", true), JsonObject.fromJson("{}", true)).toString();
        assertEquals("{\n"
                + "  \"async\": \"async\",\n"
                + "  \"includedPaths\": \"/dummy\",\n"
                + "  \"queryPaths\": \"/dummy\",\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "  \"indexRules\": {\n"
                + "    \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "    \"properties\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"dummy\": {\n"
                + "        \"name\": \"dummy\",\n"
                + "        \"propertyIndex\": true,\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", merged);
    }

    @Test
    public void boost() {
        // - "analyzed" must not be overwritten
        // - "ordered" is added
        // - "boost" is overwritten
        String merged = DiffIndexMerger.instance().processMerge(JsonObject.fromJson("{\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"acme:Test\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"abc\": {\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "            \"analyzed\": true,\n"
                + "            \"boost\": 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "", true), JsonObject.fromJson("{\n"
                        + "    \"indexRules\": {\n"
                        + "      \"acme:Test\": {\n"
                        + "        \"properties\": {\n"
                        + "          \"abc\": {\n"
                        + "            \"analyzed\": false,\n"
                        + "            \"ordered\": true,\n"
                        + "            \"boost\": 1.2\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }", true)).toString();
        assertEquals("{\n"
                + "  \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"indexRules\": {\n"
                + "    \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "    \"acme:Test\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"properties\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"abc\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"analyzed\": true,\n"
                + "          \"boost\": 1.2,\n"
                + "          \"ordered\": true\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", merged);
    }

    @Test
    public void mergeDiffsTest() {
        JsonObject a = JsonObject.fromJson("{\n"
                + "    \"indexRules\": {\n"
                + "      \"acme:Test\": {\n"
                + "        \"properties\": {\n"
                + "          \"prop1\": {\n"
                + "            \"name\": \"field1\",\n"
                + "            \"propertyIndex\": true\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    \"type\": \"lucene\"\n"
                + "  }", true);
        JsonObject b = JsonObject.fromJson("{\n"
                + "    \"indexRules\": {\n"
                + "      \"acme:Test\": {\n"
                + "        \"properties\": {\n"
                + "          \"prop2\": {\n"
                + "            \"name\": \"field2\",\n"
                + "            \"ordered\": true\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    \"async\": [\"async\", \"nrt\"]\n"
                + "  }", true);
        String merged = DiffIndexMerger.mergeDiffs(a, b).toString();
        assertEquals("{\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"async\": [\"async\", \"nrt\"],\n"
                + "  \"indexRules\": {\n"
                + "    \"acme:Test\": {\n"
                + "      \"properties\": {\n"
                + "        \"prop1\": {\n"
                + "          \"name\": \"field1\",\n"
                + "          \"propertyIndex\": true\n"
                + "        },\n"
                + "        \"prop2\": {\n"
                + "          \"name\": \"field2\",\n"
                + "          \"ordered\": true\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", merged);
    }

    @Test
    public void switchToLuceneChildrenTest() {
        JsonObject indexDef = JsonObject.fromJson("{\n"
                + "    \"type\": \"elasticsearch\",\n"
                + "    \"type@lucene\": \"lucene\",\n"
                + "    \"async@lucene\": \"[\\\"async\\\", \\\"nrt\\\"]\",\n"
                + "    \"async\": \"[\\\"async\\\"]\",\n"
                + "    \"codec@lucene\": \"Lucene46\",\n"
                + "    \"indexRules\": {\n"
                + "      \"dam:Asset\": {\n"
                + "        \"properties\": {\n"
                + "          \"test\": {\n"
                + "            \"name\": \"jcr:content/metadata/test\",\n"
                + "            \"boost@lucene\": \"2.0\",\n"
                + "            \"boost\": \"1.0\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }", true);
        DiffIndexMerger.switchToLuceneChildren(indexDef);
        String result = indexDef.toString();
        assertEquals("{\n"
                + "  \"type\": \"lucene\",\n"
                + "  \"async\": \"[\\\"async\\\", \\\"nrt\\\"]\",\n"
                + "  \"codec\": \"Lucene46\",\n"
                + "  \"indexRules\": {\n"
                + "    \"dam:Asset\": {\n"
                + "      \"properties\": {\n"
                + "        \"test\": {\n"
                + "          \"name\": \"jcr:content/metadata/test\",\n"
                + "          \"boost\": \"2.0\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", result);
    }

    @Test
    public void includesUnsupportedPathsTest() {
        DiffIndexMerger merger = new DiffIndexMerger(new String[]{"/apps", "/libs"}, false, false, false);

        assertEquals(true, merger.includesUnsupportedPaths(null));
        assertEquals(true, merger.includesUnsupportedPaths(new String[]{"/"}));
        assertEquals(true, merger.includesUnsupportedPaths(new String[]{"/apps"}));
        assertEquals(true, merger.includesUnsupportedPaths(new String[]{"/apps/acme"}));
        assertEquals(true, merger.includesUnsupportedPaths(new String[]{"/apps/acme/test"}));
        assertEquals(true, merger.includesUnsupportedPaths(new String[]{"/libs"}));
        assertEquals(true, merger.includesUnsupportedPaths(new String[]{"/libs/foundation"}));
        assertEquals(true, merger.includesUnsupportedPaths(new String[]{"/content", "/apps"}));
        assertEquals(true, merger.includesUnsupportedPaths(new String[]{"/content", "/libs/test"}));

        assertEquals(false, merger.includesUnsupportedPaths(new String[]{"/content"}));
        assertEquals(false, merger.includesUnsupportedPaths(new String[]{"/content/dam"}));
        assertEquals(false, merger.includesUnsupportedPaths(new String[]{"/var"}));
        assertEquals(false, merger.includesUnsupportedPaths(new String[]{"/etc"}));
        assertEquals(false, merger.includesUnsupportedPaths(new String[]{"/content", "/var", "/etc"}));
    }

    @Test
    public void readDiffIndexTest() throws CommitFailedException {
        NodeStore store = new MemoryNodeStore(INITIAL_CONTENT);
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder oakIndex = root.child("oak:index");
        NodeBuilder diffIndex = oakIndex.child("diff.index.optimizer");
        diffIndex.setProperty("jcr:primaryType", "nt:unstructured");
        diffIndex.setProperty("type", "lucene");
        diffIndex.setProperty("async", "async");
        diffIndex.setProperty("includedPaths", "/content");
        NodeBuilder indexRules = diffIndex.child("indexRules");
        NodeBuilder damAsset = indexRules.child("dam:Asset");
        NodeBuilder properties = damAsset.child("properties");
        NodeBuilder testProp = properties.child("test");
        testProp.setProperty("name", "jcr:content/metadata/test");
        testProp.setProperty("propertyIndex", true);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Map<String, JsonObject> result = DiffIndexMerger.instance().readDiffIndex(store, "diff.index.optimizer");

        assertEquals(1, result.size());
        assertTrue(result.containsKey("/oak:index/diff.index.optimizer"));
        JsonObject indexDef = result.get("/oak:index/diff.index.optimizer");
        assertEquals("\"lucene\"", indexDef.getProperties().get("type"));
        assertEquals("\"async\"", indexDef.getProperties().get("async"));
        assertEquals("\"/content\"", indexDef.getProperties().get("includedPaths"));
        assertTrue(indexDef.getChildren().containsKey("indexRules"));
    }
}
