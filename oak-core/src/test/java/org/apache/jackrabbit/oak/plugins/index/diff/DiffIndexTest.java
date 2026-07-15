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
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

/**
 * Tests for DiffIndex functionality.
 */
public class DiffIndexTest {

    @Test
    public void listIndexes() {
        NodeStore store = new MemoryNodeStore(INITIAL_CONTENT);
        JsonObject indexDefs = RootIndexesListService.getRootIndexDefinitions(store, "property");
        // expect at least one index
        assertFalse(indexDefs.getChildren().isEmpty());
    }

    @Test
    public void tryReadStringNull() {
        assertNull(DiffIndex.tryReadString(null));
    }

    @Test
    public void tryReadStringValidContent() {
        String content = "Hello, World!";
        PropertyState prop = BinaryPropertyState.binaryProperty("jcr:data",
                content.getBytes(StandardCharsets.UTF_8));
        assertEquals(content, DiffIndex.tryReadString(prop));
    }

    @Test
    public void tryReadStringEmpty() {
        PropertyState prop = BinaryPropertyState.binaryProperty("jcr:data", new byte[0]);
        assertEquals("", DiffIndex.tryReadString(prop));
    }

    @Test
    public void tryReadStringJsonContent() {
        String content = "{ \"key\": \"value\", \"array\": [1, 2, 3] }";
        PropertyState prop = BinaryPropertyState.binaryProperty("jcr:data",
                content.getBytes(StandardCharsets.UTF_8));
        assertEquals(content, DiffIndex.tryReadString(prop));
    }

    @Test
    public void tryReadStringIOException() throws IOException {
        PropertyState prop = mock(PropertyState.class);
        Blob blob = mock(Blob.class);
        InputStream failingStream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Simulated read failure");
            }
            @Override
            public byte[] readAllBytes() throws IOException {
                throw new IOException("Simulated read failure");
            }
        };
        when(prop.getValue(Type.BINARY)).thenReturn(blob);
        when(blob.getNewStream()).thenReturn(failingStream);

        // Should return null (not throw exception)
        assertNull(DiffIndex.tryReadString(prop));
    }

    @Test
    public void testDiffIndexUpdate() throws Exception {
        // Create a memory node store
        NodeStore store = new MemoryNodeStore(INITIAL_CONTENT);

        storeDiff(store, "2026-01-01T00:00:00.000Z", ""
                + "{ \"acme.testIndex\": {\n"
                + "        \"async\": [ \"async\", \"nrt\" ],\n"
                + "        \"compatVersion\": 2,\n"
                + "        \"evaluatePathRestrictions\": true,\n"
                + "        \"includedPaths\": [ \"/content/dam\" ],\n"
                + "        \"jcr:primaryType\": \"oak:QueryIndexDefinition\",\n"
                + "        \"queryPaths\": [ \"/content/dam\" ],\n"
                + "        \"selectionPolicy\": \"tag\",\n"
                + "        \"tags\": [ \"abc\" ],\n"
                + "        \"type\": \"lucene\",\n"
                + "        \"indexRules\": {\n"
                + "            \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "            \"dam:Asset\": {\n"
                + "                \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "                \"properties\": {\n"
                + "                    \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "                    \"created\": {\n"
                + "                        \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "                        \"name\": \"str:jcr:created\",\n"
                + "                        \"ordered\": true,\n"
                + "                        \"propertyIndex\": true,\n"
                + "                        \"type\": \"Date\"\n"
                + "                    }\n"
                + "                }\n"
                + "            }\n"
                + "        }\n"
                + "    } }");

        JsonObject repositoryDefinitions = RootIndexesListService.getRootIndexDefinitions(store, "lucene");
        assertSameJson("{\n"
                + "  \"/oak:index/acme.testIndex-1-custom-1\": {\n"
                + "    \"compatVersion\": 2,\n"
                + "    \"async\": [\"async\", \"nrt\"],\n"
                + "    \"evaluatePathRestrictions\": true,\n"
                + "    \"mergeChecksum\": \"34e7f7f0eb480ea781317b56134bc85fc59ed97031d95f518fdcff230aec28a2\",\n"
                + "    \"mergeInfo\": \"This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html\",\n"
                + "    \"selectionPolicy\": \"tag\",\n"
                + "    \"queryPaths\": [\"/content/dam\"],\n"
                + "    \"includedPaths\": [\"/content/dam\"],\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"tags\": [\"abc\"],\n"
                + "    \"merges\": [\"/oak:index/acme.testIndex\"],\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"dam:Asset\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"created\": {\n"
                + "            \"ordered\": true,\n"
                + "            \"name\": \"str:jcr:created\",\n"
                + "            \"propertyIndex\": true,\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "            \"type\": \"Date\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", repositoryDefinitions.toString());

        storeDiff(store, "2026-01-01T00:00:00.001Z", ""
                + "{ \"acme.testIndex\": {\n"
                + "        \"async\": [ \"async\", \"nrt\" ],\n"
                + "        \"compatVersion\": 2,\n"
                + "        \"evaluatePathRestrictions\": true,\n"
                + "        \"includedPaths\": [ \"/content/dam\" ],\n"
                + "        \"jcr:primaryType\": \"oak:QueryIndexDefinition\",\n"
                + "        \"queryPaths\": [ \"/content/dam\" ],\n"
                + "        \"selectionPolicy\": \"tag\",\n"
                + "        \"tags\": [ \"abc\" ],\n"
                + "        \"type\": \"lucene\",\n"
                + "        \"indexRules\": {\n"
                + "            \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "            \"dam:Asset\": {\n"
                + "                \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "                \"properties\": {\n"
                + "                    \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "                    \"created\": {\n"
                + "                        \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "                        \"name\": \"str:jcr:created\",\n"
                + "                        \"propertyIndex\": true\n"
                + "                    },\n"
                + "                    \"modified\": {\n"
                + "                        \"jcr:primaryType\": \"nt:unstructured\",\n"
                + "                        \"name\": \"str:jcr:modified\",\n"
                + "                        \"propertyIndex\": true\n"
                + "                    }\n"
                + "                }\n"
                + "            }\n"
                + "        }\n"
                + "    } }");

        repositoryDefinitions = RootIndexesListService.getRootIndexDefinitions(store, "lucene");
        assertSameJson("{\n"
                + "  \"/oak:index/acme.testIndex-1-custom-2\": {\n"
                + "    \"compatVersion\": 2,\n"
                + "    \"async\": [\"async\", \"nrt\"],\n"
                + "    \"mergeChecksum\": \"41df9c87e4d4fca446aed3f55e6d188304a2cb49bae442b75403dc23a89b266f\",\n"
                + "    \"mergeInfo\": \"This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html\",\n"
                + "    \"selectionPolicy\": \"tag\",\n"
                + "    \"queryPaths\": [\"/content/dam\"],\n"
                + "    \"includedPaths\": [\"/content/dam\"],\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"evaluatePathRestrictions\": true,\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"tags\": [\"abc\"],\n"
                + "    \"merges\": [\"/oak:index/acme.testIndex\"],\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"dam:Asset\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"created\": {\n"
                + "            \"name\": \"str:jcr:created\",\n"
                + "            \"propertyIndex\": true,\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\"\n"
                + "          },\n"
                + "          \"modified\": {\n"
                + "            \"name\": \"str:jcr:modified\",\n"
                + "            \"propertyIndex\": true,\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", repositoryDefinitions.toString());

        storeDiff(store, "2026-01-01T00:00:00.002Z", ""
                + "{}");

        repositoryDefinitions = RootIndexesListService.getRootIndexDefinitions(store, "lucene");
        assertSameJson("{}", repositoryDefinitions.toString());
    }

    @Test
    public void testDiffIndexUpdateUnversionedOotbExplicit() throws Exception {
        NodeStore store = new MemoryNodeStore(INITIAL_CONTENT);

        List<IndexEditorProvider> indexEditors = List.of(
                new ReferenceEditorProvider(), new PropertyIndexEditorProvider(), new NodeCounterEditorProvider());
        IndexEditorProvider provider = CompositeIndexEditorProvider.compose(indexEditors);
        EditorHook hook = new EditorHook(new IndexUpdateProvider(provider));

        // seed the store with an unversioned OOTB index /oak:index/test (type lucene)
        NodeBuilder builder = store.getRoot().builder();
        builder.child(INDEX_DEFINITIONS_NAME).child("test")
                .setProperty("jcr:primaryType", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME)
                .setProperty(TYPE_PROPERTY_NAME, "lucene");
        store.merge(builder, hook, CommitInfo.EMPTY);

        // diff adds indexRules to the product index
        storeDiff(store, "2026-01-01T00:00:00.000Z",
                "{ \"test\": {"
                + "  \"indexRules\": {"
                + "    \"nt:base\": {"
                + "      \"properties\": {"
                + "        \"prop1\": { \"name\": \"prop1\", \"propertyIndex\": true }"
                + "      }"
                + "    }"
                + "  }"
                + "} }");

        // expect test-custom-1 with:
        // - type "lucene" from the product index
        // - indexRules/nt:base/properties/prop1 from the diff
        assertSameJson("{\n"
                + "  \"/oak:index/test-custom-1\": {\n"
                + "    \"mergeChecksum\": \"2e4b9003683d6c179c72f94393a91b2bedc3fa67d1a60f6f7876002dbb40c070\",\n"
                + "    \"mergeInfo\": \"This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html\",\n"
                + "    \"reindex\": true,\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\",\n"
                + "    \"merges\": [\"/oak:index/test\"],\n"
                + "    \"indexRules\": {\n"
                + "      \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "      \"nt:base\": {\n"
                + "        \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "        \"properties\": {\n"
                + "          \"jcr:primaryType\": \"nam:nt:unstructured\",\n"
                + "          \"prop1\": {\n"
                + "            \"name\": \"prop1\",\n"
                + "            \"propertyIndex\": true,\n"
                + "            \"jcr:primaryType\": \"nam:nt:unstructured\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"/oak:index/test\": {\n"
                + "    \"reindex\": true,\n"
                + "    \"jcr:primaryType\": \"nam:oak:QueryIndexDefinition\",\n"
                + "    \"type\": \"lucene\"\n"
                + "  }\n"
                + "}",
                RootIndexesListService.getRootIndexDefinitions(store, "lucene").toString());
    }

    @Test
    public void testDiffIndexUpdateUnversionedOotb() throws Exception {
        NodeStore store = new MemoryNodeStore(INITIAL_CONTENT);

        // Seed the store with an unversioned OOTB index "/oak:index/test"
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder indexDefs = builder.child(INDEX_DEFINITIONS_NAME);
        indexDefs.child("test")
                .setProperty("jcr:primaryType", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME)
                .setProperty(TYPE_PROPERTY_NAME, "lucene");
        store.merge(builder, new EditorHook(new IndexUpdateProvider(
                CompositeIndexEditorProvider.compose(List.of(
                        new ReferenceEditorProvider(),
                        new PropertyIndexEditorProvider(),
                        new NodeCounterEditorProvider())))),
                CommitInfo.EMPTY);

        // Apply a diff that adds indexRules to the unversioned index
        storeDiff(store, "2026-01-01T00:00:00.000Z",
                "{ \"test\": {"
                + "  \"indexRules\": {"
                + "    \"nt:base\": {"
                + "      \"properties\": {"
                + "        \"prop1\": { \"name\": \"prop1\", \"propertyIndex\": true }"
                + "      }"
                + "    }"
                + "  }"
                + "} }");

        JsonObject repositoryDefinitions = RootIndexesListService.getRootIndexDefinitions(store, "lucene");
        // expect test-custom-1, not test-0-custom-1
        JsonObject merged = repositoryDefinitions.getChildren().get("/oak:index/test-custom-1");
        assertNotNull("Expected test-custom-1 to be created, got: " + repositoryDefinitions, merged);
        // property from the product index
        assertEquals("\"lucene\"", merged.getProperties().get("type"));
        // indexRules child and prop1 entry from the diff
        JsonObject prop1 = merged.getChildren().get("indexRules")
                .getChildren().get("nt:base")
                .getChildren().get("properties")
                .getChildren().get("prop1");
        assertNotNull("prop1 from diff should be present in the merged index", prop1);
    }

    private void assertSameJson(String a, String b) {
        JsonObject ja = JsonObject.fromJson(a, true);
        JsonObject jb = JsonObject.fromJson(b, true);
        if (!new DiffIndexMerger().isSameIgnorePropertyOrder(ja, jb)) {
            assertEquals(a, b);
        }
    }

    private void storeDiff(NodeStore store, String timestamp, String json) throws CommitFailedException {
        // Get the root builder
        NodeBuilder builder = store.getRoot().builder();

        List<IndexEditorProvider> indexEditors = List.of(
                new ReferenceEditorProvider(), new PropertyIndexEditorProvider(), new NodeCounterEditorProvider());
        IndexEditorProvider provider = CompositeIndexEditorProvider.compose(indexEditors);
        EditorHook hook = new EditorHook(new IndexUpdateProvider(provider));

        // Create the index definition at /oak:index/diff.index
        NodeBuilder indexDefs = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder diffIndex = indexDefs.child("diff.index");

        // Set index properties
        diffIndex.setProperty("jcr:primaryType", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        diffIndex.setProperty(TYPE_PROPERTY_NAME, "disabled");

        // Create the diff.json child node with primary type nt:file
        NodeBuilder diffJson = diffIndex.child("diff.json");
        diffJson.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_FILE, Type.NAME);

        // Create jcr:content child node (required for nt:file) with empty text
        NodeBuilder content = diffJson.child(JcrConstants.JCR_CONTENT);
        content.setProperty(JcrConstants.JCR_LASTMODIFIED, timestamp);
        content.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_RESOURCE, Type.NAME);

        content.setProperty("jcr:data", json);

        // Merge changes to the store
        store.merge(builder, hook, CommitInfo.EMPTY);

        // Run async indexing explicitly
        for (int i = 0; i < 5; i++) {
            try (AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider)) {
                async.run();
            }
        }
    }

    // verify @lucene and @elasticsearch are cleared
    @Test
    public void cleanedAndNormalizedRemoveAtLucene() {
        assertEquals("{\n"
                + "  \"test\": 4,\n"
                + "  \"test@abc\": 3\n"
                + "}",
                DiffIndexMerger.cleanedAndNormalized(JsonObject.fromJson(
                        "{\"test@lucene\":1, \"test@elasticsearch\": 2, \"test@abc\": 3, \"test\": 4}", true)).toString());
    }

    // verify @lucene and @elasticsearch are cleared
    @Test
    public void cleanedAndNormalizedRemovePrefixes() {
        assertEquals("{\n"
                + "  \"test\": \"hello\",\n"
                + "  \"test1\": \"world\",\n"
                + "  \"test2\": \"123\"\n"
                + "}",
                DiffIndexMerger.cleanedAndNormalized(JsonObject.fromJson(
                        "{\"test\":\"str:hello\", \"test1\": \"nam:world\", \"test2\": \"dat:123\"}", true)).toString());
    }

    @Test
    public void disableOrRemoveOldVersions() {
        NodeStore store = new MemoryNodeStore(INITIAL_CONTENT);
        NodeBuilder definitions = store.getRoot().builder().child(INDEX_DEFINITIONS_NAME);

        definitions.child("myLuceneIndex").setProperty(TYPE_PROPERTY_NAME, "lucene");
        definitions.child("myNodetypeIndex").setProperty(TYPE_PROPERTY_NAME, "property");
        DiffIndex.disableOrRemoveOldVersions(definitions, "lucene", "lucene");
        assertTrue(definitions.hasChildNode("myLuceneIndex"));
        assertTrue(definitions.hasChildNode("myNodetypeIndex"));

        definitions.child("product-1-custom-1").setProperty(TYPE_PROPERTY_NAME, "lucene");
        definitions.child("product-1-custom-2").setProperty(TYPE_PROPERTY_NAME, "lucene");
        definitions.child("product-1-custom-3").setProperty(TYPE_PROPERTY_NAME, "lucene");
        DiffIndex.disableOrRemoveOldVersions(definitions, "/oak:index/product-1-custom-3", "product-1-custom-3");
        assertFalse(definitions.hasChildNode("product-1-custom-1"));
        assertFalse(definitions.hasChildNode("product-1-custom-2"));
        assertTrue(definitions.hasChildNode("product-1-custom-3"));

        definitions.child("other-1-custom-1").setProperty(TYPE_PROPERTY_NAME, "lucene");
        definitions.child("product-1-custom-4").setProperty(TYPE_PROPERTY_NAME, "lucene");
        DiffIndex.disableOrRemoveOldVersions(definitions, "product-1-custom-4", "product-1-custom-4");
        assertTrue(definitions.hasChildNode("other-1-custom-1"));
        assertTrue(definitions.hasChildNode("product-1-custom-4"));

        definitions.child("foo-1-custom-1").setProperty(TYPE_PROPERTY_NAME, "lucene");
        definitions.child("foo-1-custom-2").setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED);
        DiffIndex.disableOrRemoveOldVersions(definitions, "/oak:index/foo-1-custom-2", "foo-1-custom-2");
        assertFalse(definitions.hasChildNode("foo-1-custom-1"));
        assertTrue(definitions.hasChildNode("foo-1-custom-2"));

        definitions.child("abc-1-custom-1").setProperty(TYPE_PROPERTY_NAME, "lucene");
        definitions.child("abc-1-custom-2").setProperty(TYPE_PROPERTY_NAME, "lucene");
        DiffIndex.disableOrRemoveOldVersions(definitions, "/oak:index/abc-1-custom-1", "abc-1-custom-2");
        assertTrue(definitions.hasChildNode("abc-1-custom-1"));
        assertTrue(definitions.hasChildNode("abc-1-custom-2"));

        definitions.child("abc-1-custom-1").setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED);
        definitions.child("abc-1-custom-2").setProperty(TYPE_PROPERTY_NAME, "lucene");
        DiffIndex.disableOrRemoveOldVersions(definitions, "/oak:index/abc-1-custom-1", "abc-1-custom-2");
        assertFalse(definitions.hasChildNode("abc-1-custom-1"));
        assertTrue(definitions.hasChildNode("abc-1-custom-2"));

    }
}

