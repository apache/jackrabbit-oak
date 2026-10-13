/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.diff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

public class DiffIndexLegacyTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Test
    public void featureToggleSelectsLegacyAndNewCollectors() {
        DiffIndex component = new DiffIndex();
        MockOsgi.activate(component, context.bundleContext(), Map.of());
        try {
            FeatureToggle toggle = context.getService(FeatureToggle.class);
            assertNotNull(toggle);
            assertEquals(DiffIndex.LEGACY_DIFF_INDEX_TOGGLE, toggle.getName());
            assertFalse(DiffIndex.isLegacyMode());
            assertProcessedFiles(false);

            toggle.setEnabled(true);
            assertTrue(DiffIndex.isLegacyMode());
            assertProcessedFiles(true);

            toggle.setEnabled(false);
            assertFalse(DiffIndex.isLegacyMode());
            assertProcessedFiles(false);
        } finally {
            MockOsgi.deactivate(component, context.bundleContext());
        }
        assertNull(context.getService(FeatureToggle.class));
        assertFalse(DiffIndex.isLegacyMode());
    }

    @Test
    public void legacyExtractionIgnoresAdditionalJsonFilesAndFileReferences() {
        DiffIndex component = new DiffIndex();
        MockOsgi.activate(component, context.bundleContext(), Map.of());
        try {
            context.getService(FeatureToggle.class).setEnabled(true);
            JsonObject definitions = JsonObject.fromJson("""
                    {
                        "/oak:index/diff.index": {
                            "additional-diff.json": {},
                            "diff": {
                                "acme.test": { "jcr:data": ":file:missing.txt" }
                            }
                        }
                    }
                    """, true);
            HashMap<String, JsonObject> target = new HashMap<>();
            assertNull(new DiffIndexMerger().tryExtractDiffIndex(definitions, "/oak:index/diff.index", target));
            assertEquals("\":file:missing.txt\"", target.get("acme.test").getProperties().get("jcr:data"));
            context.getService(FeatureToggle.class).setEnabled(false);
            assertNotNull(new DiffIndexMerger().tryExtractDiffIndex(
                    definitions, "/oak:index/diff.index", new HashMap<>()));
        } finally {
            MockOsgi.deactivate(component, context.bundleContext());
        }
    }

    @Test
    public void legacyExtractorReadsOnlyDiffJsonAndReportsMalformedFiles() {
        String path = "/oak:index/diff.index";
        JsonObject definitions = new JsonObject(true);
        JsonObject index = new JsonObject(true);
        JsonObject file = new JsonObject(true);
        JsonObject content = new JsonObject(true);
        definitions.getChildren().put(path, index);
        index.getChildren().put("diff.json", file);
        file.getChildren().put("jcr:content", content);
        content.getProperties().put("jcr:data", JsopBuilder.encode("{\"acme.test\":{\"type\":\"lucene\"}}"));
        DiffIndexMerger merger = new DiffIndexMerger();
        HashMap<String, JsonObject> target = new HashMap<>();
        assertNull(merger.tryExtractDiffIndexLegacy(definitions, path, target));
        assertEquals("\"lucene\"", target.get("acme.test").getProperties().get("type"));

        file.getChildren().clear();
        assertEquals("jcr:content child node is missing in diff.json",
                merger.tryExtractDiffIndexLegacy(definitions, path, new HashMap<>()));

        file.getChildren().put("jcr:content", content);
        content.getProperties().put("jcr:data", JsopBuilder.encode("{broken"));
        assertTrue(merger.tryExtractDiffIndexLegacy(definitions, path, new HashMap<>()).startsWith("Illegal Json"));
        assertEquals(2, merger.getAndClearWarnings().size());

        index.getChildren().clear();
        assertNull(merger.tryExtractDiffIndexLegacy(definitions, path, new HashMap<>()));
    }

    @Test
    public void legacyModePreservesCustomerVersionNumbering() {
        DiffIndex component = new DiffIndex();
        MockOsgi.activate(component, context.bundleContext(), Map.of());
        try {
            JsonObject repository = JsonObject.fromJson("""
                    {
                        "/oak:index/product-2": { "type": "lucene" },
                        "/oak:index/product-1-custom-3": {
                            "type": "lucene", "mergeInfo": "previous customization"
                        }
                    }
                    """, true);
            for (boolean legacy : new boolean[] {false, true}) {
                context.getService(FeatureToggle.class).setEnabled(legacy);
                JsonObject definitions = JsonObject.fromJson("""
                        {
                            "/oak:index/diff.index": {
                                "diff": { "product": { "additional": true } }
                            }
                        }
                        """, true);
                new DiffIndexMerger().merge(definitions, repository, null);
                String expected = "/oak:index/product-2-custom-" + (legacy ? "4" : "1");
                assertNotNull(definitions.getChildren().get(expected));
            }
        } finally {
            MockOsgi.deactivate(component, context.bundleContext());
        }
    }

    @Test
    public void legacyCollectorSkipsMissingAndUnchangedData() {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder definitions = store.getRoot().builder().child("oak:index");
        DiffIndexMerger merger = new DiffIndexMerger();
        assertNull(DiffIndex.collectDiffsLegacy(definitions, merger));

        definitions.child("diff.index").child("diff.json");
        assertNull(DiffIndex.collectDiffsLegacy(definitions, merger));
        NodeBuilder content = definitions.child("diff.index").child("diff.json").child("jcr:content");
        assertNull(DiffIndex.collectDiffsLegacy(definitions, merger));

        content.setProperty("jcr:lastModified", "2026-01-01T00:00:00.000Z", Type.DATE);
        assertNull(DiffIndex.collectDiffsLegacy(definitions, merger));
        file(definitions, "diff.json", "{broken");
        content.setProperty("jcr:lastModified", "2026-01-01T00:00:00.001Z", Type.DATE);

        assertNull(DiffIndex.collectDiffsLegacy(definitions, merger));
        assertEquals(1, merger.getAndClearWarnings().size());

        file(definitions, "diff.json", "{\"acme.test\":{\"type\":\"lucene\"}}");
        content.setProperty("jcr:lastModified", "2026-01-01T00:00:00.002Z", Type.DATE);
        assertNotNull(DiffIndex.collectDiffsLegacy(definitions, merger));
        assertNull(DiffIndex.collectDiffsLegacy(definitions, merger));
    }

    @Test
    public void legacyWarningsAreClearedOnUnchangedCommits() {
        DiffIndex component = new DiffIndex();
        MockOsgi.activate(component, context.bundleContext(), Map.of());
        try {
            context.getService(FeatureToggle.class).setEnabled(true);
            MemoryNodeStore store = new MemoryNodeStore();
            NodeBuilder definitions = store.getRoot().builder().child("oak:index");
            file(definitions, "diff.json", "{broken");
            DiffIndex.applyDiffIndexChanges(store, definitions);
            assertTrue(definitions.child("diff.index").hasProperty("warn.01"));

            DiffIndex.applyDiffIndexChanges(store, definitions);
            assertFalse(definitions.child("diff.index").hasProperty("warn.01"));

            definitions.child("diff.index").remove();
            DiffIndex.storeOrRemoveWarnings(definitions, new DiffIndexMerger());
        } finally {
            MockOsgi.deactivate(component, context.bundleContext());
        }
    }

    @Test
    public void deactivateWithoutActivationIsSafe() {
        MockOsgi.deactivate(new DiffIndex(), context.bundleContext());
    }

    private static void assertProcessedFiles(boolean legacy) {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder definitions = definitions(store);
        DiffIndex.applyDiffIndexChanges(store, definitions);
        assertTrue(definitions.hasChildNode("acme.main-1-custom-1"));
        assertEquals(!legacy, definitions.hasChildNode("acme.additional-1-custom-1"));
        NodeBuilder additional = definitions.child("diff.index").child("additional-diff.json").child("jcr:content");
        assertEquals(!legacy, additional.hasProperty(DiffIndexMerger.LAST_PROCESSED));
    }

    private static NodeBuilder definitions(MemoryNodeStore store) {
        NodeBuilder definitions = store.getRoot().builder().child("oak:index");
        file(definitions, "diff.json", "{\"acme.main\":{\"type\":\"lucene\"}}");
        file(definitions, "additional-diff.json", "{\"acme.additional\":{\"type\":\"lucene\"}}");
        return definitions;
    }

    private static void file(NodeBuilder definitions, String name, String data) {
        NodeBuilder content = definitions.child("diff.index").child(name).child("jcr:content");
        content.setProperty("jcr:lastModified", "2026-01-01T00:00:00.000Z", Type.DATE);
        content.setProperty(BinaryPropertyState.binaryProperty("jcr:data", data.getBytes(StandardCharsets.UTF_8)));
    }
}
