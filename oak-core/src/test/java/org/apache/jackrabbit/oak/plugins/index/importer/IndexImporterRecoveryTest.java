/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.importer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class IndexImporterRecoveryTest {
    private static final String PATH = "/oak:index/newIndex";
    private final NodeStore store = new MemoryNodeStore();
    private final List<String> cleaned = new ArrayList<>();
    private IOException backendFailure;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private void metadata(Map<String, String> indexes) throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.child("oak:index");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        new IndexerInfo(tmp.getRoot(), store.checkpoint(60_000)).save();
        List<String> definitions = new ArrayList<>();
        for (var entry : indexes.entrySet()) {
            File dir = new File(tmp.getRoot(), entry.getKey());
            assertTrue(dir.mkdir());
            Files.writeString(new File(dir, IndexerInfo.INDEX_METADATA_FILE_NAME).toPath(),
                    "indexPath=/oak:index/" + entry.getKey());
            definitions.add("\"/oak:index/" + entry.getKey()
                    + "\":{\"type\":\"property\",\"async\":[\"" + entry.getValue()
                    + "\"],\"propertyNames\":[\"foo\"],\"reindex\":false}");
        }
        Files.writeString(new File(tmp.getRoot(), IndexDefinitionUpdater.INDEX_DEFINITIONS_JSON).toPath(),
                "{" + String.join(",", definitions) + "}");
    }

    private IndexImporterProvider backend() {
        return new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder definition, File dir) {
            }

            @Override
            public void cleanupFailedImport(NodeStore repository, String path, NodeState definition)
                    throws IOException {
                assertFalse(NodeStateUtils.getNode(repository.getRoot(), path).exists());
                assertEquals("property", definition.getString("type"));
                cleaned.add(path);
                if (backendFailure != null) {
                    throw backendFailure;
                }
            }

            @Override
            public String getType() {
                return "property";
            }
        };
    }

    private IndexImporter importer() throws Exception {
        IndexImporter importer = new IndexImporter(store, tmp.getRoot(),
                new PropertyIndexEditorProvider(), AsyncIndexerLock.NOOP_LOCK);
        importer.addImporterProvider(backend());
        return importer;
    }

    private void lane(String name, String checkpoint) throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.child(":async").setProperty(name, checkpoint);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void missingLaneCheckpointRemovesNewDefinitionBeforeBackendCleanup() throws Exception {
        metadata(Map.of("newIndex", "async"));
        String missing = store.checkpoint(60_000);
        assertTrue(store.release(missing));
        lane("async", missing);
        CommitFailedException failure = assertThrows(CommitFailedException.class, importer()::importIndex);
        assertTrue(failure.getMessage().contains("No state found for checkpoint"));
        assertFalse(NodeStateUtils.getNode(store.getRoot(), PATH).exists());
        assertEquals(List.of(PATH), cleaned);
    }

    @Test
    public void missingLanePropertyIsReportedAndRecovered() throws Exception {
        metadata(Map.of("newIndex", "async"));
        CommitFailedException failure = assertThrows(CommitFailedException.class, importer()::importIndex);
        assertTrue(failure.getMessage().contains("No current checkpoint"));
        assertEquals(List.of(PATH), cleaned);
    }

    @Test
    public void backendFailureIsSuppressedOnOriginalFailure() throws Exception {
        metadata(Map.of("newIndex", "async"));
        backendFailure = new IOException("Backend unavailable");
        CommitFailedException failure = assertThrows(CommitFailedException.class, importer()::importIndex);
        assertEquals(1, failure.getSuppressed().length);
        assertSame(backendFailure, failure.getSuppressed()[0]);
        assertFalse(NodeStateUtils.getNode(store.getRoot(), PATH).exists());
    }

    @Test
    public void runtimeFailureAfterInstallationIsPropagatedAndRecovered() throws Exception {
        metadata(Map.of("newIndex", "async"));
        IllegalStateException failure = new IllegalStateException("Injected failure");
        IndexImporter importer = new IndexImporter(store, tmp.getRoot(),
                new PropertyIndexEditorProvider(), AsyncIndexerLock.NOOP_LOCK) {
            @Override
            void importIndexData() throws IOException, CommitFailedException {
                super.importIndexData();
                throw failure;
            }
        };
        importer.addImporterProvider(backend());
        assertSame(failure, assertThrows(IllegalStateException.class, importer::importIndex));
        assertEquals(List.of(PATH), cleaned);
    }

    @Test
    public void changedDefinitionIsRetained() throws Exception {
        metadata(Map.of("newIndex", "async"));
        IndexImporter importer = new IndexImporter(store, tmp.getRoot(),
                new PropertyIndexEditorProvider(), AsyncIndexerLock.NOOP_LOCK) {
            @Override
            void importIndexData() throws IOException, CommitFailedException {
                super.importIndexData();
                NodeBuilder root = store.getRoot().builder();
                root.getChildNode("oak:index").getChildNode("newIndex").setProperty("otherOwner", true);
                store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                throw new IllegalStateException("Injected failure");
            }
        };
        importer.addImporterProvider(backend());
        IllegalStateException failure = assertThrows(IllegalStateException.class, importer::importIndex);
        assertEquals(1, failure.getSuppressed().length);
        assertTrue(NodeStateUtils.getNode(store.getRoot(), PATH).getBoolean("otherOwner"));
        assertTrue(cleaned.isEmpty());
    }

    @Test
    public void concurrentChangeDuringRecoveryPreventsRemoval() throws Exception {
        metadata(Map.of("newIndex", "async"));
        NodeStore concurrent = spy(store);
        doAnswer(invocation -> {
            NodeBuilder root = store.getRoot().builder();
            root.getChildNode("oak:index").getChildNode("newIndex").setProperty("otherOwner", true);
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            return store.merge(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2));
        }).when(concurrent).merge(any(NodeBuilder.class), any(), org.mockito.ArgumentMatchers.eq(CommitInfo.EMPTY));
        IndexImporter importer = new IndexImporter(concurrent, tmp.getRoot(),
                new PropertyIndexEditorProvider(), AsyncIndexerLock.NOOP_LOCK);
        importer.addImporterProvider(backend());
        CommitFailedException failure = assertThrows(CommitFailedException.class, importer::importIndex);
        assertEquals(1, failure.getSuppressed().length);
        assertTrue(NodeStateUtils.getNode(store.getRoot(), PATH).getBoolean("otherOwner"));
        assertTrue(cleaned.isEmpty());
    }

    @Test
    public void incompleteExistingReplacementIsNotReactivatedOrDeleted() throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.child("oak:index").child("newIndex").setProperty("type", "property")
                .setProperty("async", "async");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        metadata(Map.of("newIndex", "async"));
        assertThrows(CommitFailedException.class, importer()::importIndex);
        NodeState definition = NodeStateUtils.getNode(store.getRoot(), PATH);
        assertEquals("temp-async", definition.getString("async"));
        assertEquals("IMPORT_INDEX_DATA", definition.getString(IndexImporter.INDEX_IMPORT_STATE_KEY));
        assertTrue(cleaned.isEmpty());
    }

    @Test
    public void completedLaneIsPreservedWhenAnotherLaneFails() throws Exception {
        metadata(Map.of("newIndex", "first", "secondIndex", "second"));
        String checkpoint = store.checkpoint(60_000);
        lane("first", checkpoint);
        lane("second", checkpoint);
        List<String> locked = new ArrayList<>();
        AsyncIndexerLock lock = new AsyncIndexerLock() {
            @Override
            public LockToken lock(String name) throws CommitFailedException {
                locked.add(name);
                if (locked.size() > 1) {
                    throw new CommitFailedException("Test", 1, "Second lane unavailable");
                }
                return AsyncIndexerLock.NOOP_LOCK.lock(name);
            }
            @Override
            public void unlock(LockToken token) {
            }
        };
        IndexImporter importer = new IndexImporter(store, tmp.getRoot(), new PropertyIndexEditorProvider(), lock);
        importer.addImporterProvider(backend());
        assertThrows(CommitFailedException.class, importer::importIndex);
        String completed = "first".equals(locked.get(0)) ? "newIndex" : "secondIndex";
        String abandoned = "newIndex".equals(completed) ? "secondIndex" : "newIndex";
        assertEquals(locked.get(0), IndexImporter.getAsyncLaneName("/oak:index/" + completed,
                NodeStateUtils.getNode(store.getRoot(), "/oak:index/" + completed)));
        assertEquals(List.of("/oak:index/" + abandoned), cleaned);
    }

    @Test
    public void successfulImportClearsPhaseStateWithoutCleanup() throws Exception {
        metadata(Map.of("newIndex", "async"));
        lane("async", store.checkpoint(60_000));
        importer().importIndex();
        NodeState definition = NodeStateUtils.getNode(store.getRoot(), PATH);
        assertEquals("async", definition.getStrings("async").iterator().next());
        assertFalse(definition.hasProperty(IndexImporter.INDEX_IMPORT_STATE_KEY));
        assertTrue(cleaned.isEmpty());
    }

    @Test
    public void recoveryCanBeDisabled() throws Exception {
        metadata(Map.of("newIndex", "async"));
        String previous = System.getProperty(IndexImporter.OAK_INDEX_IMPORTER_RECOVER_FAILED_IMPORT);
        System.setProperty(IndexImporter.OAK_INDEX_IMPORTER_RECOVER_FAILED_IMPORT, "false");
        try {
            assertThrows(CommitFailedException.class, importer()::importIndex);
            assertTrue(NodeStateUtils.getNode(store.getRoot(), PATH).exists());
            assertTrue(cleaned.isEmpty());
        } finally {
            if (previous == null) {
                System.clearProperty(IndexImporter.OAK_INDEX_IMPORTER_RECOVER_FAILED_IMPORT);
            } else {
                System.setProperty(IndexImporter.OAK_INDEX_IMPORTER_RECOVER_FAILED_IMPORT, previous);
            }
        }

    }

    @Test
    public void concurrentDefinitionBeforeInstallationIsRetained() throws Exception {
        metadata(Map.of("newIndex", "async"));
        IndexImporter importer = importer();
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").child("newIndex").setProperty("otherOwner", true);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertThrows(CommitFailedException.class, importer::importIndex);
        assertTrue(NodeStateUtils.getNode(store.getRoot(), PATH).getBoolean("otherOwner"));
        assertTrue(cleaned.isEmpty());
    }

    @Test
    public void defaultBackendRemovesRepositoryContainedDataWithoutExternalCleanup() throws Exception {
        metadata(Map.of("newIndex", "async"));
        IndexImporter importer = new IndexImporter(store, tmp.getRoot(),
                new PropertyIndexEditorProvider(), AsyncIndexerLock.NOOP_LOCK);
        importer.addImporterProvider(new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder definition, File directory) {
                definition.child(":index").setProperty("ownedData", "repository-contained");
            }
            @Override
            public String getType() {
                return "property";
            }
        });
        assertThrows(CommitFailedException.class, importer::importIndex);
        assertFalse(NodeStateUtils.getNode(store.getRoot(), PATH).exists());
    }

    @Test
    public void existingMergeHelperKeepsItsIndexingHook() throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.child("content").setProperty("foo", "bar");
        NodeStoreUtils.mergeWithConcurrentCheck(store, root, new PropertyIndexEditorProvider());
        assertEquals("bar", store.getRoot().getChildNode("content").getString("foo"));
    }

    @Test
    public void recoveryBeforeDataInstallationPreservesLatestAsyncChanges() throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.child("oak:index").child("newIndex").setProperty("type", "property")
                .setProperty("async", "async");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        metadata(Map.of("newIndex", "async"));
        IndexImporter importer = new IndexImporter(store, tmp.getRoot(),
                new PropertyIndexEditorProvider(), AsyncIndexerLock.NOOP_LOCK) {
            @Override
            void importIndexData() throws IOException {
                throw new IOException("Installation unavailable");
            }
        };
        importer.addImporterProvider(backend());
        root = store.getRoot().builder();
        root.getChildNode("oak:index").getChildNode("newIndex").child(":index").setProperty("latest", true);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState latest = NodeStateUtils.getNode(store.getRoot(), PATH);
        assertThrows(IOException.class, importer::importIndex);
        assertEquals(latest, NodeStateUtils.getNode(store.getRoot(), PATH));
        assertTrue(cleaned.isEmpty());
    }
}
