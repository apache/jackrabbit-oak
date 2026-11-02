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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.OakInitializer;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexerInfo;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticIndexImporterTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();
    private static final String PATH = "/oak:index/testIndex";
    private static final String PREFIX = "test-author";
    private final NodeStore store = new MemoryNodeStore();
    private final List<String> requests = new ArrayList<>();
    private ElasticIndexImporter importer;
    private NodeState generated;
    private String remote;
    private String alias;
    private boolean exists = true;
    private List<String> physicalAliases = List.of();
    private List<String> targets;
    private boolean missingEntry;
    private boolean failLookup;
    private boolean failDelete;
    private boolean acknowledged = true;
    private boolean remainsAfterDelete;

    @Before
    public void setup() throws Exception {
        OakInitializer.initialize(store, new InitialContent(), EmptyHook.INSTANCE);
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").getChildNode("nodetype")
                .setProperty("declaringNodeTypes", List.of("oak:QueryIndexDefinition"), Type.NAMES)
                .setProperty("reindex", true);
        store.merge(root, new EditorHook(new IndexUpdateProvider(new CompositeIndexEditorProvider(
                new PropertyIndexEditorProvider(), new ReferenceEditorProvider()))), CommitInfo.EMPTY);
        NodeBuilder definition = EMPTY_NODE.builder();
        definition.setProperty("type", "elasticsearch").setProperty(":nameSeed", 42L);
        generated = definition.getNodeState();
        remote = ElasticIndexNameHelper.getRemoteIndexName(PREFIX, PATH, definition);
        alias = ElasticIndexNameHelper.getElasticSafeIndexName(PREFIX, PATH);
        targets = List.of(remote);
        ElasticConnection connection = mock(ElasticConnection.class);
        ElasticsearchClient client = mock(ElasticsearchClient.class);
        ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);
        when(connection.getIndexPrefix()).thenReturn(PREFIX);
        when(connection.getClient()).thenReturn(client);
        when(client.indices()).thenReturn(indices);
        when(indices.exists(org.mockito.ArgumentMatchers
                .<Function<ExistsRequest.Builder, ObjectBuilder<ExistsRequest>>>any())).thenAnswer(invocation -> {
            Function<ExistsRequest.Builder, ObjectBuilder<ExistsRequest>> request = invocation.getArgument(0);
            assertEquals(List.of(remote), request.apply(new ExistsRequest.Builder()).build().index());
            requests.add("HEAD " + remote);
            return new BooleanResponse(exists);
        });
        when(indices.getAlias(org.mockito.ArgumentMatchers
                .<Function<GetAliasRequest.Builder, ObjectBuilder<GetAliasRequest>>>any())).thenAnswer(invocation -> {
            Function<GetAliasRequest.Builder, ObjectBuilder<GetAliasRequest>> request = invocation.getArgument(0);
            GetAliasRequest built = request.apply(new GetAliasRequest.Builder()).build();
            if (!built.name().isEmpty()) {
                assertTrue(built.index().isEmpty());
                assertEquals(List.of(alias), built.name());
                requests.add("ALIAS " + alias);
                if (failLookup) {
                    throw new IOException("Alias lookup unavailable");
                }
                return GetAliasResponse.of(r -> {
                    for (String target : targets) {
                        r.aliases(target, a -> a.aliases(alias, value -> value));
                    }
                    return r;
                });
            }
            assertEquals(List.of(remote), built.index());
            requests.add("ALIASES " + remote);
            return GetAliasResponse.of(r -> {
                r.aliases(Map.of());
                if (!missingEntry) {
                    r.aliases(remote, a -> {
                        a.aliases(Map.of());
                        for (String name : physicalAliases) {
                            a.aliases(name, value -> value);
                        }
                        return a;
                    });
                }
                return r;
            });
        });
        when(indices.delete(org.mockito.ArgumentMatchers
                .<Function<DeleteIndexRequest.Builder, ObjectBuilder<DeleteIndexRequest>>>any())).thenAnswer(invocation -> {
            Function<DeleteIndexRequest.Builder, ObjectBuilder<DeleteIndexRequest>> request = invocation.getArgument(0);
            assertEquals(List.of(remote), request.apply(new DeleteIndexRequest.Builder()).build().index());
            requests.add("DELETE " + remote);
            if (failDelete) {
                throw new IOException("Delete unavailable");
            }
            if (acknowledged && !remainsAfterDelete) {
                exists = false;
            }
            return DeleteIndexResponse.of(r -> r.acknowledged(acknowledged));
        });
        importer = new ElasticIndexImporter(connection);
    }

    private void cleanup() throws Exception {
        importer.cleanupFailedImport(store, PATH, generated);
    }

    private void assertRetained() throws Exception {
        cleanup();
        assertTrue(exists);
        assertFalse(requests.contains("DELETE " + remote));
    }

    @Test
    public void deletesExactUnaliasedIndexAndIsIdempotent() throws Exception {
        cleanup();
        assertFalse(exists);
        assertEquals(List.of("HEAD " + remote, "ALIASES " + remote, "DELETE " + remote, "HEAD " + remote), requests);
        cleanup();
        assertEquals(5, requests.size());
    }

    @Test
    public void deletesIndexWithExclusiveExpectedAlias() throws Exception {
        physicalAliases = List.of(alias);
        cleanup();
        assertFalse(exists);
        assertEquals(List.of("HEAD " + remote, "ALIASES " + remote, "ALIAS " + alias,
                "DELETE " + remote, "HEAD " + remote), requests);
    }

    @Test
    public void retainsIndexWithUnexpectedAlias() throws Exception {
        physicalAliases = List.of("unrelated");
        assertRetained();
    }

    @Test
    public void retainsIndexWithAdditionalAlias() throws Exception {
        physicalAliases = List.of(alias, "unrelated");
        assertRetained();
    }

    @Test
    public void retainsIndexWithSharedAlias() throws Exception {
        physicalAliases = List.of(alias);
        targets = List.of(remote, "another-index");
        assertRetained();
    }

    @Test
    public void retainsIndexWhenAliasMoves() throws Exception {
        physicalAliases = List.of(alias);
        targets = List.of("another-index");
        assertRetained();
    }

    @Test
    public void retainsIndexWhenAliasDisappears() throws Exception {
        physicalAliases = List.of(alias);
        targets = List.of();
        assertRetained();
    }

    @Test
    public void retainsIndexWhenAliasResponseOmitsIndex() throws Exception {
        missingEntry = true;
        assertRetained();
    }

    @Test
    public void reportsAliasLookupFailure() throws Exception {
        physicalAliases = List.of(alias);
        failLookup = true;
        assertThrows(IOException.class, this::cleanup);
        assertTrue(exists);
        assertFalse(requests.contains("DELETE " + remote));
    }

    @Test
    public void reportsDeleteFailure() throws Exception {
        failDelete = true;
        assertThrows(IOException.class, this::cleanup);
        assertTrue(exists);
    }

    @Test
    public void requiresDeleteAcknowledgment() throws Exception {
        acknowledged = false;
        assertThrows(IOException.class, this::cleanup);
        assertTrue(exists);
    }

    @Test
    public void requiresAbsenceAfterDeletion() throws Exception {
        remainsAfterDelete = true;
        assertThrows(IOException.class, this::cleanup);
        assertTrue(exists);
    }

    @Test
    public void toleratesAlreadyAbsentIndex() throws Exception {
        exists = false;
        cleanup();
        assertEquals(List.of("HEAD " + remote), requests);
    }

    @Test
    public void refusesDeletionWhileDefinitionExists() throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").setChildNode("testIndex", generated);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertThrows(IOException.class, this::cleanup);
        assertTrue(requests.isEmpty());
    }

    @Test
    public void retainsBackingIndexReferencedByAnotherDefinition() throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").setChildNode("TESTINDEX", generated);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertRetained();
        assertTrue(requests.isEmpty());
    }

    @Test
    public void retainsAliasReferencedByDefinitionWithDifferentSeed() throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").setChildNode("TESTINDEX", generated).setProperty(":nameSeed", 99L);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertRetained();
        assertTrue(requests.isEmpty());
    }

    @Test
    public void retainsIndexWhenNonRootReferencesCannotBeChecked() throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").getChildNode("nodetype").removeProperty("declaringNodeTypes");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertRetained();
        assertTrue(requests.isEmpty());
    }

    @Test
    public void retainsIndexReferencedByNonRootDefinition() throws Exception {
        String path = "/content/oak:index/testIndex";
        remote = ElasticIndexNameHelper.getRemoteIndexName(PREFIX, path, generated.builder());
        alias = ElasticIndexNameHelper.getElasticSafeIndexName(PREFIX, path);
        NodeBuilder root = store.getRoot().builder();
        root.child("content").child("oak:index").setChildNode("TESTINDEX", generated)
                .setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        store.merge(root, new EditorHook(new IndexUpdateProvider(new CompositeIndexEditorProvider(
                new PropertyIndexEditorProvider(), new ReferenceEditorProvider()))), CommitInfo.EMPTY);
        importer.cleanupFailedImport(store, path, generated);
        assertTrue(exists);
        assertTrue(requests.isEmpty());
    }

    @Test
    public void requiresConnectionAndGeneratedIdentity() throws Exception {
        assertThrows(IOException.class, () -> new ElasticIndexImporter().cleanupFailedImport(store, PATH, generated));
        assertThrows(IOException.class, () -> importer.cleanupFailedImport(store, PATH, EMPTY_NODE));
        assertTrue(requests.isEmpty());
    }

    @Test
    public void sharedImporterDelegatesEsCleanupAfterRealCheckpointFailure() throws Exception {
        new IndexerInfo(tmp.getRoot(), store.checkpoint(60_000)).save();
        File directory = new File(tmp.getRoot(), "testIndex");
        assertTrue(directory.mkdir());
        Files.writeString(new File(directory, IndexerInfo.INDEX_METADATA_FILE_NAME).toPath(), "indexPath=" + PATH);
        Files.writeString(new File(tmp.getRoot(), IndexDefinitionUpdater.INDEX_DEFINITIONS_JSON).toPath(),
                "{\"" + PATH + "\":{\"jcr:primaryType\":\"nam:oak:QueryIndexDefinition\","
                + "\"type\":\"elasticsearch\",\":nameSeed\":42,\"async\":[\"import-async\"]}}");
        String missing = store.checkpoint(60_000);
        assertTrue(store.release(missing));
        NodeBuilder root = store.getRoot().builder();
        root.child(":async").setProperty("import-async", missing);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        physicalAliases = List.of(alias);
        IndexImporter shared = new IndexImporter(store, tmp.getRoot(),
                new CompositeIndexEditorProvider(new PropertyIndexEditorProvider(), new ReferenceEditorProvider()),
                AsyncIndexerLock.NOOP_LOCK);
        shared.addImporterProvider(importer);
        CommitFailedException failure = assertThrows(CommitFailedException.class, shared::importIndex);
        assertTrue(failure.getMessage().contains("No state found for checkpoint"));
        assertEquals(0, failure.getSuppressed().length);
        assertFalse(store.getRoot().getChildNode("oak:index").hasChildNode("testIndex"));
        assertFalse(exists);
        assertTrue(requests.contains("ALIAS " + alias));
        assertTrue(requests.contains("DELETE " + remote));
    }
}
