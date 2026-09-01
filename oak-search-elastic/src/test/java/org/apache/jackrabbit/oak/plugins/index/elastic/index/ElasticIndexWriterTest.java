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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.GetAliasRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.util.ObjectBuilder;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConfig;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticTestUtils.randomString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticIndexWriterTest {

    @Mock
    private ElasticIndexTracker indexTrackerMock;

    @Mock
    private ElasticConnection elasticConnectionMock;

    @Mock
    private ElasticsearchClient elasticsearchClientMock;

    @Mock
    private ElasticsearchIndicesClient indicesClientMock;

    @Mock
    private ElasticIndexDefinition indexDefinitionMock;

    @Mock
    private ElasticBulkProcessorHandler bulkProcessorHandlerMock;

    private ElasticIndexWriter indexWriter;

    private String indexAlias;

    private AutoCloseable closeable;

    @Before
    public void setUp() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        when(indexDefinitionMock.getIndexAlias()).thenReturn("test-index");
        when(indexDefinitionMock.getIndexName()).thenReturn("test-index-name");
        when(elasticConnectionMock.getClient()).thenReturn(elasticsearchClientMock);
        when(elasticConnectionMock.getClient()
                .deleteByQuery(ArgumentMatchers.<Function<DeleteByQueryRequest.Builder, ObjectBuilder<DeleteByQueryRequest>>>any()))
                .thenReturn(DeleteByQueryResponse.of(d -> d.deleted(1L).failures(Collections.emptyList())));
        // LazyElasticIndexWriter.close() checks for a stale alias on an empty-reindex close();
        // report "nothing provisioned" so that path no-ops in tests that don't exercise it directly.
        when(elasticsearchClientMock.indices()).thenReturn(indicesClientMock);
        when(indicesClientMock.getAlias(ArgumentMatchers.<Function<GetAliasRequest.Builder, ObjectBuilder<GetAliasRequest>>>any()))
                .thenReturn(GetAliasResponse.of(r -> r.result(Collections.emptyMap())));
        // In this test we are explicitly disabling inference as bulkprocessor
        // is called with update document if inference is enabled.
        InferenceConfig.reInitialize(new MemoryNodeStore(), "/oak:index/:inferenceConfig", false);
        indexWriter = new ElasticIndexWriter(indexTrackerMock, elasticConnectionMock, indexDefinitionMock, bulkProcessorHandlerMock);
        indexAlias = indexDefinitionMock.getIndexAlias();
    }

    @After
    public void tearDown() throws Exception {
        closeable.close();
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(false);
        ElasticIndexEditorProvider.FT_OAK_12249_ENABLE.set(false);
    }

    @Test
    public void singleUpdateDocument() throws IOException {
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));

        ArgumentCaptor<ElasticDocument> esDocumentCaptor = ArgumentCaptor.forClass(ElasticDocument.class);
        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(bulkProcessorHandlerMock).index(eq(indexAlias), idCaptor.capture(), esDocumentCaptor.capture());

        assertEquals("/foo", idCaptor.getValue());
        assertEquals("/foo", esDocumentCaptor.getValue().path);
    }

    @Test
    public void singleDeleteDocument() throws IOException {
        indexWriter.deleteDocumentTree("/bar");

        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(bulkProcessorHandlerMock).delete(eq(indexAlias), idCaptor.capture());
        verify(elasticsearchClientMock).deleteByQuery(
                ArgumentMatchers.<Function<DeleteByQueryRequest.Builder, ObjectBuilder<DeleteByQueryRequest>>>any()
        );

        String id = idCaptor.getValue();
        assertEquals("/bar", id);
    }

    @Test
    public void multiRequests() throws IOException {
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));
        indexWriter.updateDocument("/bar", new ElasticDocument("/bar"));
        indexWriter.deleteDocumentTree("/foo");
        indexWriter.deleteDocumentTree("/bar");

        verify(bulkProcessorHandlerMock, times(2)).index(eq(indexAlias), anyString(), any(ElasticDocument.class));
        verify(bulkProcessorHandlerMock, times(2)).delete(eq(indexAlias), anyString());
        verify(elasticsearchClientMock, times(2)).deleteByQuery(
                ArgumentMatchers.<Function<DeleteByQueryRequest.Builder, ObjectBuilder<DeleteByQueryRequest>>>any()
        );
    }

    @Test
    public void longDocumentPath() throws IOException {
        String generatedPath = randomString(1024);

        indexWriter.updateDocument(generatedPath, new ElasticDocument(generatedPath));

        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(bulkProcessorHandlerMock).index(eq(indexAlias), idCaptor.capture(), any(ElasticDocument.class));

        String id = idCaptor.getValue();
        assertThat(id, not(generatedPath));
        assertThat(id.length(), lessThan(513));
    }

    @Test
    public void closeIndex() throws IOException {
        indexWriter.close(System.currentTimeMillis());
        // Closes the index but not the bulk processor
        verify(bulkProcessorHandlerMock).flushIndex(eq(indexAlias));
        verify(bulkProcessorHandlerMock, never()).close();
    }

    @Test
    public void externallyModifiableIndexes() throws IOException {
        when(indexDefinitionMock.isExternallyModifiable()).thenReturn(true);
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));
        verify(bulkProcessorHandlerMock).update(eq(indexAlias), anyString(), any(ElasticDocument.class));
    }

    @Test
    public void splitLargeString() {
        assertEquals("[a]",
                Arrays.toString(ElasticIndexWriter.splitLargeString(
                        "a", 1024)));
        assertEquals("[h, e, l, l, o,  , w, o, r, l, d]",
                Arrays.toString(ElasticIndexWriter.splitLargeString(
                        "hello world", 1)));
        assertEquals("[he, ll, o , wo, rl, d]",
                Arrays.toString(ElasticIndexWriter.splitLargeString(
                        "hello world", 2)));
    }

    @Test
    public void ft_oak_12206_toggleShouldBeRemoved() {
        // Time-bombed: if this test fails, the feature toggle FT_OAK-12206 and its guard in
        // ElasticIndexWriter#deleteDocumentTree should be removed — the fix has been in production long enough.
        assertTrue("Feature toggle " + ElasticIndexEditorProvider.FT_OAK_12206 + " is overdue for removal",
                LocalDate.now().isBefore(LocalDate.of(2027, 5, 6)));
    }

    // --- OAK-12249: lazy provisioning tests ---

    @Test
    public void lazyProvisioning_requiresGraceful404Toggle() {
        // OAK-12249 alone must not activate lazy provisioning — OAK-12248 is the hard dependency.
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(false);
        ElasticIndexEditorProvider.FT_OAK_12249_ENABLE.set(true);

        assertFalse("Lazy provisioning must be inactive when graceful 404 handling is off",
                ElasticIndexEditorProvider.isLazyProvisioningActive());
    }

    @Test
    public void lazyProvisioning_activeWhenBothTogglesEnabled() {
        // Guards against e.g. an accidental && -> || regression in isLazyProvisioningActive(),
        // which the negative-only test above would not catch.
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(true);
        ElasticIndexEditorProvider.FT_OAK_12249_ENABLE.set(true);

        assertTrue("Lazy provisioning must be active when both toggles are enabled",
                ElasticIndexEditorProvider.isLazyProvisioningActive());
    }

    @Test
    public void emptyReindex_supplierNeverCalled() throws IOException {
        // GIVEN: a LazyElasticIndexWriter whose supplier records whether it was invoked
        AtomicBoolean supplierCalled = new AtomicBoolean(false);
        NodeBuilder definitionBuilder = EmptyNodeState.EMPTY_NODE.builder();
        LazyElasticIndexWriter lazyWriter = new LazyElasticIndexWriter(() -> {
            supplierCalled.set(true);
            return indexWriter;
        }, definitionBuilder, elasticConnectionMock, indexDefinitionMock);

        // WHEN: closed without writing any documents
        lazyWriter.close(System.currentTimeMillis());

        // THEN: supplier was never called — no ElasticIndexWriter created, no ES index provisioned
        assertFalse("Supplier must not be called when no documents are written", supplierCalled.get());
        // AND: the definition is marked so the next incremental cycle provisions on demand
        assertTrue("PROP_REQUIRES_PROVISIONING must be set after an empty-reindex close()",
                definitionBuilder.getProperty(ElasticIndexDefinition.PROP_REQUIRES_PROVISIONING)
                        .getValue(Type.BOOLEAN));
    }

    @Test
    public void deleteDocumentTree_triggersSupplier() throws IOException {
        AtomicBoolean supplierCalled = new AtomicBoolean(false);
        NodeBuilder definitionBuilder = EmptyNodeState.EMPTY_NODE.builder();
        LazyElasticIndexWriter lazyWriter = new LazyElasticIndexWriter(() -> {
            supplierCalled.set(true);
            return indexWriter;
        }, definitionBuilder, elasticConnectionMock, indexDefinitionMock);

        lazyWriter.deleteDocumentTree("/foo");

        assertTrue("Supplier must be called on deleteDocumentTree", supplierCalled.get());
    }

    @Test
    public void deleteDocument_triggersSupplier() throws IOException {
        AtomicBoolean supplierCalled = new AtomicBoolean(false);
        NodeBuilder definitionBuilder = EmptyNodeState.EMPTY_NODE.builder();
        LazyElasticIndexWriter lazyWriter = new LazyElasticIndexWriter(() -> {
            supplierCalled.set(true);
            return indexWriter;
        }, definitionBuilder, elasticConnectionMock, indexDefinitionMock);

        lazyWriter.deleteDocument("/foo");

        assertTrue("Supplier must be called on deleteDocument", supplierCalled.get());
    }

    @Test
    public void nonEmptyReindex_supplierCalledOnFirstWrite() throws IOException {
        // GIVEN: a LazyElasticIndexWriter whose supplier records when it is invoked
        AtomicBoolean supplierCalled = new AtomicBoolean(false);
        NodeBuilder definitionBuilder = EmptyNodeState.EMPTY_NODE.builder();
        LazyElasticIndexWriter lazyWriter = new LazyElasticIndexWriter(() -> {
            supplierCalled.set(true);
            return indexWriter;
        }, definitionBuilder, elasticConnectionMock, indexDefinitionMock);

        // Supplier not yet called before any write
        assertFalse(supplierCalled.get());

        // WHEN: first document written
        lazyWriter.updateDocument("/foo", new ElasticDocument("/foo"));

        // THEN: supplier was called — ElasticIndexWriter (and its ES index) created on first write
        assertTrue("Supplier must be called on the first write", supplierCalled.get());
    }

}
