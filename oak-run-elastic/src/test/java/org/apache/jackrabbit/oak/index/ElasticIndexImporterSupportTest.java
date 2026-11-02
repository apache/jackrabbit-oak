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
package org.apache.jackrabbit.oak.index;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexImporter;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticIndexImporterSupportTest {
    @Test
    public void editorAndRecoveryShareOneConnectionOwnedBySupport() throws Exception {
        IndexHelper helper = mock(IndexHelper.class);
        when(helper.getMountInfoProvider()).thenReturn(Mounts.defaultMountInfoProvider());
        try (MockedConstruction<ElasticConnection> connections = mockConstruction(ElasticConnection.class,
                (connection, context) -> when(connection.getAsyncClient())
                        .thenReturn(mock(ElasticsearchAsyncClient.class, RETURNS_DEEP_STUBS)))) {
            try (ElasticIndexImporterSupport support = new ElasticIndexImporterSupport(
                    helper, "test-author", "http", "localhost", 9200, null, null)) {
                support.createIndexEditorProvider();
                IndexImporter importer = mock(IndexImporter.class);
                support.addImportProviders(importer);
                support.createIndexEditorProvider();
                assertEquals(1, connections.constructed().size());
                ArgumentCaptor<ElasticIndexImporter> provider = ArgumentCaptor.forClass(ElasticIndexImporter.class);
                verify(importer).addImporterProvider(provider.capture());
                assertNotNull(provider.getValue());
                verify(connections.constructed().get(0), never()).close();
            }
            verify(connections.constructed().get(0)).close();
        }
    }

    @Test
    public void providerCanBeCreatedBeforeEditorWithApiKeyConnection() throws Exception {
        try (MockedConstruction<ElasticConnection> connections = mockConstruction(ElasticConnection.class);
             ElasticIndexImporterSupport support = new ElasticIndexImporterSupport(
                     mock(IndexHelper.class), "test-author", "http", "localhost", 9200, "test-id", "test-secret")) {
            support.addImportProviders(mock(IndexImporter.class));
            assertEquals(1, connections.constructed().size());
        }
    }
}
