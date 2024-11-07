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

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticTestUtils.randomString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticIndexWriterTest {

    @Mock
    private ElasticIndexTracker indexTrackerMock;

    @Mock
    private ElasticConnection elasticConnectionMock;

    @Mock
    private ElasticIndexDefinition indexDefinitionMock;

    @Mock
    private ElasticBulkProcessorHandler bulkProcessorHandlerMock;

    private ElasticIndexWriter indexWriter;

    private AutoCloseable closeable;

    @Before
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        when(indexDefinitionMock.getIndexAlias()).thenReturn("test-index");
        indexWriter = new ElasticIndexWriter(indexTrackerMock, elasticConnectionMock, indexDefinitionMock, bulkProcessorHandlerMock);
    }

    @After
    public void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    public void singleUpdateDocument() throws IOException {
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));

        ArgumentCaptor<ElasticDocument> esDocumentCaptor = ArgumentCaptor.forClass(ElasticDocument.class);
        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(bulkProcessorHandlerMock).index(idCaptor.capture(), esDocumentCaptor.capture());

        assertEquals("/foo", idCaptor.getValue());
        assertEquals("/foo", esDocumentCaptor.getValue().path);
    }

    @Test
    public void singleDeleteDocument() throws IOException {
        indexWriter.deleteDocuments("/bar");

        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(bulkProcessorHandlerMock).delete(idCaptor.capture());

        String id = idCaptor.getValue();
        assertEquals("/bar", id);
    }

    @Test
    public void multiRequests() throws IOException {
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));
        indexWriter.updateDocument("/bar", new ElasticDocument("/bar"));
        indexWriter.deleteDocuments("/foo");
        indexWriter.deleteDocuments("/bar");

        verify(bulkProcessorHandlerMock, times(2)).index(anyString(), any(ElasticDocument.class));
        verify(bulkProcessorHandlerMock, times(2)).delete(anyString());
    }

    @Test
    public void longDocumentPath() throws IOException {
        String generatedPath = randomString(1024);

        indexWriter.updateDocument(generatedPath, new ElasticDocument(generatedPath));

        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        verify(bulkProcessorHandlerMock).index(idCaptor.capture(), any(ElasticDocument.class));

        String id = idCaptor.getValue();
        assertThat(id, not(generatedPath));
        assertThat(id.length(), lessThan(513));
    }

    @Test
    public void closeBulkProcessor() throws IOException {
        indexWriter.close(System.currentTimeMillis());
        verify(bulkProcessorHandlerMock).close();
    }

    @Test
    public void externallyModifiableIndexes() throws IOException {
        when(indexDefinitionMock.isExternallyModifiable()).thenReturn(true);
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));
        verify(bulkProcessorHandlerMock).update(anyString(), any(ElasticDocument.class));
    }

}
