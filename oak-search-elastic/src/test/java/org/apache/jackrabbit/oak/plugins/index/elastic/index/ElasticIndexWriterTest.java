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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
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

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(indexDefinitionMock.getIndexAlias()).thenReturn("test-index");
        indexWriter = new ElasticIndexWriter(indexTrackerMock, elasticConnectionMock, indexDefinitionMock, bulkProcessorHandlerMock);
    }

    @Test
    public void singleUpdateDocument() {
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));

        ArgumentCaptor<IndexRequest> acIndexRequest = ArgumentCaptor.forClass(IndexRequest.class);
        verify(bulkProcessorHandlerMock).add(acIndexRequest.capture());

        IndexRequest request = acIndexRequest.getValue();
        assertEquals("test-index", request.index());
        assertEquals("/foo", request.id());
    }

    @Test
    public void singleDeleteDocument() {
        indexWriter.deleteDocuments("/bar");

        ArgumentCaptor<DeleteRequest> acDeleteRequest = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(bulkProcessorHandlerMock).add(acDeleteRequest.capture());

        DeleteRequest request = acDeleteRequest.getValue();
        assertEquals("test-index", request.index());
        assertEquals("/bar", request.id());
    }

    @Test
    public void multiRequests() {
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));
        indexWriter.updateDocument("/bar", new ElasticDocument("/bar"));
        indexWriter.deleteDocuments("/foo");
        indexWriter.deleteDocuments("/bar");

        ArgumentCaptor<DocWriteRequest<?>> request = ArgumentCaptor.forClass(DocWriteRequest.class);
        verify(bulkProcessorHandlerMock, times(4)).add(request.capture());
    }

    @Test
    public void longDocumentPath() {
        String generatedPath = randomString(1024);

        indexWriter.updateDocument(generatedPath, new ElasticDocument(generatedPath));

        ArgumentCaptor<IndexRequest> acIndexRequest = ArgumentCaptor.forClass(IndexRequest.class);
        verify(bulkProcessorHandlerMock).add(acIndexRequest.capture());

        IndexRequest request = acIndexRequest.getValue();
        assertThat(request.id(), not(generatedPath));
        assertThat(request.id().length(), lessThan(513));
    }

    @Test
    public void closeBulkProcessor() throws IOException {
        indexWriter.close(System.currentTimeMillis());
        verify(bulkProcessorHandlerMock).close();
    }

}
