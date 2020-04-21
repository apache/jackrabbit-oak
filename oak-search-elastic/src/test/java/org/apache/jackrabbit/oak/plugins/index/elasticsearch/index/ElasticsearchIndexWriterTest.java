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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.index;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchConnection;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URLEncoder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticsearchIndexWriterTest {

    @Mock
    private ElasticsearchConnection elasticsearchConnectionMock;

    @Mock
    private ElasticsearchIndexDefinition indexDefinitionMock;

    @Mock
    private BulkProcessor bulkProcessorMock;

    private ElasticsearchIndexWriter indexWriter;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(indexDefinitionMock.getRemoteIndexName()).thenReturn("test-index");
        indexWriter = new ElasticsearchIndexWriter(elasticsearchConnectionMock, indexDefinitionMock, bulkProcessorMock);
    }

    @Test
    public void singleUpdateDocument() throws IOException {
        indexWriter.updateDocument("/foo", new ElasticsearchDocument("/foo"));

        ArgumentCaptor<IndexRequest> acIndexRequest = ArgumentCaptor.forClass(IndexRequest.class);
        verify(bulkProcessorMock).add(acIndexRequest.capture());

        IndexRequest request = acIndexRequest.getValue();
        assertEquals(request.index(), "test-index");
        assertEquals(request.id(), URLEncoder.encode("/foo", "UTF-8"));
    }

    @Test
    public void singleDeleteDocument() throws IOException {
        indexWriter.deleteDocuments("/bar");

        ArgumentCaptor<DeleteRequest> acDeleteRequest = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(bulkProcessorMock).add(acDeleteRequest.capture());

        DeleteRequest request = acDeleteRequest.getValue();
        assertEquals(request.index(), "test-index");
        assertEquals(request.id(), URLEncoder.encode("/bar", "UTF-8"));
    }

    @Test
    public void multiRequests() throws IOException {
        indexWriter.updateDocument("/foo", new ElasticsearchDocument("/foo"));
        indexWriter.updateDocument("/bar", new ElasticsearchDocument("/bar"));
        indexWriter.deleteDocuments("/foo");
        indexWriter.deleteDocuments("/bar");

        ArgumentCaptor<IndexRequest> acIndexRequest = ArgumentCaptor.forClass(IndexRequest.class);
        verify(bulkProcessorMock, times(2)).add(acIndexRequest.capture());
        ArgumentCaptor<DeleteRequest> acDeleteRequest = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(bulkProcessorMock, times(2)).add(acDeleteRequest.capture());
    }

    @Test
    public void closeBulkProcessor() throws IOException {
        indexWriter.close(System.currentTimeMillis());
        verify(bulkProcessorMock).close();
    }

}
