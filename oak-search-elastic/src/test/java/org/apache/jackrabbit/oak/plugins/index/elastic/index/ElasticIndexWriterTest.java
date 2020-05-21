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
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Random;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticIndexWriterTest {

    @Mock
    private ElasticConnection elasticConnectionMock;

    @Mock
    private ElasticIndexDefinition indexDefinitionMock;

    @Mock
    private BulkProcessor bulkProcessorMock;

    private ElasticIndexWriter indexWriter;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(indexDefinitionMock.getRemoteIndexAlias()).thenReturn("test-index");
        indexWriter = new ElasticIndexWriter(elasticConnectionMock, indexDefinitionMock, bulkProcessorMock);
    }

    @Test
    public void singleUpdateDocument() {
        indexWriter.updateDocument("/foo", new ElasticDocument("/foo"));

        ArgumentCaptor<IndexRequest> acIndexRequest = ArgumentCaptor.forClass(IndexRequest.class);
        verify(bulkProcessorMock).add(acIndexRequest.capture());

        IndexRequest request = acIndexRequest.getValue();
        assertEquals("test-index", request.index());
        assertEquals("/foo", request.id());
    }

    @Test
    public void singleDeleteDocument() {
        indexWriter.deleteDocuments("/bar");

        ArgumentCaptor<DeleteRequest> acDeleteRequest = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(bulkProcessorMock).add(acDeleteRequest.capture());

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

        ArgumentCaptor<IndexRequest> acIndexRequest = ArgumentCaptor.forClass(IndexRequest.class);
        verify(bulkProcessorMock, times(2)).add(acIndexRequest.capture());
        ArgumentCaptor<DeleteRequest> acDeleteRequest = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(bulkProcessorMock, times(2)).add(acDeleteRequest.capture());
    }

    @Test
    public void longDocumentPath() {
        int leftLimit = 48; // '0'
        int rightLimit = 122; // char '~'
        int targetStringLength = 1024;
        final Random random = new Random(42);

        String generatedPath = random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        indexWriter.updateDocument(generatedPath, new ElasticDocument(generatedPath));

        ArgumentCaptor<IndexRequest> acIndexRequest = ArgumentCaptor.forClass(IndexRequest.class);
        verify(bulkProcessorMock).add(acIndexRequest.capture());

        IndexRequest request = acIndexRequest.getValue();
        assertThat(request.id(), not(generatedPath));
        assertThat(request.id().length(), lessThan(513));
    }

    @Test
    public void closeBulkProcessor() {
        indexWriter.close(System.currentTimeMillis());
        verify(bulkProcessorMock).close();
    }

}
