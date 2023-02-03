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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import com.google.common.base.Ticker;
import com.google.common.cache.LoadingCache;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Duration;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticTestUtils.assertEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.answersWithDelay;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticIndexStatisticsTest {

    @Mock
    private ElasticConnection elasticConnectionMock;

    @Mock
    private ElasticIndexDefinition indexDefinitionMock;

    @Mock
    private ElasticsearchClient elasticClientMock;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(indexDefinitionMock.getIndexAlias()).thenReturn("test-index");
        when(elasticConnectionMock.getClient()).thenReturn(elasticClientMock);
    }

    @Test
    public void defaultIndexStatistics() {
        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);
        assertNotNull(indexStatistics);
    }

    @Test
    public void cachedStatistics() throws Exception {
        MutableTicker ticker = new MutableTicker();
        LoadingCache<ElasticIndexStatistics.StatsRequestDescriptor, Integer> cache =
                ElasticIndexStatistics.setupCountCache(100, 10 * 60, 60, ticker);
        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock, cache);

        CountResponse countResponse = mock(CountResponse.class);
        when(countResponse.count()).thenReturn(100L);

        // simulate some delay when invoking elastic
        when(elasticClientMock.count(any(CountRequest.class)))
                .then(answersWithDelay(250, i -> countResponse));

        // cache miss, read data from elastic
        assertEquals(100, indexStatistics.numDocs());
        verify(elasticClientMock).count(any(CountRequest.class));

        // index count changes in elastic
        when(countResponse.count()).thenReturn(1000L);

        // cache hit, old value returned
        assertEquals(100, indexStatistics.numDocs());
        verifyNoMoreInteractions(elasticClientMock);

        // move cache time ahead of 2 minutes, cache reload time expired
        ticker.tick(Duration.ofMinutes(2));
        // old value is returned, read fresh data from elastic in background
        assertEquals(100, indexStatistics.numDocs());

        assertEventually(() -> {
            try {
                verify(elasticClientMock, times(2)).count(any(CountRequest.class));
            } catch (IOException e) {
                fail(e.getMessage());
            }
            // cache hit, latest value returned
            assertEquals(1000, indexStatistics.numDocs());
        }, 1000);
        verifyNoMoreInteractions(elasticClientMock);

        // index count changes in elastic
        when(countResponse.count()).thenReturn(5000L);

        // move cache time ahead of 15 minutes, cache value expired
        ticker.tick(Duration.ofMinutes(15));

        // cache miss, read data from elastic
        assertEquals(5000, indexStatistics.numDocs());
        verify(elasticClientMock, times(3)).count(any(CountRequest.class));
    }

    private static class MutableTicker extends Ticker {

        private long nanoOffset = 0;

        @Override
        public long read() {
            return systemTicker().read() + nanoOffset;
        }

        public void tick(Duration duration) {
            nanoOffset = duration.toNanos();
        }
    }

}
