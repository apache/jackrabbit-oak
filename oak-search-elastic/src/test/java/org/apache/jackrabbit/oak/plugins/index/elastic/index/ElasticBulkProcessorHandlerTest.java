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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import java.util.Arrays;
import java.util.Collections;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ElasticBulkProcessorHandlerTest {

    @Mock
    private ElasticIndexDefinition indexDefinitionMock;

    @Mock
    private NodeState definitionNodeStateMock;

    @Mock
    private ElasticConnection elasticConnectionMock;

    @Mock
    private ElasticsearchAsyncClient esAsyncClientMock;

    @Mock
    private NodeBuilder definitionBuilder;

    @Mock
    private CommitInfo commitInfo;

    private AutoCloseable closeable;

    @Before
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        when(indexDefinitionMock.getDefinitionNodeState()).thenReturn(definitionNodeStateMock);
        when(commitInfo.getInfo()).thenReturn(Collections.emptyMap());
        when(elasticConnectionMock.getAsyncClient()).thenReturn(esAsyncClientMock);
    }

    @After
    public void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    public void defaultMode() {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);

        ElasticBulkProcessorHandler bulkProcessorHandler = ElasticBulkProcessorHandler
            .getBulkProcessorHandler(elasticConnectionMock, "index", indexDefinitionMock,
                definitionBuilder, commitInfo, true);

        assertThat(bulkProcessorHandler, instanceOf(ElasticBulkProcessorHandler.class));
    }

    @Test(expected = IllegalStateException.class)
    public void multiSyncModes() {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);
        when(definitionNodeStateMock.getProperty(eq("sync-mode")))
            .thenReturn(new MultiStringPropertyState("sync-mode", Arrays.asList("nrt", "rt")));

        ElasticBulkProcessorHandler
            .getBulkProcessorHandler(elasticConnectionMock, "index", indexDefinitionMock,
                definitionBuilder, commitInfo, true);
    }

    @Test
    public void rtMode() {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);
        when(definitionNodeStateMock.getProperty(eq("sync-mode")))
            .thenReturn(new StringPropertyState("sync-mode", "rt"));

        ElasticBulkProcessorHandler bulkProcessorHandler = ElasticBulkProcessorHandler
            .getBulkProcessorHandler(elasticConnectionMock, "index", indexDefinitionMock,
                definitionBuilder, commitInfo, true);

        assertThat(bulkProcessorHandler,
            instanceOf(ElasticBulkProcessorHandler.RealTimeBulkProcessorHandler.class));
    }

    @Test
    public void defaultModeWithCommitInfoOverride() {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);
        when(commitInfo.getInfo()).thenReturn(Collections.singletonMap("sync-mode", "rt"));

        ElasticBulkProcessorHandler bulkProcessorHandler = ElasticBulkProcessorHandler
            .getBulkProcessorHandler(elasticConnectionMock, "index", indexDefinitionMock,
                definitionBuilder, commitInfo, true);

        assertThat(bulkProcessorHandler,
            instanceOf(ElasticBulkProcessorHandler.RealTimeBulkProcessorHandler.class));
    }
}
