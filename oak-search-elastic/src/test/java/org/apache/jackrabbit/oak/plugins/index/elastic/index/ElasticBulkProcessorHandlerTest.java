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

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.internal.ElasticFeatureToggles;
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

import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

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
    private String originalRetryOnConflict;
    private boolean originalRetryOnConflictEnabled;

    @Before
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        when(indexDefinitionMock.getDefinitionNodeState()).thenReturn(definitionNodeStateMock);
        when(commitInfo.getInfo()).thenReturn(Map.of());
        when(elasticConnectionMock.getAsyncClient()).thenReturn(esAsyncClientMock);
        originalRetryOnConflict = System.getProperty(ElasticBulkProcessorHandler.RETRY_ON_CONFLICT_PROP);
        System.clearProperty(ElasticBulkProcessorHandler.RETRY_ON_CONFLICT_PROP);
        originalRetryOnConflictEnabled = ElasticFeatureToggles.FT_OAK_12415_ENABLE.get();
    }

    @After
    public void tearDown() throws Exception {
        if (originalRetryOnConflict == null) {
            System.clearProperty(ElasticBulkProcessorHandler.RETRY_ON_CONFLICT_PROP);
        } else {
            System.setProperty(ElasticBulkProcessorHandler.RETRY_ON_CONFLICT_PROP, originalRetryOnConflict);
        }
        ElasticFeatureToggles.FT_OAK_12415_ENABLE.set(originalRetryOnConflictEnabled);
        closeable.close();
    }

    @Test
    public void defaultMode() throws IOException {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);

        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        bulkProcessorHandler.registerIndex("index", indexDefinitionMock, definitionBuilder, commitInfo, true);

        ElasticBulkProcessorHandler.IndexInfo indexInfo = bulkProcessorHandler.getIndexInfo("index");
        assertNotNull(indexInfo);
        assertFalse(indexInfo.isRealTime);

        bulkProcessorHandler.flushIndex("index");
        bulkProcessorHandler.close();
    }

    @Test(expected = IllegalStateException.class)
    public void multiSyncModes() {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);
        when(definitionNodeStateMock.getProperty(eq("sync-mode")))
                .thenReturn(new MultiStringPropertyState("sync-mode", Arrays.asList("nrt", "rt")));

        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        bulkProcessorHandler.registerIndex("index", indexDefinitionMock, definitionBuilder, commitInfo, true);
    }

    @Test
    public void didNotFlushIndex() throws IOException {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);
        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        bulkProcessorHandler.registerIndex("index", indexDefinitionMock, definitionBuilder, commitInfo, true);
        // Should still close successfully, but should print a warning message
        bulkProcessorHandler.close();
    }

    @Test
    public void useAfterCloseThrowsException() throws IOException {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);
        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        bulkProcessorHandler.close();
        // It's ok to call close twice
        bulkProcessorHandler.close();

        assertThrows(IllegalStateException.class,
                () -> bulkProcessorHandler.registerIndex("index", indexDefinitionMock, definitionBuilder, commitInfo, true));
        assertThrows(IllegalStateException.class,
                () -> bulkProcessorHandler.index("index", "id", new ElasticDocument("path")));
        assertThrows(IllegalStateException.class,
                () -> bulkProcessorHandler.update("index", "id", new ElasticDocument("path")));
        assertThrows(IllegalStateException.class,
                () -> bulkProcessorHandler.delete("index", "id"));
        assertThrows(IllegalStateException.class,
                () -> bulkProcessorHandler.flushIndex("index"));
    }

    @Test
    public void rtMode() throws IOException {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);
        when(definitionNodeStateMock.getProperty(eq("sync-mode")))
                .thenReturn(new StringPropertyState("sync-mode", "rt"));

        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        bulkProcessorHandler.registerIndex("index", indexDefinitionMock, definitionBuilder, commitInfo, true);

        ElasticBulkProcessorHandler.IndexInfo indexInfo = bulkProcessorHandler.getIndexInfo("index");
        assertNotNull(indexInfo);
        assertTrue(indexInfo.isRealTime);
        bulkProcessorHandler.flushIndex("index");
        bulkProcessorHandler.close();
    }

    @Test
    public void defaultModeWithCommitInfoOverride() throws IOException {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);
        when(commitInfo.getInfo()).thenReturn(Map.of("sync-mode", "rt"));

        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        bulkProcessorHandler.registerIndex("index", indexDefinitionMock, definitionBuilder, commitInfo, true);

        ElasticBulkProcessorHandler.IndexInfo indexInfo = bulkProcessorHandler.getIndexInfo("index");
        assertNotNull(indexInfo);
        assertTrue(indexInfo.isRealTime);

        bulkProcessorHandler.flushIndex("index");
        bulkProcessorHandler.close();
    }

    @Test
    public void multipleIndexes() throws IOException {
        when(definitionNodeStateMock.getProperty(eq("async"))).thenReturn(null);

        ElasticBulkProcessorHandler bulkProcessorHandler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        bulkProcessorHandler.registerIndex("index1", indexDefinitionMock, definitionBuilder, commitInfo, true);
        bulkProcessorHandler.registerIndex("index2", indexDefinitionMock, definitionBuilder, commitInfo, true);

        ElasticBulkProcessorHandler.IndexInfo indexInfo1 = bulkProcessorHandler.getIndexInfo("index1");
        assertNotNull(indexInfo1);
        assertFalse(indexInfo1.isRealTime);

        ElasticBulkProcessorHandler.IndexInfo indexInfo2 = bulkProcessorHandler.getIndexInfo("index2");
        assertNotNull(indexInfo2);
        assertFalse(indexInfo2.isRealTime);

        assertFalse(bulkProcessorHandler.flushIndex("index1"));
        assertFalse(bulkProcessorHandler.flushIndex("index2"));
        bulkProcessorHandler.close();
    }

    @Test
    public void partialUpdateSetsRetryOnConflictByDefault() throws IOException {
        ElasticBulkProcessorHandler handler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        try {
            BulkOperation op = handler.buildUpdateOperation("index", "id", new ElasticDocument("/content/foo"));
            assertEquals(Integer.valueOf(ElasticBulkProcessorHandler.RETRY_ON_CONFLICT_DEFAULT),
                    op.update().retryOnConflict());
        } finally {
            handler.close();
        }
    }

    @Test
    public void scriptedUpdateWithRemovalsSetsRetryOnConflict() throws IOException {
        ElasticBulkProcessorHandler handler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        try {
            ElasticDocument doc = new ElasticDocument("/content/foo");
            // properties to remove => scripted-update path
            doc.removeProperty("obsoleteProp");
            BulkOperation op = handler.buildUpdateOperation("index", "id", doc);
            assertEquals(Integer.valueOf(ElasticBulkProcessorHandler.RETRY_ON_CONFLICT_DEFAULT),
                    op.update().retryOnConflict());
        } finally {
            handler.close();
        }
    }

    @Test
    public void retryOnConflictCanBeDisabledViaProperty() throws IOException {
        System.setProperty(ElasticBulkProcessorHandler.RETRY_ON_CONFLICT_PROP, "0");
        ElasticBulkProcessorHandler handler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        try {
            BulkOperation op = handler.buildUpdateOperation("index", "id", new ElasticDocument("/content/foo"));
            // disabled => no retry_on_conflict field
            assertNull(op.update().retryOnConflict());
        } finally {
            handler.close();
        }
    }

    @Test
    public void retryOnConflictCanBeDisabledViaFeatureToggle() throws IOException {
        ElasticFeatureToggles.FT_OAK_12415_ENABLE.set(false);
        ElasticBulkProcessorHandler handler = new ElasticBulkProcessorHandler(elasticConnectionMock);
        try {
            BulkOperation op = handler.buildUpdateOperation("index", "id", new ElasticDocument("/content/foo"));
            // toggle off => no retry_on_conflict, despite the count defaulting to 3
            assertNull(op.update().retryOnConflict());
        } finally {
            handler.close();
        }
    }

    @Test
    public void ft_oak_12415_toggleShouldBeRemoved() {
        assertTrue("Feature toggle " + ElasticFeatureToggles.FT_OAK_12415 + " is overdue for removal",
                LocalDate.now().isBefore(LocalDate.of(2027, 10, 31)));
    }
}
