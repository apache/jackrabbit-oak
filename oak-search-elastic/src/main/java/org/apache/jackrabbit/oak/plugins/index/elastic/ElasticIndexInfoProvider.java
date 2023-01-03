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

import java.io.IOException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import co.elastic.clients.elasticsearch._types.HealthStatus;
import co.elastic.clients.elasticsearch._types.Level;
import co.elastic.clients.elasticsearch.cluster.HealthRequest;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;

class ElasticIndexInfoProvider implements IndexInfoProvider {

    private final ElasticIndexTracker indexTracker;

    private final AsyncIndexInfoService asyncIndexInfoService;

    ElasticIndexInfoProvider(@NotNull ElasticIndexTracker indexTracker,
                                    @NotNull AsyncIndexInfoService asyncIndexInfoService) {
        this.indexTracker = indexTracker;
        this.asyncIndexInfoService = asyncIndexInfoService;
    }

    @Override
    public String getType() {
        return ElasticIndexDefinition.TYPE_ELASTICSEARCH;
    }

    @Override
    public @Nullable IndexInfo getInfo(String indexPath) {
        ElasticIndexNode node = indexTracker.acquireIndexNode(indexPath);
        try {
            String asyncName = IndexUtils.getAsyncLaneName(node.getDefinition().getDefinitionNodeState(), indexPath);
            AsyncIndexInfo asyncInfo = asyncIndexInfoService.getInfo(asyncName);

            return new ElasticIndexInfo(
                    indexPath,
                    asyncName,
                    asyncInfo != null ? asyncInfo.getLastIndexedTo() : -1L,
                    getStatusTimestamp(node.getDefinition().getDefinitionNodeState(), IndexDefinition.STATUS_LAST_UPDATED),
                    node.getIndexStatistics().numDocs(),
                    node.getIndexStatistics().primaryStoreSize(),
                    node.getIndexStatistics().creationDate(),
                    getStatusTimestamp(node.getDefinition().getDefinitionNodeState(), IndexDefinition.REINDEX_COMPLETION_TIMESTAMP)
            );
        } finally {
            node.release();
        }
    }

    @Override
    public boolean isValid(String indexPath) throws IOException {
        ElasticIndexNode node = indexTracker.acquireIndexNode(indexPath);
        try {
            HealthResponse response = node.getConnection().getClient().cluster()
                    .health(HealthRequest.of(hrb -> hrb
                            .index(node.getDefinition().getIndexAlias())
                            .level(Level.Indices)));
            return response.indices().values().stream().map(i -> i.status() == HealthStatus.Green).findFirst().orElse(false);
        } finally {
            node.release();
        }
    }

    private long getStatusTimestamp(NodeState idxState, String propName) {
        NodeState status = idxState.getChildNode(IndexDefinition.STATUS_NODE);
        if (status.exists()) {
            PropertyState updatedTime = status.getProperty(propName);
            if (updatedTime != null) {
                return ISO8601.parse(updatedTime.getValue(Type.DATE)).getTimeInMillis();
            }
        }
        return -1;
    }

    private static class ElasticIndexInfo implements IndexInfo {

        private final String indexPath;
        private final String asyncLane;
        private final long indexedUpToTime;
        private final long lastUpdatedTime;
        private final long estimatedEntryCount;
        private final long sizeInBytes;
        private final long creationTimestamp;
        private final long reindexCompletionTimestamp;

        ElasticIndexInfo(String indexPath, String asyncLane, long indexedUpToTime, long lastUpdatedTime,
                         long estimatedEntryCount, long sizeInBytes, long creationTimestamp, long reindexCompletionTimestamp) {
            this.indexPath = indexPath;
            this.asyncLane = asyncLane;
            this.indexedUpToTime = indexedUpToTime;
            this.lastUpdatedTime = lastUpdatedTime;
            this.estimatedEntryCount = estimatedEntryCount;
            this.sizeInBytes = sizeInBytes;
            this.creationTimestamp = creationTimestamp;
            this.reindexCompletionTimestamp = reindexCompletionTimestamp;
        }

        @Override
        public String getIndexPath() {
            return indexPath;
        }

        @Override
        public String getType() {
            return ElasticIndexDefinition.TYPE_ELASTICSEARCH;
        }

        @Override
        public @Nullable String getAsyncLaneName() {
            return asyncLane;
        }

        @Override
        public long getLastUpdatedTime() {
            return lastUpdatedTime;
        }

        @Override
        public long getIndexedUpToTime() {
            return indexedUpToTime;
        }

        @Override
        public long getEstimatedEntryCount() {
            return estimatedEntryCount;
        }

        @Override
        public long getSizeInBytes() {
            return sizeInBytes;
        }

        @Override
        public boolean hasIndexDefinitionChangedWithoutReindexing() {
            // not available in elastic, stored index definitions not supported
            return false;
        }

        @Override
        public @Nullable String getIndexDefinitionDiff() {
            // not available in elastic, stored index definitions not supported
            return null;
        }

        @Override
        public boolean hasHiddenOakLibsMount() {
            // not available in elastic
            return false;
        }

        @Override
        public boolean hasPropertyIndexNode() {
            // not available in elastic
            return false;
        }

        @Override
        public long getSuggestSizeInBytes() {
            // not available in elastic
            return -1;
        }

        @Override
        public long getCreationTimestamp() {
            return creationTimestamp;
        }

        @Override
        public long getReindexCompletionTimestamp() {
            return reindexCompletionTimestamp;
        }
    }
}
