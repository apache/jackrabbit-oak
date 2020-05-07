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
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

class ElasticsearchIndexWriter implements FulltextIndexWriter<ElasticsearchDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIndexWriter.class);

    private final ElasticsearchConnection elasticsearchConnection;
    private final ElasticsearchIndexDefinition indexDefinition;

    private final BulkProcessor bulkProcessor;
    private Optional<Boolean> indexUpdated = Optional.empty();

    ElasticsearchIndexWriter(@NotNull ElasticsearchConnection elasticsearchConnection,
                                       @NotNull ElasticsearchIndexDefinition indexDefinition) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.indexDefinition = indexDefinition;
        bulkProcessor = initBulkProcessor();
    }

    @TestOnly
    ElasticsearchIndexWriter(@NotNull ElasticsearchConnection elasticsearchConnection,
                                       @NotNull ElasticsearchIndexDefinition indexDefinition,
                                       @NotNull BulkProcessor bulkProcessor) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.indexDefinition = indexDefinition;
        this.bulkProcessor = bulkProcessor;
    }

    private BulkProcessor initBulkProcessor() {
        return BulkProcessor.builder((request, bulkListener) ->
                        elasticsearchConnection.getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                new OakBulkProcessorListener())
                .setBulkActions(indexDefinition.bulkActions)
                .setBulkSize(new ByteSizeValue(indexDefinition.bulkSizeBytes))
                .setFlushInterval(TimeValue.timeValueMillis(indexDefinition.bulkFlushIntervalMs))
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
                        TimeValue.timeValueMillis(indexDefinition.bulkRetriesBackoff), indexDefinition.bulkRetries)
                )
                .build();
    }

    @Override
    public void updateDocument(String path, ElasticsearchDocument doc) {
        IndexRequest request = new IndexRequest(indexDefinition.getRemoteIndexName())
                .id(idFromPath(path))
                .source(doc.build(), XContentType.JSON);
        bulkProcessor.add(request);
    }

    @Override
    public void deleteDocuments(String path) {
        DeleteRequest request = new DeleteRequest(indexDefinition.getRemoteIndexName())
                .id(idFromPath(path));
        bulkProcessor.add(request);
    }

    @Override
    public boolean close(long timestamp) {
        LOG.trace("Calling close on bulk processor {}", bulkProcessor);
        bulkProcessor.close();
        LOG.trace("Bulk Processor {} closed", bulkProcessor);

        // bulkProcessor.close() calls the OakBulkProcessorListener.beforeBulk in a blocking manner
        // indexUpdated would be unset there if it was false till now (not even a single update succeeded)
        // in this case wait for sometime for the last OakBulkProcessorListener.afterBulk to be called
        // where indexUpdated can possibly be set to true, return false in case of timeout.
        // We don't wait in case indexUpdated is already set (This would be if any of the previous flushes for this processor
        // were successful i.e index was updated at least once)
        final long start = System.currentTimeMillis();
        long timeoutMillis = indexDefinition.bulkFlushIntervalMs * 5 ;
        while (!indexUpdated.isPresent()) {
            long lastAttempt = System.currentTimeMillis();
            long elapsedTime = lastAttempt - start;
            if (elapsedTime > timeoutMillis) {
                // indexUpdate was not set till now, return false
                LOG.trace("Timed out waiting for the bulk processor response. Returning indexUpdated = false");
                return false;
            } else {
                try {
                    LOG.trace("Waiting for afterBulk response...");
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    //
                }
            }
        }
        LOG.trace("Returning indexUpdated = {}", indexUpdated.get());
        return indexUpdated.get();
    }

    // TODO: we need to check if the index already exists and in that case we have to figure out if there are
    // "breaking changes" in the index definition
    protected void provisionIndex() throws IOException {
        CreateIndexRequest request = ElasticsearchIndexHelper.createIndexRequest(indexDefinition);

        String requestMsg = Strings.toString(request.toXContent(jsonBuilder(), EMPTY_PARAMS));
        CreateIndexResponse response = elasticsearchConnection.getClient().indices().create(request, RequestOptions.DEFAULT);

        LOG.info("Updated settings for index {} = {}. Response acknowledged: {}",
                indexDefinition.getRemoteIndexName(), requestMsg, response.isAcknowledged());
    }

    private class OakBulkProcessorListener implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest bulkRequest) {
            if (indexUpdated.isPresent() && !indexUpdated.get()) {
                // Reset the state only if it's false
                // If it's true that means index was updated at least once by this processor
                // and we can return true for indexUpdate.
                indexUpdated = Optional.empty();
            }
            LOG.info("Sending bulk with id {} -> {}", executionId, bulkRequest.getDescription());
            if (LOG.isTraceEnabled()) {
                LOG.trace("Bulk Requests: \n{}", bulkRequest.requests()
                        .stream()
                        .map(DocWriteRequest::toString)
                        .collect(Collectors.joining("\n"))
                );
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
            LOG.info("Bulk with id {} processed with status {} in {}", executionId, bulkResponse.status(), bulkResponse.getTook());
            if (LOG.isTraceEnabled()) {
                try {
                    LOG.trace(Strings.toString(bulkResponse.toXContent(jsonBuilder(), EMPTY_PARAMS)));
                } catch (IOException e) {
                    LOG.error("Error decoding bulk response", e);
                }
            }
            if (bulkResponse.hasFailures()) { // check if some operations failed to execute
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        LOG.error("Bulk item with id {} failed", failure.getId(), failure.getCause());
                    } else {
                        // Set indexUpdated to true even if 1 item was updated successfully
                        indexUpdated = Optional.of(true);
                    }
                }
                // Only set indexUpdated to false if it's unset
                // If set and true, that means index was updated at least once by this processor.
                // If set and false, no need to do anything
                if (!indexUpdated.isPresent()) {
                    indexUpdated = Optional.of(false);
                }
            } else {
                indexUpdated = Optional.of(true);
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
            // Only set indexUpdated to false if it's unset
            // If set and true, that means index was updated at least once by this processor.
            // If set and false, no need to do anything
            if (!indexUpdated.isPresent()) {
                indexUpdated = Optional.of(false);
            }
            LOG.error("Bulk with id {} threw an error", executionId, throwable);
        }
    }

    private static String idFromPath(@NotNull String path) {
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
        if (pathBytes.length > 512) {
            try {
                return new String(MessageDigest.getInstance("SHA-256").digest(pathBytes));
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        return path;
    }
}
