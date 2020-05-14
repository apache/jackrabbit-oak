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
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchDocument.pathToId;
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
    public void updateDocument(String path, ElasticsearchDocument doc) throws IOException {
        IndexRequest request = new IndexRequest(indexDefinition.getRemoteIndexAlias())
                .id(pathToId(path))
                .source(doc.build(), XContentType.JSON);
        bulkProcessor.add(request);
    }

    @Override
    public void deleteDocuments(String path) throws IOException {
        DeleteRequest request = new DeleteRequest(indexDefinition.getRemoteIndexAlias())
                .id(pathToId(path));
        bulkProcessor.add(request);
    }

    @Override
    public boolean close(long timestamp) throws IOException {
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

        IndicesClient indicesClient = elasticsearchConnection.getClient().indices();
        final String indexName = indexDefinition.getRemoteIndexName();

        CreateIndexRequest createIndexRequest = constructCreateIndexRequest(indexName);
        String requestMsg = Strings.toString(createIndexRequest.toXContent(jsonBuilder(), EMPTY_PARAMS));
        CreateIndexResponse response = indicesClient.create(createIndexRequest, RequestOptions.DEFAULT);
        checkResponseAcknowledgement(response, "Create index call not acknowledged for index " + indexName);

        LOG.info("Updated settings for index {} = {}. Response acknowledged: {}",
                indexDefinition.getRemoteIndexAlias(), requestMsg, response.isAcknowledged());


        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(indexDefinition.getRemoteIndexAlias());
        GetAliasesResponse aliasesResponse = indicesClient.getAlias(getAliasesRequest, RequestOptions.DEFAULT);
        Map<String, Set<AliasMetaData>> aliases = aliasesResponse.getAliases();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        for (String oldIndexName : aliases.keySet()) {
            IndicesAliasesRequest.AliasActions removeAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE);
            removeAction.index(oldIndexName).alias(indexDefinition.getRemoteIndexAlias());
            indicesAliasesRequest.addAliasAction(removeAction);
        }
        IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
        addAction.index(indexName).alias(indexDefinition.getRemoteIndexAlias());
        indicesAliasesRequest.addAliasAction(addAction);
        AcknowledgedResponse updateAliasResponse = indicesClient.updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        checkResponseAcknowledgement(updateAliasResponse, "Update alias call not acknowledged for alias "
                + indexDefinition.getRemoteIndexAlias());
        LOG.info("Updated alias {} to index {}. Response acknowledged: {}", indexDefinition.getRemoteIndexAlias(),
                indexName, updateAliasResponse.isAcknowledged());
        deleteOldIndices(indicesClient, aliases.keySet());
    }

    private void checkResponseAcknowledgement(AcknowledgedResponse response, String exceptionMessage) {
        if (!response.isAcknowledged()) {
            throw new IllegalStateException(exceptionMessage);
        }
    }

    private void deleteOldIndices(IndicesClient indicesClient, Set<String> indices) throws IOException {
        if (indices.size() == 0)
            return;
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
        for (String oldIndexName : indices) {
            deleteIndexRequest.indices(oldIndexName);
        }
        AcknowledgedResponse deleteIndexResponse = indicesClient.delete(deleteIndexRequest, RequestOptions.DEFAULT);
        checkResponseAcknowledgement(deleteIndexResponse, "Delete index call not acknowledged for indices " + indices);
        LOG.info("Deleted indices {}. Response acknowledged: {}", indices.toString(), deleteIndexResponse.isAcknowledged());
    }

    private CreateIndexRequest constructCreateIndexRequest(String indexName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        // provision settings
        request.settings(Settings.builder()
                .put("analysis.analyzer.ancestor_analyzer.type", "custom")
                .put("analysis.analyzer.ancestor_analyzer.tokenizer", "path_hierarchy"));

        // provision mappings
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject(FieldNames.ANCESTORS)
                        .field("type", "text")
                        .field("analyzer", "ancestor_analyzer")
                        .field("search_analyzer", "keyword")
                        .field("search_quote_analyzer", "keyword")
                        .endObject();
                mappingBuilder.startObject(FieldNames.PATH_DEPTH)
                        .field("type", "integer")
                        .endObject();
                mappingBuilder.startObject(FieldNames.SUGGEST)
                        .field("type", "completion")
                        .endObject();
                mappingBuilder.startObject(FieldNames.NOT_NULL_PROPS)
                        .field("type", "keyword")
                        .endObject();
                mappingBuilder.startObject(FieldNames.NULL_PROPS)
                        .field("type", "keyword")
                        .endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        request.mapping(mappingBuilder);

        return request;
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
}
