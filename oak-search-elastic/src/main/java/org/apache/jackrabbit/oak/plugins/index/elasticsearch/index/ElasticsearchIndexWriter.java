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
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchDocument.pathToId;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

class ElasticsearchIndexWriter implements FulltextIndexWriter<ElasticsearchDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIndexWriter.class);

    private final ElasticsearchConnection elasticsearchConnection;
    private final ElasticsearchIndexDefinition indexDefinition;

    private final BulkProcessor bulkProcessor;

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
                new OakBulkProcessorLister())
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
        IndexRequest request = new IndexRequest(indexDefinition.getRemoteIndexName())
                .id(pathToId(path))
                .source(doc.build(), XContentType.JSON);
        bulkProcessor.add(request);
    }

    @Override
    public void deleteDocuments(String path) throws IOException {
        DeleteRequest request = new DeleteRequest(indexDefinition.getRemoteIndexName())
                .id(pathToId(path));
        bulkProcessor.add(request);
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        // TODO : track index updates and return accordingly
        // TODO : if/when we do async push, this is where to wait for those ops to complete
        return false;
    }

    // TODO: we need to check if the index already exists and in that case we have to figure out if there are
    // "breaking changes" in the index definition
    protected void provisionIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexDefinition.getRemoteIndexName());

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

        String requestMsg = Strings.toString(request.toXContent(jsonBuilder(), EMPTY_PARAMS));
        CreateIndexResponse response = elasticsearchConnection.getClient().indices().create(request, RequestOptions.DEFAULT);

        LOG.info("Updated settings for index {} = {}. Response acknowledged: {}",
                indexDefinition.getRemoteIndexName(), requestMsg, response.isAcknowledged());
    }

    private static class OakBulkProcessorLister implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest bulkRequest) {
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
                    }
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
            LOG.error("Bulk with id {} thrown an error", executionId, throwable);
        }
    }
}
