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

import co.elastic.clients.elasticsearch._types.AcknowledgedResponseBase;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import co.elastic.clients.json.JsonpUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

class ElasticIndexWriter implements FulltextIndexWriter<ElasticDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexWriter.class);

    private final ElasticIndexTracker indexTracker;
    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;

    private final ElasticBulkProcessorHandler bulkProcessorHandler;

    private final boolean reindex;
    private final String indexName;

    ElasticIndexWriter(@NotNull ElasticIndexTracker indexTracker,
                       @NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull NodeBuilder definitionBuilder,
                       boolean reindex, CommitInfo commitInfo) {
        this.indexTracker = indexTracker;
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.reindex = reindex;

        // We don't use stored index definitions with elastic. Every time a new writer gets created we
        // use the actual index name (based on the current seed) while reindexing, or the alias (pointing to the
        // old index until the new one gets enabled) during incremental reindexing
        if (this.reindex) {
            try {
                long seed = UUID.randomUUID().getMostSignificantBits();
                // merge gets called on node store later in the indexing flow
                definitionBuilder.setProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED, seed);

                indexName = ElasticIndexNameHelper.
                        getRemoteIndexName(elasticConnection.getIndexPrefix(), indexDefinition.getIndexPath(), seed);

                provisionIndex();
            } catch (IOException e) {
                throw new IllegalStateException("Unable to provision index", e);
            }
        } else indexName = indexDefinition.getIndexAlias();
        this.bulkProcessorHandler = ElasticBulkProcessorHandler
                .getBulkProcessorHandler(elasticConnection, indexName, indexDefinition, definitionBuilder, commitInfo);
    }

    @TestOnly
    ElasticIndexWriter(@NotNull ElasticIndexTracker indexTracker,
                       @NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull ElasticBulkProcessorHandler bulkProcessorHandler) {
        this.indexTracker = indexTracker;
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.bulkProcessorHandler = bulkProcessorHandler;
        this.indexName = indexDefinition.getIndexAlias();
        this.reindex = false;
    }

    @Override
    public void updateDocument(String path, ElasticDocument doc) {
        IndexRequest request = new IndexRequest(indexName)
                .id(ElasticIndexUtils.idFromPath(path))
                .source(doc.build(), XContentType.JSON);
        bulkProcessorHandler.add(request);
    }

    @Override
    public void deleteDocuments(String path) {
        DeleteRequest request = new DeleteRequest(indexName).id(ElasticIndexUtils.idFromPath(path));
        bulkProcessorHandler.add(request);
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        boolean updateStatus = bulkProcessorHandler.close();
        if (reindex) {
            // if we are closing a writer in reindex mode, it means we need to open the new index for queries
            this.enableIndex();
        }
        if (updateStatus) {
            // update the metrics only when ES has been updated. This is anyway a best-effort attempt since indexes are
            // refreshed asynchronously and the values could be not up-to-date. The metrics will therefore "eventually
            // converge" with the actual index values.
            saveMetrics();
        }
        return updateStatus;
    }

    private void saveMetrics() {
        ElasticIndexNode indexNode = indexTracker.acquireIndexNode(indexDefinition.getIndexPath());
        if (indexNode != null) {
            ElasticIndexStatistics stats = indexNode.getIndexStatistics();
            try {
                indexTracker.getElasticMetricHandler().markDocuments(indexName, indexNode.getIndexStatistics().numDocs());
                indexTracker.getElasticMetricHandler().markSize(indexName, stats.primaryStoreSize(), stats.storeSize());
            } catch (Exception e) {
                LOG.warn("Unable to store metrics for {}", indexNode.getDefinition().getIndexPath(), e);
            } finally {
                indexNode.release();
            }
        }
    }

    private void provisionIndex() throws IOException {
        final ElasticsearchIndicesClient esClient = elasticConnection.getClient().indices();
        // check if index already exists
        if (esClient.exists(i -> i.index(indexName)).value()) {
            LOG.info("Index {} already exists. Skip index provision", indexName);
            return;
        }

        final CreateIndexRequest request = ElasticIndexHelper.createIndexRequest(indexName, indexDefinition);
        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            JsonpUtils.toString(request, sb);
            LOG.debug("Creating Index with request {}", sb);
        }
        // create the new index
        try {
            final CreateIndexResponse response = esClient.create(request);
            LOG.info("Created index {}. Response acknowledged: {}", indexName, response.acknowledged());
            checkResponseAcknowledgement(response, "Create index call not acknowledged for index " + indexName);
        } catch (ElasticsearchStatusException ese) {
            // We already check index existence as first thing in this method, if we get here it means we have got into
            // a conflict (eg: multiple cluster nodes provision concurrently).
            // Elasticsearch does not have a CREATE IF NOT EXIST, need to inspect exception
            // https://github.com/elastic/elasticsearch/issues/19862
            if (ese.status().getStatus() == 400 && ese.getDetailedMessage().contains("resource_already_exists_exception")) {
                LOG.warn("Index {} already exists. Ignoring error", indexName);
            } else {
                throw ese;
            }
        }
    }

    private void enableIndex() throws IOException {
        ElasticsearchIndicesClient client = elasticConnection.getClient().indices();
        // check if index already exists
        if (!client.exists(i -> i.index(indexName)).value()) {
            throw new IllegalStateException("cannot enable an index that does not exist");
        }

        UpdateSettingsRequest request = ElasticIndexHelper.enableIndexRequest(indexName, indexDefinition);
        if (LOG.isDebugEnabled()) {
            final String requestMsg = Strings.toString(request.toXContent(jsonBuilder(), EMPTY_PARAMS));
            LOG.debug("Updating Index Settings with request {}", requestMsg);
        }
        IndicesClient oldClient = elasticConnection.getOldClient().indices();
        AcknowledgedResponse response = oldClient.putSettings(request, RequestOptions.DEFAULT);
        LOG.info("Updated settings for index {}. Response acknowledged: {}", indexName, response.isAcknowledged());
        checkResponseAcknowledgement(response, "Update index settings call not acknowledged for index " + indexName);

        // update the alias
        GetAliasResponse aliasResponse = client.getAlias(garb ->
                garb.index(indexDefinition.getIndexAlias()).ignoreUnavailable(true));

        UpdateAliasesRequest updateAliasesRequest = UpdateAliasesRequest.of(rb -> {
            aliasResponse.result().forEach((idx, idxAliases) -> rb.actions(ab -> // remove old aliases
                    ab.remove(rab -> rab.index(idx).aliases(new ArrayList<>(idxAliases.aliases().keySet()))))
            );
            return rb.actions(ab -> ab.add(aab -> aab.index(indexName).alias(indexDefinition.getIndexAlias()))); // add new one
        });
        UpdateAliasesResponse updateAliasesResponse = client.updateAliases(updateAliasesRequest);
        checkResponseAcknowledgement(updateAliasesResponse, "Update alias call not acknowledged for alias "
                + indexDefinition.getIndexAlias());
        LOG.info("Updated alias {} to index {}. Response acknowledged: {}", indexDefinition.getIndexAlias(),
                indexName, updateAliasesResponse.acknowledged());

        // once the alias has been updated, we can safely remove the old index
        deleteOldIndices(client, aliasResponse.result().keySet());
    }

    private void checkResponseAcknowledgement(AcknowledgedResponse response, String exceptionMessage) {
        if (!response.isAcknowledged()) {
            throw new IllegalStateException(exceptionMessage);
        }
    }

    private void checkResponseAcknowledgement(AcknowledgedResponseBase response, String exceptionMessage) {
        if (!response.acknowledged()) {
            throw new IllegalStateException(exceptionMessage);
        }
    }

    private void checkResponseAcknowledgement(CreateIndexResponse response, String exceptionMessage) {
        if (!response.acknowledged()) {
            throw new IllegalStateException(exceptionMessage);
        }
    }

    private void deleteOldIndices(ElasticsearchIndicesClient indicesClient, Set<String> indices) throws IOException {
        if (indices.size() == 0)
            return;
        DeleteIndexResponse deleteIndexResponse = indicesClient.delete(db -> db.index(new ArrayList<>(indices)));
        checkResponseAcknowledgement(deleteIndexResponse, "Delete index call not acknowledged for indices " + indices);
        LOG.info("Deleted indices {}. Response acknowledged: {}", indices, deleteIndexResponse.acknowledged());
    }
}
