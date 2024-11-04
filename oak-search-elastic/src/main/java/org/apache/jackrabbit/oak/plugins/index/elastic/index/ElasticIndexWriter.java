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
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsResponse;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import co.elastic.clients.json.JsonpUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncLaneSwitcher;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

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
                long seed = indexDefinition.indexNameSeed == 0L ? UUID.randomUUID().getMostSignificantBits() : indexDefinition.indexNameSeed;
                // merge gets called on node store later in the indexing flow
                definitionBuilder.setProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED, seed);
                // let's store the current mapping version in the index definition
                definitionBuilder.setProperty(ElasticIndexDefinition.PROP_INDEX_MAPPING_VERSION, ElasticIndexDefinition.MAPPING_VERSION.toString());

                indexName = ElasticIndexNameHelper.
                        getRemoteIndexName(elasticConnection.getIndexPrefix(), indexDefinition.getIndexPath(), seed);

                provisionIndex();
            } catch (IOException e) {
                throw new IllegalStateException("Unable to provision index", e);
            }
        } else indexName = indexDefinition.getIndexAlias();
        boolean waitForESAcknowledgement = true;
        PropertyState async = indexDefinition.getDefinitionNodeState().getProperty("async");
        if (async != null) {
            // Check if this indexing call is a part of async cycle or a commit hook or called from oak-run for offline reindex
            // In case it's from async cycle - commit info will have a indexingCheckpointTime key.
            // Otherwise, it's part of commit hook based indexing due to async property having a value nrt
            // If the IndexDefinition has a property async-previous set, this implies it's being called from oak-run for offline-reindex.
            // we need to set waitForESAcknowledgement = false only in the second case i.e.
            // when this is a part of commit hook due to async property having a value nrt
            if (!(commitInfo.getInfo().containsKey(IndexConstants.CHECKPOINT_CREATION_TIME) || AsyncLaneSwitcher.isLaneSwitched(definitionBuilder))) {
                waitForESAcknowledgement = false;
            }
        }

        this.bulkProcessorHandler = ElasticBulkProcessorHandler
                .getBulkProcessorHandler(elasticConnection, indexName, indexDefinition, definitionBuilder, commitInfo, waitForESAcknowledgement);
    }

    @TestOnly
    ElasticIndexWriter(@NotNull ElasticIndexTracker indexTracker,
                       @NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull ElasticBulkProcessorHandler bulkProcessorHandler) {
        this(indexTracker, elasticConnection, indexDefinition, bulkProcessorHandler, false);
    }

    @TestOnly
    ElasticIndexWriter(@NotNull ElasticIndexTracker indexTracker,
                       @NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull ElasticBulkProcessorHandler bulkProcessorHandler,
                       boolean reindex) {
        this.indexTracker = indexTracker;
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.bulkProcessorHandler = bulkProcessorHandler;
        this.indexName = indexDefinition.getIndexAlias();
        this.reindex = reindex;
    }

    @Override
    public void updateDocument(String path, ElasticDocument doc) throws IOException {
        // update is a heavier operation compared to index, we can always use the index operation on full reindex
        // or if the index is not externally modifiable
        if (reindex || !indexDefinition.isExternallyModifiable()) {
            bulkProcessorHandler.index(ElasticIndexUtils.idFromPath(path), doc);
        } else {
            bulkProcessorHandler.update(ElasticIndexUtils.idFromPath(path), doc);
        }
    }

    @Override
    public void deleteDocuments(String path) throws IOException {
        bulkProcessorHandler.delete(ElasticIndexUtils.idFromPath(path));
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
            try {
                ElasticIndexStatistics stats = indexNode.getIndexStatistics();
                indexTracker.getElasticMetricHandler().markDocuments(indexName, stats.numDocs());
                indexTracker.getElasticMetricHandler().markSize(indexName, stats.primaryStoreSize(), stats.storeSize());
            } catch (Exception e) {
                LOG.warn("Unable to store metrics for {}", indexDefinition.getIndexPath(), e);
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
        } catch (ElasticsearchException ese) {
            // We already check index existence as first thing in this method, if we get here it means we have got into
            // a conflict (eg: multiple cluster nodes provision concurrently).
            // Elasticsearch does not have a CREATE IF NOT EXIST, need to inspect exception
            // https://github.com/elastic/elasticsearch/issues/19862
            if (ese.status() == 400 && ese.getMessage().contains("resource_already_exists_exception")) {
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

        PutIndicesSettingsRequest request = ElasticIndexHelper.enableIndexRequest(indexName, indexDefinition);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating Index Settings with request {}", request);
        }
        PutIndicesSettingsResponse response = client.putSettings(request);
        LOG.info("Updated settings for index {}. Response acknowledged: {}", indexName, response.acknowledged());
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
        if (indices.isEmpty())
            return;
        DeleteIndexResponse deleteIndexResponse = indicesClient.delete(db -> db.index(new ArrayList<>(indices)));
        checkResponseAcknowledgement(deleteIndexResponse, "Delete index call not acknowledged for indices " + indices);
        LOG.info("Deleted indices {}. Response acknowledged: {}", indices, deleteIndexResponse.acknowledged());
    }
}
