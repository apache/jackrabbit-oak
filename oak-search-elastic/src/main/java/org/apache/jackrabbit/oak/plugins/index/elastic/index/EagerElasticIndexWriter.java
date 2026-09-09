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

import co.elastic.clients.elasticsearch._types.AcknowledgedResponse;
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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConfig;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceIndexConfig;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncLaneSwitcher;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
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

class EagerElasticIndexWriter implements ElasticIndexWriter {
    private static final Logger LOG = LoggerFactory.getLogger(EagerElasticIndexWriter.class);

    private final ElasticIndexTracker indexTracker;
    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;
    private final ElasticBulkProcessorHandler bulkProcessorHandler;
    private final boolean requiresProvisioning;
    private final String indexName;
    private final ElasticRetryPolicy retryPolicy;

    EagerElasticIndexWriter(@NotNull ElasticIndexTracker indexTracker,
                       @NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull NodeBuilder definitionBuilder,
                       boolean requiresProvisioning, CommitInfo commitInfo,
                       ElasticBulkProcessorHandler bulkProcessorHandler,
                       ElasticRetryPolicy retryPolicy) {
        this.indexTracker = indexTracker;
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.requiresProvisioning = requiresProvisioning;
        this.bulkProcessorHandler = bulkProcessorHandler;
        this.retryPolicy = retryPolicy;

        if (requiresProvisioning) {
            // Full provisioning: generate a seed-based backing index, create it in ES, and prepare
            // for alias flip on close(). Applies to both a standard reindex and an incremental write
            // arriving after a lazy reindex that produced zero documents (OAK-12249).
            try {
                //TODO we should observe changes under inference config path.
                InferenceConfig.reInitialize();
                long seed = indexDefinition.indexNameSeed == 0L ? UUID.randomUUID().getMostSignificantBits() : indexDefinition.indexNameSeed;
                definitionBuilder.setProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED, seed);
                definitionBuilder.setProperty(ElasticIndexDefinition.PROP_INDEX_MAPPING_VERSION, ElasticIndexDefinition.MAPPING_VERSION.toString());
                definitionBuilder.removeProperty(ElasticIndexDefinition.PROP_REQUIRES_PROVISIONING);
                indexName = ElasticIndexNameHelper.
                        getRemoteIndexName(elasticConnection.getIndexPrefix(), indexDefinition.getIndexPath(), seed);
                provisionIndex();
            } catch (IOException e) {
                throw new IllegalStateException("Unable to provision index", e);
            }
        } else {
            indexName = indexDefinition.getIndexAlias();
        }
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
        bulkProcessorHandler.registerIndex(indexName, indexDefinition, definitionBuilder, commitInfo, waitForESAcknowledgement);
    }

    @TestOnly
    EagerElasticIndexWriter(@NotNull ElasticIndexTracker indexTracker,
                       @NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull ElasticBulkProcessorHandler bulkProcessorHandler) {
        this(indexTracker, elasticConnection, indexDefinition, bulkProcessorHandler, ElasticRetryPolicy.NO_RETRY, false);
    }

    @TestOnly
    EagerElasticIndexWriter(@NotNull ElasticIndexTracker indexTracker,
                       @NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull ElasticBulkProcessorHandler bulkProcessorHandler,
                       @NotNull ElasticRetryPolicy retryPolicy,
                       boolean requiresProvisioning) {
        this.indexTracker = indexTracker;
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.bulkProcessorHandler = bulkProcessorHandler;
        this.indexName = indexDefinition.getIndexAlias();
        this.retryPolicy = retryPolicy;
        this.requiresProvisioning = requiresProvisioning;
    }

    @Override
    public void updateDocument(String path, ElasticDocument doc) throws IOException {
        // update is a heavier operation compared to index, we can always use the index operation on full reindex
        // or if the index is not externally modifiable
        String jcrIndexName = PathUtils.getName(indexDefinition.getIndexName());
        /*
            we directly index the document if:
            content is being reindexed
            OR
            (the index is not externally modifiable
            AND InferenceIndexConfig is NOOP
            )
         */
        if (requiresProvisioning
            || (!indexDefinition.isExternallyModifiable()
            && !InferenceConfig.getInstance().isInferenceEnabled()
            && (InferenceIndexConfig.NOOP.equals(InferenceConfig.getInstance().getInferenceIndexConfig(jcrIndexName))))) {
            retryPolicy.withRetries(() -> bulkProcessorHandler.index(indexName, ElasticIndexUtils.idFromPath(path), doc));
        } else {
            retryPolicy.withRetries(() -> bulkProcessorHandler.update(indexName, ElasticIndexUtils.idFromPath(path), doc));
        }
    }

    @Override
    public void deleteDocumentTree(String path) throws IOException {
        retryPolicy.withRetries(() -> bulkProcessorHandler.delete(indexName, ElasticIndexUtils.idFromPath(path)));
        if (!ElasticIndexEditorProvider.FT_OAK_12206_DISABLE.get()) {
            // Delete all descendants: mirrors Lucene's PrefixQuery on the path term.
            // The :ancestors field is indexed with path_hierarchy, so a term query on `path`
            // matches every document whose ancestor chain includes that path.
            // The ES Bulk API does not support delete by query, so we need to issue a separate request.
            // This is not ideal but should be ok since deletes are expected to be less frequent than updates.
            // The alternative would be to get the list of affected documents and issue a bulk delete by id,
            // but that would be more complex and potentially more expensive (if there are many descendants).
            retryPolicy.withRetries(() -> {
                var response = elasticConnection.getClient().deleteByQuery(
                        d -> d.index(indexName).query(q -> q.term(t -> t.field(FieldNames.ANCESTORS).value(path))));
                response.failures().forEach(f -> LOG.warn("Failed to delete descendants of {}: shard {} reason {}", path, f.id(), f.cause()));
                if (response.deleted() != null && response.deleted() > 0) {
                    LOG.info("Deleted {} descendants of {} in {} ms", response.deleted(), path, response.took());
                }
            });
        }
    }

    @Override
    public void deleteDocument(String path) throws IOException {
        // Exact-document delete: no descendant sweep
        retryPolicy.withRetries(() -> bulkProcessorHandler.delete(indexName, ElasticIndexUtils.idFromPath(path)));
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        boolean updateStatus = bulkProcessorHandler.flushIndex(indexName);
        if (requiresProvisioning) {
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
        if (esClient.exists(i -> i.index(indexName)).value()) {
            LOG.info("Index {} already exists. Skip index provision", indexName);
            return;
        }
        createIndex(indexName);
    }

    /**
     * Builds a {@link CreateIndexRequest} for {@code backingIndexName} and submits it to
     * Elasticsearch, with debug logging and idempotent handling of concurrent-creation races.
     */
    private void createIndex(String backingIndexName) throws IOException {
        final ElasticsearchIndicesClient esClient = elasticConnection.getClient().indices();
        CreateIndexRequest request;
        try {
            request = ElasticIndexHelper.createIndexRequest(backingIndexName, indexDefinition);
        } catch (Exception e) {
            LOG.error("Failed to create index {}: {}", backingIndexName, e.toString());
            throw e;
        }
        if (LOG.isDebugEnabled()) {
            int old = JsonpUtils.maxToStringLength();
            try {
                JsonpUtils.maxToStringLength(1_000_000);
                LOG.debug("Creating Index with request {}", request);
            } finally {
                JsonpUtils.maxToStringLength(old);
            }
        }
        try {
            final CreateIndexResponse response = esClient.create(request);
            LOG.info("Created index {}. Response acknowledged: {}", backingIndexName, response.acknowledged());
            checkResponseAcknowledgement(response, "Create index call not acknowledged for index " + backingIndexName);
        } catch (ElasticsearchException ese) {
            // We already check index existence as first thing in provisionIndex(); if we get here it
            // means a concurrent cluster node raced us. Elasticsearch has no CREATE IF NOT EXISTS:
            // https://github.com/elastic/elasticsearch/issues/19862
            if (ese.status() == 400 && ese.getMessage().contains("resource_already_exists_exception")) {
                LOG.warn("Index {} already exists. Ignoring error", backingIndexName);
            } else {
                LOG.warn("Failed to create index {}", backingIndexName, ese);
                StringBuilder sb = new StringBuilder();
                int old = JsonpUtils.maxToStringLength();
                try {
                    JsonpUtils.maxToStringLength(1_000_000);
                    JsonpUtils.toString(request, sb);
                    String[] array = splitLargeString(sb.toString(), 1024);
                    for (int i = 0; i < array.length; i++) {
                        LOG.warn("request chunk[{}] = {}", i, array[i]);
                    }
                } finally {
                    JsonpUtils.maxToStringLength(old);
                }
                throw ese;
            }
        }
    }

    public static String[] splitLargeString(String largeString, int chunkSize) {
        int totalChunks = (largeString.length() + chunkSize - 1) / chunkSize;
        String[] array = new String[totalChunks];
        for (int i = 0; i < totalChunks; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, largeString.length());
            array[i] = largeString.substring(start, end);
        }
        return array;
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

    private void checkResponseAcknowledgement(AcknowledgedResponse response, String exceptionMessage) {
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
