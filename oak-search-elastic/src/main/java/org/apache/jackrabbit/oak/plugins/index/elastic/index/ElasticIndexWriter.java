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

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

class ElasticIndexWriter implements FulltextIndexWriter<ElasticDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexWriter.class);

    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;

    private final ElasticBulkProcessorHandler bulkProcessorHandler;

    ElasticIndexWriter(@NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull NodeBuilder definitionBuilder,
                       CommitInfo commitInfo) {
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.bulkProcessorHandler = ElasticBulkProcessorHandler
                .getBulkProcessorHandler(elasticConnection, indexDefinition, definitionBuilder, commitInfo);
    }

    @TestOnly
    ElasticIndexWriter(@NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull ElasticBulkProcessorHandler bulkProcessorHandler) {
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.bulkProcessorHandler = bulkProcessorHandler;
    }

    @Override
    public void updateDocument(String path, ElasticDocument doc) {
        IndexRequest request = new IndexRequest(indexDefinition.getRemoteIndexAlias())
                .id(ElasticIndexUtils.idFromPath(path))
                .source(doc.build(), XContentType.JSON);
        bulkProcessorHandler.add(request);
    }

    @Override
    public void deleteDocuments(String path) {
        DeleteRequest request = new DeleteRequest(indexDefinition.getRemoteIndexAlias())
                .id(ElasticIndexUtils.idFromPath(path));
        bulkProcessorHandler.add(request);
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        return bulkProcessorHandler.close();
    }

    protected void provisionIndex(long seed) throws IOException {
        // check if index already exists
        final String indexName = ElasticIndexNameHelper.getRemoteIndexName(indexDefinition, seed);
        boolean exists = elasticConnection.getClient().indices().exists(
                new GetIndexRequest(indexName), RequestOptions.DEFAULT
        );
        if (exists) {
            LOG.info("Index {} already exists. Skip index provision", indexName);
            return;
        }

        final IndicesClient indicesClient = elasticConnection.getClient().indices();

        // create the new index
        final CreateIndexRequest request = ElasticIndexHelper.createIndexRequest(indexName, indexDefinition);
        try {
            if (LOG.isDebugEnabled()) {
                final String requestMsg = Strings.toString(request.toXContent(jsonBuilder(), EMPTY_PARAMS));
                LOG.debug("Creating Index with request {}", requestMsg);
            }
            CreateIndexResponse response = indicesClient.create(request, RequestOptions.DEFAULT);
            LOG.info("Updated settings for index {}. Response acknowledged: {}",
                    indexName, response.isAcknowledged());
            checkResponseAcknowledgement(response, "Create index call not acknowledged for index " + indexName);
        } catch (ElasticsearchStatusException ese) {
            // We already check index existence as first thing in this method, if we get here it means we have got into
            // a conflict (eg: multiple cluster nodes provision concurrently).
            // Elasticsearch does not have a CREATE IF NOT EXIST, need to inspect exception
            // https://github.com/elastic/elasticsearch/issues/19862
            if (ese.status().getStatus() == 400 && ese.getDetailedMessage().contains("resource_already_exists_exception")) {
                LOG.warn("Index {} already exists. Ignoring error", indexName);
            } else throw ese;
        }

        // update the mapping
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(indexDefinition.getRemoteIndexAlias());
        GetAliasesResponse aliasesResponse = indicesClient.getAlias(getAliasesRequest, RequestOptions.DEFAULT);
        Map<String, Set<AliasMetadata>> aliases = aliasesResponse.getAliases();
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

        // once the alias has been updated, we can safely remove the old index
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
}
