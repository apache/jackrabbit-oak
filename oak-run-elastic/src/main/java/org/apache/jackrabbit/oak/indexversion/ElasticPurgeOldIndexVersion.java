/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.indexversion;

import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.TYPE_ELASTICSEARCH;

public class ElasticPurgeOldIndexVersion extends PurgeOldIndexVersion {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticPurgeOldIndexVersion.class);

    private final String indexPrefix;
    private final String scheme;
    private final String host;
    private final int port;
    private final String apiKeyId;
    private final String apiSecretId;
    private long SEED_VALUE;

    public ElasticPurgeOldIndexVersion(String indexPrefix, String scheme, String host, int port, String apiKeyId, String apiSecretId) {
        super();
        this.indexPrefix = indexPrefix;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.apiKeyId = apiKeyId;
        this.apiSecretId = apiSecretId;
    }

    @Override
    protected String getIndexType() {
        return TYPE_ELASTICSEARCH;
    }

    @Override
    protected void postDeleteOp(String idxPath) {
        final ElasticConnection.Builder.BuildStep buildStep = ElasticConnection.newBuilder()
                .withIndexPrefix(indexPrefix)
                .withConnectionParameters(
                        scheme,
                        host,
                        port
                );
        final ElasticConnection coordinate;
        if (apiKeyId != null && apiSecretId != null) {
            coordinate = buildStep.withApiKeys(apiKeyId, apiSecretId).build();
        } else {
            coordinate = buildStep.build();
        }

        closer.register(coordinate);

        String remoteIndexName = ElasticIndexNameHelper.getRemoteIndexName(indexPrefix, idxPath, SEED_VALUE);

        ElasticsearchIndicesClient client = coordinate.getClient().indices();
        DeleteIndexResponse deleteIndexResponse;
        try {
            deleteIndexResponse = client.delete(db -> db.index(remoteIndexName));
            LOG.info("Deleted index operation called on {}. Response acknowledged: {}", remoteIndexName, deleteIndexResponse.acknowledged());
            if (!deleteIndexResponse.acknowledged()) {
                LOG.warn("Delete index call not acknowledged for index {}. " +
                        "This could potentially mean the index is left in dangling state on ES cluster and needs to be removed manually", remoteIndexName);
            }
        } catch (IOException e) {
            LOG.error("Exception while deleting index {}", remoteIndexName, e);
        }
    }

    @Override
    protected void preserveDetailsFromIndexDefForPostOp(NodeBuilder builder) {
        PropertyState seedProp = builder.getProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED);
        if (seedProp == null) {
            throw new IllegalStateException("Index full name cannot be computed without name seed");
        }
        SEED_VALUE = seedProp.getValue(Type.LONG);
    }

    @Override
    protected IndexVersionOperation getIndexVersionOperationInstance(IndexName indexName) {
        return new ElasticIndexVersionOperation(indexName);
    }
}
