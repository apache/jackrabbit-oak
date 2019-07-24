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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class ElasticsearchManagementRule extends ExternalResource
        implements ElasticsearchIndexCoordinateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchManagementRule.class);

    private final ElasticsearchDockerRule elasticsearch = new ElasticsearchDockerRule();

    private final ElasticsearchConnectionFactory connectionFactory = new ElasticsearchConnectionFactory();

    private final Set<ElasticsearchIndexCoordinate> indices = Sets.newHashSet();

    private boolean usingDocker;

    @Override
    public Statement apply(Statement base, Description description) {
        Statement s = super.apply(base, description);
        // see if local instance is available... initialize docker rule only if that's not the case
        ElasticsearchCoordinate esCoord = ElasticsearchCoordinateImpl.construct(connectionFactory,
                null, null);
        if (!ElasticsearchTestUtils.isAvailable(esCoord) && elasticsearch.isDockerAvailable()) {
            s = elasticsearch.apply(s, description);
            usingDocker = true;
        }

        return s;
    }

    @Override
    public ElasticsearchIndexCoordinate getElasticsearchIndexCoordinate(IndexDefinition indexDefinition) {
        ElasticsearchCoordinate esCoord = getElasticsearchCoordinate(indexDefinition.getDefinitionNodeState());
        ElasticsearchIndexCoordinate esIdxCoord = new ElasticsearchIndexCoordinateImpl(esCoord, indexDefinition);
        indices.add(esIdxCoord);
        return esIdxCoord;
    }

    @Override
    protected void after() {
        deletedIndices();
        connectionFactory.close();
    }

    private ElasticsearchCoordinate getElasticsearchCoordinate(NodeState indexDefinition) {
        ElasticsearchCoordinate esCoord = ElasticsearchCoordinateImpl.construct(connectionFactory,
                indexDefinition, null);

        if (!ElasticsearchTestUtils.isAvailable(esCoord) && usingDocker) {
            int port = elasticsearch.getPort();
            esCoord = new ElasticsearchCoordinateImpl(connectionFactory, "http", "localhost", port);
        }

        Assume.assumeTrue(ElasticsearchTestUtils.isAvailable(esCoord));

        return esCoord;
    }

    private void deletedIndices() {
        indices.forEach(idxCoord -> {
            DeleteIndexRequest request = new DeleteIndexRequest(idxCoord.getEsIndexName());
            try {
                idxCoord.getClient().indices().delete(request, RequestOptions.DEFAULT);

                LOG.info("Cleaned up index {}", idxCoord.getEsIndexName());

            } catch (IOException e) {
                LOG.warn("Failed to cleanup index {}", idxCoord.getEsIndexName());
            }
        });
    }
}