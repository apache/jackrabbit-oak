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
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ElasticIndexNode implements IndexNode {

    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;
    private final ElasticIndexStatistics indexStatistics;
    private final ElasticMetricHandler elasticMetricHandler;

    public ElasticIndexNode(@NotNull NodeState root, @NotNull String indexPath,
                            @NotNull ElasticConnection elasticConnection, @NotNull ElasticMetricHandler elasticMetricHandler) {
        final NodeState indexNS = NodeStateUtils.getNode(root, indexPath);
        this.elasticConnection = elasticConnection;
        this.elasticMetricHandler = elasticMetricHandler;
        this.indexDefinition = new ElasticIndexDefinition(root, indexNS, indexPath, elasticConnection.getIndexPrefix());
        this.indexStatistics = new ElasticIndexStatistics(elasticConnection, indexDefinition);
    }

    @Override
    public void release() {
        // do nothing
    }

    @Override
    public ElasticIndexDefinition getDefinition() {
        return indexDefinition;
    }

    public ElasticConnection getConnection() {
        return elasticConnection;
    }

    @Override
    public int getIndexNodeId() {
        // TODO: does it matter that we simply return 0 as there's no observation based _refresh_ going on here
        // and we always defer to ES to its own thing
        return 0;
    }

    @Override
    public @Nullable IndexStatistics getIndexStatistics() {
        return indexStatistics;
    }

    public ElasticMetricHandler getElasticMetricHandler() {
        return elasticMetricHandler;
    }
}
