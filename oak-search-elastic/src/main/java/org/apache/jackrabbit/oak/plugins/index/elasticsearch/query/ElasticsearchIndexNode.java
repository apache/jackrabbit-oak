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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.query;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchConnection;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDescriptor;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ElasticsearchIndexNode implements IndexNode {

    private final ElasticsearchIndexDefinition indexDefinition;
    private final ElasticsearchIndexDescriptor indexDescriptor;

    protected ElasticsearchIndexNode(@NotNull NodeState root, @NotNull String indexPath,
                                     @NotNull ElasticsearchConnection elasticsearchConnection) {
        final NodeState indexNS = NodeStateUtils.getNode(root, indexPath);
        this.indexDefinition = new ElasticsearchIndexDefinition(root, indexNS, indexPath);
        this.indexDescriptor = new ElasticsearchIndexDescriptor(elasticsearchConnection, indexDefinition);
    }

    @Override
    public void release() {
        // do nothing
    }

    @Override
    public ElasticsearchIndexDefinition getDefinition() {
        return indexDefinition;
    }

    public ElasticsearchIndexDescriptor getIndexDescriptor() {
        return indexDescriptor;
    }

    @Override
    public int getIndexNodeId() {
        // TODO: does it matter that we simply return 0 as there's no observation based _refresh_ going on here
        // and we always defer to ES to its own thing
        return 0;
    }

    @Override
    public @Nullable IndexStatistics getIndexStatistics() {
        return new ElasticsearchIndexStatistics(indexDescriptor);
    }
}
