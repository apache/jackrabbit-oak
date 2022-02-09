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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexTracker;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

public class ElasticIndexTracker extends FulltextIndexTracker<ElasticIndexNodeManager, ElasticIndexNode> implements Observer {

    private final ElasticConnection elasticConnection;
    private final ElasticMetricHandler elasticMetricHandler;

    public ElasticIndexTracker(@NotNull ElasticConnection elasticConnection, @NotNull ElasticMetricHandler elasticMetricHandler) {
        this.elasticConnection = elasticConnection;
        this.elasticMetricHandler = elasticMetricHandler;
    }

    @Override
    public boolean isUpdateNeeded(NodeState before, NodeState after) {
        // for Elastic we are not interested in checking for updates on :status, index definition is enough.
        // The :status gets updated every time the indexed content is changed (with properties like last_update_ts),
        // removing the check on :status reduces drastically the contention between queries (that need to acquire the
        // read lock) and updates (need to acquire the write lock).
        // Moreover, we don't check diffs in stored index definitions since are not created for elastic.
        return !EqualsDiff.equals(before, after);
    }

    @Override
    protected ElasticIndexNodeManager openIndex(String path, NodeState root, NodeState node) {
        return new ElasticIndexNodeManager(path, root, elasticConnection);
    }

    public ElasticIndexNode acquireIndexNode(String path) {
        return super.acquireIndexNode(path, ElasticIndexDefinition.TYPE_ELASTICSEARCH);
    }

    @Override
    public void contentChanged(@NotNull NodeState root, @NotNull CommitInfo info) {
        update(root);
    }

    public ElasticMetricHandler getElasticMetricHandler() {
        return elasticMetricHandler;
    }
}
