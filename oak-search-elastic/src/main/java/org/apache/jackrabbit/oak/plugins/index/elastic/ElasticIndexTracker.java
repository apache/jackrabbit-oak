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

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
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
        // for Elastic, we are not interested in checking for updates on :status, index definition is enough.
        // The :status gets updated every time the indexed content is changed (with properties like last_update_ts),
        // removing the check on :status reduces drastically the contention between queries (that need to acquire the
        // read lock) and updates (need to acquire the write lock).
        // Moreover, we don't check diffs in stored index definitions since are not created for elastic.
        return !IgnoreStatusDiff.equals(before, after);
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

    static class IgnoreStatusDiff extends EqualsDiff {

        public static boolean equals(NodeState before, NodeState after) {
            return before.exists() == after.exists()
                    && after.compareAgainstBaseState(before, new IgnoreStatusDiff());
        }

        /**
         * The behaviour of this method is not immediately obvious from the signature.
         * For this reason, the javadoc from NodeStateDiff is copied below.
         * When false is returned, the comparison is aborted because something has changed.
         * If the changed node is the status node, the comparison is continued otherwise we call the super method.
         * The super method will return false if the node is not the status node, and it has changed.
         * <p>
         * Called for all child nodes that may contain changes between the before
         * and after states. The comparison implementation is expected to make an
         * effort to avoid calling this method on child nodes under which nothing
         * has changed.
         *
         * @param name name of the changed child node
         * @param before child node state before the change
         * @param after child node state after the change
         * @return {@code true} to continue the comparison, {@code false} to abort.
         *         Abort will stop comparing completely, that means sibling nodes
         *         and sibling nodes of all parents are not further compared.
         */
        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            return IndexDefinition.STATUS_NODE.equals(name) || super.childNodeChanged(name, before, after);
        }
    }
}
