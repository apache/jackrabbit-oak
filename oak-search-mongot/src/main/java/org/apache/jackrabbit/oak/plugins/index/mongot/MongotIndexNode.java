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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

public class MongotIndexNode implements IndexNode {

    private final MongoConnection connection;
    private final MongotIndexDefinition definition;
    private final MongotIndexStatistics statistics;

    public MongotIndexNode(NodeState root, String indexPath, MongoConnection connection) {
        this.connection = connection;
        this.definition = new MongotIndexDefinition(
                root, NodeStateUtils.getNode(root, indexPath), indexPath);
        this.statistics = new MongotIndexStatistics(connection, definition);
    }

    @Override
    public void release() {
        // Managed by MongotIndexNodeManager.
    }

    @Override
    public MongotIndexDefinition getDefinition() {
        return definition;
    }

    @Override
    public int getIndexNodeId() {
        return 0;
    }

    @Override
    public MongotIndexStatistics getIndexStatistics() {
        return statistics;
    }

    public MongoConnection getConnection() {
        return connection;
    }
}
