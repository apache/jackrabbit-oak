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

import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexTracker;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

public final class MongotIndexTracker
        extends FulltextIndexTracker<MongotIndexNodeManager, MongotIndexNode>
        implements Observer {

    private final MongoConnection connection;

    public MongotIndexTracker(MongoConnection connection) {
        this.connection = connection;
    }

    @Override
    protected MongotIndexNodeManager openIndex(String path, NodeState root, NodeState node) {
        return new MongotIndexNodeManager(path, root, connection);
    }

    public MongotIndexNode acquireIndexNode(String path) {
        return super.acquireIndexNode(path, MongotIndexDefinition.TYPE_MONGOT);
    }

    @Override
    public void contentChanged(@NotNull NodeState root, @NotNull CommitInfo info) {
        update(root);
    }
}
