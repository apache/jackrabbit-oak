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

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexNodeManager;
import org.apache.jackrabbit.oak.plugins.index.search.update.ReaderRefreshPolicy;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public final class MongotIndexNodeManager extends IndexNodeManager<MongotIndexNode> {

    private final String path;
    private final MongotIndexNode indexNode;

    MongotIndexNodeManager(String path, NodeState root, MongoConnection connection) {
        this.path = path;
        this.indexNode = new MongotIndexNode(root, path, connection) {
            @Override
            public void release() {
                MongotIndexNodeManager.this.release();
            }
        };
    }

    @Override
    protected String getName() {
        return path;
    }

    @Override
    protected MongotIndexNode getIndexNode() {
        return indexNode;
    }

    @Override
    protected IndexDefinition getDefinition() {
        return indexNode.getDefinition();
    }

    @Override
    protected ReaderRefreshPolicy getReaderRefreshPolicy() {
        return ReaderRefreshPolicy.NEVER;
    }

    @Override
    protected void refreshReaders() {
        // Mongot owns Search reader refresh.
    }

    @Override
    protected void releaseResources() {
        // The shared service owns MongoConnection.
    }
}
