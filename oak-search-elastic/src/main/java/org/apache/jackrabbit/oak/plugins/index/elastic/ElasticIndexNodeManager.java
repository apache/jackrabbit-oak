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
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexNodeManager;
import org.apache.jackrabbit.oak.plugins.index.search.update.ReaderRefreshPolicy;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ElasticIndexNodeManager extends IndexNodeManager<ElasticIndexNode> {

    private final ElasticIndexNode elasticIndexNode;
    private final String path;

    ElasticIndexNodeManager(String path, NodeState root,
                            ElasticConnection elasticConnection) {
        this.path = path;
        this.elasticIndexNode = new ElasticIndexNode(root, path, elasticConnection) {
            @Override
            public void release() {
                ElasticIndexNodeManager.this.release();
                super.release();
            }
        };
    }

    @Override
    protected String getName() {
        return path;
    }

    @Override
    protected ElasticIndexNode getIndexNode() {
        return elasticIndexNode;
    }

    @Override
    protected IndexDefinition getDefinition() {
        return elasticIndexNode.getDefinition();
    }

    @Override
    protected ReaderRefreshPolicy getReaderRefreshPolicy() {
        return ReaderRefreshPolicy.NEVER;
    }

    @Override
    protected void refreshReaders() {
        // do nothing
    }

    @Override
    protected void releaseResources() {
        // do nothing
    }
}
