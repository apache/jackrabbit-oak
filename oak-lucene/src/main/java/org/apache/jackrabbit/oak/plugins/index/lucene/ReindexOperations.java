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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorContext.configureUniqueId;

public class ReindexOperations {
    private final NodeState root;
    private final NodeBuilder definitionBuilder;
    private final String indexPath;

    public ReindexOperations(NodeState root, NodeBuilder definitionBuilder, String indexPath) {
        this.root = root;
        this.definitionBuilder = definitionBuilder;
        this.indexPath = indexPath;
    }

    public IndexDefinition apply(boolean useStateFromBuilder) {
        IndexFormatVersion version = IndexDefinition.determineVersionForFreshIndex(definitionBuilder);
        definitionBuilder.setProperty(IndexDefinition.INDEX_VERSION, version.getVersion());

        //Avoid obtaining the latest NodeState from builder as that would force purge of current transient state
        //as index definition does not get modified as part of IndexUpdate run in most case we rely on base state
        //For case where index definition is rewritten there we get fresh state
        NodeState defnState = useStateFromBuilder ? definitionBuilder.getNodeState() : definitionBuilder.getBaseState();
        if (!IndexDefinition.isDisableStoredIndexDefinition()) {
            definitionBuilder.setChildNode(INDEX_DEFINITION_NODE, NodeStateCloner.cloneVisibleState(defnState));
        }
        String uid = configureUniqueId(definitionBuilder);

        //Refresh the index definition based on update builder state
        return IndexDefinition
                .newBuilder(root, defnState, indexPath)
                .version(version)
                .uid(uid)
                .reindex()
                .build();
    }
}
