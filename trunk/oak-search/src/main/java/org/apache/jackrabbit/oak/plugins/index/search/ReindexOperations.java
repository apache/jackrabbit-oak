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

package org.apache.jackrabbit.oak.plugins.index.search;

import org.apache.jackrabbit.oak.plugins.index.search.util.NodeStateCloner;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext.configureUniqueId;

/**
 * Reindexing operations
 */
public class ReindexOperations {
    private final NodeState root;
    private final NodeBuilder definitionBuilder;
    private final String indexPath;
    private final IndexDefinition.Builder indexDefBuilder;

    public ReindexOperations(NodeState root, NodeBuilder definitionBuilder, String indexPath,
                             IndexDefinition.Builder indexDefBuilder) {
        this.root = root;
        this.definitionBuilder = definitionBuilder;
        this.indexPath = indexPath;
        this.indexDefBuilder = indexDefBuilder;
    }

    /**
     * Update index definition based on base or latest builder state
     * @param useStateFromBuilder whether to use the latest builder state
     * @return the up to date index definition
     */
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
        return indexDefBuilder
                .root(root)
                .defn(defnState)
                .indexPath(indexPath)
                .version(version)
                .uid(uid)
                .reindex()
                .build();
    }
}
