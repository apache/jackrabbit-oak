/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.old;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class PropertyIndexer implements QueryIndexProvider, CommitHook,
        PropertyIndexConstants {

    private final String indexConfigPath = IndexUtils.DEFAULT_INDEX_HOME;

    private final Indexer indexer;

    public PropertyIndexer(Indexer indexer) {
        this.indexer = indexer;
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {
        // TODO update index data see OAK-298
        return after;
    }

    @Override
    @Nonnull
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        List<QueryIndex> queryIndexList = new ArrayList<QueryIndex>();
        List<IndexDefinition> indexDefinitions = IndexUtils
                .buildIndexDefinitions(nodeState, indexConfigPath,
                        INDEX_TYPE_PROPERTY);
        for (IndexDefinition def : indexDefinitions) {
            NodeBuilder builder = childBuilder(nodeState, def.getPath());
            for (String k : builder.getChildNodeNames()) {
                PropertyIndex prop = PropertyIndex.fromNodeName(indexer, k);
                if (prop != null) {
                    // create the :data node
                    builder.child(prop.getIndexNodeName()).child(INDEX_CONTENT);
                    queryIndexList.add(new PropertyContentIndex(prop));
                }
                PrefixIndex pref = PrefixIndex.fromNodeName(indexer, k);
                if (pref != null) {
                    // create the :data node
                    builder.child(pref.getIndexNodeName()).child(INDEX_CONTENT);
                    queryIndexList.add(new PrefixContentIndex(pref));
                }
            }
            // create the global :data node
            builder.child(INDEX_CONTENT);
        }
        return queryIndexList;
    }

    private static NodeBuilder childBuilder(NodeState state, String path) {
        NodeBuilder builder = state.builder();
        for (String p : elements(path)) {
            builder = builder.child(p);
        }
        return builder;
    }
}
