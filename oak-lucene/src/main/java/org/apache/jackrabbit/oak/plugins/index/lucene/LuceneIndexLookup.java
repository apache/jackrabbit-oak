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

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.isLuceneIndexNode;

public class LuceneIndexLookup {
    private final NodeState root;

    public LuceneIndexLookup(NodeState root) {
        this.root = root;
    }

    /**
     * Returns the path of the first Lucene index node which supports
     * fulltext search
     */
    public String getFullTextIndexPath(Filter filter, IndexTracker tracker) {
        Collection<String> indexPaths = collectIndexNodePaths(filter);
        IndexNode indexNode = null;
        for (String path : indexPaths) {
            try {
                indexNode = tracker.acquireIndexNode(path);
                if (indexNode != null
                        && indexNode.getDefinition().isFullTextEnabled()) {
                    return path;
                }
            } finally {
                if (indexNode != null) {
                    indexNode.release();
                }
            }
        }
        return null;
    }

    public Collection<String> collectIndexNodePaths(Filter filter){
        Set<String> paths = Sets.newHashSet();
        collectIndexNodePaths(filter.getPath(), paths);
        return paths;
    }

    private void collectIndexNodePaths(String filterPath, Collection<String> paths) {
        //TODO Add support for looking index nodes from non root paths
        NodeState state = root.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (isLuceneIndexNode(entry.getNodeState())) {
                paths.add(createIndexNodePath(entry.getName()));
            }
        }
    }

    private String createIndexNodePath(String name){
        //TODO getPath would lead to duplicate path constru
        return PathUtils.concat("/", INDEX_DEFINITIONS_NAME, name);
    }
}
