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

class LuceneIndexLookup {
    private final NodeState root;

    public LuceneIndexLookup(NodeState root) {
        this.root = root;
    }

    /**
     * Returns the path of the first Lucene index node which supports
     * fulltext search
     */
    public String getOldFullTextIndexPath(Filter filter, IndexTracker tracker) {
        Collection<String> indexPaths = collectIndexNodePaths(filter, false);
        IndexNode indexNode = null;
        for (String path : indexPaths) {
            try {
                indexNode = tracker.acquireIndexNode(path);
                if (indexNode != null
                        && indexNode.getDefinition().isFullTextEnabled()
                        && indexNode.getDefinition().getVersion() == IndexFormatVersion.V1) {
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
        return collectIndexNodePaths(filter, true);
    }

    private Collection<String> collectIndexNodePaths(Filter filter, boolean recurse){
        Set<String> paths = Sets.newHashSet();

        collectIndexNodePaths(root, "/", paths);

        if (recurse) {
            StringBuilder sb = new StringBuilder();
            NodeState nodeState = root;
            for (String element : PathUtils.elements(filter.getPath())) {
                nodeState = nodeState.getChildNode(element);
                collectIndexNodePaths(nodeState,
                        sb.append("/").append(element).toString(),
                        paths);
            }
        }

        return paths;
    }

    private static void collectIndexNodePaths(NodeState nodeState, String parentPath, Collection<String> paths) {
        NodeState state = nodeState.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (isLuceneIndexNode(entry.getNodeState())) {
                paths.add(createIndexNodePath(parentPath, entry.getName()));
            }
        }
    }

    private static String createIndexNodePath(String parentPath, String name){
        return PathUtils.concat(parentPath, INDEX_DEFINITIONS_NAME, name);
    }
}
