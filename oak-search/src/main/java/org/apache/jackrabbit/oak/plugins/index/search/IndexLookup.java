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

import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

/**
 * Allows to check which indexes can possibly be used for a certain query.
 *
 * For example, for a query of the form "/jcr:root/content//*", the indexes
 * under "/" and the indexes under "/content" can be used.
 */
public class IndexLookup {

    private final NodeState root;
    private final Predicate<NodeState> definitionPredicate;

    public IndexLookup(NodeState root, Predicate<NodeState> definitionPredicate) {
        this.root = root;
        this.definitionPredicate = definitionPredicate;
    }

    public Collection<String> collectIndexNodePaths(Filter filter) {
        return collectIndexNodePaths(filter, true);
    }

    public Collection<String> collectIndexNodePaths(Filter filter, boolean recurse) {
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

    private void collectIndexNodePaths(NodeState nodeState, String parentPath, Collection<String> paths) {
        NodeState state = nodeState.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (definitionPredicate.test(entry.getNodeState())) {
                paths.add(createIndexNodePath(parentPath, entry.getName()));
            }
        }
    }

    private static String createIndexNodePath(String parentPath, String name){
        return PathUtils.concat(parentPath, INDEX_DEFINITIONS_NAME, name);
    }
}
