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
package org.apache.jackrabbit.oak.plugins.index.nodetype;

import java.util.Set;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Sets;

/**
 * <code>NodeTypeIndex</code> implements a {@link QueryIndex} using
 * {@link Property2IndexLookup}s on <code>jcr:primaryType</code> and
 * <code>jcr:mixinTypes</code> to evaluate a node type restriction on
 * {@link Filter}. The cost for this index is the sum of the costs of the
 * {@link Property2IndexLookup} for queries on <code>jcr:primaryType</code> and
 * <code>jcr:mixinTypes</code>.
 */
class NodeTypeIndex implements QueryIndex, JcrConstants {

    @Override
    public double getCost(Filter filter, NodeState root) {
        if (!hasNodeTypeRestriction(filter)) {
            // this is not an appropriate index if the filter
            // doesn't have a node type restriction
            return Double.POSITIVE_INFINITY;
        }
        NodeTypeIndexLookup lookup = new NodeTypeIndexLookup(root);
        if (lookup.isIndexed(filter.getPath())) {
            return lookup.getCost(resolveNodeType(root, filter.getNodeType()));
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        NodeTypeIndexLookup lookup = new NodeTypeIndexLookup(root);
        if (!hasNodeTypeRestriction(filter) || !lookup.isIndexed(filter.getPath())) {
            throw new IllegalStateException(
                    "NodeType index is used even when no index is available for filter " + filter);
        }
        return Cursors.newPathCursor(lookup.query(
                resolveNodeType(root, filter.getNodeType())));
    }
    
    @Override
    public String getPlan(Filter filter, NodeState root) {
        return "nodeType " + filter.getNodeType() + " path " + filter.getPath();
    }

    @Deprecated
    public Cursor queryOld(Filter filter, NodeState root) {
        NodeTypeIndexLookup lookup = new NodeTypeIndexLookup(root);
        if (!hasNodeTypeRestriction(filter) || !lookup.isIndexed(filter.getPath())) {
            throw new IllegalStateException(
                    "NodeType index is used even when no index is available for filter " + filter);
        }
        return Cursors.newPathCursor(lookup.find(
                resolveNodeType(root, filter.getNodeType())));
    }

    @Override
    public String getIndexName() {
        return "nodeType";
    }
    
    //----------------------------< internal >----------------------------------

    private static boolean hasNodeTypeRestriction(Filter filter) {
        return filter.getNodeType() != null
                && !filter.getNodeType().equals(NT_BASE);
    }

    private static Iterable<String> resolveNodeType(NodeState root, String nodeType) {
        ReadOnlyNodeTypeManager ntMgr = new NTManager(root);
        Set<String> ntNames = Sets.newHashSet();
        try {
            NodeType nt = ntMgr.getNodeType(nodeType);
            for (NodeTypeIterator types = nt.getSubtypes(); types.hasNext();) {
                ntNames.add(types.nextNodeType().getName());
            }
            ntNames.add(nodeType);
        } catch (RepositoryException e) {
            // unknown node type
        }
        return ntNames;
    }

    private static final class NTManager extends ReadOnlyNodeTypeManager {

        private final NodeState root;

        NTManager(NodeState root) {
            this.root = root;
        }

        @Override
        protected Tree getTypes() {
            Tree t = new ReadOnlyTree(root);
            for (String name : PathUtils.elements(NodeTypeConstants.NODE_TYPES_PATH)) {
                t = t.getChild(name);
                if (t == null) {
                    break;
                }
            }
            return t;
        }
    }
}
