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

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code NodeTypeIndex} implements a {@link QueryIndex} using
 * {@link org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup}s
 * on {@code jcr:primaryType} and {@code jcr:mixinTypes} to evaluate a node type
 * restriction on {@link Filter}. The cost for this index is the sum of the costs
 * of the {@link org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup}
 * for queries on {@code jcr:primaryType} and {@code jcr:mixinTypes}.
 */
class NodeTypeIndex implements QueryIndex, JcrConstants {

    private final MountInfoProvider mountInfoProvider;

    public NodeTypeIndex(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
    }

    @Override
    public double getMinimumCost() {
        return NodeTypeIndexLookup.MINIMUM_COST;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        // TODO don't call getCost for such queries
        if (filter.getFullTextConstraint() != null) {
            // not an appropriate index for full-text search
            return Double.POSITIVE_INFINITY;
        }
        if (filter.containsNativeConstraint()) {
            // not an appropriate index for native search
            return Double.POSITIVE_INFINITY;
        }
        if (!hasNodeTypeRestriction(filter)) {
            // this is not an appropriate index if the filter
            // doesn't have a node type restriction
            return Double.POSITIVE_INFINITY;
        }
        if (wrongIndex(filter)) {
            return Double.POSITIVE_INFINITY;
        }
        
        NodeTypeIndexLookup lookup = new NodeTypeIndexLookup(root, mountInfoProvider);
        if (lookup.isIndexed(filter.getPath(), filter)) {
            return lookup.getCost(filter);
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }
    
    private static boolean wrongIndex(Filter filter) {
        // skip index if "option(index ...)" doesn't match
        PropertyRestriction indexName = filter.getPropertyRestriction(IndexConstants.INDEX_NAME_OPTION);
        if (indexName != null && indexName.first != null) {
            // index name specified: just verify this, and ignore tags
            return !"nodetype".equals(indexName.first.getValue(Type.STRING));
        }
        PropertyRestriction indexTag = filter.getPropertyRestriction(IndexConstants.INDEX_TAG_OPTION);
        // index tag specified (the nodetype index doesn't support tags)
        return indexTag != null && indexTag.first != null;
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        NodeTypeIndexLookup lookup = new NodeTypeIndexLookup(root, mountInfoProvider);
        if (!hasNodeTypeRestriction(filter) || !lookup.isIndexed(filter.getPath(), filter)) {
            throw new IllegalStateException(
                    "NodeType index is used even when no index is available for filter " + filter);
        }
        return Cursors.newPathCursorDistinct(lookup.query(filter), filter.getQueryLimits());
    }
    
    @Override
    public String getPlan(Filter filter, NodeState root) {
        return "nodeType " + filter.toString();
    }

    @Override
    public String getIndexName() {
        return "nodeType";
    }
    
    //----------------------------< internal >----------------------------------

    private static boolean hasNodeTypeRestriction(Filter filter) {
        return !filter.matchesAllTypes();
    }

}
