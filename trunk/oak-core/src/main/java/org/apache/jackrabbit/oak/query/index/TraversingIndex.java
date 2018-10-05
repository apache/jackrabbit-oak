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
package org.apache.jackrabbit.oak.query.index;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.query.ast.JoinConditionImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An index that traverses over a given subtree.
 */
public class TraversingIndex implements QueryIndex {

    @Override
    public double getMinimumCost() {
        return 0;
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        return Cursors.newTraversingCursor(filter, rootState);
    }
    
    public boolean isPotentiallySlow(Filter filter, NodeState rootState) {
        if (filter.getFullTextConstraint() != null) {
            // not an appropriate index for full-text search
            return true;
        }
        if (filter.containsNativeConstraint()) {
            // not an appropriate index for native search
            return true;
        }
        if (filter.isAlwaysFalse()) {
            return false;
        }
        PathRestriction restriction = filter.getPathRestriction();
        switch (restriction) {
        case EXACT:
        case PARENT:
        case DIRECT_CHILDREN:
            return false;
        case NO_RESTRICTION:
        case ALL_CHILDREN:
            return true;
        default:
            throw new IllegalArgumentException("Unknown restriction: " + restriction);
        }
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        if (filter.getFullTextConstraint() != null) {
            // not an appropriate index for full-text search
            return Double.POSITIVE_INFINITY;
        }
        if (filter.containsNativeConstraint()) {
            // not an appropriate index for native search
            return Double.POSITIVE_INFINITY;
        }
        if (filter.isAlwaysFalse()) {
            return 0;
        }
        // if the path is from a join, then the depth is not correct
        // (the path might be the root node), but that's OK
        String path = filter.getPath();
        PathRestriction restriction = filter.getPathRestriction();
        // the simple cases
        switch (restriction) {
        case EXACT:
            return 1;
        case PARENT:
            if (PathUtils.denotesRoot(path)) {
                return 0;
            }
            return 1;
        case NO_RESTRICTION:
        case ALL_CHILDREN:
        case DIRECT_CHILDREN:
            break;
        default:
            throw new IllegalArgumentException("Unknown restriction: " + restriction);
        }
        
        if (!path.startsWith(JoinConditionImpl.SPECIAL_PATH_PREFIX)) {
            String testPath = path;
            if (restriction == PathRestriction.NO_RESTRICTION) {
                testPath = "/";
            }
            long count = NodeCounter.getEstimatedNodeCount(rootState, testPath, true);
            if (count >= 0) {
                if (restriction == PathRestriction.DIRECT_CHILDREN) {
                    count = count / 2;
                }
                return count;
            }
        }
        
        // worst case 100 million descendant nodes
        double nodeCount = 100000000;
        // worst case 100 thousand children
        double nodeCountChildren = 100000;
        switch (restriction) {
        case NO_RESTRICTION:
            break;
        case ALL_CHILDREN:
            if (!PathUtils.denotesRoot(path)) {
                int depth = PathUtils.getDepth(path);
                for (int i = depth; i > 0; i--) {
                    // estimate 10 child nodes per node,
                    // but higher than the cost for DIRECT_CHILDREN
                    // (about 100'000)
                    // in any case, the higher the depth of the path,
                    // the lower the cost
                    nodeCount = Math.max(nodeCountChildren * 2 - depth, nodeCount / 10);
                }
            }
            break;
        case DIRECT_CHILDREN:
            // estimate 100'000 children for now, 
            // to ensure an index is used if there is one
            // TODO we need to have better estimates, see also OAK-1898
             nodeCount = nodeCountChildren;
            break;
        default:
            throw new IllegalArgumentException("Unknown restriction: " + restriction);
        }        
        return nodeCount;
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        return "traverse \"" + filter.getPathPlan() + '"';
    }

    @Override
    public String getIndexName() {
        return "traverse";
    }

}
