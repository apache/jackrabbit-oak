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
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An index that traverses over a given subtree.
 */
public class TraversingIndex implements QueryIndex {

    @Override
    public Cursor query(Filter filter, String revisionId, NodeState root) {
        return new TraversingCursor(filter, root);
    }

    @Override
    public double getCost(Filter filter) {
        String path = filter.getPath();
        // TODO estimate or read the node count
        double nodeCount = 10000000;
        if (!PathUtils.denotesRoot(path)) {
            for (int depth = PathUtils.getDepth(path); depth > 0; depth--) {
                // estimate 10 child nodes per node
                nodeCount /= 10;
            }
        }
        return nodeCount;
    }

    @Override
    public String getPlan(Filter filter) {
        String p = filter.getPath();
        String r = filter.getPathRestriction().toString();
        if (PathUtils.denotesRoot(p)) {
            p = "";
        }
        return "traverse \"" + p + r + '"';
    }

    @Override
    public String getIndexName() {
        return "traverse";
    }

}
