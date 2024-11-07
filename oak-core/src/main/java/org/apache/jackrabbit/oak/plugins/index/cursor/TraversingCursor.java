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
package org.apache.jackrabbit.oak.plugins.index.cursor;

import static org.apache.jackrabbit.oak.spi.query.QueryConstants.REP_FACET;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.query.FilterIterators;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.collect.Iterators;

/**
 * A cursor that reads all nodes in a given subtree.
 */
class TraversingCursor extends AbstractCursor {

    private static final Logger LOG = LoggerFactory.getLogger(TraversingCursor.class);

    private final Filter filter;

    private final Deque<Iterator<? extends ChildNodeEntry>> nodeIterators = new ArrayDeque<>();

    private String parentPath;

    private String currentPath;

    private long readCount;

    private boolean init;
    
    private boolean closed;
    
    private final QueryLimits settings;
    
    public TraversingCursor(Filter filter, NodeState rootState) {
        this.filter = filter;
        this.settings = filter.getQueryLimits();

        String path = filter.getPath();
        parentPath = null;
        currentPath = "/";
        NodeState parent = null;
        NodeState node = rootState;

        if (filter.containsNativeConstraint()) {
            // OAK-4313: if no other index was found,
            // then, for native queries, we won't match anything
            return;
        }

        if (filter.isAlwaysFalse()) {
            // nothing can match this filter, leave nodes empty
            return;
        }

        Filter.PropertyRestriction facetRestriction = filter.getPropertyRestriction(REP_FACET);
        if (facetRestriction != null) {
            // we don't evaluate facets by traversal
            throw new IllegalArgumentException(facetRestriction + " can't be evaluated by traversal");
        }

        if (!path.equals("/")) {
            for (String name : path.substring(1).split("/")) {
                parentPath = currentPath;
                currentPath = PathUtils.concat(parentPath, name);

                parent = node;
                node = parent.getChildNode(name);
            }
            if (!node.exists()) {
                // nothing can match this filter, leave nodes empty
                return;
            }
        }
        Filter.PathRestriction restriction = filter.getPathRestriction();
        switch (restriction) {
        case NO_RESTRICTION:
        case EXACT:
        case ALL_CHILDREN:
            nodeIterators.add(Iterators.singletonIterator(
                    new MemoryChildNodeEntry(currentPath, node)));
            parentPath = "";
            break;
        case PARENT:
            if (parent != null) {
                nodeIterators.add(Iterators.singletonIterator(
                        new MemoryChildNodeEntry(parentPath, parent)));
                parentPath = "";
            }
            break;
        case DIRECT_CHILDREN:
            nodeIterators.add(node.getChildNodeEntries().iterator());
            parentPath = currentPath;
            break;
        default:
            throw new IllegalArgumentException("Unknown restriction: " + restriction);
        }
    }

    @Override
    public IndexRow next() {
        if (closed) {
            throw new IllegalStateException("This cursor is closed");
        }
        if (!init) {
            fetchNext();
            init = true;
        }
        IndexRowImpl result = new IndexRowImpl(currentPath);
        fetchNext();
        return result;
    }

    @Override 
    public boolean hasNext() {
        if (!closed && !init) {
            fetchNext();
            init = true;
        }
        return !closed;
    }

    private void fetchNext() {
        while (!nodeIterators.isEmpty()) {
            Iterator<? extends ChildNodeEntry> iterator = nodeIterators.getLast();
            if (iterator.hasNext()) {
                ChildNodeEntry entry = iterator.next();

                readCount++;
                if (readCount % 1000 == 0) {
                    if (readCount == 20000) {
                        LOG.warn("Traversed {} nodes with filter {}; consider creating an index or changing the query",
                                readCount, filter, new Exception("call stack"));
                    } else {
                        FilterIterators.checkReadLimit(readCount, settings);
                        String caller = IndexUtils.getCaller(this.settings.getIgnoredClassNamesInCallTrace());
                        LOG.warn("Traversed {} nodes with filter {} called by {}; consider creating an index or changing the query",
                                readCount, filter, caller);
                    }
                }

                NodeState node = entry.getNodeState();

                String name = entry.getName();
                if (NodeStateUtils.isHidden(name)) {
                    continue;
                }
                currentPath = PathUtils.concat(parentPath, name);

                PathRestriction r = filter.getPathRestriction();
                if (r == PathRestriction.ALL_CHILDREN || 
                        r == PathRestriction.NO_RESTRICTION) {
                    nodeIterators.addLast(node.getChildNodeEntries().iterator());
                    parentPath = currentPath;
                }
                return;
            } else {
                nodeIterators.removeLast();
                parentPath = PathUtils.getParentPath(parentPath);
            }
        }
        currentPath = null;
        closed = true;
    }

}