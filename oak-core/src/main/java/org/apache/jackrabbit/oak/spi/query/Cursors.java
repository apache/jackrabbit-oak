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
package org.apache.jackrabbit.oak.spi.query;

import java.util.Deque;
import java.util.Iterator;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.query.index.TraversingIndex;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;

import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;
import static org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction.ALL_CHILDREN;

/**
 * This utility class provides factory methods to create commonly used types of
 * {@link Cursor}s.
 */
public class Cursors {

    private Cursors() {
    }

    /**
     * Creates a {@link Cursor} over paths.
     *
     * @param paths the paths to iterate over.
     * @return the Cursor.
     */
    public static Cursor newPathCursor(Iterable<String> paths) {
        return new PathCursor(paths);
    }

    /**
     * Returns a traversing cursor based on the path restriction in the given
     * {@link Filter}.
     * 
     * @param filter the filter.
     * @param rootState the root {@link NodeState}.
     * @return the {@link Cursor}.
     */
    public static Cursor newTraversingCursor(Filter filter,
                                             NodeState rootState) {
        return new TraversingCursor(filter, rootState);
    }
    
    /**
     * A Cursor implementation where the remove method throws an
     * UnsupportedOperationException.
     */
    public abstract static class AbstractCursor implements Cursor {
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }

    /**
     * <code>PathCursor</code> implements a simple {@link Cursor} that iterates
     * over a {@link String} based path {@link Iterable}.
     */
    private static class PathCursor extends AbstractCursor {

        private final Iterator<String> iterator;

        public PathCursor(Iterable<String> paths) {
            this.iterator = paths.iterator();
        }

        @Override
        public IndexRow next() {
            // TODO support jcr:score and possibly rep:exceprt
            String path = iterator.next();
            return new IndexRowImpl(isAbsolute(path) ? path : "/" + path);
        }
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
        
    }

    /**
     * A cursor that reads all nodes in a given subtree.
     */
    private static class TraversingCursor extends AbstractCursor {

        private static final Logger LOG = LoggerFactory.getLogger(TraversingIndex.class);

        private final Filter filter;

        private final Deque<Iterator<? extends ChildNodeEntry>> nodeIterators =
                Queues.newArrayDeque();

        private String parentPath;

        private String currentPath;

        private long readCount;

        private boolean init;
        
        private boolean closed;

        public TraversingCursor(Filter filter, NodeState rootState) {
            this.filter = filter;

            String path = filter.getPath();
            parentPath = null;
            currentPath = "/";
            NodeState parent = null;
            NodeState node = rootState;
            if (!path.equals("/")) {
                for (String name : path.substring(1).split("/")) {
                    parentPath = currentPath;
                    currentPath = PathUtils.concat(parentPath, name);

                    parent = node;
                    node = parent.getChildNode(name);

                    if (node == null) {
                        // nothing can match this filter, leave nodes empty
                        return;
                    }
                }
            }
            Filter.PathRestriction restriciton = filter.getPathRestriction();
            switch (restriciton) {
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
                throw new IllegalArgumentException("Unknown restriction: " + restriciton);
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
                    if (readCount % 100 == 0) {
                        LOG.warn("Traversed " + readCount + " nodes with filter " + filter + "; consider creating an index or changing the query");
                    }

                    NodeState node = entry.getNodeState();

                    String name = entry.getName();
                    if (NodeStateUtils.isHidden(name)) {
                        continue;
                    }
                    currentPath = PathUtils.concat(parentPath, name);

                    if (filter.getPathRestriction() == ALL_CHILDREN) {
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
}
