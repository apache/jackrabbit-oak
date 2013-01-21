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
package org.apache.jackrabbit.oak.plugins.index.p2.strategy;

import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

/**
 * TODO document
 */
public class ContentMirrorStoreStrategy implements IndexStoreStrategy {

    static final Logger LOG = LoggerFactory.getLogger(ContentMirrorStoreStrategy.class);

    @Override
    public void remove(NodeBuilder index, String key, Iterable<String> values) {
        if (!index.hasChildNode(key)) {
            return;
        }
        NodeBuilder child = index.child(key);
        Map<String, NodeBuilder> parents = new TreeMap<String, NodeBuilder>(Collections.reverseOrder());

        for (String rm : values) {
            if (PathUtils.denotesRoot(rm)) {
                child.removeProperty("match");
            } else {
                String parentPath = PathUtils.getParentPath(rm);
                String name = PathUtils.getName(rm);
                NodeBuilder indexEntry = parents.get(parentPath);
                if (indexEntry == null) {
                    indexEntry = child;
                    String segmentPath = "";
                    Iterator<String> segments = PathUtils.elements(parentPath)
                            .iterator();
                    while (segments.hasNext()) {
                        String segment = segments.next();
                        segmentPath = PathUtils.concat(segmentPath, segment);
                        indexEntry = indexEntry.child(segment);
                        parents.put(segmentPath, indexEntry);
                    }
                }
                if (indexEntry.hasChildNode(name)) {
                    NodeBuilder childEntry = indexEntry.child(name);
                    childEntry.removeProperty("match");
                    if (childEntry.getChildNodeCount() == 0) {
                        indexEntry.removeNode(name);
                    }
                }
            }
        }
        // prune the index: remove all children that have no children
        // and no "match" property progressing bottom up
        Iterator<String> it = parents.keySet().iterator();
        while (it.hasNext()) {
            String path = it.next();
            NodeBuilder parent = parents.get(path);
            pruneNode(parent);
        }

        // finally prune the index node
        pruneNode(child);
        if (child.getChildNodeCount() == 0
                && child.getProperty("match") == null) {
            index.removeNode(key);
        }
    }

    private static void pruneNode(NodeBuilder parent) {
        if (parent.isRemoved()) {
            return;
        }
        for (String name : parent.getChildNodeNames()) {
            NodeBuilder segment = parent.child(name);
            if (segment.getChildNodeCount() == 0
                    && segment.getProperty("match") == null) {
                parent.removeNode(name);
            }
        }
    }

    @Override
    public void insert(NodeBuilder index, String key, boolean unique,
            Iterable<String> values) throws CommitFailedException {
        NodeBuilder child = index.child(key);

        for (String add : values) {
            NodeBuilder indexEntry = child;
            for (String segment : PathUtils.elements(add)) {
                indexEntry = indexEntry.child(segment);
            }
            indexEntry.setProperty("match", true);
        }
        CountingNodeVisitor v = new CountingNodeVisitor(2);
        v.visit(child.getNodeState());
        int matchCount = v.getCount();
        if (matchCount == 0) {
            index.removeNode(key);
        } else if (unique && matchCount > 1) {
            throw new CommitFailedException("Uniqueness constraint violated");
        }
    }
    
    @Override
    public Iterable<String> query(final Filter filter, final String indexName, 
            final NodeState index, final Iterable<String> values) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                PathIterator it = new PathIterator(filter, indexName);
                if (values == null) {
                    it.setPathContainsValue(true);
                    it.enqueue(index.getChildNodeEntries().iterator());
                } else {
                    for (String p : values) {
                        NodeState property = index.getChildNode(p);
                        if (property != null) {
                            // we have an entry for this value, so use it
                            it.enqueue(Iterators.singletonIterator(
                                    new MemoryChildNodeEntry("", property)));
                        }
                    }
                }
                return it;
            }
        };
    }
    
    @Override
    public int count(NodeState index, List<String> values, int max) {
        int count = 0;
        if (values == null) {
            CountingNodeVisitor v = new CountingNodeVisitor(max);
            v.visit(index);
            count = v.getEstimatedCount();
        } else {
            int size = values.size();
            if (size == 0) {
                return 0;
            }
            max = Math.max(10, max / size);
            int i = 0;
            for (String p : values) {
                if (count > max && i > 3) {
                    count = count / size / i;
                    break;
                }
                NodeState s = index.getChildNode(p);
                if (s != null) {
                    CountingNodeVisitor v = new CountingNodeVisitor(max);
                    v.visit(s);
                    count += v.getEstimatedCount();
                }
                i++;
            }
        }
        return count;
    }
    
    static boolean matches(NodeState state) {
        PropertyState ps = state.getProperty("match");
        return ps != null && !ps.isArray() && ps.getValue(Type.BOOLEAN);
    }

    /**
     * An iterator over paths within an index node.
     */
    static class PathIterator implements Iterator<String> {
        
        private final Filter filter;
        private final String indexName;
        private final Deque<Iterator<? extends ChildNodeEntry>> nodeIterators =
                Queues.newArrayDeque();
        private int readCount;
        private boolean init;
        private boolean closed;
        private String parentPath;
        private String currentPath;
        private boolean pathContainsValue;
        
        /**
         * Keep the returned path, to avoid returning duplicate entries.
         */
        private final Set<String> knownPaths = Sets.newHashSet();
        
        PathIterator(Filter filter, String indexName) {
            this.filter = filter;
            this.indexName = indexName;
            parentPath = "";
            currentPath = "/";
        }
        
        void enqueue(Iterator<? extends ChildNodeEntry> it) {
            nodeIterators.addLast(it);
        }
        
        void setPathContainsValue(boolean pathContainsValue) {
            this.pathContainsValue = pathContainsValue;
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
                        LOG.warn("Traversed " + readCount + " nodes using index " + indexName + " with filter " + filter);
                    }

                    NodeState node = entry.getNodeState();

                    String name = entry.getName();
                    if (NodeStateUtils.isHidden(name)) {
                        continue;
                    }
                    currentPath = PathUtils.concat(parentPath, name);

                    nodeIterators.addLast(node.getChildNodeEntries().iterator());
                    parentPath = currentPath;

                    if (matches(node)) {
                        return;
                    }
                    
                } else {
                    nodeIterators.removeLast();
                    parentPath = PathUtils.getParentPath(parentPath);
                }
            }
            currentPath = null;
            closed = true;
        }


        @Override
        public String next() {
            if (closed) {
                throw new IllegalStateException("This iterator is closed");
            }
            if (!init) {
                fetchNext();
                init = true;
            }
            while (true) {
                String result = currentPath;
                fetchNext();
                if (pathContainsValue) {
                    String value = PathUtils.elements(result).iterator().next();
                    result = PathUtils.relativize(value, result);
                    // don't return duplicate paths:
                    // Set.add returns true if the entry was new,
                    // so if it returns false, it was already known
                    if (!knownPaths.add(result)) {
                        continue;
                    }
                }
                return result;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    
    /**
     * A node visitor to recursively traverse a number of nodes.
     */
    interface NodeVisitor {
        void visit(NodeState state);
    }
    
    /**
     * A node visitor that counts the number of matching nodes up to a given
     * maximum, in order to estimate the number of matches.
     */
    static class CountingNodeVisitor implements NodeVisitor {
        
        /**
         * The maximum number of matching nodes to count.
         */
        final int maxCount;
        
        /**
         * The current count of matching nodes.
         */
        int count;
        
        /**
         * The current depth (number of parent nodes).
         */
        int depth;
        
        /**
         * The total number of child nodes per node, for those nodes that were
         * fully traversed and do have child nodes. This value is used to
         * calculate the average width.
         */
        long widthTotal;
        
        /**
         * The number of nodes that were fully traversed and do have child
         * nodes. This value is used to calculate the average width.
         */
        int widthCount;
        
        /**
         * The sum of the depth of all matching nodes. This value is used to
         * calculate the average depth.
         */
        long depthTotal;
        
        CountingNodeVisitor(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public void visit(NodeState state) {
            if (state.getProperty("match") != null) {
                count++;
                depthTotal += depth;
            }
            if (count < maxCount) {
                depth++;
                int width = 0;
                boolean finished = true;
                for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                    if (count >= maxCount) {
                        finished = false;
                        break;
                    }
                    width++;
                    visit(entry.getNodeState());
                }
                if (finished && width > 0) {
                    widthTotal += width;
                    widthCount++;
                }
                depth--;
            }
        }
        
        /**
         * The number of matches (at most the maximum count).
         * 
         * @return the match count
         */
        int getCount() {
            return count;
        }
        
        /**
         * The number of estimated matches. This value might be higher than the
         * number of counted matches, if the maximum number of matches has been
         * reached. It is based on the average depth of matches, and the average
         * number of child nodes.
         * 
         * @return the estimated matches
         */
        int getEstimatedCount() {
            if (count < maxCount) {
                return count;
            }
            double averageDepth = (int) (depthTotal / count);
            double averageWidth = 2;
            if (widthCount > 0) {
                averageWidth = (int) (widthTotal / widthCount);
            }
            // calculate with an average width of at least 2
            averageWidth = Math.max(2, averageWidth);
            // the number of estimated matches is calculated as the
            // of a estimated
            long estimatedNodes = (long) Math.pow(averageWidth, 2 * averageDepth);
            estimatedNodes = Math.min(estimatedNodes, Integer.MAX_VALUE);
            return Math.max(count, (int) estimatedNodes);
        }
        
    }

}
