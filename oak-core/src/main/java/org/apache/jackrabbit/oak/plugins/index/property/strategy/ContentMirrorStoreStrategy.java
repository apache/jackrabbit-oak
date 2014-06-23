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
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static com.google.common.collect.Queues.newArrayDeque;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;

import java.util.Deque;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.query.FilterIterators;
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
 * An IndexStoreStrategy implementation that saves the nodes under a hierarchy
 * that mirrors the repository tree. <br>
 * This should minimize the chance that concurrent updates overlap on the same
 * content node.<br>
 * <br>
 * For example for a node that is under {@code /test/node}, the index
 * structure will be {@code /oak:index/index/test/node}:
 * 
 * <pre>
 * {@code
 * /
 *   test
 *     node
 *   oak:index
 *     index
 *       test
 *         node
 * }
 * </pre>
 *
 */
public class ContentMirrorStoreStrategy implements IndexStoreStrategy {

    static final Logger LOG = LoggerFactory.getLogger(ContentMirrorStoreStrategy.class);

    @Override
    public void update(
            NodeBuilder index, String path,
            Set<String> beforeKeys, Set<String> afterKeys) {
        for (String key : beforeKeys) {
            remove(index, key, path);
        }
        for (String key : afterKeys) {
            insert(index, key, path);
        }
    }

    private void remove(NodeBuilder index, String key, String value) {
        NodeBuilder builder = index.getChildNode(key);
        if (builder.exists()) {
            // Collect all builders along the given path
            Deque<NodeBuilder> builders = newArrayDeque();
            builders.addFirst(builder);

            // Descend to the correct location in the index tree
            for (String name : PathUtils.elements(value)) {
                builder = builder.getChildNode(name);
                builders.addFirst(builder);
            }

            // Drop the match value,  if present
            if (builder.exists()) {
                builder.removeProperty("match");
            }

            // Prune all index nodes that are no longer needed
            prune(index, builders, key);
        }
    }

    private void insert(NodeBuilder index, String key, String value) {
        // NodeBuilder builder = index.child(key);
        NodeBuilder builder = fetchKeyNode(index, key);
        for (String name : PathUtils.elements(value)) {
            builder = builder.child(name);
        }
        builder.setProperty("match", true);
    }

    public Iterable<String> query(final Filter filter, final String indexName,
            final NodeState indexMeta, final String indexStorageNodeName,
            final Iterable<String> values) {
        final NodeState index = indexMeta.getChildNode(indexStorageNodeName);
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                PathIterator it = new PathIterator(filter, indexName);
                if (values == null) {
                    it.setPathContainsValue(true);
                    it.enqueue(getChildNodeEntries(index).iterator());
                } else {
                    for (String p : values) {
                        NodeState property = index.getChildNode(p);
                        if (property.exists()) {
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

    @Nonnull
    Iterable<? extends ChildNodeEntry> getChildNodeEntries(@Nonnull
    final NodeState index) {
        return index.getChildNodeEntries();
    }

    @Override
    public Iterable<String> query(final Filter filter, final String indexName,
            final NodeState indexMeta, final Iterable<String> values) {
        return query(filter, indexName, indexMeta, INDEX_CONTENT_NODE_NAME, values);
    }

    @Override
    public long count(NodeState indexMeta, Set<String> values, int max) {
        return count(indexMeta, INDEX_CONTENT_NODE_NAME, values, max);
    }

    public long count(NodeState indexMeta, final String indexStorageNodeName,
            Set<String> values, int max) {
        NodeState index = indexMeta.getChildNode(indexStorageNodeName);
        int count = 0;
        if (values == null) {
            PropertyState ec = indexMeta.getProperty(ENTRY_COUNT_PROPERTY_NAME);
            if (ec != null) {
                return ec.getValue(Type.LONG);
            }
            CountingNodeVisitor v = new CountingNodeVisitor(max);
            v.visit(index);
            count = v.getEstimatedCount();
            if (count >= max) {
                // "is not null" queries typically read more data
                count *= 10;
            }
        } else {
            int size = values.size();
            if (size == 0) {
                return 0;
            }
            max = Math.max(10, max / size);
            int i = 0;
            for (String p : values) {
                if (count > max && i > 3) {
                    // the total count is extrapolated from the the number 
                    // of values counted so far to the total number of values
                    count = count * size / i;
                    break;
                }
                NodeState s = index.getChildNode(p);
                if (s.exists()) {
                    CountingNodeVisitor v = new CountingNodeVisitor(max);
                    v.visit(s);
                    count += v.getEstimatedCount();
                }
                i++;
            }
        }
        return count;
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
        private final long maxMemoryEntries;

        PathIterator(Filter filter, String indexName) {
            this.filter = filter;
            this.indexName = indexName;
            parentPath = "";
            currentPath = "/";
            this.maxMemoryEntries = filter.getQueryEngineSettings().getLimitInMemory();
        }

        void enqueue(Iterator<? extends ChildNodeEntry> it) {
            nodeIterators.addLast(it);
        }

        void setPathContainsValue(boolean pathContainsValue) {
            if (init) {
                throw new IllegalStateException("This iterator is already initialized");
            }
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
            while (true) {
                fetchNextPossiblyDuplicate();
                if (closed) {
                    return;
                }
                if (pathContainsValue) {
                    String value = PathUtils.elements(currentPath).iterator().next();
                    currentPath = PathUtils.relativize(value, currentPath);
                    // don't return duplicate paths:
                    // Set.add returns true if the entry was new,
                    // so if it returns false, it was already known
                    if (!knownPaths.add(currentPath)) {
                        continue;
                    }
                }
                break;
            }
        }

        private void fetchNextPossiblyDuplicate() {
            while (!nodeIterators.isEmpty()) {
                Iterator<? extends ChildNodeEntry> iterator = nodeIterators.getLast();
                if (iterator.hasNext()) {
                    ChildNodeEntry entry = iterator.next();

                    readCount++;
                    if (readCount % 1000 == 0) {
                        FilterIterators.checkReadLimit(readCount, maxMemoryEntries);
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

                    if (node.getBoolean("match")) {
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
            String result = currentPath;
            fetchNext();
            return result;
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
         * The sum of the depth of all matching nodes. This value is used to
         * calculate the average depth.
         */
        long depthTotal;

        CountingNodeVisitor(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public void visit(NodeState state) {
            if (state.hasProperty("match")) {
                count++;
                depthTotal += depth;
            }
            if (count < maxCount) {
                depth++;
                for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                    if (count >= maxCount) {
                        break;
                    }
                    visit(entry.getNodeState());
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
            // the number of estimated matches is higher
            // the higher the average depth of the first hits
            long estimatedNodes = (long) (count * Math.pow(1.1, averageDepth));
            estimatedNodes = Math.min(estimatedNodes, Integer.MAX_VALUE);
            return Math.max(count, (int) estimatedNodes);
        }

    }
    
    /**
     * fetch from the index the <i>key</i> node
     * 
     * @param index
     *            the current index root
     * @param key
     *            the 'key' to fetch from the repo
     * @return the node representing the key
     */
    NodeBuilder fetchKeyNode(@Nonnull NodeBuilder index, 
                             @Nonnull String key) {
        return index.child(key);
    }

    /**
     * Physically prune a list of nodes from the index
     * 
     * @param index
     *            the current index
     * @param builders
     *            list of nodes to prune
     * @param key the key of the index we're processing
     */
    void prune(final NodeBuilder index, final Deque<NodeBuilder> builders, final String key) {
        for (NodeBuilder node : builders) {
            if (node.getBoolean("match") || node.getChildNodeCount(1) > 0) {
                return;
            } else if (node.exists()) {
                node.remove();
            }
        }
    }
}