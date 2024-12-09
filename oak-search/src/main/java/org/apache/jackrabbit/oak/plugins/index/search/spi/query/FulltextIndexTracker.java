/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.search.spi.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.search.BadIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexHelper;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.jackrabbit.guava.common.collect.Maps.filterKeys;
import static org.apache.jackrabbit.guava.common.collect.Maps.filterValues;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.STATUS_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public abstract class FulltextIndexTracker<I extends IndexNodeManager<N>, N extends IndexNode> {

    private static final Logger LOG = LoggerFactory.getLogger(FulltextIndexTracker.class);
    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(FulltextIndexTracker.class.getName() + ".perf"));

    private final BadIndexTracker badIndexTracker = new BadIndexTracker();

    private NodeState root = EMPTY_NODE;

    private AsyncIndexInfoService asyncIndexInfoService;

    private volatile Map<String, I> indices = emptyMap();

    private volatile boolean refresh;

    protected abstract I openIndex(String path, NodeState root, NodeState node);

    synchronized void close() {
        Map<String, I> indices = this.indices;
        this.indices = emptyMap();

        for (Map.Entry<String, I> entry : indices.entrySet()) {
            entry.getValue().close();
        }
    }

    public synchronized void update(final NodeState root) {
        if (refresh) {
            this.root = root;
            close();
            refresh = false;
            LOG.info("Refreshed the opened indexes");
        } else {
            diffAndUpdate(root);
        }
    }

    /**
     * Receives the before and after state to decide when to reload the {@link IndexNode}.
     * By default, it checks for changes of the :status node and the index definition.
     *
     * @param before before state, non-existent if this node was added
     * @param after after state, non-existent if this node was removed
     * @return true if the {@link IndexNode} need to be opened and updated
     */
    public boolean isUpdateNeeded(NodeState before, NodeState after) {
        return isStatusChanged(before, after) || isIndexDefinitionChanged(before, after);
    }

    public void setAsyncIndexInfoService(AsyncIndexInfoService asyncIndexInfoService) {
        this.asyncIndexInfoService = asyncIndexInfoService;
    }

    AsyncIndexInfoService getAsyncIndexInfoService() {
        return asyncIndexInfoService;
    }

    private synchronized void diffAndUpdate(final NodeState root) {
        if (asyncIndexInfoService != null && !asyncIndexInfoService.hasIndexerUpdatedForAnyLane(this.root, root)) {
            LOG.trace("No changed detected in async indexer state. Skipping further diff");
            this.root = root;
            return;
        }

        Map<String, I> original = indices;
        final Map<String, I> updates = new HashMap<>();

        Set<String> indexPaths = new HashSet<>();
        indexPaths.addAll(original.keySet());
        indexPaths.addAll(badIndexTracker.getIndexPaths());

        List<Editor> editors = new ArrayList<>(indexPaths.size());
        for (final String path : indexPaths) {
            editors.add(new SubtreeEditor(new DefaultEditor() {
                @Override
                public void leave(NodeState before, NodeState after) {
                    try {
                        if (isUpdateNeeded(before, after)) {
                            long start = PERF_LOGGER.start();
                            I index = openIndex(path, root, after);
                            PERF_LOGGER.end(start, -1, "[{}] Index found to be updated. Reopening the IndexNode", path);
                            updates.put(path, index); // index can be null
                        }
                    } catch (Exception e) {
                        badIndexTracker.markBadPersistedIndex(path, e);
                    }
                }
            }, Iterables.toArray(PathUtils.elements(path), String.class)));
        }

        EditorDiff.process(CompositeEditor.compose(editors), this.root, root);
        this.root = root;

        if (!updates.isEmpty()) {
            Map<String, I> builder = new HashMap<>();
            builder.putAll(filterKeys(original, x -> !updates.keySet().contains(x)));
            builder.putAll(filterValues(updates, x -> x != null));
            indices = Collections.unmodifiableMap(builder);

            badIndexTracker.markGoodIndexes(updates.keySet());

            //This might take some time as close need to acquire the
            //write lock which might be held by current running searches
            //Given that Tracker is now invoked from a BackgroundObserver
            //not a high concern
            for (String path : updates.keySet()) {
                I index = original.get(path);
                if (index != null) {
                    index.close();
                }
            }
        }
    }

    void refresh() {
        LOG.info("Marked tracker to refresh upon next cycle");
        refresh = true;
    }

    public N acquireIndexNode(String path, String type) {
        I index = indices.get(path);
        N indexNode = index != null ? index.acquire() : null;
        if (indexNode != null) {
            return indexNode;
        } else {
            return findIndexNode(path, type);
        }
    }

    @Nullable
    public IndexDefinition getIndexDefinition(String indexPath){
        I node = indices.get(indexPath);
        if (node != null){
            //Accessing the definition should not require
            //locking as its immutable state
            return node.getDefinition();
        }
        return null;
    }

    public Set<String> getIndexNodePaths(){
        return indices.keySet();
    }

    BadIndexTracker getBadIndexTracker() {
        return badIndexTracker;
    }

    NodeState getRoot() {
        return root;
    }

    private synchronized N findIndexNode(String path, String type) {
        // Retry the lookup from acquireIndexNode now that we're
        // synchronized. The acquire() call is guaranteed to succeed
        // since the close() method is also synchronized.
        I index = indices.get(path);
        if (index != null) {
            N indexNode = index.acquire();
            return requireNonNull(indexNode);
        }

        if (badIndexTracker.isIgnoredBadIndex(path)){
            return null;
        }

        NodeState node = root;
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }

        try {
            if (IndexHelper.isIndexNodeOfType(node, type)) {
                index = openIndex(path, root, node);
                if (index != null) {
                    N indexNode = index.acquire();
                    requireNonNull(indexNode);
                    Map<String, I> builder = new HashMap<>();
                    builder.putAll(indices);
                    builder.put(path, index);
                    indices = Collections.unmodifiableMap(builder);
                    badIndexTracker.markGoodIndex(path);
                    return indexNode;
                }
            } else if (node.exists()) {
                LOG.warn("Cannot open Index at path {} as the index is not of type {}", path, type);
            }
        } catch (Throwable e) {
            badIndexTracker.markBadIndexForRead(path, e);
        }

        return null;
    }

    private static boolean isStatusChanged(NodeState before, NodeState after) {
        return !EqualsDiff.equals(before.getChildNode(STATUS_NODE), after.getChildNode(STATUS_NODE));
    }

    protected static boolean isIndexDefinitionChanged(NodeState before, NodeState after) {
        return !EqualsDiff.equals(before.getChildNode(INDEX_DEFINITION_NODE), after.getChildNode(INDEX_DEFINITION_NODE));
    }
}
