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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.search.BadIndexTracker;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;


import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.isLuceneIndexNode;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.STATUS_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * Keeps track of all Lucene indexes in a repository (all readers, writers, and
 * definitions).
 */
public class IndexTracker {

    // auto-refresh
    // the default is to not refresh (refresh every 100 years)
    // to refresh every hour, set it to 3600000
    // we don't use Long.MAX_VALUE to avoid (now + AUTO_REFRESH_MILLIS) to become negative
    private static final long AUTO_REFRESH_MILLIS = Long.getLong(
            "oak.indexTracker.autoRefresh",
            100L * 365 * 24 * 60 * 60 * 1000);

    /** Logger instance. */
    private static final Logger log = LoggerFactory.getLogger(IndexTracker.class);
    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(IndexTracker.class.getName() + ".perf"));

    private final LuceneIndexReaderFactory readerFactory;
    private final NRTIndexFactory nrtFactory;
    private final BadIndexTracker badIndexTracker = new BadIndexTracker();

    private NodeState root = EMPTY_NODE;

    private AsyncIndexInfoService asyncIndexInfoService;

    private volatile Map<String, LuceneIndexNodeManager> indices = emptyMap();

    private volatile boolean refresh;
    private volatile long nextAutoRefresh = System.currentTimeMillis() + AUTO_REFRESH_MILLIS;

    public IndexTracker() {
        this((IndexCopier)null);
    }

    public IndexTracker(IndexCopier cloner){
        this(new DefaultIndexReaderFactory(Mounts.defaultMountInfoProvider(), cloner));
    }

    public IndexTracker(LuceneIndexReaderFactory readerFactory) {
        this(readerFactory, null);
    }

    public IndexTracker(LuceneIndexReaderFactory readerFactory, @Nullable NRTIndexFactory nrtFactory){
        this.readerFactory = readerFactory;
        this.nrtFactory = nrtFactory;
    }

    public MountInfoProvider getMountInfoProvider() {
        return readerFactory.getMountInfoProvider();
    }

    public synchronized void close() {
        Map<String, LuceneIndexNodeManager> indices = this.indices;
        this.indices = emptyMap();

        for (Map.Entry<String, LuceneIndexNodeManager> entry : indices.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.error("Failed to close the Lucene index at {}", entry.getKey(), e);
            }
        }
    }

    public synchronized void update(final NodeState root) {
        if (refresh) {
            this.root = root;
            close();
            refresh = false;
            log.info("Refreshed the opened indexes");
        } else {
            diffAndUpdate(root);
            long now = System.currentTimeMillis();
            if (!refresh && now > nextAutoRefresh) {
                refresh = true;
                nextAutoRefresh = now + AUTO_REFRESH_MILLIS;
            }
        }
    }

    public void setAsyncIndexInfoService(AsyncIndexInfoService asyncIndexInfoService) {
        this.asyncIndexInfoService = asyncIndexInfoService;
    }

    public AsyncIndexInfoService getAsyncIndexInfoService() {
        return asyncIndexInfoService;
    }

    private synchronized void diffAndUpdate(final NodeState root) {
        if (asyncIndexInfoService != null && !asyncIndexInfoService.hasIndexerUpdatedForAnyLane(this.root, root)) {
            log.trace("No changed detected in async indexer state. Skipping further diff");
            this.root = root;
            return;
        }

        Map<String, LuceneIndexNodeManager> original = indices;
        final Map<String, LuceneIndexNodeManager> updates = new HashMap<>();

        Set<String> indexPaths = new HashSet<>();
        indexPaths.addAll(original.keySet());
        indexPaths.addAll(badIndexTracker.getIndexPaths());

        List<Editor> editors = new ArrayList<>(indexPaths.size());
        for (final String path : indexPaths) {
            editors.add(new SubtreeEditor(new DefaultEditor() {
                @Override
                public void leave(NodeState before, NodeState after) {
                    try {
                        if (isStatusChanged(before, after) || isIndexDefinitionChanged(before, after)) {
                            long start = PERF_LOGGER.start();
                            LuceneIndexNodeManager index = LuceneIndexNodeManager.open(path, root, after, readerFactory, nrtFactory);
                            PERF_LOGGER.end(start, -1, "[{}] Index found to be updated. Reopening the LuceneIndexNode", path);
                            updates.put(path, index); // index can be null
                        }
                    } catch (IOException e) {
                        badIndexTracker.markBadPersistedIndex(path, e);
                    }
                }
            }, Iterables.toArray(PathUtils.elements(path), String.class)));
        }

        EditorDiff.process(CompositeEditor.compose(editors), this.root, root);
        this.root = root;

        if (!updates.isEmpty()) {
            Map<String, LuceneIndexNodeManager> builder = new HashMap<>();
            builder.putAll(Maps.filterKeys(original, x -> !updates.keySet().contains(x)));
            builder.putAll(Maps.filterValues(updates, x -> x != null));
            indices = Collections.unmodifiableMap(builder);

            badIndexTracker.markGoodIndexes(updates.keySet());

            //This might take some time as close need to acquire the
            //write lock which might be held by current running searches
            //Given that Tracker is now invoked from a BackgroundObserver
            //not a high concern
            for (String path : updates.keySet()) {
                LuceneIndexNodeManager index = original.get(path);
                try {
                    if (index != null) {
                        index.close();
                    }
                } catch (IOException e) {
                    log.error("Failed to close Lucene index at {}", path, e);
                }
            }
        }
    }

    public void refresh() {
        log.info("Marked tracker to refresh upon next cycle");
        refresh = true;
    }

    /**
     * Acquire the index node, if the index is good.
     *
     * @param path the index path
     * @return the index node, or null if it's a bad (corrupt) index
     */
    @Nullable
    public LuceneIndexNode acquireIndexNode(String path) {
        LuceneIndexNodeManager index = indices.get(path);
        LuceneIndexNode indexNode = index != null ? index.acquire() : null;
        if (indexNode != null) {
            return indexNode;
        }
        return findIndexNode(path);
    }

    /**
     * Get the index node, if the index is good.
     *
     * @param path the index path
     * @return the index node, or null if it's a bad (corrupt) index
     */
    @Nullable
    private synchronized LuceneIndexNode findIndexNode(String path) {
        // Retry the lookup from acquireIndexNode now that we're
        // synchronized. The acquire() call is guaranteed to succeed
        // since the close() method is also synchronized.
        LuceneIndexNodeManager index = indices.get(path);
        if (index != null) {
            LuceneIndexNode indexNode = index.acquire();
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
            if (isLuceneIndexNode(node)) {
                index = LuceneIndexNodeManager.open(path, root, node, readerFactory, nrtFactory);
                if (index != null) {
                    LuceneIndexNode indexNode = index.acquire();
                    requireNonNull(indexNode);
                    Map<String, LuceneIndexNodeManager> builder = new HashMap<>();
                    builder.putAll(indices);
                    builder.put(path, index);
                    indices = Collections.unmodifiableMap(builder);
                    badIndexTracker.markGoodIndex(path);
                    return indexNode;
                }
            } else if (node.exists()) {
                log.warn("Cannot open Lucene Index at path {} as the index is not of type {}", path, TYPE_LUCENE);
            }
        } catch (Throwable e) {
            badIndexTracker.markBadIndexForRead(path, e);
        }

        return null;
    }

    @Nullable
    public LuceneIndexDefinition getIndexDefinition(String indexPath){
        LuceneIndexNodeManager indexNodeManager = indices.get(indexPath);
        if (indexNodeManager != null) {
            // Accessing the definition should not require
            // locking as its immutable state
            return indexNodeManager.getDefinition();
        }
        // fallback - create definition from scratch
        NodeState node = NodeStateUtils.getNode(root, indexPath);
        if (!node.exists()) {
            return null;
        }
        // only if there exists a stored index definition
        if (!node.hasChildNode(INDEX_DEFINITION_NODE)) {
            return null;
        }
        if (!isLuceneIndexNode(node)) {
            return null;
        }
        // this will internally use the stored index definition
        return new LuceneIndexDefinition(root, node, indexPath);
    }

    public Set<String> getIndexNodePaths(){
        return indices.keySet();
    }

    public BadIndexTracker getBadIndexTracker() {
        return badIndexTracker;
    }

    public NodeState getRoot() {
        return root;
    }

    private static boolean isStatusChanged(NodeState before, NodeState after) {
        return !EqualsDiff.equals(before.getChildNode(STATUS_NODE), after.getChildNode(STATUS_NODE));
    }

    private static boolean isIndexDefinitionChanged(NodeState before, NodeState after) {
        return !EqualsDiff.equals(before.getChildNode(INDEX_DEFINITION_NODE), after.getChildNode(INDEX_DEFINITION_NODE));
    }

}
