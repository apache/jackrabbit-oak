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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.STATUS_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_REFRESH_DEFN;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.isLuceneIndexNode;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class IndexTracker {

    /** Logger instance. */
    private static final Logger log = LoggerFactory.getLogger(IndexTracker.class);
    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(IndexTracker.class.getName() + ".perf"));

    private final LuceneIndexReaderFactory readerFactory;
    private final NRTIndexFactory nrtFactory;
    private final BadIndexTracker badIndexTracker = new BadIndexTracker();

    private NodeState root = EMPTY_NODE;

    private AsyncIndexInfoService asyncIndexInfoService;

    private volatile Map<String, IndexNodeManager> indices = emptyMap();

    private volatile boolean refresh;

    public IndexTracker() {
        this((IndexCopier)null);
    }

    IndexTracker(IndexCopier cloner){
        this(new DefaultIndexReaderFactory(Mounts.defaultMountInfoProvider(), cloner));
    }

    IndexTracker(LuceneIndexReaderFactory readerFactory) {
        this(readerFactory, null);
    }

    public IndexTracker(LuceneIndexReaderFactory readerFactory, @Nullable NRTIndexFactory nrtFactory){
        this.readerFactory = readerFactory;
        this.nrtFactory = nrtFactory;
    }

    synchronized void close() {
        Map<String, IndexNodeManager> indices = this.indices;
        this.indices = emptyMap();

        for (Map.Entry<String, IndexNodeManager> entry : indices.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.error("Failed to close the Lucene index at " + entry.getKey(), e);
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
        }
    }

    public void setAsyncIndexInfoService(AsyncIndexInfoService asyncIndexInfoService) {
        this.asyncIndexInfoService = asyncIndexInfoService;
    }

    AsyncIndexInfoService getAsyncIndexInfoService() {
        return asyncIndexInfoService;
    }

    private synchronized void diffAndUpdate(final NodeState root) {
        if (asyncIndexInfoService != null && !asyncIndexInfoService.hasIndexerUpdatedForAnyLane(this.root, root)) {
            log.trace("No changed detected in async indexer state. Skipping further diff");
            this.root = root;
            return;
        }

        Map<String, IndexNodeManager> original = indices;
        final Map<String, IndexNodeManager> updates = newHashMap();

        Set<String> indexPaths = Sets.newHashSet();
        indexPaths.addAll(original.keySet());
        indexPaths.addAll(badIndexTracker.getIndexPaths());

        List<Editor> editors = newArrayListWithCapacity(indexPaths.size());
        for (final String path : indexPaths) {
            editors.add(new SubtreeEditor(new DefaultEditor() {
                @Override
                public void leave(NodeState before, NodeState after) {
                    try {
                        if (isStatusChanged(before, after) || isIndexDefinitionChanged(before, after)) {
                            long start = PERF_LOGGER.start();
                            IndexNodeManager index = IndexNodeManager.open(path, root, after, readerFactory, nrtFactory);
                            PERF_LOGGER.end(start, -1, "[{}] Index found to be updated. Reopening the IndexNode", path);
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
            indices = ImmutableMap.<String, IndexNodeManager>builder()
                    .putAll(filterKeys(original, not(in(updates.keySet()))))
                    .putAll(filterValues(updates, notNull()))
                    .build();

            badIndexTracker.markGoodIndexes(updates.keySet());

            //This might take some time as close need to acquire the
            //write lock which might be held by current running searches
            //Given that Tracker is now invoked from a BackgroundObserver
            //not a high concern
            for (String path : updates.keySet()) {
                IndexNodeManager index = original.get(path);
                try {
                    if (index != null) {
                        index.close();
                    }
                } catch (IOException e) {
                    log.error("Failed to close Lucene index at " + path, e);
                }
            }
        }
    }

    void refresh() {
        log.info("Marked tracker to refresh upon next cycle");
        refresh = true;
    }

    public IndexNode acquireIndexNode(String path) {
        IndexNodeManager index = indices.get(path);
        IndexNode indexNode = index != null ? index.acquire() : null;
        if (indexNode != null) {
            return indexNode;
        } else {
            return findIndexNode(path);
        }
    }

    @CheckForNull
    public IndexDefinition getIndexDefinition(String indexPath){
        IndexNodeManager node = indices.get(indexPath);
        if (node != null){
            //Accessing the definition should not require
            //locking as its immutable state
            return node.getDefinition();
        }
        return null;
    }

    Set<String> getIndexNodePaths(){
        return indices.keySet();
    }

    BadIndexTracker getBadIndexTracker() {
        return badIndexTracker;
    }

    NodeState getRoot() {
        return root;
    }

    private synchronized IndexNode findIndexNode(String path) {
        // Retry the lookup from acquireIndexNode now that we're
        // synchronized. The acquire() call is guaranteed to succeed
        // since the close() method is also synchronized.
        IndexNodeManager index = indices.get(path);
        if (index != null) {
            IndexNode indexNode = index.acquire();
            return checkNotNull(indexNode);
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
                index = IndexNodeManager.open(path, root, node, readerFactory, nrtFactory);
                if (index != null) {
                    IndexNode indexNode = index.acquire();
                    checkNotNull(indexNode);
                    indices = ImmutableMap.<String, IndexNodeManager>builder()
                            .putAll(indices)
                            .put(path, index)
                            .build();
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

    private static boolean isStatusChanged(NodeState before, NodeState after) {
        return !EqualsDiff.equals(before.getChildNode(STATUS_NODE), after.getChildNode(STATUS_NODE));
    }

    private static boolean isIndexDefinitionChanged(NodeState before, NodeState after) {
        return !EqualsDiff.equals(before.getChildNode(INDEX_DEFINITION_NODE), after.getChildNode(INDEX_DEFINITION_NODE));
    }
}
