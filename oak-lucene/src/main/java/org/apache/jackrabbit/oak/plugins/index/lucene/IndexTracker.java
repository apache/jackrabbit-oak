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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.isLuceneIndexNode;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.PerfLogger;
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

    private NodeState root = EMPTY_NODE;

    private volatile Map<String, IndexNode> indices = emptyMap();

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
        Map<String, IndexNode> indices = this.indices;
        this.indices = emptyMap();

        for (Map.Entry<String, IndexNode> entry : indices.entrySet()) {
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

    private synchronized void diffAndUpdate(final NodeState root) {
        Map<String, IndexNode> original = indices;
        final Map<String, IndexNode> updates = newHashMap();

        List<Editor> editors = newArrayListWithCapacity(original.size());
        for (Map.Entry<String, IndexNode> entry : original.entrySet()) {
            final String path = entry.getKey();

            editors.add(new SubtreeEditor(new DefaultEditor() {
                @Override
                public void leave(NodeState before, NodeState after) {
                    try {
                        long start = PERF_LOGGER.start();
                        IndexNode index = IndexNode.open(path, root, after, readerFactory, nrtFactory);
                        PERF_LOGGER.end(start, -1, "[{}] Index found to be updated. Reopening the IndexNode", path);
                        updates.put(path, index); // index can be null
                    } catch (IOException e) {
                        log.error("Failed to open Lucene index at " + path, e);
                    }
                }
            }, Iterables.toArray(PathUtils.elements(path), String.class)));
        }

        EditorDiff.process(CompositeEditor.compose(editors), this.root, root);
        this.root = root;

        if (!updates.isEmpty()) {
            indices = ImmutableMap.<String, IndexNode>builder()
                    .putAll(filterKeys(original, not(in(updates.keySet()))))
                    .putAll(filterValues(updates, notNull()))
                    .build();

            //This might take some time as close need to acquire the
            //write lock which might be held by current running searches
            //Given that Tracker is now invoked from a BackgroundObserver
            //not a high concern
            for (String path : updates.keySet()) {
                IndexNode index = original.get(path);
                try {
                    index.close();
                } catch (IOException e) {
                    log.error("Failed to close Lucene index at " + path, e);
                }
            }
        }
    }

    void refresh() {
        refresh = true;
    }

    public IndexNode acquireIndexNode(String path) {
        IndexNode index = indices.get(path);
        if (index != null && index.acquire()) {
            return index;
        } else {
            return findIndexNode(path);
        }
    }

    @CheckForNull
    public IndexDefinition getIndexDefinition(String indexPath){
        IndexNode node = indices.get(indexPath);
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

    private synchronized IndexNode findIndexNode(String path) {
        // Retry the lookup from acquireIndexNode now that we're
        // synchronized. The acquire() call is guaranteed to succeed
        // since the close() method is also synchronized.
        IndexNode index = indices.get(path);
        if (index != null) {
            checkState(index.acquire());
            return index;
        }

        NodeState node = root;
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }

        try {
            if (isLuceneIndexNode(node)) {
                index = IndexNode.open(path, root, node, readerFactory, nrtFactory);
                if (index != null) {
                    checkState(index.acquire());
                    indices = ImmutableMap.<String, IndexNode>builder()
                            .putAll(indices)
                            .put(path, index)
                            .build();
                    return index;
                }
            } else if (node.exists()) {
                log.warn("Cannot open Lucene Index at path {} as the index is not of type {}", path, TYPE_LUCENE);
            }
        } catch (IOException e) {
            log.error("Could not access the Lucene index at " + path, e);
        }

        return null;
    }
}
