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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexStorage;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a Lucene 9 index with its definition and a cached searcher.
 *
 * <p>One instance is built per generation of the index (whenever the tracker detects a
 * definition or storage change) — it is never mutated or reopened in place. Wrapped by
 * {@link LuceneNgIndexNodeManager}, whose inherited {@code IndexNodeManager} read/write
 * lock is what makes {@link #release()} / {@link #closeResources()} safe: {@code close()}
 * on the manager cannot return, and therefore {@link #closeResources()} cannot run, until
 * every {@code acquire()}-holder has called {@link #release()}. Do not reintroduce
 * per-call {@code IndexReader.tryIncRef()/decRef()} bookkeeping here — it is redundant
 * with that lock and duplicating it reintroduces a concurrency race.</p>
 */
public class LuceneNgIndexNode implements IndexNode {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexNode.class);
    private static final AtomicInteger ID_COUNTER = new AtomicInteger();

    private final String indexPath;
    /** Immutable snapshot of the index definition — used for definition change detection. */
    private final NodeState indexState;
    /**
     * Immutable snapshot of the storage node ({@link LuceneNgIndexStorage#STORAGE_NODE_NAME} child).
     * Used together with {@link #indexState} to detect when data changes independently
     * of the definition (which is the normal case during incremental indexing).
     */
    private final NodeState storageState;
    private final LuceneNgIndexDefinition definition;
    /** Cached searcher; null when index has not been populated yet. */
    private final IndexSearcherHolder searcherHolder;
    private final int indexNodeId = ID_COUNTER.incrementAndGet();

    /** Set once by {@link LuceneNgIndexNodeManager}'s constructor. Package-private:
     *  only the owning manager binds itself, and only {@link #release()} reads it. */
    private LuceneNgIndexNodeManager owner;

    /**
     * Creates a new index node, opening a cached {@link IndexSearcher} from
     * {@link LuceneNgIndexStorage}.
     * If the storage path does not exist yet the searcher is left null and
     * {@link #getSearcher()} returns null.
     *
     * @param indexPath  path to the index definition (e.g. "/oak:index/myIndex")
     * @param root       repository root state
     * @param indexState index definition node state (immutable snapshot)
     */
    public LuceneNgIndexNode(@NotNull String indexPath,
                             @NotNull NodeState root,
                             @NotNull NodeState indexState) {
        this.indexPath = indexPath;
        this.indexState = indexState;
        this.definition = new LuceneNgIndexDefinition(root, indexState, indexPath);

        String indexName = PathUtils.getName(indexPath);
        this.storageState = LuceneNgIndexStorage.storageState(indexState);

        IndexSearcherHolder holder = null;
        try {
            holder = new IndexSearcherHolder(storageState, indexName);
        } catch (IOException e) {
            LOG.debug("No index data for {} yet, searcher not opened: {}", indexPath, e.getMessage());
        }
        this.searcherHolder = holder;
    }

    void bindOwner(@NotNull LuceneNgIndexNodeManager owner) {
        this.owner = owner;
    }

    /** Whether this generation of the index has any data yet. Used by
     *  {@link org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexTracker#openIndex}
     *  to return {@code null} (per {@code FulltextIndexTracker}'s documented contract: "index
     *  can be null") when nothing has been indexed yet. */
    public boolean hasSearcher() {
        return searcherHolder != null;
    }

    /** Returns the index path (e.g. "/oak:index/myIndex"). */
    public String getIndexPath() {
        return indexPath;
    }

    /** Returns the immutable index definition state this node was built from. */
    public NodeState getIndexState() {
        return indexState;
    }

    /**
     * Returns the immutable storage state ({@link LuceneNgIndexStorage#storageState(NodeState)})
     * captured when this node was constructed. Used alongside {@link #getIndexState()}
     * to detect commits that only changed data (not the definition).
     */
    public NodeState getStorageState() {
        return storageState;
    }

    @Override
    public LuceneNgIndexDefinition getDefinition() {
        return definition;
    }

    @Override
    public int getIndexNodeId() {
        return indexNodeId;
    }

    @Override
    @Nullable
    public IndexStatistics getIndexStatistics() {
        return searcherHolder != null ? new LuceneNgIndexStatistics(searcherHolder.getReader()) : null;
    }

    public IndexSearcher getSearcher() {
        return searcherHolder != null ? searcherHolder.getSearcher() : null;
    }

    public DefaultSortedSetDocValuesReaderState getFacetReaderState(String fieldName) throws IOException {
        return searcherHolder.getFacetReaderState(fieldName);
    }

    /**
     * Called on every {@code IndexNodeManager.acquire()}/per-query release. Delegates to the
     * owning manager's {@link LuceneNgIndexNodeManager#releaseNode()} (a package-private
     * wrapper around the inherited, otherwise cross-package-inaccessible, {@code protected
     * IndexNodeManager.release()}), which unlocks its read lock — this does NOT close any
     * resource; see {@link #closeResources()} for the once-only teardown path.
     */
    @Override
    public void release() {
        if (owner != null) {
            owner.releaseNode();
        }
    }

    /**
     * Called exactly once, by {@link LuceneNgIndexNodeManager#releaseResources()}, when the
     * manager itself is torn down (superseded by a newer generation, or the tracker/provider
     * shuts down). Never call this directly.
     */
    void closeResources() {
        if (searcherHolder != null) {
            try {
                searcherHolder.close();
            } catch (IOException e) {
                LOG.warn("Error closing searcher for {}", indexPath, e);
            }
        }
    }
}
