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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.search.IndexSearcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a Lucene 9 index with its definition and a cached searcher.
 *
 * <p>The {@link IndexSearcher} is opened once at construction time from the
 * index data at {@link LuceneNgIndexStorage#storagePath(String) LuceneNgIndexStorage.storagePath(indexPath)}
 * and reused for all queries against this version of the index. When the index data changes the
 * tracker closes this node and creates a new one with a fresh reader.</p>
 */
public class LuceneNgIndexNode {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexNode.class);

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

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean closed = false;

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

    /** Returns the index definition. */
    public LuceneNgIndexDefinition getDefinition() {
        return definition;
    }

    /**
     * Acquires this node for a query. The caller MUST call {@link AcquiredNode#release()} when
     * done — typically in a try-finally, or by passing the node to a {@link LuceneNgCursor}
     * which releases it on close.
     *
     * @return an acquired node, or {@code null} if the node is closed or has no index data yet
     */
    @Nullable
    public AcquiredNode acquire() {
        lock.readLock().lock();
        if (closed || searcherHolder == null) {
            lock.readLock().unlock();
            return null;
        }
        boolean success = false;
        try {
            if (!searcherHolder.getReader().tryIncRef()) {
                return null;
            }
            success = true;
            return new AcquiredNode(searcherHolder.getSearcher());
        } finally {
            if (!success) {
                lock.readLock().unlock();
            }
        }
    }

    private void releaseReadLock() {
        lock.readLock().unlock();
    }

    /**
     * Closes this node. Blocks until all in-flight {@link AcquiredNode}s have been released,
     * then closes the underlying searcher. Called by the tracker on eviction.
     */
    public void close() {
        lock.writeLock().lock();
        try {
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
        if (searcherHolder != null) {
            try {
                searcherHolder.close();
            } catch (IOException e) {
                LOG.warn("Error closing searcher for {}", indexPath, e);
            }
        }
    }

    /**
     * A live reference to this node's searcher, valid until {@link #release()} is called.
     * Returned by {@link LuceneNgIndexNode#acquire()}.
     */
    public class AcquiredNode {
        private final IndexSearcher searcher;
        private final AtomicBoolean released = new AtomicBoolean();

        AcquiredNode(IndexSearcher searcher) {
            this.searcher = searcher;
        }

        public IndexSearcher getSearcher() {
            return searcher;
        }

        public LuceneNgIndexDefinition getDefinition() {
            return definition;
        }

        /**
         * Returns a cached {@link DefaultSortedSetDocValuesReaderState} for the given Lucene
         * field name. The cache is held by the underlying {@link IndexSearcherHolder} and
         * discarded when the index is refreshed.
         *
         * @throws IllegalArgumentException if {@code fieldName} is not a sortedset field
         */
        public DefaultSortedSetDocValuesReaderState getFacetReaderState(String fieldName)
                throws IOException {
            return searcherHolder.getFacetReaderState(fieldName);
        }

        public void release() {
            if (released.compareAndSet(false, true)) {
                try {
                    searcher.getIndexReader().decRef();
                } catch (IOException e) {
                    LOG.warn("Error decrementing reader ref for {}", indexPath, e);
                } finally {
                    releaseReadLock();
                }
            }
        }
    }
}
