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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndex;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.update.ReaderRefreshPolicy;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getAsyncLaneName;

/**
 * Keeps track of the open read sessions for an index.
 */
public class LuceneIndexNodeManager {
    // TODO oak-search: should extend LuceneIndexNodeManager
    /**
     * Name of the hidden node under which information about the checkpoints
     * seen and indexed by each async indexer is kept.
     */
    static final String ASYNC = ":async";

    private static final AtomicInteger SEARCHER_ID_COUNTER = new AtomicInteger();

    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(LuceneIndexNodeManager.class.getName() + ".perf"));

    static LuceneIndexNodeManager open(String indexPath, NodeState root, NodeState defnNodeState,
                                       LuceneIndexReaderFactory readerFactory, @Nullable NRTIndexFactory nrtFactory)
            throws IOException {
        LuceneIndexDefinition definition = new LuceneIndexDefinition(root, defnNodeState, indexPath);
        List<LuceneIndexReader> readers = readerFactory.createReaders(definition, defnNodeState, indexPath);
        NRTIndex nrtIndex = nrtFactory != null ? nrtFactory.createIndex(definition) : null;
        if (!readers.isEmpty() || (nrtIndex != null && !hasAsyncIndexerRun(root, indexPath, defnNodeState))){
            return new LuceneIndexNodeManager(PathUtils.getName(indexPath), definition, readers, nrtIndex);
        }
        return null;
    }

    static boolean hasAsyncIndexerRun(NodeState root, String indexPath, NodeState defnNodeState) {
        boolean hasAsyncNode = root.hasChildNode(ASYNC);

        String asyncLaneName = getAsyncLaneName(defnNodeState, indexPath, defnNodeState.getProperty(ASYNC_PROPERTY_NAME));

        if (asyncLaneName != null) {
            return hasAsyncNode && root.getChildNode(ASYNC).hasProperty(asyncLaneName);
        } else {
            // useful only for tests - basically non-async index defs which don't rely on /:async
            // hence either readers are there (and this method doesn't come into play during open)
            // OR there is no cycle (where we return false correctly)
            return  false;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(LuceneIndexNodeManager.class);

    private final List<LuceneIndexReader> readers;

    private final String name;

    private final LuceneIndexDefinition definition;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile SearcherHolder searcherHolder;

    private final NRTIndex nrtIndex;

    private final ReaderRefreshPolicy refreshPolicy;

    private final Semaphore refreshLock = new Semaphore(1);

    private final Runnable refreshCallback = new Runnable() {
        @Override
        public void run() {

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            FacetTestHelper.log("LuceneIndexNodeManager.refreshCallback.run");
            if (refreshLock.tryAcquire()) {
                try {
                    FacetTestHelper.log("LuceneIndexNodeManager.refreshCallback refreshReaders");
                    refreshReaders();
                    FacetTestHelper.log("LuceneIndexNodeManager.refreshCallback refreshReaders done");
                } finally {
                    refreshLock.release();
                }
            }
        }
    };

    private boolean closed = false;

    LuceneIndexNodeManager(String name, LuceneIndexDefinition definition, List<LuceneIndexReader> readers, @Nullable NRTIndex nrtIndex)
            throws IOException {
        checkArgument(!readers.isEmpty() || nrtIndex != null);
        this.name = name;
        this.definition = definition;
        this.readers = readers;
        this.nrtIndex = nrtIndex;
        this.searcherHolder = createHolder(getNRTReaders());
        this.refreshPolicy = nrtIndex != null ? nrtIndex.getRefreshPolicy() : ReaderRefreshPolicy.NEVER;
    }

    private String getName() {
        return name;
    }

    LuceneIndexDefinition getDefinition() {
        return definition;
    }

    @Nullable
    private Directory getSuggestDirectory() {
        return readers.isEmpty() ? null : getDefaultReader().getSuggestDirectory();
    }

    @Nullable
    private AnalyzingInfixSuggester getLookup() {
        return readers.isEmpty() ? null : getDefaultReader().getLookup();
    }

    @Nullable
    LuceneIndexNode acquire() {
        lock.readLock().lock();
        if (closed) {
            lock.readLock().unlock();
            return null;
        } else {
            boolean success = false;
            try {
                refreshPolicy.refreshOnReadIfRequired(refreshCallback);
                SearcherHolder local = searcherHolder;
                int tryCount = 0;
                FacetTestHelper.log("LuceneIndexNodeManager.acquire inc from " + 
                        local.searcher.getIndexReader().getRefCount() + " of searcher id " + 
                        System.identityHashCode(local.searcher.getIndexReader()));
                while (!local.searcher.getIndexReader().tryIncRef()) {
                    checkState(++tryCount < 10, "Not able to " +
                            "get open searcher in %s attempts", tryCount);
                    local = searcherHolder;
                }
                LuceneIndexNode indexNode = new IndexNodeImpl(local);
                success = true;
                return indexNode;
            } finally {
                if (!success) {
                    lock.readLock().unlock();
                }
            }
        }
    }

    private void release() {
        lock.readLock().unlock();
    }

    void close() throws IOException {
        lock.writeLock().lock();
        try {
            checkState(!closed);
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }

        releaseHolder(searcherHolder);
        closeReaders(readers);
    }

    private List<LuceneIndexReader> getPrimaryReaders() {
        return readers;
    }

    @Nullable
    private LuceneIndexWriter getLocalWriter() throws IOException{
        return nrtIndex != null ? nrtIndex.getWriter() : null;
    }

    private void refreshReadersOnWriteIfRequired() {
        refreshPolicy.refreshOnWriteIfRequired(refreshCallback);
    }

    private void refreshReaders(){
        long start = PERF_LOGGER.start();
        List<LuceneIndexReader> newNRTReaders = getNRTReaders();
        //The list reference would differ if index got updated
        //so if they are same no need to reinitialize the searcher
        if (newNRTReaders != searcherHolder.nrtReaders) {
            SearcherHolder old = searcherHolder;
            searcherHolder = createHolder(newNRTReaders);
            FacetTestHelper.log("LuceneIndexNodeManager.refreshReaders new search holder set");
            releaseHolder(old);
            FacetTestHelper.log("LuceneIndexNodeManager.refreshReaders old released");
            PERF_LOGGER.end(start, 0, "Refreshed reader for index [{}]", definition);
        }
    }

    private LuceneIndexReader getDefaultReader(){
        //TODO This is still required to support Suggester, Spellcheck etc OAK-4643
        return readers.get(0);
    }

    private IndexReader createReader(List<LuceneIndexReader> nrtReaders) {
        //Increment count by 1. MultiReader does it for all readers
        //So no need for an explicit increment for MultiReader

        if (readers.size() == 1 && nrtReaders.isEmpty()){
            IndexReader reader = readers.get(0).getReader();
            reader.incRef();
            FacetTestHelper.log("LuceneIndexNodeManager.createReader1 inc to " + 
                    reader.getRefCount() + " of reader " + 
                    System.identityHashCode(reader));
            return reader;
        }
        if (nrtReaders.size() == 1 && readers.isEmpty()){
            IndexReader reader = nrtReaders.get(0).getReader();
            reader.incRef();
            FacetTestHelper.log("LuceneIndexNodeManager.createReader2 inc to " + 
                    reader.getRefCount() + " of reader " + 
                    System.identityHashCode(reader));
            return reader;
        }
        FacetTestHelper.log("LuceneIndexNodeManager.createReader3 MultiReader");
        IndexReader[] readerArr = new IndexReader[readers.size() + nrtReaders.size()];
        int i = 0;
        for (LuceneIndexReader r : Iterables.concat(readers, nrtReaders)){
            readerArr[i++] = r.getReader();
        }
        return new MultiReader(readerArr, false);
    }

    private List<LuceneIndexReader> getNRTReaders() {
        return nrtIndex != null ? nrtIndex.getReaders() : Collections.<LuceneIndexReader>emptyList();
    }

    private SearcherHolder createHolder(List<LuceneIndexReader> newNRTReaders) {
        return new SearcherHolder(new IndexSearcher(createReader(newNRTReaders)), newNRTReaders);
    }

    private void closeReaders(Iterable<LuceneIndexReader> readers) {
        for (LuceneIndexReader r : readers){
            try {
                r.close();
            } catch (IOException e) {
                log.warn("Error occurred while releasing reader for index [{}]", definition.getIndexPath(), e);
            }
        }
    }

    private void releaseHolder(SearcherHolder holder) {
        FacetTestHelper.log("LuceneIndexNodeManager.releaseHolder");
        decrementSearcherUsageCount(holder.searcher);
    }

    private void decrementSearcherUsageCount(IndexSearcher searcher) {
        try {
            //Decrement the count by 1 as we increased it while creating the searcher
            //in createReader
            searcher.getIndexReader().decRef();
            FacetTestHelper.log("LuceneIndexNodeManager.decrement to " + 
                    searcher.getIndexReader().getRefCount() + " of reader id " + 
                    System.identityHashCode(searcher.getIndexReader()));
        } catch (IOException e) {
            log.warn("Error occurred while releasing reader for index [{}]", definition.getIndexPath(), e);
        }
    }

    private static class SearcherHolder {
        final IndexSearcher searcher;
        final List<LuceneIndexReader> nrtReaders;
        final int searcherId = SEARCHER_ID_COUNTER.incrementAndGet();
        final LuceneIndexStatistics indexStatistics;

        public SearcherHolder(IndexSearcher searcher, List<LuceneIndexReader> nrtReaders) {
            this.searcher = searcher;
            this.nrtReaders = nrtReaders;
            this.indexStatistics = new LuceneIndexStatistics(searcher.getIndexReader());
        }

        public LuceneIndexStatistics getIndexStatistics() {
            return indexStatistics;
        }
    }

    private class IndexNodeImpl implements LuceneIndexNode {
        private final SearcherHolder holder;
        private final AtomicBoolean released = new AtomicBoolean();

        private IndexNodeImpl(SearcherHolder searcherHolder) {
            this.holder = searcherHolder;
        }

        @Override
        public void release() {
            if (released.compareAndSet(false, true)) {
                try {
                    //Decrement on each release
                    decrementSearcherUsageCount(holder.searcher);
                } finally {
                    LuceneIndexNodeManager.this.release();
                }
            }
        }

        @Override
        public IndexSearcher getSearcher() {
            return holder.searcher;
        }

        @Override
        public LuceneIndexStatistics getIndexStatistics() {
            return holder.getIndexStatistics();
        }

        @Override
        public LuceneIndexDefinition getDefinition() {
            return definition;
        }

        @Override
        public List<LuceneIndexReader> getPrimaryReaders() {
            return LuceneIndexNodeManager.this.getPrimaryReaders();
        }

        @Override
        public Directory getSuggestDirectory() {
            return LuceneIndexNodeManager.this.getSuggestDirectory();
        }

        @Override
        public List<LuceneIndexReader> getNRTReaders() {
            return holder.nrtReaders;
        }

        @Override
        public AnalyzingInfixSuggester getLookup() {
            return LuceneIndexNodeManager.this.getLookup();
        }

        @Override
        public int getIndexNodeId() {
            return holder.searcherId;
        }

        @Override
        public LuceneIndexWriter getLocalWriter() throws IOException {
            return LuceneIndexNodeManager.this.getLocalWriter();
        }

        @Override
        public void refreshReadersOnWriteIfRequired() {
            LuceneIndexNodeManager.this.refreshReadersOnWriteIfRequired();
        }
    }

}
