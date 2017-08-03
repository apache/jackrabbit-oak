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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndex;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.ReaderRefreshPolicy;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.commons.benchmark.PerfLogger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexNodeManager {
    /**
     * Name of the hidden node under which information about the checkpoints
     * seen and indexed by each async indexer is kept.
     */
    static final String ASYNC = ":async";

    private static final AtomicInteger INDEX_NODE_COUNTER = new AtomicInteger();

    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(IndexNodeManager.class.getName() + ".perf"));

    static IndexNodeManager open(String indexPath, NodeState root, NodeState defnNodeState,
                                 LuceneIndexReaderFactory readerFactory, @Nullable NRTIndexFactory nrtFactory)
            throws IOException {
        IndexDefinition definition = new IndexDefinition(root, defnNodeState, indexPath);
        List<LuceneIndexReader> readers = readerFactory.createReaders(definition, defnNodeState, indexPath);
        NRTIndex nrtIndex = nrtFactory != null ? nrtFactory.createIndex(definition) : null;
        if (!readers.isEmpty() || (nrtIndex != null && !hasAsyncIndexerRun(root))){
            return new IndexNodeManager(PathUtils.getName(indexPath), definition, readers, nrtIndex);
        }
        return null;
    }

    static boolean hasAsyncIndexerRun(NodeState root) {
        return root.hasChildNode(ASYNC);
    }

    private static final Logger log = LoggerFactory.getLogger(IndexNodeManager.class);

    private final List<LuceneIndexReader> readers;

    private final String name;

    private final IndexDefinition definition;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile IndexSearcher indexSearcher;

    private final NRTIndex nrtIndex;

    private final ReaderRefreshPolicy refreshPolicy;

    private final Runnable refreshCallback = new Runnable() {
        @Override
        public void run() {
            refreshReaders();
        }
    };

    private boolean closed = false;

    private List<LuceneIndexReader> nrtReaders;

    private final int indexNodeId = INDEX_NODE_COUNTER.incrementAndGet();

    IndexNodeManager(String name, IndexDefinition definition, List<LuceneIndexReader> readers, @Nullable NRTIndex nrtIndex)
            throws IOException {
        checkArgument(!readers.isEmpty() || nrtIndex != null);
        this.name = name;
        this.definition = definition;
        this.readers = readers;
        this.nrtIndex = nrtIndex;
        this.nrtReaders = getNRTReaders();
        this.indexSearcher = new IndexSearcher(createReader(nrtReaders));
        this.refreshPolicy = nrtIndex != null ? nrtIndex.getRefreshPolicy() : ReaderRefreshPolicy.NEVER;
    }

    String getName() {
        return name;
    }

    IndexDefinition getDefinition() {
        return definition;
    }

    @CheckForNull
    private Directory getSuggestDirectory() {
        return readers.isEmpty() ? null : getDefaultReader().getSuggestDirectory();
    }

    @CheckForNull
    private AnalyzingInfixSuggester getLookup() {
        return readers.isEmpty() ? null : getDefaultReader().getLookup();
    }

    IndexNode acquire() {
        lock.readLock().lock();
        if (closed) {
            lock.readLock().unlock();
            return null;
        } else {
            boolean success = false;
            try {
                refreshPolicy.refreshOnReadIfRequired(refreshCallback);
                success = true;
                return new IndexNodeImpl(indexSearcher);
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

    private int getIndexNodeId() {
        return indexNodeId;
    }

    void close() throws IOException {
        lock.writeLock().lock();
        try {
            checkState(!closed);
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }

        //Do not close the NRTIndex here as it might be in use
        //by newer IndexNode. Just close the readers obtained from
        //them
        for (LuceneIndexReader reader : Iterables.concat(readers, nrtReaders)){
           reader.close();
        }
    }

    private List<LuceneIndexReader> getPrimaryReaders() {
        return readers;
    }

    @CheckForNull
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
        if (newNRTReaders != nrtReaders) {
            nrtReaders = newNRTReaders;
            indexSearcher = new IndexSearcher(createReader(nrtReaders));
            PERF_LOGGER.end(start, 0, "Refreshed reader for index [{}]", definition);
        }
    }

    private LuceneIndexReader getDefaultReader(){
        //TODO This is still required to support Suggester, Spellcheck etc OAK-4643
        return readers.get(0);
    }

    private IndexReader createReader(List<LuceneIndexReader> nrtReaders) {
        if (readers.size() == 1 && nrtReaders.isEmpty()){
            return readers.get(0).getReader();
        }
        if (nrtReaders.size() == 1 && readers.isEmpty()){
            return nrtReaders.get(0).getReader();
        }
        IndexReader[] readerArr = new IndexReader[readers.size() + nrtReaders.size()];
        int i = 0;
        for (LuceneIndexReader r : Iterables.concat(readers, nrtReaders)){
            readerArr[i++] = r.getReader();
        }
        return new MultiReader(readerArr, true);
    }

    private List<LuceneIndexReader> getNRTReaders() {
        return nrtIndex != null ? nrtIndex.getReaders() : Collections.<LuceneIndexReader>emptyList();
    }

    private class IndexNodeImpl implements IndexNode {
        private final IndexSearcher searcher;

        private IndexNodeImpl(IndexSearcher searcher) {
            this.searcher = searcher;
        }

        @Override
        public void release() {
            IndexNodeManager.this.release();
        }

        @Override
        public IndexSearcher getSearcher() {
            return searcher;
        }

        @Override
        public IndexDefinition getDefinition() {
            return definition;
        }

        @Override
        public List<LuceneIndexReader> getPrimaryReaders() {
            return IndexNodeManager.this.getPrimaryReaders();
        }

        @Override
        public Directory getSuggestDirectory() {
            return IndexNodeManager.this.getSuggestDirectory();
        }

        @Override
        public List<LuceneIndexReader> getNRTReaders() {
            return IndexNodeManager.this.nrtReaders;
        }

        @Override
        public AnalyzingInfixSuggester getLookup() {
            return IndexNodeManager.this.getLookup();
        }

        @Override
        public int getIndexNodeId() {
            return IndexNodeManager.this.getIndexNodeId();
        }

        @Override
        public LuceneIndexWriter getLocalWriter() throws IOException {
            return IndexNodeManager.this.getLocalWriter();
        }

        @Override
        public void refreshReadersOnWriteIfRequired() {
            IndexNodeManager.this.refreshReadersOnWriteIfRequired();
        }
    }

}
