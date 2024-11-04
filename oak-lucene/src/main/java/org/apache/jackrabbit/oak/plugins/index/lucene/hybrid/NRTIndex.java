/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.update.IndexUpdateListener;
import org.apache.jackrabbit.oak.plugins.index.search.update.ReaderRefreshPolicy;
import org.apache.jackrabbit.oak.plugins.metric.util.StatsProviderUtil;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryUtils.dirSize;


public class NRTIndex implements Closeable {

    private static final boolean REGULAR_CLOSE = Boolean.getBoolean("oak.lucene.nrt.regularClose");
    private static final boolean RETAIN_DURING_CLOSE = Boolean.getBoolean("oak.lucene.nrt.retainDuringClose");

    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final Logger log = LoggerFactory.getLogger(NRTIndex.class);

    /**
     * Prefix used for naming the directory created for NRT indexes
     */
    public static final String NRT_DIR_PREFIX = "nrt-";

    private final LuceneIndexDefinition definition;
    private final IndexCopier indexCopier;
    private final IndexUpdateListener refreshPolicy;

    private final StatisticsProvider statisticsProvider;
    private final TimerStats refreshTimer;
    private final HistogramStats sizeHisto;
    private final TimerStats.Context openTime;
    private final NRTDirectoryFactory directoryFactory;

    private NRTIndex previous;

    private IndexWriter indexWriter;
    private NRTIndexWriter nrtIndexWriter;
    private File indexDir;
    private Directory directory;
    private DirectoryReader dirReader;
    private DirectoryReader dirReaderUsedForPrevious;
    private boolean closed;
    private boolean previousModeEnabled;
    private List<LuceneIndexReader> readers;
    private final List<IndexReader> openedReaders;
    private final boolean assertAllReadersClosed;
    private final StatsProviderUtil statsProviderUtil;
    private final Map<String, String> labels;


    public NRTIndex(LuceneIndexDefinition definition, IndexCopier indexCopier,
                    IndexUpdateListener refreshPolicy, @Nullable NRTIndex previous,
                    StatisticsProvider statisticsProvider, NRTDirectoryFactory directoryFactory,
                    boolean assertAllReadersClosed) {
        this.definition = definition;
        this.indexCopier = indexCopier;
        this.refreshPolicy = refreshPolicy;
        this.previous = previous;
        this.statisticsProvider = statisticsProvider;
        this.directoryFactory = directoryFactory;
        this.assertAllReadersClosed = assertAllReadersClosed;
        this.openedReaders = assertAllReadersClosed ? new LinkedList<>() : Collections.emptyList();
        statsProviderUtil = new StatsProviderUtil(statisticsProvider);
        labels = Collections.singletonMap("index", definition.getIndexPath());

        this.refreshTimer = statsProviderUtil.getTimerStats().apply(metricName("REFRESH_TIME"), labels);
        this.sizeHisto = statsProviderUtil.getHistoStats().apply(metricName("SIZE"), labels);
        this.openTime = statsProviderUtil.getTimerStats().apply(metricName("OPEN_TIME"), labels).time();
    }

    /**
     * Note that this method is called from a different NRTIndex instance getReaders
     * call. So "dirReader" instance changed here is different
     */
    @Nullable
    private LuceneIndexReader getPrimaryReader() {
        DirectoryReader latestReader = createReader(dirReaderUsedForPrevious);
        while (latestReader != null && !latestReader.tryIncRef()) {
            latestReader = createReader(dirReaderUsedForPrevious);
        }
        if (latestReader != dirReaderUsedForPrevious) {
            decrementReaderUseCount(dirReaderUsedForPrevious);
            dirReaderUsedForPrevious = latestReader;
        }
        return latestReader != null ? new NRTReader(latestReader, directory) : null;
    }

    public LuceneIndexWriter getWriter() throws IOException {
        Validate.checkState(!closed);
        if (nrtIndexWriter == null) {
            nrtIndexWriter = createWriter();
        }
        return nrtIndexWriter;
    }

    /**
     * Returns the list of LuceneIndexReader. If the writer has not received
     * any updates between 2 calls to this method then same list would be
     * returned.
     */
    public synchronized List<LuceneIndexReader> getReaders() {
        Validate.checkState(!closed);
        Validate.checkState(!previousModeEnabled);
        DirectoryReader latestReader = createReader(dirReader);
        //reader not changed i.e. no change in index
        //reuse old readers
        if (latestReader == dirReader && readers != null){
            return readers;
        }

        List<LuceneIndexReader> newReaders = new ArrayList<>(2);
        if (latestReader != null) {
            newReaders.add(new NRTReader(latestReader, directory));
        }

        //Old reader should be added later
        LuceneIndexReader previousReader = previous != null ? previous.getPrimaryReader() : null;
        if (previousReader != null) {
            newReaders.add(previousReader);
        }

        decrementReaderUseCount(readers);

        dirReader = latestReader;
        readers = ImmutableList.copyOf(newReaders);
        return readers;
    }

    public ReaderRefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    /**
     * Disconnects the previous reader used by this NRTIndex. Note that this would be
     * different from 'dirReaderUsedForPrevious' which is meant to be used by newer NRTIndex
     * which refers to this NRTIndex as previous
     */
    public void disconnectPrevious(){
        decrementReaderUseCount(readers);
        readers = Collections.emptyList();

        //From now onwards no caller should be invoked getReaders
        //only call for getPrimaryReader would be allowed
        previousModeEnabled = true;
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }

        log.debug("[{}] Closing NRTIndex [{}]", definition.getIndexPath(), getName());

        decrementReaderUseCount(dirReaderUsedForPrevious);
        //'readers' already has dirReader so no need to close it explicitly
        decrementReaderUseCount(readers);

        assertAllReadersAreClosed();

        if (indexWriter != null) {

            long time = System.nanoTime();
            if (REGULAR_CLOSE) {
                indexWriter.close();
            } else {
                if (RETAIN_DURING_CLOSE) {
                    // retain entries (old style behavior)
                } else {
                    // delete all documents before closing,
                    // so that closing is faster (less writes are needed)
                    // (we anyway delete the directory after closing)
                    indexWriter.deleteAll();
                }
                // don't merge, as anyway only keep two generations
                indexWriter.close(false);
            }
            time = System.nanoTime() - time;
            if (time > 100_000_000) {
                // slower than 100 ms
                log.info("Closing time: {} ns", time);
            } else if (log.isTraceEnabled()) {
                log.trace("Closing time: {} ns", time);
            }

            sizeHisto.update(dirSize(directory));
            directory.close();
            FileUtils.deleteQuietly(indexDir);
            log.debug("[{}] Removed directory [{}]", this, indexDir);
        }
        //Null the reference to previous so as to let it
        //garbage collect. It would not be accessed post close
        previous = null;

        closed = true;
        openTime.stop();
    }

    public boolean isClosed() {
        return closed;
    }

    NRTIndex getPrevious() {
        return previous;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", definition.getIndexPath(), getName());
    }

    //For test
    File getIndexDir() {
        return indexDir;
    }

    private String getName(){
        return indexDir != null ? indexDir.getName() : "UNKNOWN";
    }

    private void assertAllReadersAreClosed() {
        for (IndexReader r : openedReaders){
            if (r.getRefCount() != 0){
                String msg = String.format("Unclosed reader found with refCount %d for index %s", r.getRefCount(), toString());
                throw new IllegalStateException(msg);
            }
        }
    }

    private void decrementReaderUseCount(@Nullable List<LuceneIndexReader> readers) {
        if (readers != null) {
            for (LuceneIndexReader r : readers) {
                decrementReaderUseCount(r.getReader());
            }
        }
    }

    private void decrementReaderUseCount(IndexReader reader) {
        try {
            if (reader != null) {
                reader.decRef();
            }
        } catch (IOException e) {
            log.warn("[{}] Error occurred while releasing reader instance {}",
                    definition.getIndexPath(), toString(), e);
        }
    }

    /**
     * If index was updated then a new reader would be returned otherwise
     * existing reader would be returned
     */
    @Nullable
    private synchronized DirectoryReader createReader(DirectoryReader dirReader) {
        Validate.checkState(!closed);
        //Its possible that readers are obtained
        //before anything gets indexed
        if (indexWriter == null) {
            return null;
        }
        DirectoryReader result = dirReader;
        try {
            TimerStats.Context ctx = refreshTimer.time();
            //applyDeletes is false as layers above would take care of
            //stale result
            if (dirReader == null || dirReader.getRefCount() == 0) {
                result = DirectoryReader.open(indexWriter, false);
            } else {
                DirectoryReader newReader = DirectoryReader.openIfChanged(dirReader, indexWriter, false);
                if (newReader != null) {
                    result = newReader;
                }
            }
            ctx.stop();

            if (assertAllReadersClosed && result != null && result != dirReader) {
                openedReaders.add(result);
            }

            return result;
        } catch (IOException e) {
            log.warn("Error opening index [{}]", e);
        }
        return null;
    }

    private synchronized NRTIndexWriter createWriter() throws IOException {
        String dirName = generateDirName();
        indexDir = indexCopier.getIndexDir(definition, definition.getIndexPath(), dirName);
        directory = directoryFactory.createNRTDir(definition, indexDir);
        IndexWriterConfig config = IndexWriterUtils.getIndexWriterConfig(definition, false);

        //TODO Explore following for optimizing indexing speed
        //config.setUseCompoundFile(false);
        //config.setRAMBufferSizeMB(1024*1024*25);

        indexWriter = new IndexWriter(directory, config);
        log.debug("[{}] Created NRTIndex [{}]", definition.getIndexPath(), getName());
        return new NRTIndexWriter(indexWriter);
    }

    IndexReader getPrimaryReaderForTest(){
        return getReaders().get(0).getReader();
    }

    public static String generateDirName() {
        long uniqueCount = System.currentTimeMillis() + COUNTER.incrementAndGet();
        return NRT_DIR_PREFIX + uniqueCount;
    }

    private static class NRTReader implements LuceneIndexReader {
        private final IndexReader indexReader;
        private final Directory directory;

        public NRTReader(IndexReader indexReader, Directory directory) {
            this.indexReader = requireNonNull(indexReader);
            this.directory = directory;
        }

        @Override
        public IndexReader getReader() {
            return indexReader;
        }

        @Override
        public AnalyzingInfixSuggester getLookup() {
            return null;
        }

        @Override
        public Directory getSuggestDirectory() {
            return null;
        }

        @Override
        public long getIndexSize() throws IOException {
            return dirSize(directory);
        }

        @Override
        public void close() throws IOException {

        }
    }

    private class NRTIndexWriter implements LuceneIndexWriter {
        private final IndexWriter indexWriter;
        private final MeterStats updateMeter;

        public NRTIndexWriter(IndexWriter indexWriter) {
            this.indexWriter = indexWriter;
            this.updateMeter = statsProviderUtil.getMeterStats().apply(metricName("UPDATES"), labels);
        }

        @Override
        public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
            //For NRT case documents are never updated
            //instead they are just added. This would cause duplicates
            //That should be taken care at query side via unique cursor
            indexWriter.addDocument(doc);
            refreshPolicy.updated();
            updateMeter.mark();
        }

        @Override
        public void deleteDocuments(String path) throws IOException {
            //Do not delete documents. Query side would handle it
        }

        @Override
        public boolean close(long timestamp) throws IOException {
            throw new IllegalStateException("Close should not be called");
        }
    }

    private String metricName(String suffix){
        return String.format("NRT_%s", suffix);
    }
}
