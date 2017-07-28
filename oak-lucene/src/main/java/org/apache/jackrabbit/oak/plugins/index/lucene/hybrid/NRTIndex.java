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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryUtils.dirSize;


public class NRTIndex implements Closeable {
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final Logger log = LoggerFactory.getLogger(NRTIndex.class);

    /**
     * Prefix used for naming the directory created for NRT indexes
     */
    public static final String NRT_DIR_PREFIX = "nrt-";

    private final IndexDefinition definition;
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
    private boolean closed;
    private List<LuceneIndexReader> readers;

    public NRTIndex(IndexDefinition definition, IndexCopier indexCopier,
                    IndexUpdateListener refreshPolicy, @Nullable NRTIndex previous,
                    StatisticsProvider statisticsProvider, NRTDirectoryFactory directoryFactory) {
        this.definition = definition;
        this.indexCopier = indexCopier;
        this.refreshPolicy = refreshPolicy;
        this.previous = previous;
        this.statisticsProvider = statisticsProvider;
        this.directoryFactory = directoryFactory;

        this.refreshTimer = statisticsProvider.getTimer(metricName("REFRESH_TIME"), StatsOptions.METRICS_ONLY);
        this.sizeHisto = statisticsProvider.getHistogram(metricName("SIZE"), StatsOptions.METRICS_ONLY);
        this.openTime = statisticsProvider.getTimer(metricName("OPEN_TIME"), StatsOptions.METRICS_ONLY).time();
    }

    @CheckForNull
    LuceneIndexReader getPrimaryReader() {
        DirectoryReader reader = createReader();
        return reader != null ? new NRTReader(reader, directory) : null;
    }

    public LuceneIndexWriter getWriter() throws IOException {
        checkState(!closed);
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
        checkState(!closed);
        DirectoryReader latestReader = createReader();
        //reader not changed i.e. no change in index
        //reuse old readers
        if (latestReader == dirReader && readers != null){
            return readers;
        }
        List<LuceneIndexReader> newReaders = Lists.newArrayListWithCapacity(2);
        if (latestReader != null) {
            newReaders.add(new NRTReader(latestReader, directory));
        }

        //Old reader should be added later
        LuceneIndexReader previousReader = previous != null ? previous.getPrimaryReader() : null;
        if (previousReader != null) {
            newReaders.add(previousReader);
        }
        dirReader = latestReader;
        readers = ImmutableList.copyOf(newReaders);
        return readers;
    }

    public ReaderRefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        if (indexWriter != null) {
            //TODO Close call can possibly be speeded up by
            //avoiding merge and dropping stuff in memory. To be explored
            //indexWrite.close(waitForMerges)
            indexWriter.close();
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
        return definition.getIndexPath();
    }

    //For test
    File getIndexDir() {
        return indexDir;
    }

    /**
     * If index was updated then a new reader would be returned otherwise
     * existing reader would be returned
     */
    @CheckForNull
    private synchronized DirectoryReader createReader() {
        checkState(!closed);
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
            if (dirReader == null) {
                result = DirectoryReader.open(indexWriter, false);
            } else {
                DirectoryReader newReader = DirectoryReader.openIfChanged(dirReader, indexWriter, false);
                if (newReader != null) {
                    result = newReader;
                }
            }
            ctx.stop();
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
        return new NRTIndexWriter(indexWriter);
    }

    public static String generateDirName() {
        long uniqueCount = System.currentTimeMillis() + COUNTER.incrementAndGet();
        return NRT_DIR_PREFIX + uniqueCount;
    }

    private static class NRTReader implements LuceneIndexReader {
        private final IndexReader indexReader;
        private final Directory directory;

        public NRTReader(IndexReader indexReader, Directory directory) {
            this.indexReader = checkNotNull(indexReader);
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
            this.updateMeter = statisticsProvider.getMeter(metricName("UPDATES"), StatsOptions.METRICS_ONLY);
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
        return String.format("%s_NRT_%s", definition.getIndexPath(), suffix);
    }
}
