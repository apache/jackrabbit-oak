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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.lucene.store.NoLockFactory.getNoLockFactory;

public class LuceneIndexEditorContext {

    private static final Logger log = LoggerFactory
            .getLogger(LuceneIndexEditorContext.class);

    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(LuceneIndexEditorContext.class.getName() + ".perf"));

    static IndexWriterConfig getIndexWriterConfig(IndexDefinition definition, boolean remoteDir) {
        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            Analyzer definitionAnalyzer = definition.getAnalyzer();
            Map<String, Analyzer> analyzers = new HashMap<String, Analyzer>();
            analyzers.put(FieldNames.SPELLCHECK, new ShingleAnalyzerWrapper(LuceneIndexConstants.ANALYZER, 3));
            if (!definition.isSuggestAnalyzed()) {
                analyzers.put(FieldNames.SUGGEST, SuggestHelper.getAnalyzer());
            }
            Analyzer analyzer = new PerFieldAnalyzerWrapper(definitionAnalyzer, analyzers);
            IndexWriterConfig config = new IndexWriterConfig(VERSION, analyzer);
            if (remoteDir) {
                config.setMergeScheduler(new SerialMergeScheduler());
            }
            if (definition.getCodec() != null) {
                config.setCodec(definition.getCodec());
            }
            return config;
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    static Directory newIndexDirectory(IndexDefinition indexDefinition, NodeBuilder definition)
            throws IOException {
        String path = definition.getString(PERSISTENCE_PATH);
        if (path == null) {
            return new OakDirectory(definition, indexDefinition, false);
        } else {
            // try {
            File file = new File(path);
            file.mkdirs();
            // TODO: close() is never called
            // TODO: no locking used
            // --> using the FS backend for the index is in any case
            // troublesome in clustering scenarios and for backup
            // etc. so instead of fixing these issues we'd better
            // work on making the in-content index work without
            // problems (or look at the Solr indexer as alternative)
            return FSDirectory.open(file, getNoLockFactory());
            // } catch (IOException e) {
            // throw new CommitFailedException("Lucene", 1,
            // "Failed to open the index in " + path, e);
            // }
        }
    }

    private static final Parser defaultParser = createDefaultParser();

    private final IndexDefinition definition;

    private final NodeBuilder definitionBuilder;

    private IndexWriter writer = null;

    private long indexedNodes;

    private final IndexUpdateCallback updateCallback;

    private boolean reindex;

    private Parser parser;

    @Nullable
    private final IndexCopier indexCopier;

    private Directory directory;

    private final TextExtractionStats textExtractionStats = new TextExtractionStats();

    private final ExtractedTextCache extractedTextCache;
    /**
     * The media types supported by the parser used.
     */
    private Set<MediaType> supportedMediaTypes;

    LuceneIndexEditorContext(NodeState root, NodeBuilder definition, IndexUpdateCallback updateCallback,
                             @Nullable IndexCopier indexCopier, ExtractedTextCache extractedTextCache) {
        this.definitionBuilder = definition;
        this.indexCopier = indexCopier;
        this.definition = new IndexDefinition(root, definition);
        this.indexedNodes = 0;
        this.updateCallback = updateCallback;
        this.extractedTextCache = extractedTextCache;
        if (this.definition.isOfOldFormat()){
            IndexDefinition.updateDefinition(definition);
        }
    }

    Parser getParser() {
        if (parser == null){
            parser = initializeTikaParser(definition);
        }
        return parser;
    }

    IndexWriter getWriter() throws IOException {
        if (writer == null) {
            final long start = PERF_LOGGER.start();
            directory = newIndexDirectory(definition, definitionBuilder);
            IndexWriterConfig config;
            if (indexCopier != null){
                directory = indexCopier.wrapForWrite(definition, directory, reindex);
                config = getIndexWriterConfig(definition, false);
            } else {
                config = getIndexWriterConfig(definition, true);
            }
            writer = new IndexWriter(directory, config);
            PERF_LOGGER.end(start, -1, "Created IndexWriter for directory {}", definition);
        }
        return writer;
    }

    private static void trackIndexSizeInfo(@Nonnull IndexWriter writer,
                                           @Nonnull IndexDefinition definition,
                                           @Nonnull Directory directory) throws IOException {
        checkNotNull(writer);
        checkNotNull(definition);
        checkNotNull(directory);

        int docs = writer.numDocs();
        int ram = writer.numRamDocs();

        log.trace("Writer for direcory {} - docs: {}, ramDocs: {}", definition, docs, ram);

        String[] files = directory.listAll();
        long overallSize = 0;
        StringBuilder sb = new StringBuilder();
        for (String f : files) {
            sb.append(f).append(":");
            if (directory.fileExists(f)) {
                long size = directory.fileLength(f);
                overallSize += size;
                sb.append(size);
            } else {
                sb.append("--");
            }
            sb.append(", ");
        }
        log.trace("Directory overall size: {}, files: {}",
            org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount(overallSize),
            sb.toString());
    }

    /**
     * close writer if it's not null
     */
    void closeWriter() throws IOException {
        //If reindex or fresh index and write is null on close
        //it indicates that the index is empty. In such a case trigger
        //creation of write such that an empty Lucene index state is persisted
        //in directory
        if (reindex && writer == null){
            getWriter();
        }

        if (writer != null) {
            if (log.isTraceEnabled()) {
                trackIndexSizeInfo(writer, definition, directory);
            }

            final long start = PERF_LOGGER.start();

            updateSuggester(writer.getAnalyzer());
            PERF_LOGGER.end(start, -1, "Completed suggester for directory {}", definition);

            writer.close();
            PERF_LOGGER.end(start, -1, "Closed writer for directory {}", definition);

            directory.close();
            PERF_LOGGER.end(start, -1, "Closed directory for directory {}", definition);

            //OAK-2029 Record the last updated status so
            //as to make IndexTracker detect changes when index
            //is stored in file system
            NodeBuilder status = definitionBuilder.child(":status");
            status.setProperty("lastUpdated", ISO8601.format(Calendar.getInstance()), Type.DATE);
            status.setProperty("indexedNodes",indexedNodes);
            PERF_LOGGER.end(start, -1, "Overall Closed IndexWriter for directory {}", definition);

            textExtractionStats.log(reindex);
            textExtractionStats.collectStats(extractedTextCache);
        }
    }

    /**
     * eventually update suggest dictionary
     * @throws IOException if suggest dictionary update fails
     * @param analyzer the analyzer used to update the suggester
     */
    private void updateSuggester(Analyzer analyzer) throws IOException {

        if (definition.isSuggestEnabled()) {

            boolean updateSuggester = false;
            NodeBuilder suggesterStatus = definitionBuilder.child(":suggesterStatus");
            if (suggesterStatus.hasProperty("lastUpdated")) {
                PropertyState suggesterLastUpdatedValue = suggesterStatus.getProperty("lastUpdated");
                Calendar suggesterLastUpdatedTime = ISO8601.parse(suggesterLastUpdatedValue.getValue(Type.DATE));
                int updateFrequency = definition.getSuggesterUpdateFrequencyMinutes();
                suggesterLastUpdatedTime.add(Calendar.MINUTE, updateFrequency);
                if (Calendar.getInstance().after(suggesterLastUpdatedTime)) {
                    updateSuggester = true;
                }
            } else {
                updateSuggester = true;
            }

            if (updateSuggester) {
                DirectoryReader reader = DirectoryReader.open(writer, false);
                final OakDirectory suggestDirectory = new OakDirectory(definitionBuilder, ":suggest-data", definition, false);
                try {
                    SuggestHelper.updateSuggester(suggestDirectory, analyzer, reader);
                    suggesterStatus.setProperty("lastUpdated", ISO8601.format(Calendar.getInstance()), Type.DATE);
                } catch (Throwable e) {
                    log.warn("could not update suggester", e);
                } finally {
                    suggestDirectory.close();
                    reader.close();
                }
            }
        }
    }

    public void enableReindexMode(){
        reindex = true;
        IndexFormatVersion version = IndexDefinition.determineVersionForFreshIndex(definitionBuilder);
        definitionBuilder.setProperty(IndexDefinition.INDEX_VERSION, version.getVersion());
    }

    public long incIndexedNodes() {
        indexedNodes++;
        return indexedNodes;
    }

    public long getIndexedNodes() {
        return indexedNodes;
    }

    public boolean isSupportedMediaType(String type) {
        if (supportedMediaTypes == null) {
            supportedMediaTypes = getParser().getSupportedTypes(new ParseContext());
        }
        return supportedMediaTypes.contains(MediaType.parse(type));
    }

    void indexUpdate() throws CommitFailedException {
        updateCallback.indexUpdate();
    }

    public IndexDefinition getDefinition() {
        return definition;
    }

    @Deprecated
    public void recordTextExtractionStats(long timeInMillis, long bytesRead) {
        //Keeping deprecated method to avoid major version change
        recordTextExtractionStats(timeInMillis, bytesRead, 0);
    }

    public void recordTextExtractionStats(long timeInMillis, long bytesRead, int textLength) {
        textExtractionStats.addStats(timeInMillis, bytesRead, textLength);
    }

    ExtractedTextCache getExtractedTextCache() {
        return extractedTextCache;
    }

    public boolean isReindex() {
        return reindex;
    }

    private static Parser initializeTikaParser(IndexDefinition definition) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        try {
            if (definition.hasCustomTikaConfig()) {
                Thread.currentThread().setContextClassLoader(LuceneIndexEditorContext.class.getClassLoader());
                InputStream is = definition.getTikaConfig();
                try {
                    return new AutoDetectParser(getTikaConfig(is, definition));
                } finally {
                    IOUtils.closeQuietly(is);
                }
            }
        }finally {
            Thread.currentThread().setContextClassLoader(current);
        }
        return defaultParser;
    }

    private static AutoDetectParser createDefaultParser() {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        URL configUrl = LuceneIndexEditorContext.class.getResource("tika-config.xml");
        InputStream is = null;
        if (configUrl != null) {
            try {
                Thread.currentThread().setContextClassLoader(LuceneIndexEditorContext.class.getClassLoader());
                is = configUrl.openStream();
                TikaConfig config = new TikaConfig(is);
                log.info("Loaded default Tika Config from classpath {}", configUrl);
                return new AutoDetectParser(config);
            } catch (Exception e) {
                log.warn("Tika configuration not available : " + configUrl, e);
            } finally {
                IOUtils.closeQuietly(is);
                Thread.currentThread().setContextClassLoader(current);
            }
        } else {
            log.warn("Default Tika configuration not found from {}", configUrl);
        }
        return new AutoDetectParser();
    }

    private static TikaConfig getTikaConfig(InputStream configStream, Object source){
        try {
            return new TikaConfig(configStream);
        } catch (Exception e) {
            log.warn("Tika configuration not available : "+source, e);
        }
        return TikaConfig.getDefaultConfig();
    }

    static class TextExtractionStats {
        /**
         * Log stats only if time spent is more than 2 min
         */
        private static final long LOGGING_THRESHOLD = TimeUnit.MINUTES.toMillis(1);
        private int count;
        private long totalBytesRead;
        private long totalTime;
        private long totalTextLength;

        public void addStats(long timeInMillis, long bytesRead, int textLength) {
            count++;
            totalBytesRead += bytesRead;
            totalTime += timeInMillis;
            totalTextLength += textLength;
        }

        public void log(boolean reindex) {
            if (log.isDebugEnabled()) {
                log.debug("Text extraction stats {}", this);
            } else if (anyParsingDone() && (reindex || isTakingLotsOfTime())) {
                log.info("Text extraction stats {}", this);
            }
        }

        public void collectStats(ExtractedTextCache cache){
            cache.addStats(count, totalTime, totalBytesRead, totalTextLength);
        }

        private boolean isTakingLotsOfTime() {
            return totalTime > LOGGING_THRESHOLD;
        }

        private boolean anyParsingDone() {
            return count > 0;
        }

        @Override
        public String toString() {
            return String.format(" %d (Time Taken %s, Bytes Read %s, Extracted text size %s)",
                    count,
                    timeInWords(totalTime),
                    humanReadableByteCount(totalBytesRead),
                    humanReadableByteCount(totalTextLength));
        }

        private static String timeInWords(long millis) {
            return String.format("%d min, %d sec",
                    TimeUnit.MILLISECONDS.toMinutes(millis),
                    TimeUnit.MILLISECONDS.toSeconds(millis) -
                            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
            );
        }
    }
}
