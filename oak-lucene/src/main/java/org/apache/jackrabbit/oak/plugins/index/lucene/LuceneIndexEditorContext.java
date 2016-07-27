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
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.facet.FacetsConfig;
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

    private final FacetsConfig facetsConfig;

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

    private IndexDefinition definition;

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

    private final IndexAugmentorFactory augmentorFactory;

    private final NodeState root;
    /**
     * The media types supported by the parser used.
     */
    private Set<MediaType> supportedMediaTypes;

    //Intentionally static, so that it can be set without passing around clock objects
    //Set for testing ONLY
    private static Clock clock = Clock.SIMPLE;

    LuceneIndexEditorContext(NodeState root, NodeBuilder definition, IndexUpdateCallback updateCallback,
                             @Nullable IndexCopier indexCopier, ExtractedTextCache extractedTextCache,
                             IndexAugmentorFactory augmentorFactory) {
        configureUniqueId(definition);
        this.root = root;
        this.definitionBuilder = definition;
        this.indexCopier = indexCopier;
        this.definition = new IndexDefinition(root, definition);
        this.indexedNodes = 0;
        this.updateCallback = updateCallback;
        this.extractedTextCache = extractedTextCache;
        this.augmentorFactory = augmentorFactory;
        if (this.definition.isOfOldFormat()){
            IndexDefinition.updateDefinition(definition);
        }
        this.facetsConfig = FacetHelper.getFacetsConfig(definition);
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
                directory = indexCopier.wrapForWrite(definition, directory, reindex, LuceneIndexConstants.INDEX_DATA_CHILD_NAME);
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

        boolean updateSuggestions = shouldUpdateSuggestions();
        if (writer == null && updateSuggestions) {
            log.debug("Would update suggester dictionary although no index changes were detected in current cycle");
            getWriter();
        }

        if (writer != null) {
            if (log.isTraceEnabled()) {
                trackIndexSizeInfo(writer, definition, directory);
            }

            final long start = PERF_LOGGER.start();

            Calendar lastUpdated = null;
            if (updateSuggestions) {
                lastUpdated = updateSuggester(writer.getAnalyzer());
                PERF_LOGGER.end(start, -1, "Completed suggester for directory {}", definition);
            }
            if (lastUpdated == null) {
                lastUpdated = getCalendar();
            }

            writer.close();
            PERF_LOGGER.end(start, -1, "Closed writer for directory {}", definition);

            directory.close();
            PERF_LOGGER.end(start, -1, "Closed directory for directory {}", definition);

            //OAK-2029 Record the last updated status so
            //as to make IndexTracker detect changes when index
            //is stored in file system
            NodeBuilder status = definitionBuilder.child(":status");
            status.setProperty("lastUpdated", ISO8601.format(lastUpdated), Type.DATE);
            status.setProperty("indexedNodes", indexedNodes);

            PERF_LOGGER.end(start, -1, "Overall Closed IndexWriter for directory {}", definition);

            textExtractionStats.log(reindex);
            textExtractionStats.collectStats(extractedTextCache);
        }
    }

    /**
     * eventually update suggest dictionary
     * @throws IOException if suggest dictionary update fails
     * @param analyzer the analyzer used to update the suggester
     * @return {@link Calendar} object representing the lastUpdated value written by suggestions
     */
    private Calendar updateSuggester(Analyzer analyzer) throws IOException {
        Calendar ret = null;
        NodeBuilder suggesterStatus = definitionBuilder.child(":suggesterStatus");
        DirectoryReader reader = DirectoryReader.open(writer, false);
        final OakDirectory suggestDirectory = new OakDirectory(definitionBuilder, ":suggest-data", definition, false);
        try {
            SuggestHelper.updateSuggester(suggestDirectory, analyzer, reader);
            ret = getCalendar();
            suggesterStatus.setProperty("lastUpdated", ISO8601.format(ret), Type.DATE);
        } catch (Throwable e) {
            log.warn("could not update suggester", e);
        } finally {
            suggestDirectory.close();
            reader.close();
        }

        return ret;
    }

    /**
     * Checks if last suggestion build time was done sufficiently in the past AND that there were non-zero indexedNodes
     * stored in the last run. Note, if index is updated only to rebuild suggestions, even then we update indexedNodes,
     * which would be zero in case it was a forced update of suggestions.
     * @return is suggest dict should be updated
     */
    private boolean shouldUpdateSuggestions() {
        boolean updateSuggestions = false;

        if (definition.isSuggestEnabled()) {
            NodeBuilder suggesterStatus = definitionBuilder.child(":suggesterStatus");

            PropertyState suggesterLastUpdatedValue = suggesterStatus.getProperty("lastUpdated");

            if (suggesterLastUpdatedValue != null) {
                Calendar suggesterLastUpdatedTime = ISO8601.parse(suggesterLastUpdatedValue.getValue(Type.DATE));

                int updateFrequency = definition.getSuggesterUpdateFrequencyMinutes();
                Calendar nextSuggestUpdateTime = (Calendar)suggesterLastUpdatedTime.clone();
                nextSuggestUpdateTime.add(Calendar.MINUTE, updateFrequency);
                if (getCalendar().after(nextSuggestUpdateTime)) {
                    updateSuggestions = (writer != null || isIndexUpdatedAfter(suggesterLastUpdatedTime));
                }
            } else {
                updateSuggestions = true;
            }
        }

        return updateSuggestions;
    }

    /**
     * @return {@code false} if persisted lastUpdated time for index is after {@code calendar}. {@code true} otherwise
     */
    private boolean isIndexUpdatedAfter(Calendar calendar) {
        NodeBuilder indexStats = definitionBuilder.child(":status");
        PropertyState indexLastUpdatedValue = indexStats.getProperty("lastUpdated");
        if (indexLastUpdatedValue != null) {
            Calendar indexLastUpdatedTime = ISO8601.parse(indexLastUpdatedValue.getValue(Type.DATE));
            return indexLastUpdatedTime.after(calendar);
        } else {
            return true;
        }
    }

    /** Only set for testing */
    static void setClock(Clock c) {
        checkNotNull(c);
        clock = c;
    }

    static private Calendar getCalendar() {
        Calendar ret = Calendar.getInstance();
        ret.setTime(clock.getDate());
        return ret;
    }

    public void enableReindexMode(){
        reindex = true;
        IndexFormatVersion version = IndexDefinition.determineVersionForFreshIndex(definitionBuilder);
        definitionBuilder.setProperty(IndexDefinition.INDEX_VERSION, version.getVersion());
        configureUniqueId(definitionBuilder);

        //Refresh the index definition based on update builder state
        definition = new IndexDefinition(root, definitionBuilder);
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

    FacetsConfig getFacetsConfig() {
        return facetsConfig;
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

    IndexAugmentorFactory getAugmentorFactory() {
        return augmentorFactory;
    }

    public boolean isReindex() {
        return reindex;
    }

    public static void configureUniqueId(NodeBuilder definition) {
        NodeBuilder status = definition.child(IndexDefinition.STATUS_NODE);
        if (!status.hasProperty(IndexDefinition.PROP_UID)) {
            String uid;
            try {
                uid = String.valueOf(Clock.SIMPLE.getTimeIncreasing());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                uid = String.valueOf(Clock.SIMPLE.getTime());
            }
            status.setProperty(IndexDefinition.PROP_UID, uid);
        }
    }

    private static Parser initializeTikaParser(IndexDefinition definition) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        try {
            if (definition.hasCustomTikaConfig()) {
                log.debug("[{}] Using custom tika config", definition.getIndexName());
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
