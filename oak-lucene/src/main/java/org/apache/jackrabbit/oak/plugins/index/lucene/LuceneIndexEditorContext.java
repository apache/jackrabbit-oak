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
import java.io.InputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

public class LuceneIndexEditorContext {

    private static final Logger log = LoggerFactory
            .getLogger(LuceneIndexEditorContext.class);

    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(LuceneIndexEditorContext.class.getName() + ".perf"));

    private final FacetsConfig facetsConfig;

    private static final Parser defaultParser = createDefaultParser();

    private IndexDefinition definition;

    private final NodeBuilder definitionBuilder;

    private final LuceneIndexWriterFactory indexWriterFactory;

    private LuceneIndexWriter writer = null;

    private long indexedNodes;

    private final IndexUpdateCallback updateCallback;

    private boolean reindex;

    private Parser parser;

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
                             LuceneIndexWriterFactory indexWriterFactory,
                             ExtractedTextCache extractedTextCache,
                             IndexAugmentorFactory augmentorFactory) {
        configureUniqueId(definition);
        this.root = root;
        this.definitionBuilder = definition;
        this.indexWriterFactory = indexWriterFactory;
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

    LuceneIndexWriter getWriter() throws IOException {
        if (writer == null) {
            //Lazy initialization so as to ensure that definition is based
            //on latest NodeBuilder state specially in case of reindexing
            writer = indexWriterFactory.newInstance(definition, definitionBuilder, reindex);
        }
        return writer;
    }

    /**
     * close writer if it's not null
     */
    void closeWriter() throws IOException {
        Calendar currentTime = getCalendar();
        final long start = PERF_LOGGER.start();
        boolean indexUpdated = getWriter().close(currentTime.getTimeInMillis());
        PERF_LOGGER.end(start, -1, "Closed writer for directory {}", definition);


        if (indexUpdated) {
            //OAK-2029 Record the last updated status so
            //as to make IndexTracker detect changes when index
            //is stored in file system
            NodeBuilder status = definitionBuilder.child(":status");
            status.setProperty("lastUpdated", ISO8601.format(currentTime), Type.DATE);
            status.setProperty("indexedNodes", indexedNodes);

            PERF_LOGGER.end(start, -1, "Overall Closed IndexWriter for directory {}", definition);

            textExtractionStats.log(reindex);
            textExtractionStats.collectStats(extractedTextCache);
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
