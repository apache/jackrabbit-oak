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
package org.apache.jackrabbit.oak.plugins.index.search.spi.binary;

import org.apache.jackrabbit.guava.common.io.CountingInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.io.LazyInputStream;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.WriteLimitReachedException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditor.TEXT_EXTRACTION_ERROR;

/**
 *
 */
public class FulltextBinaryTextExtractor {
    private final static String TEXT_EXTRACTION_TIMER_METRIC_NAME = "TEXT_EXTRACTION_TIME";

    private static final Logger log = LoggerFactory.getLogger(FulltextBinaryTextExtractor.class);
    private static final Parser defaultParser = createDefaultParser();
    private static final long SMALL_BINARY = Long.getLong("oak.search.smallBinary", 16 * 1024);
    private final TextExtractionStats textExtractionStats = new TextExtractionStats();
    private final ExtractedTextCache extractedTextCache;
    private final IndexDefinition definition;
    private final boolean reindex;
    private Parser parser;
    private TikaConfigHolder tikaConfig;
    /**
     * The media types supported by the parser used.
     */
    private Set<MediaType> supportedMediaTypes;
    private Set<MediaType> nonIndexedMediaType;

    public FulltextBinaryTextExtractor(ExtractedTextCache extractedTextCache, IndexDefinition definition, boolean reindex) {
        this.extractedTextCache = extractedTextCache;
        this.definition = definition;
        this.reindex = reindex;
    }

    public void resetStartTime() {
        textExtractionStats.reset();
    }

    public void done(boolean reindex){
        textExtractionStats.log(reindex);
        textExtractionStats.collectStats(extractedTextCache);
    }

    public void logStats() {
        log.info("[{}] Text extraction statistics: {}", getIndexName(), textExtractionStats.formatStats());
    }

    public List<String> newBinary(PropertyState property, NodeState state, String path) {
        List<String> values = new ArrayList<>();
        Metadata metadata = new Metadata();

        //jcr:mimeType is mandatory for a binary to be indexed
        String type = state.getString(JcrConstants.JCR_MIMETYPE);
        type = definition.getTikaMappedMimeType(type);

        if (type == null || !isSupportedMediaType(type)) {
            log.trace("[{}] Ignoring binary content for node {} due to unsupported (or null) jcr:mimeType [{}]",
                    getIndexName(), path, type);
            return values;
        }

        metadata.set(Metadata.CONTENT_TYPE, type);
        if (JCR_DATA.equals(property.getName())) {
            String encoding = state.getString(JcrConstants.JCR_ENCODING);
            if (encoding != null) { // not mandatory
                metadata.set(Metadata.CONTENT_ENCODING, encoding);
            }
        }

        for (Blob v : property.getValue(Type.BINARIES)) {
            String value = parseStringValue(v, metadata, path, property.getName());
            if (value == null) {
                continue;
            }

            values.add(value);
        }
        return values;
    }

    private String parseStringValue(Blob v, Metadata metadata, String path, String propertyName) {
        String text = extractedTextCache.get(path, propertyName, v, reindex);
        if (text == null) {
            StatisticsProvider stats = extractedTextCache.getStatisticsProvider();
            if (stats != null) {
                TimerStats textExtractionTimerMetricStats = stats
                        .getTimer(TEXT_EXTRACTION_TIMER_METRIC_NAME, StatsOptions.METRICS_ONLY);
                TimerStats.Context context = textExtractionTimerMetricStats.time();
                text = parseStringValue0(v, metadata, path);
                context.stop();
            } else {
                text = parseStringValue0(v, metadata, path);
            }
        }
        return text;
    }

    private String parseStringValue0(Blob blob, Metadata metadata, String path) {
        WriteOutContentHandler handler = new WriteOutContentHandler(definition.getMaxExtractLength());
        long bytesRead = 0;
        long blobLength = blob.length();
        if (log.isDebugEnabled()) {
            log.debug("Extracting {}. Length: {}, reference: {}", path, blobLength, blob.getReference());
        }
        textExtractionStats.startExtraction();
        try {
            CountingInputStream stream = new CountingInputStream(new LazyInputStream(blob::getNewStream));
            try {
                if (blobLength > SMALL_BINARY) {
                    // Extracting can take a long time, so if a binary is large enough, delegate extraction to the
                    // ExtractedTextCache#process, which may execute the extraction with a timeout (depends on configuration).
                    String threadName = "Extracting " + path + ", " + blobLength + " bytes";
                    extractedTextCache.process(threadName, () -> {
                        getParser().parse(stream, handler, metadata, new ParseContext());
                        return null;
                    });
                } else {
                    getParser().parse(stream, handler, metadata, new ParseContext());
                }
            } finally {
                bytesRead = stream.getCount();
                stream.close();
            }
        } catch (LinkageError e) {
            // Capture errors caused by extraction libraries
            // not being present. This is equivalent to disabling
            // selected media types in configuration, so we can simply
            // ignore these errors.
            String format = "[{}] Failed to extract text from a binary property: {}. "
                    + "This often happens when the media types is disabled by configuration.";
            String indexName = getIndexName();
            log.info(format, indexName, path);
            log.debug(format, indexName, path, e);
            extractedTextCache.put(blob, ExtractedText.ERROR);
            return TEXT_EXTRACTION_ERROR;
        } catch (TimeoutException t) {
            log.warn("[{}] Failed to extract text from a binary property due to timeout: {}.",
                    getIndexName(), path);
            extractedTextCache.put(blob, ExtractedText.ERROR);
            extractedTextCache.putTimeout(blob, ExtractedText.ERROR);
            return TEXT_EXTRACTION_ERROR;
        } catch (Throwable t) {
            // Capture and report any other full text extraction problems.
            // The special STOP exception is used for normal termination.
            if (!WriteLimitReachedException.isWriteLimitReached(t)) {
                String format = "[{}] Failed to extract text from a binary property: {}. "
                        + "This is quite common, and usually nothing to worry about.";
                String indexName = getIndexName();
                log.info(format + " Error: " + t, indexName, path);
                log.debug(format, indexName, path, t);
                extractedTextCache.put(blob, ExtractedText.ERROR);
                return TEXT_EXTRACTION_ERROR;
            } else {
                log.debug("Extracted text size exceeded configured limit({})", definition.getMaxExtractLength());
            }
        }
        String result = handler.toString();
        if (bytesRead > 0) {
            int len = result.length();
            long extractionTimeMillis = textExtractionStats.finishExtraction(bytesRead, len);
            if (log.isDebugEnabled()) {
                log.debug("Extracted {}. Time: {} ms, Bytes read: {}, Text size: {}", path, extractionTimeMillis, bytesRead, len);
            }
        }
        extractedTextCache.put(blob, new ExtractedText(ExtractedText.ExtractionResult.SUCCESS, result));
        return result;
    }

    private String getIndexName() {
        return definition.getIndexName();
    }

    //~-------------------------------------------< Tika >

    public TikaConfig getTikaConfig() {
        if (tikaConfig == null) {
            tikaConfig = initializeTikaConfig(definition);
        }
        return tikaConfig.config;
    }

    private Parser getParser() {
        if (parser == null) {
            parser = initializeTikaParser(definition);
        }
        return parser;
    }

    boolean isSupportedMediaType(String type) {
        // we need to initialize the fields separately
        // to prevent null pointer exceptions if the method
        // is called concurrently from multiple threads
        if (supportedMediaTypes == null) {
            supportedMediaTypes = getParser().getSupportedTypes(new ParseContext());
        }
        if (nonIndexedMediaType == null) {
            nonIndexedMediaType = getNonIndexedMediaTypes();
        }
        MediaType mediaType = MediaType.parse(type);
        return supportedMediaTypes.contains(mediaType) && !nonIndexedMediaType.contains(mediaType);
    }

    private Set<MediaType> getNonIndexedMediaTypes() {
        InputStream configStream = null;
        String configSource = null;
        try {
            if (definition.hasCustomTikaConfig()) {
                configSource = String.format("Custom config at %s", definition.getIndexPath());
                configStream = definition.getTikaConfig();
            } else {
                URL configUrl = FulltextIndexEditorContext.class.getResource("tika-config.xml");
                configSource = "Default : tika-config.xml";
                if (configUrl != null) {
                    configStream = configUrl.openStream();
                }
            }

            if (configStream != null) {
                return TikaParserConfig.getNonIndexedMediaTypes(configStream);
            }
        } catch (TikaException | IOException | SAXException e) {
            log.warn("Tika configuration not available : {}", configSource, e);
        } finally {
            IOUtils.closeQuietly(configStream);
        }
        return Collections.emptySet();
    }


    private static TikaConfigHolder initializeTikaConfig(@Nullable IndexDefinition definition) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        InputStream configStream = null;
        String configSource = null;

        try {
            Thread.currentThread().setContextClassLoader(FulltextIndexEditorContext.class.getClassLoader());
            if (definition != null && definition.hasCustomTikaConfig()) {
                log.debug("[{}] Using custom tika config", definition.getIndexName());
                configSource = "Custom config at " + definition.getIndexPath();
                configStream = definition.getTikaConfig();
            } else {
                URL configUrl = FulltextIndexEditorContext.class.getResource("tika-config.xml");
                if (configUrl != null) {
                    configSource = configUrl.toString();
                    configStream = configUrl.openStream();
                }
            }

            if (configStream != null) {
                return new TikaConfigHolder(new TikaConfig(configStream), configSource);
            }
        } catch (TikaException | IOException | SAXException e) {
            log.warn("Tika configuration not available : {}", configSource, e);
        } finally {
            IOUtils.closeQuietly(configStream);
            Thread.currentThread().setContextClassLoader(current);
        }
        return new TikaConfigHolder(TikaConfig.getDefaultConfig(), "Default Config");
    }

    private Parser initializeTikaParser(IndexDefinition definition) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        try {
            if (definition.hasCustomTikaConfig()) {
                Thread.currentThread().setContextClassLoader(FulltextIndexEditorContext.class.getClassLoader());
                return new AutoDetectParser(getTikaConfig());
            }
        } finally {
            Thread.currentThread().setContextClassLoader(current);
        }
        return defaultParser;
    }

    private static AutoDetectParser createDefaultParser() {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        TikaConfigHolder configHolder = null;
        try {
            configHolder = initializeTikaConfig(null);
            Thread.currentThread().setContextClassLoader(FulltextIndexEditorContext.class.getClassLoader());
            log.info("Loaded default Tika Config from classpath {}", configHolder);
            return new AutoDetectParser(configHolder.config);
        } catch (Exception e) {
            log.warn("Tika configuration not available : {}", configHolder, e);
        } finally {
            Thread.currentThread().setContextClassLoader(current);
        }
        return new AutoDetectParser();
    }

    private static final class TikaConfigHolder {
        final TikaConfig config;
        final String sourceInfo;

        public TikaConfigHolder(TikaConfig config, String sourceInfo) {
            this.config = config;
            this.sourceInfo = sourceInfo;
        }

        @Override
        public String toString() {
            return sourceInfo;
        }
    }

}
