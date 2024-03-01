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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.commons.io.FilenameUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(metatype = true, label = "Apache Jackrabbit Oak ElasticsearchIndexProvider")
public class ElasticsearchIndexProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIndexProviderService.class);

    private static final String REPOSITORY_HOME = "repository.home";

    private static final int PROP_EXTRACTED_TEXT_CACHE_SIZE_DEFAULT = 20;
    @Property(
            intValue = PROP_EXTRACTED_TEXT_CACHE_SIZE_DEFAULT,
            label = "Extracted text cache size (MB)",
            description = "Cache size in MB for caching extracted text for some time. When set to 0 then " +
                    "cache would be disabled"
    )
    private static final String PROP_EXTRACTED_TEXT_CACHE_SIZE = "extractedTextCacheSizeInMB";

    private static final int PROP_EXTRACTED_TEXT_CACHE_EXPIRY_DEFAULT = 300;
    @Property(
            intValue = PROP_EXTRACTED_TEXT_CACHE_EXPIRY_DEFAULT,
            label = "Extracted text cache expiry (secs)",
            description = "Time in seconds for which the extracted text would be cached in memory"
    )
    private static final String PROP_EXTRACTED_TEXT_CACHE_EXPIRY = "extractedTextCacheExpiryInSecs";

    private static final boolean PROP_PRE_EXTRACTED_TEXT_ALWAYS_USE_DEFAULT = false;
    @Property(
            boolValue = PROP_PRE_EXTRACTED_TEXT_ALWAYS_USE_DEFAULT,
            label = "Always use pre-extracted text cache",
            description = "By default pre extracted text cache would only be used for reindex case. If this setting " +
                    "is enabled then it would also be used in normal incremental indexing"
    )
    private static final String PROP_PRE_EXTRACTED_TEXT_ALWAYS_USE = "alwaysUsePreExtractedCache";

    @Property(
            value = ElasticsearchConnection.DEFAULT_SCHEME,
            label = "Elasticsearch connection scheme"
    )
    private static final String PROP_ELASTICSEARCH_SCHEME = ElasticsearchConnection.SCHEME_PROP;

    @Property(
            value = ElasticsearchConnection.DEFAULT_HOST,
            label = "Elasticsearch connection host"
    )
    private static final String PROP_ELASTICSEARCH_HOST = ElasticsearchConnection.HOST_PROP;

    @Property(
            intValue = ElasticsearchConnection.DEFAULT_PORT,
            label = "Elasticsearch connection port"
    )
    private static final String PROP_ELASTICSEARCH_PORT = ElasticsearchConnection.PORT_PROP;

    @Property(
            label = "Local text extraction cache path",
            description = "Local file system path where text extraction cache stores/load entries to recover from timed out operation"
    )
    private static final String PROP_LOCAL_TEXT_EXTRACTION_DIR = "localTextExtractionDir";

    @Reference
    private StatisticsProvider statisticsProvider;

    @Reference(policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY
    )
    private volatile PreExtractedTextProvider extractedTextProvider;

    private ExtractedTextCache extractedTextCache;

    private final List<ServiceRegistration> regs = new ArrayList<>();
    private final List<Registration> oakRegs = new ArrayList<>();

    private Whiteboard whiteboard;
    private File textExtractionDir;

    private ElasticsearchConnection elasticsearchConnection;

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config) {
        whiteboard = new OsgiWhiteboard(bundleContext);

        //initializeTextExtractionDir(bundleContext, config);
        //initializeExtractedTextCache(config, statisticsProvider);

        elasticsearchConnection = getElasticsearchCoordinate(config);

        registerIndexProvider(bundleContext);
        registerIndexEditor(bundleContext);
    }

    @Deactivate
    private void deactivate() {
        for (ServiceRegistration reg : regs) {
            reg.unregister();
        }

        for (Registration reg : oakRegs) {
            reg.unregister();
        }

        IOUtils.closeQuietly(elasticsearchConnection);

        if (extractedTextCache != null) {
            extractedTextCache.close();
        }
    }

    private void registerIndexProvider(BundleContext bundleContext) {
        ElasticsearchIndexProvider indexProvider = new ElasticsearchIndexProvider(elasticsearchConnection);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put("type", ElasticsearchIndexConstants.TYPE_ELASTICSEARCH);
        regs.add(bundleContext.registerService(QueryIndexProvider.class.getName(), indexProvider, props));
    }

    private void registerIndexEditor(BundleContext bundleContext) {
        ElasticsearchIndexEditorProvider editorProvider = new ElasticsearchIndexEditorProvider(elasticsearchConnection, extractedTextCache);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put("type", ElasticsearchIndexConstants.TYPE_ELASTICSEARCH);
        regs.add(bundleContext.registerService(IndexEditorProvider.class.getName(), editorProvider, props));
//        oakRegs.add(registerMBean(whiteboard,
//                TextExtractionStatsMBean.class,
//                editorProvider.getExtractedTextCache().getStatsMBean(),
//                TextExtractionStatsMBean.TYPE,
//                "TextExtraction statistics"));
    }

    private void initializeExtractedTextCache(Map<String, ?> config, StatisticsProvider statisticsProvider) {
        int cacheSizeInMB = PropertiesUtil.toInteger(config.get(PROP_EXTRACTED_TEXT_CACHE_SIZE),
                PROP_EXTRACTED_TEXT_CACHE_SIZE_DEFAULT);
        int cacheExpiryInSecs = PropertiesUtil.toInteger(config.get(PROP_EXTRACTED_TEXT_CACHE_EXPIRY),
                PROP_EXTRACTED_TEXT_CACHE_EXPIRY_DEFAULT);
        boolean alwaysUsePreExtractedCache = PropertiesUtil.toBoolean(config.get(PROP_PRE_EXTRACTED_TEXT_ALWAYS_USE),
                PROP_PRE_EXTRACTED_TEXT_ALWAYS_USE_DEFAULT);

        extractedTextCache = new ExtractedTextCache(
                cacheSizeInMB * ONE_MB,
                cacheExpiryInSecs,
                alwaysUsePreExtractedCache,
                textExtractionDir,
                statisticsProvider);
        if (extractedTextProvider != null) {
            registerExtractedTextProvider(extractedTextProvider);
        }
        CacheStats stats = extractedTextCache.getCacheStats();
        if (stats != null) {
            oakRegs.add(registerMBean(whiteboard,
                    CacheStatsMBean.class, stats,
                    CacheStatsMBean.TYPE, stats.getName()));
            LOG.info("Extracted text caching enabled with maxSize {} MB, expiry time {} secs",
                    cacheSizeInMB, cacheExpiryInSecs);
        }
    }

    private void initializeTextExtractionDir(BundleContext bundleContext, Map<String, ?> config) {
        String textExtractionDir = PropertiesUtil.toString(config.get(PROP_LOCAL_TEXT_EXTRACTION_DIR), null);
        if (textExtractionDir == null || textExtractionDir.trim().isEmpty()) {
            String repoHome = bundleContext.getProperty(REPOSITORY_HOME);
            if (repoHome != null) {
                textExtractionDir = FilenameUtils.concat(repoHome, "index");
            }
        }

        if (textExtractionDir == null) {
            throw new IllegalStateException(String.format("Text extraction directory cannot be determined as neither " +
                    "directory path [%s] nor repository home [%s] defined", PROP_LOCAL_TEXT_EXTRACTION_DIR, REPOSITORY_HOME));
        }

        this.textExtractionDir = new File(textExtractionDir);
    }

    private void registerExtractedTextProvider(PreExtractedTextProvider provider) {
        if (extractedTextCache != null) {
            if (provider != null) {
                String usage = extractedTextCache.isAlwaysUsePreExtractedCache() ?
                        "always" : "only during reindexing phase";
                LOG.info("Registering PreExtractedTextProvider {} with extracted text cache. " +
                        "It would be used {}", provider, usage);
            } else {
                LOG.info("Unregistering PreExtractedTextProvider with extracted text cache");
            }
            extractedTextCache.setExtractedTextProvider(provider);
        }
    }

    private ElasticsearchConnection getElasticsearchCoordinate(Map<String, ?> contextConfig) {
        // system properties have priority
        ElasticsearchConnection connection = build(System.getProperties().entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue()))
                )
        );

        if (connection == null) {
            connection = build(contextConfig);
        }

        return connection != null ? connection : ElasticsearchConnection.defaultConnection.get();
    }

    private ElasticsearchConnection build(@NotNull Map<String, ?> config) {
        ElasticsearchConnection coordinate = null;
        Object p = config.get(PROP_ELASTICSEARCH_PORT);
        if (p != null) {
            try {
                Integer port = Integer.parseInt(p.toString());
                coordinate = new ElasticsearchConnection((String) config.get(PROP_ELASTICSEARCH_SCHEME),
                        (String) config.get(PROP_ELASTICSEARCH_HOST), port);
            } catch (NumberFormatException nfe) {
                LOG.warn("{} value ({}) cannot be parsed to a valid number", PROP_ELASTICSEARCH_PORT, p);
            }
        }
        return coordinate;
    }
}
