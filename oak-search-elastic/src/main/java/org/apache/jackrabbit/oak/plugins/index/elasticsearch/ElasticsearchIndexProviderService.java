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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.*;
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
import org.apache.jackrabbit.oak.plugins.index.search.TextExtractionStatsMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(metatype = true, label = "Apache Jackrabbit Oak ElasticsearchIndexProvider")
public class ElasticsearchIndexProviderService {

    private static Logger LOG = LoggerFactory.getLogger(ElasticsearchIndexProviderService.class);

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

    private static final String PROP_ELASTICSEARCH_SCHEME_DEFAULT = "http";
//    @Property(
//            value = PROP_ELASTICSEARCH_SCHEME_DEFAULT,
//            label = "Elasticsearch connection scheme"
//    )
    private static final String PROP_ELASTICSEARCH_SCHEME = "elasticsearch.scheme";

    private static final String PROP_ELASTICSEARCH_HOST_DEFAULT = "localhost";
//    @Property(
//            value = PROP_ELASTICSEARCH_HOST_DEFAULT,
//            label = "Elasticsearch connection host"
//    )
    private static final String PROP_ELASTICSEARCH_HOST = "elasticsearch.host";

    private static final int PROP_ELASTICSEARCH_PORT_DEFAULT = 9200;
//    @Property(
//            intValue = PROP_ELASTICSEARCH_PORT_DEFAULT,
//            label = "Elasticsearch connection port"
//    )
    private static final String PROP_ELASTICSEARCH_PORT = "elasticsearch.port";

    @Reference
    private StatisticsProvider statisticsProvider;

    @Reference(policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY
    )
    private volatile PreExtractedTextProvider extractedTextProvider;

    private ExtractedTextCache extractedTextCache;

    private ElasticsearchConnectionFactory connectionFactory = null;

    private final List<ServiceRegistration> regs = Lists.newArrayList();
    private final List<Registration> oakRegs = Lists.newArrayList();

    private Whiteboard whiteboard;

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config) {
        whiteboard = new OsgiWhiteboard(bundleContext);

        initializeExtractedTextCache(config, statisticsProvider);

        connectionFactory = new ElasticsearchConnectionFactory();
        ElasticsearchIndexCoordinateFactory esIndexCoordFactory = getElasticsearchIndexCoordinateFactory(config);

        registerIndexProvider(bundleContext, esIndexCoordFactory);
        registerIndexEditor(bundleContext, esIndexCoordFactory);
    }

    @Deactivate
    private void deactivate() {
        for (ServiceRegistration reg : regs) {
            reg.unregister();
        }

        for (Registration reg : oakRegs) {
            reg.unregister();
        }

        IOUtils.closeQuietly(connectionFactory);
        connectionFactory = null;

        if (extractedTextCache != null) {
            extractedTextCache.close();
        }
    }

    private void registerIndexProvider(BundleContext bundleContext, ElasticsearchIndexCoordinateFactory esIndexCoordFactory) {
        ElasticsearchIndexProvider indexProvider = new ElasticsearchIndexProvider(esIndexCoordFactory);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put("type", ElasticsearchIndexConstants.TYPE_ELASTICSEARCH);
        regs.add(bundleContext.registerService(IndexEditorProvider.class.getName(), indexProvider, props));
    }

    private void registerIndexEditor(BundleContext bundleContext, ElasticsearchIndexCoordinateFactory esIndexCoordFactory) {
        ElasticsearchIndexEditorProvider editorProvider = new ElasticsearchIndexEditorProvider(esIndexCoordFactory, extractedTextCache);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put("type", ElasticsearchIndexConstants.TYPE_ELASTICSEARCH);
        regs.add(bundleContext.registerService(IndexEditorProvider.class.getName(), editorProvider, props));
        oakRegs.add(registerMBean(whiteboard,
                TextExtractionStatsMBean.class,
                editorProvider.getExtractedTextCache().getStatsMBean(),
                TextExtractionStatsMBean.TYPE,
                "TextExtraction statistics"));
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
                null /* TODO: have a temp dir to enable tracking timed out binary extraction */,
                statisticsProvider);
        if (extractedTextProvider != null){
            registerExtractedTextProvider(extractedTextProvider);
        }
        CacheStats stats = extractedTextCache.getCacheStats();
        if (stats != null){
            oakRegs.add(registerMBean(whiteboard,
                    CacheStatsMBean.class, stats,
                    CacheStatsMBean.TYPE, stats.getName()));
            LOG.info("Extracted text caching enabled with maxSize {} MB, expiry time {} secs",
                    cacheSizeInMB, cacheExpiryInSecs);
        }
    }

    private void registerExtractedTextProvider(PreExtractedTextProvider provider){
        if (extractedTextCache != null){
            if (provider != null){
                String usage = extractedTextCache.isAlwaysUsePreExtractedCache() ?
                        "always" : "only during reindexing phase";
                LOG.info("Registering PreExtractedTextProvider {} with extracted text cache. " +
                        "It would be used {}",  provider, usage);
            } else {
                LOG.info("Unregistering PreExtractedTextProvider with extracted text cache");
            }
            extractedTextCache.setExtractedTextProvider(provider);
        }
    }

    private ElasticsearchIndexCoordinateFactory getElasticsearchIndexCoordinateFactory(Map<String, ?> config) {
        ElasticsearchIndexCoordinateFactory esIndexCoordFactory;
        Map<String, String> esCfg = Maps.newHashMap();
        esCfg.put(ElasticsearchCoordinate.SCHEME_PROP,
                PropertiesUtil.toString(config.get(PROP_ELASTICSEARCH_SCHEME), PROP_ELASTICSEARCH_SCHEME_DEFAULT));
        esCfg.put(ElasticsearchCoordinate.HOST_PROP,
                PropertiesUtil.toString(config.get(PROP_ELASTICSEARCH_HOST), PROP_ELASTICSEARCH_HOST_DEFAULT));
        esCfg.put(ElasticsearchCoordinate.PORT_PROP, String.valueOf(
                PropertiesUtil.toInteger(config.get(PROP_ELASTICSEARCH_PORT), PROP_ELASTICSEARCH_PORT_DEFAULT)));
        esIndexCoordFactory = new DefaultElasticsearchIndexCoordinateFactory(connectionFactory, esCfg);
        return esIndexCoordFactory;
    }
}
