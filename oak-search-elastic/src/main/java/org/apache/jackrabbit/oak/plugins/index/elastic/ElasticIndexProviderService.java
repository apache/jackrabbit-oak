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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;

import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = ElasticIndexProviderService.Config.class)
public class ElasticIndexProviderService {

    protected static final String OAK_ELASTIC_PREFIX = "oak.elastic.";

    protected static final String PROP_DISABLED = "disabled";
    protected static final String PROP_INDEX_PREFIX = "indexPrefix";
    protected static final String PROP_ELASTIC_SCHEME = "elasticsearch.scheme";
    protected static final String PROP_ELASTIC_HOST = "elasticsearch.host";
    protected static final String PROP_ELASTIC_PORT = "elasticsearch.port";
    protected static final String PROP_ELASTIC_API_KEY_ID = "elasticsearch.apiKeyId";
    protected static final String PROP_ELASTIC_API_KEY_SECRET = "elasticsearch.apiKeySecret";
    protected static final String PROP_LOCAL_TEXT_EXTRACTION_DIR = "localTextExtractionDir";

    @ObjectClassDefinition(name = "ElasticIndexProviderService", description = "Apache Jackrabbit Oak ElasticIndexProvider")
    public @interface Config {
        @AttributeDefinition(name = "Disable the OAK Elastic service",
                description = "If true, does not start the Elastic component")
        boolean disabled() default false;

        @AttributeDefinition(name = "Extracted text cache size (MB)",
                description = "Cache size in MB for caching extracted text for some time. When set to 0 then " +
                        "cache would be disabled")
        int extractedTextCacheSizeInMB() default 20 ;

        @AttributeDefinition(name = "Extracted text cache expiry (secs)",
                description = "Time in seconds for which the extracted text would be cached in memory")
        int extractedTextCacheExpiryInSecs() default 300;

        @AttributeDefinition(name = "Always use pre-extracted text cache",
                description = "By default pre extracted text cache would only be used for reindex case. If this setting " +
                        "is enabled then it would also be used in normal incremental indexing")
        boolean alwaysUsePreExtractedCache() default false;

        @AttributeDefinition(name = "Index prefix",
                description = "Prefix to be added to name of each elastic search index")
        String indexPrefix() default "oak-elastic";

        @AttributeDefinition(name = "Elasticsearch connection scheme", description = "Elasticsearch connection scheme")
        String elasticsearch_scheme() default ElasticConnection.DEFAULT_SCHEME;

        @AttributeDefinition(name = "Elasticsearch connection host", description = "Elasticsearch connection host")
        String elasticsearch_host() default ElasticConnection.DEFAULT_HOST;

        @AttributeDefinition(name = "Elasticsearch connection port", description = "Elasticsearch connection port")
        String elasticsearch_port() default ("" + ElasticConnection.DEFAULT_PORT);

        @AttributeDefinition(name = "Elasticsearch API key ID", description = "Elasticsearch API key ID")
        String elasticsearch_apiKeyId() default ElasticConnection.DEFAULT_API_KEY_ID;

        @AttributeDefinition(name = "Elasticsearch API key secret", description = "Elasticsearch API key secret")
        String elasticsearch_apiKeySecret() default ElasticConnection.DEFAULT_API_KEY_SECRET;

        @AttributeDefinition(name = "Local text extraction cache path",
                description = "Local file system path where text extraction cache stores/load entries to recover from timed out operation")
        String localTextExtractionDir();

        @AttributeDefinition(name = "Remote index cleanup frequency", description = "Frequency (in seconds) of running remote index deletion scheduled task." +
                "Default is -1 (disabled)).")
        int remoteIndexCleanupFrequency() default -1;

        @AttributeDefinition(name = "Remote index deletion threshold", description = "Time in seconds after which a remote index whose local index is not found gets deleted." +
                "Default is 1 day.")
        int remoteIndexDeletionThreshold() default 24*60*60;
    }


    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexProviderService.class);

    private static final String REPOSITORY_HOME = "repository.home";

    @Reference
    private StatisticsProvider statisticsProvider;

    @Reference
    private NodeStore nodeStore;

    @Reference
    private AsyncIndexInfoService asyncIndexInfoService;

    @Reference(policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.OPTIONAL,
            policyOption = ReferencePolicyOption.GREEDY
    )
    private volatile PreExtractedTextProvider extractedTextProvider;

    private ExtractedTextCache extractedTextCache;

    private final List<ServiceRegistration> regs = new ArrayList<>();
    private final List<Registration> oakRegs = new ArrayList<>();

    private Whiteboard whiteboard;
    private File textExtractionDir;

    private ElasticConnection elasticConnection;
    private ElasticMetricHandler metricHandler;
    private ElasticIndexTracker indexTracker;

    @Activate
    private void activate(BundleContext bundleContext, Config config) {
        boolean disabled = Boolean.parseBoolean(System.getProperty(OAK_ELASTIC_PREFIX + PROP_DISABLED, Boolean.toString(config.disabled())));
        if (disabled) {
            LOG.info("Component disabled by configuration");
            return;
        }

        whiteboard = new OsgiWhiteboard(bundleContext);

        //initializeTextExtractionDir(bundleContext, config);
        //initializeExtractedTextCache(config, statisticsProvider);

        elasticConnection = getElasticConnection(config);
        metricHandler = new ElasticMetricHandler(statisticsProvider);
        indexTracker = new ElasticIndexTracker(elasticConnection, metricHandler);

        // register observer needed for index tracking
        regs.add(bundleContext.registerService(Observer.class.getName(), indexTracker, null));

        // register info provider for oak index stats
        regs.add(bundleContext.registerService(IndexInfoProvider.class.getName(),
                new ElasticIndexInfoProvider(indexTracker, asyncIndexInfoService), null));

        // register mbean for detailed elastic stats and utility actions
        ElasticIndexMBean mBean = new ElasticIndexMBean(indexTracker);
        oakRegs.add(registerMBean(whiteboard,
                ElasticIndexMBean.class,
                mBean,
                ElasticIndexMBean.TYPE,
                "Elastic Index statistics"));

        LOG.info("Registering Index and Editor providers with connection {}", elasticConnection);

        registerIndexProvider(bundleContext);
        registerIndexEditor(bundleContext);
        registerIndexCleaner(config);
    }

    @Deactivate
    private void deactivate() {
        for (ServiceRegistration reg : regs) {
            reg.unregister();
        }

        for (Registration reg : oakRegs) {
            reg.unregister();
        }

        IOUtils.closeQuietly(elasticConnection);

        if (extractedTextCache != null) {
            extractedTextCache.close();
        }
    }

    private void registerIndexCleaner(Config contextConfig) {
        if (!elasticConnection.isAvailable()) {
            LOG.warn("The Elastic cluster at {} is not reachable. The index cleaner job has not been enabled", elasticConnection);
            return;
        }
        if (contextConfig.remoteIndexCleanupFrequency() <= 0) {
            LOG.info("Index Cleaner disabled by configuration");
            return;
        }
        ElasticIndexCleaner task = new ElasticIndexCleaner(elasticConnection, nodeStore, contextConfig.remoteIndexDeletionThreshold());
        oakRegs.add(scheduleWithFixedDelay(whiteboard, task, contextConfig.remoteIndexCleanupFrequency()));
    }

    private void registerIndexProvider(BundleContext bundleContext) {
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(indexTracker);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put("type", ElasticIndexDefinition.TYPE_ELASTICSEARCH);
        regs.add(bundleContext.registerService(QueryIndexProvider.class.getName(), indexProvider, props));
    }

    private void registerIndexEditor(BundleContext bundleContext) {
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(indexTracker, elasticConnection, extractedTextCache);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put("type", ElasticIndexDefinition.TYPE_ELASTICSEARCH);
        regs.add(bundleContext.registerService(IndexEditorProvider.class.getName(), editorProvider, props));
//        oakRegs.add(registerMBean(whiteboard,
//                TextExtractionStatsMBean.class,
//                editorProvider.getExtractedTextCache().getStatsMBean(),
//                TextExtractionStatsMBean.TYPE,
//                "TextExtraction statistics"));
    }

    private void initializeExtractedTextCache(final Config config, StatisticsProvider statisticsProvider) {

        extractedTextCache = new ExtractedTextCache(
                config.extractedTextCacheSizeInMB() * ONE_MB,
                config.extractedTextCacheExpiryInSecs(),
                config.alwaysUsePreExtractedCache(),
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
                    config.extractedTextCacheSizeInMB(), config.extractedTextCacheExpiryInSecs());
        }
    }

    private void initializeTextExtractionDir(BundleContext bundleContext, Config config) {
        String textExtractionDir = config.localTextExtractionDir();
        if (textExtractionDir.trim().isEmpty()) {
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

    private ElasticConnection getElasticConnection(Config contextConfig) {
        // system properties have priority, get mandatory params first
        String indexPrefix = System.getProperty(PROP_INDEX_PREFIX, contextConfig.indexPrefix());
        final String scheme = System.getProperty(PROP_ELASTIC_SCHEME, contextConfig.elasticsearch_scheme());
        final String host = System.getProperty(PROP_ELASTIC_HOST, contextConfig.elasticsearch_host());
        final String portString = System.getProperty(PROP_ELASTIC_PORT, contextConfig.elasticsearch_port());
        final int port = Integer.getInteger(PROP_ELASTIC_PORT, Integer.parseInt(portString));

        // optional params
        final String apiKeyId = System.getProperty(PROP_ELASTIC_API_KEY_ID, contextConfig.elasticsearch_apiKeyId());
        final String apiSecretId = System.getProperty(PROP_ELASTIC_API_KEY_SECRET, contextConfig.elasticsearch_apiKeySecret());

        return ElasticConnection.newBuilder()
                .withIndexPrefix(indexPrefix)
                .withConnectionParameters(scheme, host, port)
                .withApiKeys(apiKeyId, apiSecretId)
                .build();
    }
}
