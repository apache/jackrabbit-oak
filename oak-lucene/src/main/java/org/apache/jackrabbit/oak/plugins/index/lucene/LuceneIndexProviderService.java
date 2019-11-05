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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.management.NotCompliantMBeanException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.InfoStream;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;
import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@SuppressWarnings("UnusedDeclaration")
@Component(metatype = true, label = "Apache Jackrabbit Oak LuceneIndexProvider")
public class LuceneIndexProviderService {
    public static final String REPOSITORY_HOME = "repository.home";

    private LuceneIndexProvider indexProvider;

    private final List<ServiceRegistration> regs = Lists.newArrayList();
    private final List<Registration> oakRegs = Lists.newArrayList();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY,
            policy = ReferencePolicy.DYNAMIC
    )
    private NodeAggregator nodeAggregator;

    private static final boolean PROP_DISABLED_DEFAULT = false;

    @Property(
            boolValue = PROP_DISABLED_DEFAULT,
            label = "Disable this component",
            description = "If true, this component is disabled."
    )
    private static final String PROP_DISABLED = "disabled";

    @Property(
            boolValue = false,
            label = "Enable Debug Logging",
            description = "Enables debug logging in Lucene. After enabling this actual logging can be " +
            "controlled via changing log level for category 'oak.lucene' to debug")
    private static final String PROP_DEBUG = "debug";

    @Property(
            boolValue = true,
            label = "Enable CopyOnRead",
            description = "Enable copying of Lucene index to local file system to improve query performance"
    )
    private static final String PROP_COPY_ON_READ = "enableCopyOnReadSupport";

    @Property(
            label = "Local index storage path",
            description = "Local file system path where Lucene indexes would be copied when CopyOnRead is enabled. " +
                    "If not specified then indexes would be stored under 'index' dir under Repository Home"
    )
    private static final String PROP_LOCAL_INDEX_DIR = "localIndexDir";


    private static final boolean PROP_COPY_ON_WRITE_DEFAULT = true;
    @Property(
            boolValue = PROP_COPY_ON_WRITE_DEFAULT,
            label = "Enable CopyOnWrite",
            description = "Enable copying of Lucene index to local file system to improve index writer performance"
    )
    private static final String PROP_COPY_ON_WRITE = "enableCopyOnWriteSupport";

    @Property(
            boolValue = true,
            label = "Open index asynchronously",
            description = "Enable opening of indexes in asynchronous mode"
    )
    private static final String PROP_ASYNC_INDEX_OPEN = "enableOpenIndexAsync";

    private static final int PROP_THREAD_POOL_SIZE_DEFAULT = 5;
    @Property(
            intValue = PROP_THREAD_POOL_SIZE_DEFAULT,
            label = "Thread pool size",
            description = "Thread pool size used to perform various asynchronous task in Oak Lucene"
    )
    private static final String PROP_THREAD_POOL_SIZE = "threadPoolSize";

    private static final boolean PROP_PREFETCH_INDEX_FILES_DEFAULT = true;
    @Property(
            boolValue = PROP_PREFETCH_INDEX_FILES_DEFAULT,
            label = "Prefetch Index Files",
            description = "Prefetch the index files when CopyOnRead is enabled. When enabled all new Lucene" +
                    " index files would be copied locally before the index is made available to QueryEngine"
    )
    private static final String PROP_PREFETCH_INDEX_FILES = "prefetchIndexFiles";

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

    private static final int PROP_BOOLEAN_CLAUSE_LIMIT_DEFAULT = 1024;
    @Property(
            intValue = PROP_BOOLEAN_CLAUSE_LIMIT_DEFAULT,
            label = "Boolean Clause Limit",
            description = "Limit for number of boolean clauses generated for handling of OR query"
    )
    private static final String PROP_BOOLEAN_CLAUSE_LIMIT = "booleanClauseLimit";

    private Whiteboard whiteboard;

    private BackgroundObserver backgroundObserver;

    @Reference
    ScorerProviderFactory scorerFactory;

    @Reference
    private IndexAugmentorFactory augmentorFactory;

    @Reference(policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY
    )
    private volatile PreExtractedTextProvider extractedTextProvider;

    private IndexCopier indexCopier;

    private File indexDir;

    private ExecutorService executorService;

    private int threadPoolSize;

    private ExtractedTextCache extractedTextCache;

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config)
            throws NotCompliantMBeanException, IOException {
        boolean disabled = PropertiesUtil.toBoolean(config.get(PROP_DISABLED), PROP_DISABLED_DEFAULT);

        if (disabled) {
            log.info("Component disabled by configuration");
            return;
        }

        configureBooleanClauseLimit(config);
        initializeFactoryClassLoaders(getClass().getClassLoader());

        whiteboard = new OsgiWhiteboard(bundleContext);
        threadPoolSize = PropertiesUtil.toInteger(config.get(PROP_THREAD_POOL_SIZE), PROP_THREAD_POOL_SIZE_DEFAULT);
        initializeExtractedTextCache(bundleContext, config);
        indexProvider = new LuceneIndexProvider(createTracker(bundleContext, config), scorerFactory, augmentorFactory);
        initializeLogging(config);
        initialize();

        regs.add(bundleContext.registerService(QueryIndexProvider.class.getName(), indexProvider, null));
        registerObserver(bundleContext, config);
        registerIndexEditor(bundleContext, config);

        oakRegs.add(registerMBean(whiteboard,
                LuceneIndexMBean.class,
                new LuceneIndexMBeanImpl(indexProvider.getTracker()),
                LuceneIndexMBean.TYPE,
                "Lucene Index statistics"));
        registerGCMonitor(whiteboard, indexProvider.getTracker());
    }

    @Deactivate
    private void deactivate() throws InterruptedException, IOException {
        for (ServiceRegistration reg : regs) {
            reg.unregister();
        }

        for (Registration reg : oakRegs){
            reg.unregister();
        }

        if (backgroundObserver != null){
            backgroundObserver.close();
        }

        if (indexProvider != null) {
            indexProvider.close();
            indexProvider = null;
        }

        //Close the copier first i.e. before executorService
        if (indexCopier != null){
            indexCopier.close();
        }

        if (executorService != null){
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        }

        InfoStream.setDefault(InfoStream.NO_OUTPUT);
    }

    IndexCopier getIndexCopier() {
        return indexCopier;
    }

    ExtractedTextCache getExtractedTextCache() {
        return extractedTextCache;
    }

    private void initialize(){
        if(indexProvider == null){
            return;
        }

        if(nodeAggregator != null){
            log.debug("Using NodeAggregator {}", nodeAggregator.getClass());
        }

        indexProvider.setAggregator(nodeAggregator);
    }

    private void initializeLogging(Map<String, ?> config) {
        boolean debug = PropertiesUtil.toBoolean(config.get(PROP_DEBUG), false);
        if (debug) {
            InfoStream.setDefault(LoggingInfoStream.INSTANCE);
            log.info("Registered LoggingInfoStream with Lucene. Lucene logs can be enabled " +
                    "now via category [{}]", LoggingInfoStream.PREFIX);
        }
    }

    private void registerIndexEditor(BundleContext bundleContext, Map<String, ?> config) throws IOException {
        boolean enableCopyOnWrite = PropertiesUtil.toBoolean(config.get(PROP_COPY_ON_WRITE), PROP_COPY_ON_WRITE_DEFAULT);
        LuceneIndexEditorProvider editorProvider;
        if (enableCopyOnWrite){
            initializeIndexCopier(bundleContext, config);
            editorProvider = new LuceneIndexEditorProvider(indexCopier, extractedTextCache, augmentorFactory);
            log.info("Enabling CopyOnWrite support. Index files would be copied under {}", indexDir.getAbsolutePath());
        } else {
            editorProvider = new LuceneIndexEditorProvider(null, extractedTextCache, augmentorFactory);
        }
        regs.add(bundleContext.registerService(IndexEditorProvider.class.getName(), editorProvider, null));
        oakRegs.add(registerMBean(whiteboard,
                TextExtractionStatsMBean.class,
                editorProvider.getExtractedTextCache().getStatsMBean(),
                TextExtractionStatsMBean.TYPE,
                "TextExtraction statistics"));
    }

    private IndexTracker createTracker(BundleContext bundleContext, Map<String, ?> config) throws IOException {
        boolean enableCopyOnRead = PropertiesUtil.toBoolean(config.get(PROP_COPY_ON_READ), true);
        if (enableCopyOnRead){
            initializeIndexCopier(bundleContext, config);
            log.info("Enabling CopyOnRead support. Index files would be copied under {}", indexDir.getAbsolutePath());
            return new IndexTracker(indexCopier);
        }

        return new IndexTracker();
    }

    private void initializeIndexCopier(BundleContext bundleContext, Map<String, ?> config) throws IOException {
        if(indexCopier != null){
            return;
        }
        String indexDirPath = PropertiesUtil.toString(config.get(PROP_LOCAL_INDEX_DIR), null);
        boolean prefetchEnabled = PropertiesUtil.toBoolean(config.get(PROP_PREFETCH_INDEX_FILES),
                PROP_PREFETCH_INDEX_FILES_DEFAULT);
        if (Strings.isNullOrEmpty(indexDirPath)) {
            String repoHome = bundleContext.getProperty(REPOSITORY_HOME);
            if (repoHome != null){
                indexDirPath = FilenameUtils.concat(repoHome, "index");
            }
        }

        checkNotNull(indexDirPath, "Index directory cannot be determined as neither index " +
                "directory path [%s] nor repository home [%s] defined", PROP_LOCAL_INDEX_DIR, REPOSITORY_HOME);

        if (prefetchEnabled){
            log.info("Prefetching of index files enabled. Index would be opened after copying all new files locally");
        }

        indexDir = new File(indexDirPath);
        indexCopier = new IndexCopier(getExecutorService(), indexDir, prefetchEnabled);

        oakRegs.add(registerMBean(whiteboard,
                CopyOnReadStatsMBean.class,
                indexCopier,
                CopyOnReadStatsMBean.TYPE,
                "IndexCopier support statistics"));

    }

    private ExecutorService getExecutorService(){
        if (executorService == null){
            executorService = createExecutor();
        }
        return executorService;
    }

    private ExecutorService createExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 5, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            private final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    log.warn("Error occurred in asynchronous processing ", e);
                }
            };
            @Override
            public Thread newThread(@Nonnull Runnable r) {
                Thread thread = new Thread(r, createName());
                thread.setDaemon(true);
                thread.setPriority(Thread.MIN_PRIORITY);
                thread.setUncaughtExceptionHandler(handler);
                return thread;
            }

            private String createName() {
                return "oak-lucene-" + counter.getAndIncrement();
            }
        });
        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    private void registerObserver(BundleContext bundleContext, Map<String, ?> config) {
        boolean enableAsyncIndexOpen = PropertiesUtil.toBoolean(config.get(PROP_ASYNC_INDEX_OPEN), true);
        Observer observer = indexProvider;
        if (enableAsyncIndexOpen) {
            backgroundObserver = new BackgroundObserver(indexProvider, getExecutorService(), 5);
            observer = backgroundObserver;
            oakRegs.add(registerMBean(whiteboard,
                    BackgroundObserverMBean.class,
                    backgroundObserver.getMBean(),
                    BackgroundObserverMBean.TYPE,
                    "LuceneIndexConfigObserver queue stats"));
            log.info("Registering the LuceneIndexProvider as a BackgroundObserver");
        }
        regs.add(bundleContext.registerService(Observer.class.getName(), observer, null));
    }

    private void initializeFactoryClassLoaders(ClassLoader classLoader) {
        ClassLoader originalClassLoader = Thread.currentThread()
                .getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            //Access TokenizerFactory etc trigger a static initialization
            //so switch the TCCL so that static initializer picks up the right
            //classloader
            initializeFactoryClassLoaders0(classLoader);
            initializeClasses();
        } catch (Throwable t) {
            log.warn("Error occurred while initializing the Lucene " +
                    "Factories", t);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    private void initializeFactoryClassLoaders0(ClassLoader classLoader) {
        //Factories use the Threads context classloader to perform SPI classes
        //lookup by default which would not work in OSGi world. So reload the
        //factories by providing the bundle classloader
        TokenizerFactory.reloadTokenizers(classLoader);
        CharFilterFactory.reloadCharFilters(classLoader);
        TokenFilterFactory.reloadTokenFilters(classLoader);
    }

    private void initializeClasses() {
        // prevent LUCENE-6482
        // (also done in IndexDefinition, just to be save)
        OakCodec ensureLucene46CodecLoaded = new OakCodec();
        // to ensure the JVM doesn't optimize away object creation
        // (probably not really needed; just to be save)
        log.debug("Lucene46Codec is loaded: {}", ensureLucene46CodecLoaded);
    }

    private void initializeExtractedTextCache(BundleContext bundleContext, Map<String, ?> config) {
        int cacheSizeInMB = PropertiesUtil.toInteger(config.get(PROP_EXTRACTED_TEXT_CACHE_SIZE),
                PROP_EXTRACTED_TEXT_CACHE_SIZE_DEFAULT);
        int cacheExpiryInSecs = PropertiesUtil.toInteger(config.get(PROP_EXTRACTED_TEXT_CACHE_EXPIRY),
                PROP_EXTRACTED_TEXT_CACHE_EXPIRY_DEFAULT);
        boolean alwaysUsePreExtractedCache = PropertiesUtil.toBoolean(config.get(PROP_PRE_EXTRACTED_TEXT_ALWAYS_USE),
                PROP_PRE_EXTRACTED_TEXT_ALWAYS_USE_DEFAULT);

        extractedTextCache = new ExtractedTextCache(cacheSizeInMB * ONE_MB, cacheExpiryInSecs, alwaysUsePreExtractedCache);
        if (extractedTextProvider != null){
            registerExtractedTextProvider(extractedTextProvider);
        }
        CacheStats stats = extractedTextCache.getCacheStats();
        if (stats != null){
            oakRegs.add(registerMBean(whiteboard,
                    CacheStatsMBean.class, stats,
                    CacheStatsMBean.TYPE, stats.getName()));
            log.info("Extracted text caching enabled with maxSize {} MB, expiry time {} secs",
                    cacheSizeInMB, cacheExpiryInSecs);
        }
    }

    private void registerExtractedTextProvider(PreExtractedTextProvider provider){
        if (extractedTextCache != null){
            if (provider != null){
                String usage = extractedTextCache.isAlwaysUsePreExtractedCache() ?
                        "always" : "only during reindexing phase";
                log.info("Registering PreExtractedTextProvider {} with extracted text cache. " +
                        "It would be used {}",  provider, usage);
            } else {
                log.info("Unregistering PreExtractedTextProvider with extracted text cache");
            }
            extractedTextCache.setExtractedTextProvider(provider);
        }
    }

    private void configureBooleanClauseLimit(Map<String, ?> config) {
        int booleanClauseLimit = PropertiesUtil.toInteger(config.get(PROP_BOOLEAN_CLAUSE_LIMIT),
                PROP_BOOLEAN_CLAUSE_LIMIT_DEFAULT);
        if (booleanClauseLimit != BooleanQuery.getMaxClauseCount()){
            BooleanQuery.setMaxClauseCount(booleanClauseLimit);
            log.info("Changed the Max boolean clause limit to {}", booleanClauseLimit);
        }
    }

    private void registerGCMonitor(Whiteboard whiteboard,
            final IndexTracker tracker) {
        GCMonitor gcMonitor = new GCMonitor.Empty() {
            @Override
            public void compacted(long[] segmentCounts, long[] recordCounts,
                    long[] compactionMapWeights) {
                tracker.refresh();
            }
        };
        oakRegs.add(whiteboard.register(GCMonitor.class, gcMonitor, emptyMap()));
    }

    protected void bindNodeAggregator(NodeAggregator aggregator) {
        this.nodeAggregator = aggregator;
        initialize();
    }

    protected void unbindNodeAggregator(NodeAggregator aggregator) {
        this.nodeAggregator = null;
        initialize();
    }

    protected void bindExtractedTextProvider(PreExtractedTextProvider preExtractedTextProvider){
        this.extractedTextProvider = preExtractedTextProvider;
        registerExtractedTextProvider(preExtractedTextProvider);
    }

    protected void unbindExtractedTextProvider(PreExtractedTextProvider preExtractedTextProvider){
        this.extractedTextProvider = null;
        registerExtractedTextProvider(null);
    }
}
