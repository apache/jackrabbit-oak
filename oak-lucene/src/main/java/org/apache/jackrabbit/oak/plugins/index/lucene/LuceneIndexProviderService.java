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
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.guava.common.base.Strings;

import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalPropertyService;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporterProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.BufferedOakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LuceneIndexFileSystemStatistics;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LuceneIndexImporter;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.DocumentQueue;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.ExternalObserverBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.LocalIndexObserver;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.LuceneJournalPropertyService;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.NRTIndexFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.PropertyIndexCleaner;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.TextExtractionStatsMBean;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.InfoStream;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
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

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.ONLY_STANDALONE_TARGET;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

@SuppressWarnings("UnusedDeclaration")
@Component
@Designate(ocd = LuceneIndexProviderService.Configuration.class)
public class LuceneIndexProviderService {

    private static final boolean PROP_DISABLED_DEFAULT = false;
    private static final boolean PROP_DEBUG_DEFAULT = false;
    private static final boolean PROP_OPEN_INDEX_ASYNC_DEFAULT = true;
    private static final int PROP_THREAD_POOL_SIZE_DEFAULT = 5;
    private static final boolean PROP_PREFETCH_INDEX_FILES_DEFAULT = true;
    private static final int PROP_EXTRACTED_TEXT_CACHE_EXPIRY_IN_SECS_DEFAULT = 300;
    private static final int PROP_EXTRACTED_TEXT_CACHE_SIZE_IN_MB_DEFAULT = 20;
    private static final boolean PROP_ALWAYS_USE_PRE_EXTRACTED_TEXT_DEFAULT = false;
    private static final int PROP_BOOLEAN_CLAUSE_LIMIT_DEFAULT = 1024;
    private static final boolean PROP_ENABLE_HYBRID_INDEXING_DEFAULT = true;
    private static final int PROP_HYBRID_QUEUE_SIZE_DEFAULT = 10000;
    public static final long PROP_HYBRID_QUEUE_TIMEOUT_DEFAULT = 100;
    private static final boolean PROP_DISABLE_STORED_INDEX_DEFINITION_DEFAULT = false;
    private static final boolean PROP_DELETED_BLOBS_COLLECTION_ENABLED_DEFAULT = true;
    private static final int PROP_LUCENE_INDEX_STATS_UPDATE_INTERVAL_DEFAULT = 300;
    private static final int PROP_INDEX_CLEANER_INTERVAL_IN_SECS_DEFAULT = 10*60;
    private static final boolean PROP_ENABLE_SINGLE_BLOB_INDEX_FILES_DEFAULT = true;
    private static final long PROP_INDEX_FS_STATS_INTERVAL_IN_SECS_DEFAULT = 300;

    @ObjectClassDefinition(
            id = "org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProviderService",
            name = "Apache Jackrabbit Oak LuceneIndexProvider"
    )
    @interface Configuration {

        @AttributeDefinition(
                name = "Disable this component",
                description = "If true, this component is disabled."
        )
        boolean disabled() default PROP_DISABLED_DEFAULT;

        @AttributeDefinition(
                name = "Enable Debug Logging",
                description = "Enables debug logging in Lucene. After enabling this actual logging can be " +
                        "controlled via changing log level for category 'oak.lucene' to debug"
        )
        boolean debug() default PROP_DEBUG_DEFAULT;

        @AttributeDefinition(
                name = "Local index storage path",
                description = "Local file system path where Lucene indexes would be copied when CopyOnRead is enabled. " +
                        "If not specified then indexes would be stored under 'index' dir under Repository Home"
        )
        String localIndexDir();

        @AttributeDefinition(
                name = "Open index asynchronously",
                description = "Enable opening of indexes in asynchronous mode"
        )
        boolean enableOpenIndexAsync() default PROP_OPEN_INDEX_ASYNC_DEFAULT;

        @AttributeDefinition(
                name = "Thread pool size",
                description = "Thread pool size used to perform various asynchronous task in Oak Lucene"
        )
        int threadPoolSize() default PROP_THREAD_POOL_SIZE_DEFAULT;

        @AttributeDefinition(
                name = "Prefetch Index Files",
                description = "Prefetch the index files when CopyOnRead is enabled. When enabled all new Lucene" +
                        " index files would be copied locally before the index is made available to QueryEngine"
        )
        boolean prefetchIndexFiles() default PROP_PREFETCH_INDEX_FILES_DEFAULT;

        @AttributeDefinition(
                name = "Extracted text cache size (MB)",
                description = "Cache size in MB for caching extracted text for some time. When set to 0 then " +
                        "cache would be disabled"
        )
        int extractedTextCacheSizeInMB() default PROP_EXTRACTED_TEXT_CACHE_SIZE_IN_MB_DEFAULT;

        @AttributeDefinition(
                name = "Extracted text cache expiry (secs)",
                description = "Time in seconds for which the extracted text would be cached in memory"
        )
        int extractedTextCacheExpiryInSecs() default PROP_EXTRACTED_TEXT_CACHE_EXPIRY_IN_SECS_DEFAULT;

        @AttributeDefinition(
                name = "Always use pre-extracted text cache",
                description = "By default pre extracted text cache would only be used for reindex case. If this setting " +
                        "is enabled then it would also be used in normal incremental indexing"
        )
        boolean alwaysUsePreExtractedCache() default PROP_ALWAYS_USE_PRE_EXTRACTED_TEXT_DEFAULT;

        @AttributeDefinition(
                name = "Boolean Clause Limit",
                description = "Limit for number of boolean clauses generated for handling of OR query"
        )
        int booleanClauseLimit() default PROP_BOOLEAN_CLAUSE_LIMIT_DEFAULT;

        @AttributeDefinition(
                name = "Hybrid Indexing",
                description = "When enabled Lucene NRT Indexing mode would be enabled"
        )
        boolean enableHybridIndexing() default PROP_ENABLE_HYBRID_INDEXING_DEFAULT;

        @AttributeDefinition(
                name = "Queue size",
                description = "Size of in memory queue used for storing Lucene Documents which need to be " +
                        "added to local index"
        )
        int hybridQueueSize() default PROP_HYBRID_QUEUE_SIZE_DEFAULT;

        @AttributeDefinition(
                name = "Queue timeout",
                description = "Maximum time to wait for adding entries to the queue used for storing Lucene Documents which need to be " +
                        "added to local index"
        )
        long hybridQueueTimeout() default PROP_HYBRID_QUEUE_TIMEOUT_DEFAULT;

        @AttributeDefinition(
                name = "Disable index definition storage",
                description = "By default index definitions would be stored at time of reindexing to ensure that future " +
                        "modifications to it are not effective untill index is reindex. Set this to true would disable " +
                        "this feature"
        )
        boolean disableStoredIndexDefinition() default PROP_DISABLE_STORED_INDEX_DEFINITION_DEFAULT;

        @AttributeDefinition(
                name = "Enable actively removing deleted index blobs from blob store",
                description = "Index blobs are explicitly unique and don't require mark-sweep type collection." +
                        "This is used to enable the feature. Cleanup implies purging index blobs marked as deleted " +
                        "earlier during some indexing cycle."
        )
        boolean deletedBlobsCollectionEnabled() default PROP_DELETED_BLOBS_COLLECTION_ENABLED_DEFAULT;

        @AttributeDefinition(
                name = "Lucene index stats update interval (seconds)",
                description = "Delay in seconds after which Lucene stats are updated in async index update cycle."
        )
        int luceneIndexStatsUpdateInterval() default PROP_LUCENE_INDEX_STATS_UPDATE_INTERVAL_DEFAULT;

        @AttributeDefinition(
                name = "Property Index Cleaner Interval (seconds)",
                description = "Cleaner interval time (in seconds) for synchronous property indexes configured as " +
                        "part of lucene indexes"
        )
        int propIndexCleanerIntervalInSecs() default PROP_INDEX_CLEANER_INTERVAL_IN_SECS_DEFAULT;

        @AttributeDefinition(
                name = "With CoW enabled, should index files by written as single blobs",
                description = "Index files can be written as single blobs as chunked into smaller blobs. Enable" +
                        " this to write single blob per index file. This would reduce number of blobs in the data store."
        )
        boolean enableSingleBlobIndexFiles() default PROP_ENABLE_SINGLE_BLOB_INDEX_FILES_DEFAULT;

        @AttributeDefinition(
                name = "Lucene Index File System Stats Interval (seconds)",
                description = "Interval (in seconds) for calculation of File System metrics for Lucene Index such as Local Index Directory Size"
        )
        long propIndexFSStatsIntervalInSecs() default PROP_INDEX_FS_STATS_INTERVAL_IN_SECS_DEFAULT;

        boolean enableCopyOnReadSupport() default true;

        boolean enableCopyOnWriteSupport() default true;
    }

    public static final String REPOSITORY_HOME = "repository.home";

    private LuceneIndexProvider indexProvider;

    private final List<ServiceRegistration> regs = new ArrayList<>();
    private final List<Registration> oakRegs = new ArrayList<>();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(
            cardinality = ReferenceCardinality.OPTIONAL,
            policyOption = ReferencePolicyOption.GREEDY,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindNodeAggregator",
            unbind = "unbindNodeAggregator"
    )
    private volatile QueryIndex.NodeAggregator nodeAggregator;

    private final Clock clock = Clock.SIMPLE;

    private Whiteboard whiteboard;

    private BackgroundObserver backgroundObserver;

    private BackgroundObserver externalIndexObserver;

    @Reference
    private IndexAugmentorFactory augmentorFactory;

    @Reference
    private StatisticsProvider statisticsProvider;

    @Reference(
            policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.OPTIONAL,
            policyOption = ReferencePolicyOption.GREEDY
    )
    private volatile PreExtractedTextProvider extractedTextProvider;

    @Reference
    private MountInfoProvider mountInfoProvider;

    @Reference
    private NodeStore nodeStore;

    @Reference
    private IndexPathService indexPathService;

    @Reference
    private AsyncIndexInfoService asyncIndexInfoService;

    @Reference(
            cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.STATIC,
            policyOption = ReferencePolicyOption.GREEDY,
            target = ONLY_STANDALONE_TARGET
    )
    private GarbageCollectableBlobStore blobStore;

    @Reference
    private CheckpointMBean checkpointMBean;

    private IndexCopier indexCopier;

    private ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollector activeDeletedBlobCollector;

    private File indexDir;

    private ExecutorService executorService;

    private int threadPoolSize;

    private ExtractedTextCache extractedTextCache;

    private boolean hybridIndex;

    private NRTIndexFactory nrtIndexFactory;

    private DocumentQueue documentQueue;

    private LuceneIndexEditorProvider editorProvider;

    private IndexTracker tracker;

    private PropertyIndexCleaner cleaner;
    private AsyncIndexesSizeStatsUpdate asyncIndexesSizeStatsUpdate;

    @Activate
    private void activate(BundleContext bundleContext, Configuration config) throws IOException {
        asyncIndexesSizeStatsUpdate = new AsyncIndexesSizeStatsUpdateImpl(config.luceneIndexStatsUpdateInterval() * 1000L); // convert seconds to millis
        boolean disabled = config.disabled(), PROP_DISABLED_DEFAULT;
        hybridIndex = config.enableHybridIndexing();

        if (disabled) {
            log.info("Component disabled by configuration");
            return;
        }

        configureIndexDefinitionStorage(config);
        configureBooleanClauseLimit(config);
        initializeFactoryClassLoaders(getClass().getClassLoader());
        if (System.getProperty(BufferedOakDirectory.ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM) == null) {
            BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(config.enableSingleBlobIndexFiles());
        } else {
            log.info("Not setting config for single blob for an index file as it's set by command line!");
        }

        whiteboard = new OsgiWhiteboard(bundleContext);
        threadPoolSize = config.threadPoolSize();
        initializeIndexDir(bundleContext, config);
        initializeExtractedTextCache(bundleContext, config, statisticsProvider);
        tracker = createTracker(bundleContext, config);
        indexProvider = new LuceneIndexProvider(tracker, augmentorFactory);
        initializeActiveBlobCollector(whiteboard, config);
        initializeLogging(config);
        initialize();

        regs.add(bundleContext.registerService(QueryIndexProvider.class.getName(), indexProvider, null));
        registerObserver(bundleContext, config);
        registerLocalIndexObserver(bundleContext, tracker, config);

        registerIndexInfoProvider(bundleContext);
        registerIndexImporterProvider(bundleContext);
        registerPropertyIndexCleaner(config, bundleContext);

        LuceneIndexMBeanImpl mBean = new LuceneIndexMBeanImpl(tracker, nodeStore, indexPathService, getIndexCheckDir(), cleaner);
        oakRegs.add(registerMBean(whiteboard,
                LuceneIndexMBean.class,
                mBean,
                LuceneIndexMBean.TYPE,
                "Lucene Index statistics"));
        registerGCMonitor(whiteboard, tracker);

        registerIndexEditor(bundleContext, tracker, mBean, config);

        LuceneIndexFileSystemStatistics luceneIndexFSStats = new LuceneIndexFileSystemStatistics(statisticsProvider, indexCopier);
        registerLuceneFileSystemStats(luceneIndexFSStats, config.propIndexFSStatsIntervalInSecs());
    }

    private File getIndexCheckDir() {
        return new File(requireNonNull(indexDir), "indexCheckDir");
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

        if (externalIndexObserver != null){
            externalIndexObserver.close();
        }

        if (indexProvider != null) {
            indexProvider.close();
            indexProvider = null;
        }

        if (documentQueue != null){
            documentQueue.close();
        }

        if (nrtIndexFactory != null){
            nrtIndexFactory.close();
        }

        //Close the copier first i.e. before executorService
        if (indexCopier != null){
            indexCopier.close();
        }

        if (executorService != null){
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        }

        if (extractedTextCache != null) {
            extractedTextCache.close();
        }

        InfoStream.setDefault(InfoStream.NO_OUTPUT);
    }

    void initializeIndexDir(BundleContext bundleContext, Configuration config) {
        String indexDirPath = config.localIndexDir();
        if (Strings.isNullOrEmpty(indexDirPath)) {
            String repoHome = bundleContext.getProperty(REPOSITORY_HOME);
            if (repoHome != null){
                indexDirPath = FilenameUtils.concat(repoHome, "index");
            }
        }

        requireNonNull(indexDirPath, String.format("Index directory cannot be determined as neither index " +
                "directory path [%s] nor repository home [%s] defined", "localIndexDir", REPOSITORY_HOME));

        indexDir = new File(indexDirPath);
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

    private void initializeLogging(Configuration config) {
        boolean debug = config.debug();
        if (debug) {
            InfoStream.setDefault(LoggingInfoStream.INSTANCE);
            log.info("Registered LoggingInfoStream with Lucene. Lucene logs can be enabled " +
                    "now via category [{}]", LoggingInfoStream.PREFIX);
        }
    }

    private void registerIndexEditor(BundleContext bundleContext, IndexTracker tracker, LuceneIndexMBean mBean, Configuration config) throws IOException {
        if (config.enableCopyOnWriteSupport()){
            initializeIndexCopier(bundleContext, config);
            editorProvider = new LuceneIndexEditorProvider(indexCopier, tracker, extractedTextCache,
                    augmentorFactory,  mountInfoProvider, activeDeletedBlobCollector, mBean, statisticsProvider);
            log.info("Enabling CopyOnWrite support. Index files would be copied under {}", indexDir.getAbsolutePath());
        } else {
            editorProvider = new LuceneIndexEditorProvider(null, tracker, extractedTextCache, augmentorFactory,
                    mountInfoProvider, activeDeletedBlobCollector, mBean, statisticsProvider);
        }
        editorProvider.setBlobStore(blobStore);
        editorProvider.withAsyncIndexesSizeStatsUpdate(asyncIndexesSizeStatsUpdate);

        if (hybridIndex){
            editorProvider.setIndexingQueue(requireNonNull(documentQueue));
        }

        Dictionary<String, Object> props = new Hashtable<>();
        props.put("type", TYPE_LUCENE);
        regs.add(bundleContext.registerService(IndexEditorProvider.class.getName(), editorProvider, props));
        oakRegs.add(registerMBean(whiteboard,
                TextExtractionStatsMBean.class,
                editorProvider.getExtractedTextCache().getStatsMBean(),
                TextExtractionStatsMBean.TYPE,
                "TextExtraction statistics"));
    }

    private IndexTracker createTracker(BundleContext bundleContext, Configuration config) throws IOException {
        IndexTracker tracker;
        if (config.enableCopyOnReadSupport()){
            initializeIndexCopier(bundleContext, config);
            log.info("Enabling CopyOnRead support. Index files would be copied under {}", indexDir.getAbsolutePath());
            if (hybridIndex) {
                nrtIndexFactory = new NRTIndexFactory(indexCopier, statisticsProvider);
            }
            tracker = new IndexTracker(new DefaultIndexReaderFactory(mountInfoProvider, indexCopier), nrtIndexFactory);
        } else {
            tracker = new IndexTracker(new DefaultIndexReaderFactory(mountInfoProvider, null));
        }

        tracker.setAsyncIndexInfoService(asyncIndexInfoService);
        return tracker;
    }

    private void initializeIndexCopier(BundleContext bundleContext, Configuration config) throws IOException {
        if(indexCopier != null){
            return;
        }
        boolean prefetchEnabled = config.prefetchIndexFiles();

        if (prefetchEnabled){
            log.info("Prefetching of index files enabled. Index would be opened after copying all new files locally");
        }

        indexCopier = new IndexCopier(getExecutorService(), indexDir, prefetchEnabled);

        oakRegs.add(registerMBean(whiteboard,
                CopyOnReadStatsMBean.class,
                indexCopier,
                CopyOnReadStatsMBean.TYPE,
                "IndexCopier support statistics"));

    }

    ExecutorService getExecutorService(){
        if (executorService == null){
            executorService = createExecutor();
        }
        return executorService;
    }

    private ExecutorService createExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            private final Thread.UncaughtExceptionHandler handler = (t, e) -> log.warn("Error occurred in asynchronous processing ", e);
            @Override
            public Thread newThread(@NotNull Runnable r) {
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

    private void registerObserver(BundleContext bundleContext, Configuration config) {
        boolean enableAsyncIndexOpen = config.enableOpenIndexAsync();
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

    private void registerLocalIndexObserver(BundleContext bundleContext, IndexTracker tracker, Configuration config) {
        if (!hybridIndex){
            log.info("Hybrid indexing feature disabled");
            return;
        }

        int queueSize = config.hybridQueueSize();
        long queueOfferTimeoutMillis = config.hybridQueueTimeout();
        documentQueue = new DocumentQueue(queueSize, queueOfferTimeoutMillis, tracker, getExecutorService(), statisticsProvider);
        LocalIndexObserver localIndexObserver = new LocalIndexObserver(documentQueue, statisticsProvider);
        regs.add(bundleContext.registerService(Observer.class.getName(), localIndexObserver, null));

        int observerQueueSize = 1000;
        int builderMaxSize = 5000;
        regs.add(bundleContext.registerService(JournalPropertyService.class.getName(),
                new LuceneJournalPropertyService(builderMaxSize), null));
        ExternalObserverBuilder builder = new ExternalObserverBuilder(documentQueue, tracker, statisticsProvider,
                getExecutorService(), observerQueueSize);
        log.info("Configured JournalPropertyBuilder with max size {} and backed by BackgroundObserver " +
                "with queue size {}", builderMaxSize, observerQueueSize);

        Observer observer = builder.build();
        externalIndexObserver = builder.getBackgroundObserver();
        regs.add(bundleContext.registerService(Observer.class.getName(), observer, null));
        oakRegs.add(registerMBean(whiteboard,
                BackgroundObserverMBean.class,
                externalIndexObserver.getMBean(),
                BackgroundObserverMBean.TYPE,
                "LuceneExternalIndexObserver queue stats"));
        log.info("Hybrid indexing enabled for configured indexes with queue size of {}", queueSize);
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

    private void initializeExtractedTextCache(BundleContext bundleContext, Configuration config, StatisticsProvider statisticsProvider) {
        int cacheSizeInMB = config.extractedTextCacheSizeInMB();
        int cacheExpiryInSecs = config.extractedTextCacheExpiryInSecs();
        boolean alwaysUsePreExtractedCache = config.alwaysUsePreExtractedCache();

        extractedTextCache = new ExtractedTextCache(
                cacheSizeInMB * ONE_MB,
                cacheExpiryInSecs,
                alwaysUsePreExtractedCache,
                indexDir, statisticsProvider);
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

    private void configureBooleanClauseLimit(Configuration config) {
        int booleanClauseLimit = config.booleanClauseLimit();
        if (booleanClauseLimit != BooleanQuery.getMaxClauseCount()){
            BooleanQuery.setMaxClauseCount(booleanClauseLimit);
            log.info("Changed the Max boolean clause limit to {}", booleanClauseLimit);
        }
    }

    private void configureIndexDefinitionStorage(Configuration config) {
        boolean disableStorage = config.disableStoredIndexDefinition();
        if (disableStorage){
            log.info("Feature to ensure that index definition matches the index state is disabled. Change in " +
                    "index definition would now affect query plans and might lead to inconsistent results.");
            IndexDefinition.setDisableStoredIndexDefinition(disableStorage);
        }
    }

    private void registerGCMonitor(Whiteboard whiteboard,
                                   final IndexTracker tracker) {
        GCMonitor gcMonitor = new GCMonitor.Empty() {
            @Override
            public void compacted() {
                tracker.refresh();
            }
        };
        oakRegs.add(whiteboard.register(GCMonitor.class, gcMonitor, emptyMap()));
    }

    private void registerIndexInfoProvider(BundleContext bundleContext) {
        IndexInfoProvider infoProvider = new LuceneIndexInfoProvider(nodeStore, asyncIndexInfoService, getIndexCheckDir());
        regs.add(bundleContext.registerService(IndexInfoProvider.class.getName(), infoProvider, null));
    }

    private void registerIndexImporterProvider(BundleContext bundleContext) {
        LuceneIndexImporter importer = new LuceneIndexImporter(blobStore);
        regs.add(bundleContext.registerService(IndexImporterProvider.class.getName(), importer, null));
    }

    private void initializeActiveBlobCollector(Whiteboard whiteboard, Configuration config) {
        boolean activeDeletionEnabled = config.deletedBlobsCollectionEnabled();
        if (activeDeletionEnabled && blobStore!= null) {
            File blobCollectorWorkingDir = new File(indexDir, "deleted-blobs");
            activeDeletedBlobCollector = ActiveDeletedBlobCollectorFactory.newInstance(blobCollectorWorkingDir,
                    getExecutorService());
            ActiveDeletedBlobCollectorMBean bean =
                    new ActiveDeletedBlobCollectorMBeanImpl(activeDeletedBlobCollector, whiteboard, nodeStore,
                            indexPathService, asyncIndexInfoService, blobStore, getExecutorService());

            oakRegs.add(registerMBean(whiteboard, ActiveDeletedBlobCollectorMBean.class, bean,
                    ActiveDeletedBlobCollectorMBean.TYPE, "Active lucene files collection"));
            log.info("Active blob collector initialized at working dir: {}", blobCollectorWorkingDir);
        } else {
            activeDeletedBlobCollector = ActiveDeletedBlobCollectorFactory.NOOP;
            log.info("Active blob collector set to NOOP. enabled: {} seconds; blobStore: {}",
                    activeDeletionEnabled, blobStore);
        }
    }

    private void registerPropertyIndexCleaner(Configuration config, BundleContext bundleContext) {
        int cleanerInterval = config.propIndexCleanerIntervalInSecs();

        if (cleanerInterval <= 0) {
            log.info("Property index cleaner would not be registered");
            return;
        }

        cleaner = new PropertyIndexCleaner(nodeStore, indexPathService, asyncIndexInfoService, statisticsProvider);

        //Proxy check for DocumentNodeStore
        if (nodeStore instanceof Clusterable) {
            cleaner.setRecursiveDelete(true);
            log.info("PropertyIndexCleaner configured to perform recursive delete");
        }
        oakRegs.add(scheduleWithFixedDelay(whiteboard, cleaner,
                Map.of("scheduler.name", PropertyIndexCleaner.class.getName()),
                cleanerInterval, true, true));
        log.info("Property index cleaner configured to run every [{}] seconds", cleanerInterval);
    }

    private void registerLuceneFileSystemStats(LuceneIndexFileSystemStatistics luceneIndexFSStats, long delayInSeconds) {
        Map<String, Object> config = Map.of(
                "scheduler.name", LuceneIndexFileSystemStatistics.class.getName()
        );
        oakRegs.add(scheduleWithFixedDelay(whiteboard, luceneIndexFSStats, config, delayInSeconds, false, true));
        log.info("Lucene FileSystem Statistics calculator configured to run every [{}] seconds", delayInSeconds);
    }


    protected void bindNodeAggregator(QueryIndex.NodeAggregator aggregator) {
        this.nodeAggregator = aggregator;
        initialize();
    }

    protected void unbindNodeAggregator(QueryIndex.NodeAggregator aggregator) {
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
