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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toLong;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.isShared;
import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.getOrCreateId;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_STRING_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_TEMPLATE_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.BACKUP_DIRECTORY;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.COMPACTION_DISABLE_ESTIMATION;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.COMPACTION_FORCE_TIMEOUT;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.COMPACTION_RETRY_COUNT;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.COMPACTION_SIZE_DELTA_ESTIMATION;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.CUSTOM_BLOB_STORE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.DEFAULT_BLOB_GC_MAX_AGE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.DEFAULT_BLOB_SNAPSHOT_INTERVAL;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.GC_PROGRESS_LOG;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.MEMORY_THRESHOLD;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.MODE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.NODE_DEDUPLICATION_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.PAUSE_COMPACTION;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.PROP_BLOB_GC_MAX_AGE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.PROP_BLOB_SNAPSHOT_INTERVAL;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.REPOSITORY_HOME_DIRECTORY;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.RETAINED_GENERATIONS;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.SEGMENT_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.STANDBY;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.STRING_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.STRING_DEDUPLICATION_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.TEMPLATE_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.TEMPLATE_DEDUPLICATION_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener.IGNORE_SNFE;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_NODE_CACHE_SIZE_OSGi;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_STRING_CACHE_SIZE_OSGi;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_TEMPLATE_CACHE_SIZE_OSGi;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.DISABLE_ESTIMATION_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.FORCE_TIMEOUT_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GC_PROGRESS_LOG_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.MEMORY_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.PAUSE_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.RETAINED_GENERATIONS_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.RETRY_COUNT_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.SIZE_DELTA_ESTIMATION_DEFAULT;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.DEFAULT_MAX_FILE_SIZE;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.ONLY_STANDALONE_TARGET;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.api.jmx.FileStoreBackupRestoreMBean;
import org.apache.jackrabbit.oak.backup.impl.FileStoreBackupRestoreImpl;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.compaction.SegmentRevisionGC;
import org.apache.jackrabbit.oak.segment.compaction.SegmentRevisionGCMBean;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.FileStoreGCMonitor;
import org.apache.jackrabbit.oak.segment.file.FileStoreStatsMBean;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.MetricsIOMonitor;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitorTracker;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.RevisionGC;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.spi.descriptors.GenericDescriptors;
import org.osgi.framework.Constants;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.io.Closer;

/**
 * An OSGi wrapper for the segment node store.
 */
@Component(policy = ConfigurationPolicy.REQUIRE,
        metatype = true,
        label = "Oak Segment Tar NodeStore service",
        description = "Apache Jackrabbit Oak NodeStore implementation based on the segment model. " +
                "For configuration refer to http://jackrabbit.apache.org/oak/docs/osgi_config.html#SegmentNodeStore. " +
                "Note that for system stability purpose it is advisable to not change these settings " +
                "at runtime. Instead the config change should be done via file system based config " +
                "file and this view should ONLY be used to determine which options are supported."
)
public class SegmentNodeStoreService {

    private static final Logger log = LoggerFactory.getLogger(SegmentNodeStoreService.class);

    @Property(
            label = "Repository Home Directory",
            description = "Path on the file system where repository data will be stored. "
                    + "Defaults to the value of the framework property 'repository.home' or to 'repository' "
                    + "if that is neither specified."
    )
    public static final String REPOSITORY_HOME_DIRECTORY = "repository.home";

    @Property(
            label = "Mode",
            description = "TarMK mode (64 for memory mapped file access, 32 for normal file access). " +
                    "Default value is taken from the 'sun.arch.data.model' system property."
    )
    public static final String MODE = "tarmk.mode";

    @Property(
            intValue = DEFAULT_MAX_FILE_SIZE,
            label = "Maximum tar file size (MB)",
            description = "The maximum size of the tar files in megabytes. " +
                    "Default value is '" + DEFAULT_MAX_FILE_SIZE + "'."
    )
    public static final String SIZE = "tarmk.size";

    @Property(
            intValue = DEFAULT_SEGMENT_CACHE_MB,
            label = "Segment cache size (MB)",
            description = "Cache size for storing most recently used segments in megabytes. " +
                    "Default value is '" + DEFAULT_SEGMENT_CACHE_MB + "'."
    )
    public static final String SEGMENT_CACHE_SIZE = "segmentCache.size";

    @Property(
            intValue = DEFAULT_STRING_CACHE_MB,
            label = "String cache size (MB)",
            description = "Cache size for storing most recently used strings in megabytes. " +
                    "Default value is '" + DEFAULT_STRING_CACHE_MB + "'."
    )
    public static final String STRING_CACHE_SIZE = "stringCache.size";

    @Property(
            intValue = DEFAULT_TEMPLATE_CACHE_MB,
            label = "Template cache size (MB)",
            description = "Cache size for storing most recently used templates in megabytes. " +
                    "Default value is '" + DEFAULT_TEMPLATE_CACHE_MB + "'."
    )
    public static final String TEMPLATE_CACHE_SIZE = "templateCache.size";

    @Property(
            intValue = DEFAULT_STRING_CACHE_SIZE_OSGi,
            label = "String deduplication cache size (#items)",
            description = "Maximum number of strings to keep in the deduplication cache. " +
                    "Default value is '" + DEFAULT_STRING_CACHE_SIZE_OSGi + "'."
    )
    public static final String STRING_DEDUPLICATION_CACHE_SIZE = "stringDeduplicationCache.size";

    @Property(
            intValue = DEFAULT_TEMPLATE_CACHE_SIZE_OSGi,
            label = "Template deduplication cache size (#items)",
            description = "Maximum number of templates to keep in the deduplication cache. " +
                    "Default value is '" + DEFAULT_TEMPLATE_CACHE_SIZE_OSGi + "'."
    )
    public static final String TEMPLATE_DEDUPLICATION_CACHE_SIZE = "templateDeduplicationCache.size";

    @Property(
            intValue = DEFAULT_NODE_CACHE_SIZE_OSGi,
            label = "Node deduplication cache size (#items)",
            description = "Maximum number of node to keep in the deduplication cache. If the supplied " +
                    "value is not a power of 2, it will be rounded up to the next power of 2. " +
                    "Default value is '" + DEFAULT_NODE_CACHE_SIZE_OSGi + "'."
    )
    public static final String NODE_DEDUPLICATION_CACHE_SIZE = "nodeDeduplicationCache.size";

    @Property(
            boolValue = PAUSE_DEFAULT,
            label = "Pause compaction",
            description = "When set to true the compaction phase is skipped during garbage collection. " +
                    "Default value is '" + PAUSE_DEFAULT + "'."
    )
    public static final String PAUSE_COMPACTION = "pauseCompaction";

    @Property(
            intValue = RETRY_COUNT_DEFAULT,
            label = "Compaction retries",
            description = "Number of tries to compact concurrent commits on top of already " +
                    "compacted commits. " +
                    "Default value is '" + RETRY_COUNT_DEFAULT + "'."
    )
    public static final String COMPACTION_RETRY_COUNT = "compaction.retryCount";

    @Property(
            intValue = FORCE_TIMEOUT_DEFAULT,
            label = "Force compaction timeout",
            description = "Number of seconds to attempt to force compact concurrent commits on top " +
                    "of already compacted commits after the maximum number of retries has been " +
                    "reached. Forced compaction tries to acquire an exclusive write lock on the " +
                    "node store, blocking concurrent write access as long as the lock is held. " +
                    "Default value is '" + FORCE_TIMEOUT_DEFAULT + "'."
    )
    public static final String COMPACTION_FORCE_TIMEOUT = "compaction.force.timeout";

    @Property(
            longValue = SIZE_DELTA_ESTIMATION_DEFAULT,
            label = "Garbage collection repository size threshold",
            description = "Garbage collection will be skipped unless the repository grew at least by " +
                    "the number of bytes specified. " +
                    "Default value is '" + SIZE_DELTA_ESTIMATION_DEFAULT + "'."
    )
    public static final String COMPACTION_SIZE_DELTA_ESTIMATION = "compaction.sizeDeltaEstimation";

    @Property(
            boolValue = DISABLE_ESTIMATION_DEFAULT,
            label = "Disable estimation phase",
            description = "Disables the estimation phase allowing garbage collection to run unconditionally. " +
                    "Default value is '" + DISABLE_ESTIMATION_DEFAULT + "'."
    )
    public static final String COMPACTION_DISABLE_ESTIMATION = "compaction.disableEstimation";

    @Property(
            intValue = RETAINED_GENERATIONS_DEFAULT,
            label = "Compaction retained generations",
            description = "Number of segment generations to retain during garbage collection. " +
                    "Must be set to at least 2. " +
                    "Default value is '" + RETAINED_GENERATIONS_DEFAULT + "'."
    )
    public static final String RETAINED_GENERATIONS = "compaction.retainedGenerations";

    @Property(
            intValue = MEMORY_THRESHOLD_DEFAULT,
            label = "Compaction memory threshold",
            description = "Threshold of available heap memory in percent of total heap memory below " +
                    "which the compaction phase is canceled. 0 disables heap memory monitoring. " +
                    "Default value is '" + MEMORY_THRESHOLD_DEFAULT + "'."
    )
    public static final String MEMORY_THRESHOLD = "compaction.memoryThreshold";

    @Property(
            longValue = GC_PROGRESS_LOG_DEFAULT,
            label = "Compaction progress log",
            description = "The number of nodes compacted after which a status message is logged. " +
                    "-1 disables progress logging. " +
                    "Default value is '" + GC_PROGRESS_LOG_DEFAULT + "'."
    )
    public static final String GC_PROGRESS_LOG = "compaction.progressLog";

    @Property(
            boolValue = false,
            label = "Standby mode",
            description = "Flag indicating this component will not register as a NodeStore but as a " +
                    "NodeStoreProvider instead. "  +
                    "Default value is 'false'."
    )
    public static final String STANDBY = "standby";

    @Property(boolValue = false,
            label = "Custom blob store",
            description = "Boolean value indicating that a custom BlobStore is used for storing " +
                    "large binary values."
    )
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    @Property(
            label = "Backup directory",
            description = "Directory (relative to current working directory) for storing repository backups. " +
                    "Defaults to 'repository.home/segmentstore-backup'."
    )
    public static final String BACKUP_DIRECTORY = "repository.backup.dir";

    @Reference(
            cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.STATIC,
            policyOption = ReferencePolicyOption.GREEDY,
            target = ONLY_STANDALONE_TARGET
    )
    private volatile BlobStore blobStore;

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    private Closer closer;

    /**
     * Blob modified before this time duration would be considered for Blob GC
     */
    static final long DEFAULT_BLOB_GC_MAX_AGE = 24 * 60 * 60;

    @Property(longValue = DEFAULT_BLOB_GC_MAX_AGE,
            label = "Blob gc max age (in secs)",
            description = "The blob garbage collection logic will only consider those blobs which " +
                    "are not accessed recently (currentTime - lastModifiedTime > blobGcMaxAgeInSecs). " +
                    "For example with the default setting only those blobs which have been created " +
                    "at least 24 hours ago will be considered for garbage collection. " +
                    "Default value is '" + DEFAULT_BLOB_GC_MAX_AGE + "'."
    )
    public static final String PROP_BLOB_GC_MAX_AGE = "blobGcMaxAgeInSecs";

    /**
     * Default interval for taking snapshots of locally tracked blob ids.
     */
    static final long DEFAULT_BLOB_SNAPSHOT_INTERVAL = 12 * 60 * 60;

    @Property(longValue = DEFAULT_BLOB_SNAPSHOT_INTERVAL,
            label = "Blob tracking snapshot interval",
            description = "Interval in seconds in which snapshots of locally tracked blob ids are " +
                    "taken and synchronized with the blob store. This should be configured to be " +
                    "less than the frequency of blob garbage collection so that deletions during blob " +
                    "garbage collection can be accounted for in the next garbage collection execution. " +
                    "Default value is '" + DEFAULT_BLOB_SNAPSHOT_INTERVAL + "'."
    )
    public static final String PROP_BLOB_SNAPSHOT_INTERVAL = "blobTrackSnapshotIntervalInSecs";

    @Activate
    public void activate(ComponentContext context) throws IOException {
        Configuration configuration = new Configuration(context);
        if (blobStore == null && configuration.hasCustomBlobStore()) {
            log.info("BlobStore enabled. SegmentNodeStore will be initialized once the blob " +
                    "store becomes available");
            return;
        }
        closer = Closer.create();
        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        registerSegmentStore(context, blobStore, statisticsProvider, closer, whiteboard, null, true);
    }

    /**
     * Configures and registers a new SegmentNodeStore instance together will
     * all required components. Anything that must be disposed of (like
     * registered services or MBeans) will be registered via the
     * {@code registration} parameter.
     *
     * @param context            An instance of {@link ComponentContext}.
     * @param blobStore          An instance of {@link BlobStore}. It can be
     *                           {@code null}.
     * @param statisticsProvider An instance of {@link StatisticsProvider}.
     * @param closer             An instance of {@link Closer}. It will be used
     *                           to track every registered service or
     *                           component.
     * @param whiteboard         An instance of {@link Whiteboard}. It will be
     *                           used to register services in the OSGi
     *                           framework.
     * @param role               The role of this component. It can be {@code
     *                           null}.
     * @param descriptors        Determines if repository descriptors related to
     *                           discovery services should be registered.
     * @return A configured {@link SegmentNodeStore}, or {@code null} if the
     * setup failed.
     * @throws IOException In case an unrecoverable error occurs.
     */
    static SegmentNodeStore registerSegmentStore(
            @Nonnull ComponentContext context,
            @Nullable BlobStore blobStore,
            @Nonnull StatisticsProvider statisticsProvider,
            @Nonnull Closer closer,
            @Nonnull Whiteboard whiteboard,
            @Nullable String role,
            boolean descriptors
    ) throws IOException {
        Configuration configuration = new Configuration(context, role);
        Closeables closeables = new Closeables(closer);
        Registrations registrations = new Registrations(whiteboard, role);

        // Listen for GCMonitor services
        GCMonitor gcMonitor = GCMonitor.EMPTY;

        if (configuration.isPrimarySegmentStore()) {
            GCMonitorTracker tracker = new GCMonitorTracker();
            tracker.start(whiteboard);
            closeables.add(tracker);
            gcMonitor = tracker;
        }

        // Create the gc options
        if (configuration.getCompactionGainThreshold() != null) {
            log.warn("Detected deprecated flag 'compaction.gainThreshold'. "
                    + "Please use 'compaction.sizeDeltaEstimation' instead and "
                    + "'compaction.disableEstimation' to disable estimation.");
        }
        SegmentGCOptions gcOptions = new SegmentGCOptions(configuration.getPauseCompaction(), configuration.getRetryCount(), configuration.getForceCompactionTimeout())
                .setRetainedGenerations(configuration.getRetainedGenerations())
                .setGcSizeDeltaEstimation(configuration.getSizeDeltaEstimation())
                .setMemoryThreshold(configuration.getMemoryThreshold())
                .setEstimationDisabled(configuration.getDisableEstimation())
                .setGCLogInterval(configuration.getGCProcessLog());

        // Build the FileStore
        FileStoreBuilder builder = fileStoreBuilder(configuration.getSegmentDirectory())
                .withSegmentCacheSize(configuration.getSegmentCacheSize())
                .withStringCacheSize(configuration.getStringCacheSize())
                .withTemplateCacheSize(configuration.getTemplateCacheSize())
                .withStringDeduplicationCacheSize(configuration.getStringDeduplicationCacheSize())
                .withTemplateDeduplicationCacheSize(configuration.getTemplateDeduplicationCacheSize())
                .withNodeDeduplicationCacheSize(configuration.getNodeDeduplicationCacheSize())
                .withMaxFileSize(configuration.getMaxFileSize())
                .withMemoryMapping(configuration.getMemoryMapping())
                .withGCMonitor(gcMonitor)
                .withIOMonitor(new MetricsIOMonitor(statisticsProvider))
                .withStatisticsProvider(statisticsProvider)
                .withGCOptions(gcOptions);

        if (configuration.hasCustomBlobStore() && blobStore != null) {
            log.info("Initializing SegmentNodeStore with BlobStore [{}]", blobStore);
            builder.withBlobStore(blobStore);
        }

        if (configuration.isStandbyInstance()) {
            builder.withSnfeListener(IGNORE_SNFE);
        }

        final FileStore store;
        try {
            store = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            log.error("The storage format is not compatible with this version of Oak Segment Tar", e);
            return null;
        }
        // store should be closed last
        closeables.add(store);

        // Listen for Executor services on the whiteboard

        WhiteboardExecutor executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        closeables.add(executor);

        // Expose stats about the segment cache

        CacheStatsMBean segmentCacheStats = store.getSegmentCacheStats();
        closeables.add(registrations.registerMBean(
                CacheStatsMBean.class,
                segmentCacheStats,
                CacheStats.TYPE,
                segmentCacheStats.getName()
        ));

        // Expose stats about the string and template caches

        CacheStatsMBean stringCacheStats = store.getStringCacheStats();
        closeables.add(registrations.registerMBean(
                CacheStatsMBean.class,
                stringCacheStats,
                CacheStats.TYPE,
                stringCacheStats.getName()
        ));

        CacheStatsMBean templateCacheStats = store.getTemplateCacheStats();
        closeables.add(registrations.registerMBean(
                CacheStatsMBean.class,
                templateCacheStats,
                CacheStats.TYPE,
                templateCacheStats.getName()
        ));

        WriterCacheManager cacheManager = builder.getCacheManager();
        CacheStatsMBean stringDeduplicationCacheStats = cacheManager.getStringCacheStats();
        if (stringDeduplicationCacheStats != null) {
            closeables.add(registrations.registerMBean(
                    CacheStatsMBean.class,
                    stringDeduplicationCacheStats,
                    CacheStats.TYPE,
                    stringDeduplicationCacheStats.getName()
            ));
        }

        CacheStatsMBean templateDeduplicationCacheStats = cacheManager.getTemplateCacheStats();
        if (templateDeduplicationCacheStats != null) {
            closeables.add(registrations.registerMBean(
                    CacheStatsMBean.class,
                    templateDeduplicationCacheStats,
                    CacheStats.TYPE,
                    templateDeduplicationCacheStats.getName()
            ));
        }

        CacheStatsMBean nodeDeduplicationCacheStats = cacheManager.getNodeCacheStats();
        if (nodeDeduplicationCacheStats != null) {
            closeables.add(registrations.registerMBean(
                    CacheStatsMBean.class,
                    nodeDeduplicationCacheStats,
                    CacheStats.TYPE,
                    nodeDeduplicationCacheStats.getName()
            ));
        }

        // Expose an MBean to managing and monitoring garbage collection
        final FileStoreGCMonitor monitor = new FileStoreGCMonitor(Clock.SIMPLE);
        closeables.add(registrations.register(
                GCMonitor.class,
                monitor
        ));
        if (!configuration.isStandbyInstance()) {
            closeables.add(registrations.registerMBean(
                    SegmentRevisionGC.class,
                    new SegmentRevisionGCMBean(store, gcOptions, monitor),
                    SegmentRevisionGC.TYPE,
                    "Segment node store revision garbage collection"
            ));
        }

        Runnable cancelGC = new Runnable() {

            @Override
            public void run() {
                store.cancelGC();
            }

        };
        Supplier<String> statusMessage = new Supplier<String>() {

            @Override
            public String get() {
                return monitor.getStatus();
            }

        };
        closeables.add(registrations.registerMBean(
                RevisionGCMBean.class,
                new RevisionGC(store.getGCRunner(), cancelGC, statusMessage, executor),
                RevisionGCMBean.TYPE,
                "Revision garbage collection"
        ));

        // Expose statistics about the FileStore

        closeables.add(registrations.registerMBean(
                FileStoreStatsMBean.class,
                store.getStats(),
                FileStoreStatsMBean.TYPE,
                "FileStore statistics"
        ));

        // register segment node store

        SegmentNodeStore.SegmentNodeStoreBuilder segmentNodeStoreBuilder = SegmentNodeStoreBuilders.builder(store).withStatisticsProvider(statisticsProvider);
        if (configuration.isStandbyInstance() || !configuration.isPrimarySegmentStore()) {
            segmentNodeStoreBuilder.dispatchChanges(false);
        }
        SegmentNodeStore segmentNodeStore = segmentNodeStoreBuilder.build();

        if (configuration.isPrimarySegmentStore()) {
            ObserverTracker observerTracker = new ObserverTracker(segmentNodeStore);
            observerTracker.start(context.getBundleContext());
            closeables.add(observerTracker);
        }

        if (configuration.isPrimarySegmentStore()) {
            closeables.add(registrations.registerMBean(
                    CheckpointMBean.class,
                    new SegmentCheckpointMBean(segmentNodeStore),
                    CheckpointMBean.TYPE,
                    "Segment node store checkpoint management"
            ));
        }

        if (descriptors) {
            // ensure a clusterId is initialized
            // and expose it as 'oak.clusterid' repository descriptor
            GenericDescriptors clusterIdDesc = new GenericDescriptors();
            clusterIdDesc.put(
                    ClusterRepositoryInfo.OAK_CLUSTERID_REPOSITORY_DESCRIPTOR_KEY,
                    new SimpleValueFactory().createValue(getOrCreateId(segmentNodeStore)),
                    true,
                    false
            );
            closeables.add(registrations.register(Descriptors.class, clusterIdDesc));
            // Register "discovery lite" descriptors
            closeables.add(registrations.register(Descriptors.class, new SegmentDiscoveryLiteDescriptors(segmentNodeStore)));
        }

        // If a shared data store register the repo id in the data store
        if (configuration.isPrimarySegmentStore() && isShared(blobStore)) {
            SharedDataStore sharedDataStore = (SharedDataStore) blobStore;
            try {
                sharedDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]), SharedStoreRecordType.REPOSITORY.getNameFromId(getOrCreateId(segmentNodeStore)));
            } catch (Exception e) {
                throw new IOException("Could not register a unique repositoryId", e);
            }
            if (blobStore instanceof BlobTrackingStore) {
                BlobTrackingStore trackingStore = (BlobTrackingStore) blobStore;
                if (trackingStore.getTracker() != null) {
                    trackingStore.getTracker().close();
                }
                trackingStore.addTracker(new BlobIdTracker(configuration.getRepositoryHome(), getOrCreateId(segmentNodeStore), configuration.getBlobSnapshotInterval(), sharedDataStore));
            }
        }

        if (configuration.isPrimarySegmentStore() && blobStore instanceof GarbageCollectableBlobStore) {
            BlobGarbageCollector gc = new MarkSweepGarbageCollector(
                    new SegmentBlobReferenceRetriever(store),
                    (GarbageCollectableBlobStore) blobStore,
                    executor,
                    TimeUnit.SECONDS.toMillis(configuration.getBlobGcMaxAge()),
                    getOrCreateId(segmentNodeStore),
                    whiteboard
            );
            closeables.add(registrations.registerMBean(
                    BlobGCMBean.class,
                    new BlobGC(gc, executor),
                    BlobGCMBean.TYPE,
                    "Segment node store blob garbage collection"
            ));
        }

        // Expose an MBean for backup/restore operations

        closeables.add(registrations.registerMBean(
                FileStoreBackupRestoreMBean.class,
                new FileStoreBackupRestoreImpl(
                        segmentNodeStore,
                        store.getRevisions(),
                        store.getReader(),
                        configuration.getBackupDirectory(),
                        executor
                ),
                FileStoreBackupRestoreMBean.TYPE,
                "Segment node store backup/restore"
        ));

        // Expose statistics about the SegmentNodeStore

        closeables.add(registrations.registerMBean(
                SegmentNodeStoreStatsMBean.class,
                segmentNodeStore.getStats(),
                SegmentNodeStoreStatsMBean.TYPE,
                "SegmentNodeStore statistics"
        ));

        if (configuration.isPrimarySegmentStore()) {
            log.info("Primary SegmentNodeStore initialized");
        } else {
            log.info("Secondary SegmentNodeStore initialized, role={}", role);
        }

        // Register a factory service to expose the FileStore
        closeables.add(registrations.register(
                SegmentStoreProvider.class,
                new DefaultSegmentStoreProvider(store)
        ));

        if (configuration.isStandbyInstance()) {
            return segmentNodeStore;
        }

        if (configuration.isPrimarySegmentStore()) {
            Map<String, Object> props = new HashMap<String, Object>();
            props.put(Constants.SERVICE_PID, SegmentNodeStore.class.getName());
            props.put("oak.nodestore.description", new String[] {"nodeStoreType=segment"});
            closeables.add(registrations.register(NodeStore.class, segmentNodeStore, props));
        }

        return segmentNodeStore;
    }

    @Deactivate
    public void deactivate() {
        closeQuietly(closer);
        closer = null;
    }

}

/**
 * Encapsulates a {@link Closer} and makes it easier to track the lifecycle
 * of entities that can be disposed.
 */
class Closeables implements Closeable {

    private final Closer closer;

    Closeables(Closer closer) {
        this.closer = closer;
    }

    void add(Closeable c) {
        closer.register(c);
    }

    void add(final AbstractServiceTracker<?> t) {
        add(new Closeable() {

            @Override
            public void close() {
                t.stop();
            }

        });
    }

    void add(final Registration r) {
        add(new Closeable() {

            @Override
            public void close() {
                r.unregister();
            }

        });
    }

    void add(final ObserverTracker t) {
        add(new Closeable() {

            @Override
            public void close() {
                t.stop();
            }

        });
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

}

/**
 * Allows simple access to the configuration of this component. Provides
 * default values for unspecified properties and type conversion.
 */
class Configuration {

    private static int roundToNextPowerOfTwo(int size) {
        return 1 << (32 - Integer.numberOfLeadingZeros(Math.max(0, size - 1)));
    }

    private final ComponentContext context;

    private final String role;

    Configuration(ComponentContext context) {
        this(context, null);
    }

    Configuration(ComponentContext context, String role) {
        this.context = context;
        this.role = role;
    }

    String property(String name) {
        return lookupConfigurationThenFramework(context, name);
    }

    /**
     * Chooses repository home directory name based on <b>repository.home</b>
     * property, defaulting to <b>repository</b> if property is not set.
     * 
     * @return repository home directory name.
     */
    String getRepositoryHome() {
        String root = property(REPOSITORY_HOME_DIRECTORY);
        if (isNullOrEmpty(root)) {
            return "repository";
        }
        return root;
    }

    /**
     * Creates a new sub-directory relative to {@link #getRepositoryHome()} for
     * storing segments.
     * 
     * @return directory for storing segments.
     */
    File getSegmentDirectory() {
        return new File(getRepositoryHome(), appendRole("segmentstore"));
    }

    /**
     * Creates a new sub-directory relative to {@link #getRepositoryHome()} for 
     * storing repository backups.
     * 
     * @return directory for storing repository backups.
     */
    File getBackupDirectory() {
        String backupDirectory = property(BACKUP_DIRECTORY);
        if (backupDirectory != null) {
            return new File(backupDirectory);
        }
        return new File(getRepositoryHome(), appendRole("segmentstore-backup"));
    }

    int getSegmentCacheSize() {
        return toInteger(getCacheSize(SEGMENT_CACHE_SIZE), DEFAULT_SEGMENT_CACHE_MB);
    }

    int getStringCacheSize() {
        return toInteger(getCacheSize(STRING_CACHE_SIZE), DEFAULT_STRING_CACHE_MB);
    }

    int getTemplateCacheSize() {
        return toInteger(getCacheSize(TEMPLATE_CACHE_SIZE), DEFAULT_TEMPLATE_CACHE_MB);
    }

    int getStringDeduplicationCacheSize() {
        return toInteger(getCacheSize(STRING_DEDUPLICATION_CACHE_SIZE), DEFAULT_STRING_CACHE_SIZE_OSGi);
    }

    int getTemplateDeduplicationCacheSize() {
        return toInteger(getCacheSize(TEMPLATE_DEDUPLICATION_CACHE_SIZE), DEFAULT_TEMPLATE_CACHE_SIZE_OSGi);
    }

    int getNodeDeduplicationCacheSize() {
        return roundToNextPowerOfTwo(toInteger(getCacheSize(NODE_DEDUPLICATION_CACHE_SIZE), DEFAULT_NODE_CACHE_SIZE_OSGi));
    }

    boolean getPauseCompaction() {
        return toBoolean(property(PAUSE_COMPACTION), PAUSE_DEFAULT);
    }

    int getRetryCount() {
        return toInteger(property(COMPACTION_RETRY_COUNT), RETRY_COUNT_DEFAULT);
    }

    int getForceCompactionTimeout() {
        return toInteger(property(COMPACTION_FORCE_TIMEOUT), FORCE_TIMEOUT_DEFAULT);
    }

    int getRetainedGenerations() {
        return toInteger(property(RETAINED_GENERATIONS), RETAINED_GENERATIONS_DEFAULT);
    }

    long getSizeDeltaEstimation() {
        return toLong(property(COMPACTION_SIZE_DELTA_ESTIMATION), SIZE_DELTA_ESTIMATION_DEFAULT);
    }

    int getMemoryThreshold() {
        return toInteger(property(MEMORY_THRESHOLD), MEMORY_THRESHOLD_DEFAULT);
    }

    boolean getDisableEstimation() {
        return toBoolean(property(COMPACTION_DISABLE_ESTIMATION), DISABLE_ESTIMATION_DEFAULT);
    }

    String getCompactionGainThreshold() {
        return property("compaction.gainThreshold");
    }

    long getGCProcessLog() {
        return toLong(property(GC_PROGRESS_LOG), GC_PROGRESS_LOG_DEFAULT);
    }

    int getMaxFileSize() {
        return toInteger(property(SIZE), DEFAULT_MAX_FILE_SIZE);
    }

    String getMode() {
        String mode = property(MODE);
        if (mode != null) {
            return mode;
        }
        return System.getProperty(MODE, System.getProperty("sun.arch.data.model", "32"));
    }

    boolean getMemoryMapping() {
        return getMode().equals("64");
    }

    long getBlobSnapshotInterval() {
        return toLong(property(PROP_BLOB_SNAPSHOT_INTERVAL), DEFAULT_BLOB_SNAPSHOT_INTERVAL);
    }

    boolean isStandbyInstance() {
        return toBoolean(property(STANDBY), false);
    }

    boolean hasCustomBlobStore() {
        return toBoolean(property(CUSTOM_BLOB_STORE), false);
    }

    long getBlobGcMaxAge() {
        return toLong(property(PROP_BLOB_GC_MAX_AGE), DEFAULT_BLOB_GC_MAX_AGE);
    }

    boolean isPrimarySegmentStore() {
        return role == null;
    }

    private String appendRole(String name) {
        if (role == null) {
            return name;
        } else {
            return name + "-" + role;
        }
    }

    private String getCacheSize(String propertyName) {
        String cacheSize = property(propertyName);
        if (cacheSize != null) {
            return cacheSize;
        }
        return System.getProperty(propertyName);
    }

}

/**
 * Performs registrations of services and MBean in a uniform way. Augments
 * the metadata of services and MBeans with an optionally provided role
 * name.
 */
class Registrations {

    private final Whiteboard whiteboard;

    private final String role;

    Registrations(Whiteboard whiteboard, String role) {
        this.whiteboard = whiteboard;
        this.role = role;
    }

    <T> Registration registerMBean(Class<T> clazz, T bean, String type, String name) {
        return registerMBean(clazz, bean, type, name, new HashMap<String, String>());
    }

    <T> Registration registerMBean(Class<T> clazz, T bean, String type, String name, Map<String, String> attributes) {
        return WhiteboardUtils.registerMBean(whiteboard, clazz, bean, type, maybeAppendRole(name), maybePutRoleAttribute(attributes));
    }

    <T> Registration register(Class<T> clazz, T service) {
        return register(clazz, service, new HashMap<String, Object>());
    }

    <T> Registration register(Class<T> clazz, T service, Map<String, Object> properties) {
        return whiteboard.register(clazz, service, maybePutRoleProperty(properties));
    }

    private String maybeAppendRole(String name) {
        if (role != null) {
            return name + " - " + role;
        }
        return name;
    }

    private String jmxRole() {
        return role.replaceAll(":", "-");
    }

    private Map<String, String> maybePutRoleAttribute(Map<String, String> attributes) {
        if (role != null) {
            attributes.put("role", jmxRole());
        }
        return attributes;
    }

    private Map<String, Object> maybePutRoleProperty(Map<String, Object> attributes) {
        if (role != null) {
            attributes.put("role", role);
        }
        return attributes;
    }

}
