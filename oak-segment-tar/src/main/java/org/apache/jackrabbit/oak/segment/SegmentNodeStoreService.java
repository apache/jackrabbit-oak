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

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toLong;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.isShared;
import static org.apache.jackrabbit.oak.plugins.identifier.ClusterRepositoryInfo.getOrCreateId;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_STRING_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_TEMPLATE_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
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
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
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
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.plugins.identifier.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.compaction.SegmentRevisionGC;
import org.apache.jackrabbit.oak.segment.compaction.SegmentRevisionGCMBean;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.FileStoreGCMonitor;
import org.apache.jackrabbit.oak.segment.file.FileStoreStatsMBean;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitorTracker;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.RevisionGC;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.util.GenericDescriptors;
import org.osgi.framework.Constants;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OSGi wrapper for the segment node store.
 */
@Component(policy = ConfigurationPolicy.REQUIRE,
        metatype = true,
        label = "Apache Jackrabbit Oak Segment NodeStore Service",
        description = "NodeStore implementation based on Segment model. For configuration option refer " +
                "to http://jackrabbit.apache.org/oak/docs/osgi_config.html#SegmentNodeStore. Note that for system " +
                "stability purpose it is advisable to not change these settings at runtime. Instead the config change " +
                "should be done via file system based config file and this view should ONLY be used to determine which " +
                "options are supported"
)
public class SegmentNodeStoreService {

    private static final Logger log = LoggerFactory.getLogger(SegmentNodeStoreService.class);

    @Property(
            label = "Directory",
            description="Directory location used to store the segment tar files. If not specified then looks " +
                    "for framework property 'repository.home' otherwise use a subdirectory with name 'tarmk'"
    )
    public static final String DIRECTORY = "repository.home";

    @Property(
            label = "Mode",
            description="TarMK mode (64 for memory mapping, 32 for normal file access)"
    )
    public static final String MODE = "tarmk.mode";

    @Property(
            intValue = DEFAULT_MAX_FILE_SIZE,
            label = "Maximum Tar File Size (MB)",
            description = "TarMK maximum file size (MB)"
    )
    public static final String SIZE = "tarmk.size";

    @Property(
            intValue = DEFAULT_SEGMENT_CACHE_MB,
            label = "Segment cache size (MB)",
            description = "Cache size for storing most recently used segments"
    )
    public static final String SEGMENT_CACHE_SIZE = "segmentCache.size";

    @Property(
            intValue = DEFAULT_STRING_CACHE_MB,
            label = "String cache size (MB)",
            description = "Cache size for storing most recently used strings"
    )
    public static final String STRING_CACHE_SIZE = "stringCache.size";

    @Property(
            intValue = DEFAULT_TEMPLATE_CACHE_MB,
            label = "Template cache size (MB)",
            description = "Cache size for storing most recently used templates"
    )
    public static final String TEMPLATE_CACHE_SIZE = "templateCache.size";

    @Property(
            intValue = DEFAULT_STRING_CACHE_SIZE_OSGi,
            label = "String deduplication cache size (#items)",
            description = "Maximum number of strings to keep in the deduplication cache"
    )
    public static final String STRING_DEDUPLICATION_CACHE_SIZE = "stringDeduplicationCache.size";

    @Property(
            intValue = DEFAULT_TEMPLATE_CACHE_SIZE_OSGi,
            label = "Template deduplication cache size (#items)",
            description = "Maximum number of templates to keep in the deduplication cache"
    )
    public static final String TEMPLATE_DEDUPLICATION_CACHE_SIZE = "templateDeduplicationCache.size";

    @Property(
            intValue = DEFAULT_NODE_CACHE_SIZE_OSGi,
            label = "Node deduplication cache size (#items)",
            description = "Maximum number of node to keep in the deduplication cache. If the supplied" +
                    " value is not a power of 2, it will be rounded up to the next power of 2."
    )
    public static final String NODE_DEDUPLICATION_CACHE_SIZE = "nodeDeduplicationCache.size";

    @Property(
            boolValue = PAUSE_DEFAULT,
            label = "Pause Compaction",
            description = "When enabled compaction would not be performed"
    )
    public static final String PAUSE_COMPACTION = "pauseCompaction";

    @Property(
            intValue = RETRY_COUNT_DEFAULT,
            label = "Compaction Retries",
            description = "Number of tries to compact concurrent commits on top of already " +
                    "compacted commits"
    )
    public static final String COMPACTION_RETRY_COUNT = "compaction.retryCount";

    @Property(
            intValue = FORCE_TIMEOUT_DEFAULT,
            label = "Force Compaction Timeout",
            description = "Number of seconds to attempt to force compact concurrent commits on top " +
                    "of already compacted commits after the maximum number of retries has been " +
                    "reached. Forced compaction tries to acquire an exclusive write lock on the " +
                    "node store."
    )
    public static final String COMPACTION_FORCE_TIMEOUT = "compaction.force.timeout";

    @Property(
            longValue = SIZE_DELTA_ESTIMATION_DEFAULT,
            label = "Compaction Repository Size Delta",
            description = "Amount of increase in repository size that will trigger compaction (bytes)"
    )
    public static final String COMPACTION_SIZE_DELTA_ESTIMATION = "compaction.sizeDeltaEstimation";

    @Property(
            boolValue = DISABLE_ESTIMATION_DEFAULT,
            label = "Disable Compaction Estimation Phase",
            description = "Disables compaction estimation phase, thus allowing compaction to run every time."
    )
    public static final String COMPACTION_DISABLE_ESTIMATION = "compaction.disableEstimation";

    @Property(
            intValue = RETAINED_GENERATIONS_DEFAULT,
            label = "Compaction retained generations",
            description = "Number of segment generations to retain."
    )
    public static final String RETAINED_GENERATIONS = "compaction.retainedGenerations";

    @Property(
            intValue = MEMORY_THRESHOLD_DEFAULT,
            label = "Compaction Memory Threshold",
            description = "Set the available memory threshold beyond which revision gc will be canceled. "
                    + "Value represents a percentage so an input between 0 and 100 is expected. "
                    + "Setting this to 0 will disable the check."
    )
    public static final String MEMORY_THRESHOLD = "compaction.memoryThreshold";

    @Property(
            longValue = GC_PROGRESS_LOG_DEFAULT,
            label = "Compaction Progress Log",
            description = "Enables compaction progress logging at each set of compacted nodes. A value of -1 disables the log."
    )
    public static final String GC_PROGRESS_LOG = "compaction.progressLog";

    @Property(
            boolValue = false,
            label = "Standby Mode",
            description = "Flag indicating that this component will not register as a NodeStore but just as a NodeStoreProvider"
    )
    public static final String STANDBY = "standby";

    @Property(boolValue = false,
            label = "Custom BlobStore",
            description = "Boolean value indicating that a custom BlobStore is to be used. " +
                    "By default large binary content would be stored within segment tar files"
    )
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    @Property(
            label = "Backup Directory",
            description="Directory location for storing repository backups. If not set, defaults to" +
                    " 'segmentstore-backup' subdirectory under 'repository.home'."
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

    private Closer registrations = Closer.create();

    /**
     * Blob modified before this time duration would be considered for Blob GC
     */
    private static final long DEFAULT_BLOB_GC_MAX_AGE = 24 * 60 * 60;

    @Property (longValue = DEFAULT_BLOB_GC_MAX_AGE,
            label = "Blob GC Max Age (in secs)",
            description = "Blob Garbage Collector (GC) logic will only consider those blobs for GC which " +
                    "are not accessed recently (currentTime - lastModifiedTime > blobGcMaxAgeInSecs). For " +
                    "example as per default only those blobs which have been created 24 hrs ago will be " +
                    "considered for GC"
    )
    public static final String PROP_BLOB_GC_MAX_AGE = "blobGcMaxAgeInSecs";

    /**
     * Default interval for taking snapshots of locally tracked blob ids.
     */
    private static final long DEFAULT_BLOB_SNAPSHOT_INTERVAL = 12 * 60 * 60;

    @Property (longValue = DEFAULT_BLOB_SNAPSHOT_INTERVAL,
            label = "Blob tracking snapshot interval (in secs)",
            description = "This is the default interval in which the snapshots of locally tracked blob ids will"
                    + "be taken and synchronized with the blob store. This should be configured to be less than the "
                    + "frequency of blob GC so that deletions during blob GC can be accounted for "
                    + "in the next GC execution."
    )
    public static final String PROP_BLOB_SNAPSHOT_INTERVAL = "blobTrackSnapshotIntervalInSecs";

    @Activate
    public void activate(ComponentContext context) throws IOException {
        if (blobStore == null && hasCustomBlobStore(context)) {
            log.info("BlobStore use enabled. SegmentNodeStore would be initialized when BlobStore would be available");
            return;
        }
        registrations = Closer.create();
        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        registerSegmentStore(context, blobStore, statisticsProvider, registrations, whiteboard, null, true);
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
     * @param registrations      An instance of {@link Closer}. It will be used
     *                           to track every registered service or
     *                           component.
     * @param whiteboard         An instance of {@link OsgiWhiteboard}. It will
     *                           be used to register services in the OSGi
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
            @Nonnull Closer registrations,
            @Nonnull OsgiWhiteboard whiteboard,
            @Nullable String role,
            boolean descriptors
    ) throws IOException {
        // Listen for GCMonitor services
        GCMonitorTracker gcMonitor = new GCMonitorTracker();
        gcMonitor.start(whiteboard);

        // Create the gc options
        SegmentGCOptions gcOptions = newGCOptions(context);

        // Build the FileStore
        FileStoreBuilder builder = fileStoreBuilder(getDirectory(context, role))
                .withSegmentCacheSize(getSegmentCacheSize(context))
                .withStringCacheSize(getStringCacheSize(context))
                .withTemplateCacheSize(getTemplateCacheSize(context))
                .withStringDeduplicationCacheSize(getStringDeduplicationCacheSize(context))
                .withTemplateDeduplicationCacheSize(getTemplateDeduplicationCacheSize(context))
                .withNodeDeduplicationCacheSize(getNodeDeduplicationCacheSize(context))
                .withMaxFileSize(getMaxFileSize(context))
                .withMemoryMapping(getMode(context).equals("64"))
                .withGCMonitor(gcMonitor)
                .withStatisticsProvider(statisticsProvider)
                .withGCOptions(gcOptions);

        if (hasCustomBlobStore(context) && blobStore != null) {
            log.info("Initializing SegmentNodeStore with BlobStore [{}]", blobStore);
            builder.withBlobStore(blobStore);
        }

        if (!isStandbyInstance(context)) {
            builder.withSnfeListener(IGNORE_SNFE);
        }

        final FileStore store;
        try {
            store = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            log.error("The segment store data is not compatible with the current version. Please use oak-segment or a different version of oak-segment-tar.");
            return null;
        }
        // store should be closed last
        registrations.register(store);
        registrations.register(asCloseable(gcMonitor));

        // Listen for Executor services on the whiteboard

        WhiteboardExecutor executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        registrations.register(asCloseable(executor));

        List<Registration> mbeans = Lists.newArrayList();

        // Expose stats about the segment cache

        CacheStatsMBean segmentCacheStats = store.getSegmentCacheStats();
        mbeans.add(registerMBean(
                whiteboard,
                CacheStatsMBean.class,
                segmentCacheStats,
                CacheStats.TYPE,
                appendRole(segmentCacheStats.getName(), role),
                withRole(properties(), role)
        ));

        // Expose stats about the string and template caches

        CacheStatsMBean stringCacheStats = store.getStringCacheStats();
        mbeans.add(registerMBean(
                whiteboard,
                CacheStatsMBean.class,
                stringCacheStats, CacheStats.TYPE,
                appendRole(stringCacheStats.getName(), role),
                withRole(properties(), role)
        ));

        CacheStatsMBean templateCacheStats = store.getTemplateCacheStats();
        mbeans.add(registerMBean(
                whiteboard,
                CacheStatsMBean.class,
                templateCacheStats, CacheStats.TYPE,
                appendRole(templateCacheStats.getName(), role),
                withRole(properties(), role)
        ));

        CacheStatsMBean stringDeduplicationCacheStats = store.getStringDeduplicationCacheStats();
        if (stringDeduplicationCacheStats != null) {
            mbeans.add(registerMBean(
                    whiteboard,
                    CacheStatsMBean.class,
                    stringDeduplicationCacheStats, CacheStats.TYPE,
                    appendRole(stringDeduplicationCacheStats.getName(), role),
                    withRole(properties(), role)
            ));
        }

        CacheStatsMBean templateDeduplicationCacheStats = store.getTemplateDeduplicationCacheStats();
        if (templateDeduplicationCacheStats != null) {
            mbeans.add(registerMBean(
                    whiteboard,
                    CacheStatsMBean.class,
                    templateDeduplicationCacheStats, CacheStats.TYPE,
                    appendRole(templateDeduplicationCacheStats.getName(), role),
                    withRole(properties(), role)
            ));
        }

        CacheStatsMBean nodeDeduplicationCacheStats = store.getNodeDeduplicationCacheStats();
        if (nodeDeduplicationCacheStats != null) {
            mbeans.add(registerMBean(
                    whiteboard,
                    CacheStatsMBean.class,
                    nodeDeduplicationCacheStats, CacheStats.TYPE,
                    appendRole(nodeDeduplicationCacheStats.getName(), role),
                    withRole(properties(), role)
            ));
        }

        // Expose an MBean to managing and monitoring garbage collection

        final FileStoreGCMonitor fsgcm = new FileStoreGCMonitor(Clock.SIMPLE);
        mbeans.add(new CompositeRegistration(
                whiteboard.register(GCMonitor.class, fsgcm, emptyMap()),
                registerMBean(
                        whiteboard,
                        SegmentRevisionGC.class,
                        new SegmentRevisionGCMBean(store, gcOptions, fsgcm),
                        SegmentRevisionGC.TYPE,
                        appendRole("Segment node store revision garbage collection", role),
                        withRole(properties(), role)
                )));

        Runnable cancelGC = new Runnable() {

            @Override
            public void run() {
                store.cancelGC();
            }
        };
        Supplier<String> statusMessage = new Supplier<String>() {

            @Override
            public String get() {
                return fsgcm.getStatus();
            }
        };
        mbeans.add(registerMBean(
                whiteboard,
                RevisionGCMBean.class,
                new RevisionGC(store.getGCRunner(), cancelGC, statusMessage, executor),
                RevisionGCMBean.TYPE,
                appendRole("Revision garbage collection", role),
                withRole(properties(), role)
        ));

        // Expose statistics about the FileStore

        mbeans.add(registerMBean(
                whiteboard,
                FileStoreStatsMBean.class,
                store.getStats(),
                FileStoreStatsMBean.TYPE,
                appendRole("FileStore statistics", role),
                withRole(properties(), role)
        ));

        // register segment node store

        SegmentNodeStore.SegmentNodeStoreBuilder segmentNodeStoreBuilder = SegmentNodeStoreBuilders.builder(store)
                        .withStatisticsProvider(statisticsProvider);
        if (isStandbyInstance(context) || !isPrimarySegmentStore(role)) {
            segmentNodeStoreBuilder.dispatchChanges(false);
        }
        SegmentNodeStore segmentNodeStore = segmentNodeStoreBuilder.build();

        if (isPrimarySegmentStore(role)) {
            ObserverTracker observerTracker = new ObserverTracker(segmentNodeStore);
            observerTracker.start(context.getBundleContext());
            registrations.register(asCloseable(observerTracker));
        }

        if (isPrimarySegmentStore(role)) {
            mbeans.add(registerMBean(
                    whiteboard,
                    CheckpointMBean.class,
                    new SegmentCheckpointMBean(segmentNodeStore), CheckpointMBean.TYPE,
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
            mbeans.add(whiteboard.register(
                    Descriptors.class,
                    clusterIdDesc,
                    withRole(properties(), role)
            ));

            // Register "discovery lite" descriptors
            mbeans.add(whiteboard.register(
                    Descriptors.class,
                    new SegmentDiscoveryLiteDescriptors(segmentNodeStore),
                    withRole(properties(), role)
            ));
        }

        // If a shared data store register the repo id in the data store
        if (isPrimarySegmentStore(role) && isShared(blobStore)) {
            SharedDataStore sharedDataStore = (SharedDataStore) blobStore;
            try {
                sharedDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]), SharedStoreRecordType.REPOSITORY.getNameFromId(getOrCreateId(segmentNodeStore)));
            } catch (Exception e) {
                throw new IOException("Could not register a unique repositoryId", e);
            }
            if (blobStore instanceof BlobTrackingStore) {
                long trackSnapshotInterval = toLong(property(PROP_BLOB_SNAPSHOT_INTERVAL, context), DEFAULT_BLOB_SNAPSHOT_INTERVAL);
                String root = property(DIRECTORY, context);
                if (Strings.isNullOrEmpty(root)) {
                    root = "repository";
                }
                BlobTrackingStore trackingStore = (BlobTrackingStore) blobStore;
                if (trackingStore.getTracker() != null) {
                    trackingStore.getTracker().close();
                }
                trackingStore.addTracker(new BlobIdTracker(root, getOrCreateId(segmentNodeStore), trackSnapshotInterval, sharedDataStore));
            }
        }

        if (isPrimarySegmentStore(role) && isGarbageCollectable(blobStore)) {
            BlobGarbageCollector gc = new MarkSweepGarbageCollector(
                    new SegmentBlobReferenceRetriever(store),
                    (GarbageCollectableBlobStore) blobStore,
                    executor,
                    TimeUnit.SECONDS.toMillis(getBlobGcMaxAge(context)),
                    getOrCreateId(segmentNodeStore)
            );
            mbeans.add(registerMBean(
                    whiteboard,
                    BlobGCMBean.class,
                    new BlobGC(gc, executor),
                    BlobGCMBean.TYPE,
                    "Segment node store blob garbage collection"
            ));
        }

        // Expose an MBean for backup/restore operations

        mbeans.add(registerMBean(
                whiteboard,
                FileStoreBackupRestoreMBean.class,
                new FileStoreBackupRestoreImpl(
                        segmentNodeStore,
                        store.getRevisions(),
                        store.getReader(),
                        getBackupDirectory(context, role),
                        executor
                ),
                FileStoreBackupRestoreMBean.TYPE,
                appendRole("Segment node store backup/restore", role),
                withRole(properties(), role)
        ));

        // Expose statistics about the SegmentNodeStore

        mbeans.add(registerMBean(
                whiteboard,
                SegmentNodeStoreStatsMBean.class,
                segmentNodeStore.getStats(),
                SegmentNodeStoreStatsMBean.TYPE,
                appendRole("SegmentNodeStore statistics", role),
                withRole(properties(), role)
        ));

        if (isPrimarySegmentStore(role)) {
            log.info("Primary SegmentNodeStore initialized");
        } else {
            log.info("Secondary SegmentNodeStore initialized, role={}", role);
        }

        // Register a factory service to expose the FileStore
        registrations.register(asCloseable(whiteboard.register(
                SegmentStoreProvider.class,
                new DefaultSegmentStoreProvider(store),
                withRole(properties(), role)
        )));

        registrations.register(asCloseable(new CompositeRegistration(mbeans)));

        if (isStandbyInstance(context)) {
            return segmentNodeStore;
        }

        if (isPrimarySegmentStore(role)) {
            Map<String, Object> props = new HashMap<String, Object>();
            props.put(Constants.SERVICE_PID, SegmentNodeStore.class.getName());
            props.put("oak.nodestore.description", new String[] {"nodeStoreType=segment"});
            registrations.register(asCloseable(whiteboard.register(NodeStore.class, segmentNodeStore, props)));
        }

        return segmentNodeStore;
    }

    @Deactivate
    public void deactivate() {
        if (registrations != null) {
            IOUtils.closeQuietly(registrations);
            registrations = null;
        }
    }

    private static Map<String, String> properties() {
        return new HashMap<>();
    }

    private static Map<String, String> withRole(Map<String, String> map, String role) {
        if (role != null) {
            map.put("role", role);
        }
        return map;
    }

    private static boolean isGarbageCollectable(BlobStore store) {
        return store instanceof GarbageCollectableBlobStore;
    }

    private static SegmentGCOptions newGCOptions(ComponentContext context) {
        boolean pauseCompaction = toBoolean(property(PAUSE_COMPACTION, context), PAUSE_DEFAULT);
        int retryCount = toInteger(property(COMPACTION_RETRY_COUNT, context), RETRY_COUNT_DEFAULT);
        int forceTimeout = toInteger(property(COMPACTION_FORCE_TIMEOUT, context), FORCE_TIMEOUT_DEFAULT);
        int retainedGenerations = toInteger(property(RETAINED_GENERATIONS, context), RETAINED_GENERATIONS_DEFAULT);

        long sizeDeltaEstimation = toLong(property(COMPACTION_SIZE_DELTA_ESTIMATION, context), SIZE_DELTA_ESTIMATION_DEFAULT);
        int memoryThreshold = toInteger(property(MEMORY_THRESHOLD, context), MEMORY_THRESHOLD_DEFAULT);
        boolean disableEstimation = toBoolean(property(COMPACTION_DISABLE_ESTIMATION, context), DISABLE_ESTIMATION_DEFAULT);

        if (property("compaction.gainThreshold", context) != null) {
            log.warn("Deprecated property compaction.gainThreshold was detected. In order to configure compaction please use the new property "
                    + "compaction.sizeDeltaEstimation. For turning off estimation, the new property compaction.disableEstimation should be used.");
        }
        long gcProgressLog = toLong(property(GC_PROGRESS_LOG, context), GC_PROGRESS_LOG_DEFAULT);

        return new SegmentGCOptions(pauseCompaction, retryCount, forceTimeout)
                .setRetainedGenerations(retainedGenerations)
                .setGcSizeDeltaEstimation(sizeDeltaEstimation)
                .setMemoryThreshold(memoryThreshold)
                .setEstimationDisabled(disableEstimation)
                .withGCNodeWriteMonitor(gcProgressLog);
    }

    private static boolean isStandbyInstance(ComponentContext context) {
        return Boolean.parseBoolean(property(STANDBY, context));
    }

    private static boolean hasCustomBlobStore(ComponentContext context) {
        return Boolean.parseBoolean(property(CUSTOM_BLOB_STORE, context));
    }

    private static File getBaseDirectory(ComponentContext context) {
        String directory = property(DIRECTORY, context);

        if (directory != null) {
            return new File(directory);
        }

        return new File("tarmk");
    }

    private static File getDirectory(ComponentContext context, String role) {
        return new File(getBaseDirectory(context), appendRole("segmentstore", role));
    }

    private static File getBackupDirectory(ComponentContext context, String role) {
        String backupDirectory = property(BACKUP_DIRECTORY, context);
        if (backupDirectory != null) {
            return new File(backupDirectory);
        }
        return new File(getBaseDirectory(context), appendRole("segmentstore-backup", role));
    }

    private static String getMode(ComponentContext context) {
        String mode = property(MODE, context);

        if (mode != null) {
            return mode;
        }

        return System.getProperty(MODE, System.getProperty("sun.arch.data.model", "32"));
    }

    private static String getCacheSize(String propertyName, ComponentContext context) {
        String cacheSize = property(propertyName, context);

        if (cacheSize != null) {
            return cacheSize;
        }

        return System.getProperty(propertyName);
    }

    private static long getBlobGcMaxAge(ComponentContext context) {
        return toLong(property(PROP_BLOB_GC_MAX_AGE, context), DEFAULT_BLOB_GC_MAX_AGE);
    }

    private static int getSegmentCacheSize(ComponentContext context) {
        return toInteger(getCacheSize(SEGMENT_CACHE_SIZE, context), DEFAULT_SEGMENT_CACHE_MB);
    }

    private static int getStringCacheSize(ComponentContext context) {
        return toInteger(getCacheSize(STRING_CACHE_SIZE, context), DEFAULT_STRING_CACHE_MB);
    }

    private static int getTemplateCacheSize(ComponentContext context) {
        return toInteger(getCacheSize(TEMPLATE_CACHE_SIZE, context), DEFAULT_TEMPLATE_CACHE_MB);
    }

    private static int getStringDeduplicationCacheSize(ComponentContext context) {
        return toInteger(getCacheSize(STRING_DEDUPLICATION_CACHE_SIZE, context), DEFAULT_STRING_CACHE_SIZE_OSGi);
    }

    private static int getTemplateDeduplicationCacheSize(ComponentContext context) {
        return toInteger(getCacheSize(TEMPLATE_DEDUPLICATION_CACHE_SIZE, context), DEFAULT_TEMPLATE_CACHE_SIZE_OSGi);
    }

    private static int getNodeDeduplicationCacheSize(ComponentContext context) {
        // Round to the next power of 2
        int size = Math.max(1,
                toInteger(getCacheSize(NODE_DEDUPLICATION_CACHE_SIZE, context), DEFAULT_NODE_CACHE_SIZE_OSGi));
        return 1 << (32 - Integer.numberOfLeadingZeros(size - 1));
    }

    private static int getMaxFileSize(ComponentContext context) {
        return toInteger(property(SIZE, context), DEFAULT_MAX_FILE_SIZE);
    }

    static String property(String name, ComponentContext context) {
        return lookupConfigurationThenFramework(context, name);
    }

    private static String appendRole(@Nonnull String name, @Nullable String role) {
        if (role == null) {
            return name;
        } else {
            return name + "-" + role;
        }
    }

    static Closeable asCloseable(final Registration r) {
        return new Closeable() {

            @Override
            public void close() {
                r.unregister();
            }

        };
    }

    private static Closeable asCloseable(final AbstractServiceTracker<?> t) {
        return new Closeable() {

            @Override
            public void close() {
                t.stop();
            }

        };
    }

    private static Closeable asCloseable(final ObserverTracker t) {
        return new Closeable() {

            @Override
            public void close() {
                t.stop();
            }

        };
    }

    private static boolean isPrimarySegmentStore(String role) {
        return role == null;
    }

}
