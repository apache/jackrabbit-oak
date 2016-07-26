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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toLong;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.FORCE_AFTER_FAIL_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GAIN_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.MEMORY_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.PAUSE_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.RETRY_COUNT_DEFAULT;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.ONLY_STANDALONE_TARGET;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.plugins.identifier.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.compaction.SegmentRevisionGC;
import org.apache.jackrabbit.oak.segment.compaction.SegmentRevisionGCMBean;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.FileStoreGCMonitor;
import org.apache.jackrabbit.oak.segment.file.FileStoreStatsMBean;
import org.apache.jackrabbit.oak.segment.file.GCMonitorMBean;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitorTracker;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;
import org.apache.jackrabbit.oak.spi.state.RevisionGC;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.util.GenericDescriptors;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
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
public class SegmentNodeStoreService extends ProxyNodeStore
        implements Observable, SegmentStoreProvider {

    public static final String NAME = "name";

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
            intValue = 256,
            label = "Maximum Tar File Size (MB)",
            description = "TarMK maximum file size (MB)"
    )
    public static final String SIZE = "tarmk.size";

    @Property(
            intValue = 256,
            label = "Segment cache size (MB)",
            description = "Cache size for storing most recently used segments"
    )
    public static final String SEGMENT_CACHE_SIZE = "segmentCache.size";

    @Property(
            intValue = 256,
            label = "String cache size (MB)",
            description = "Cache size for storing most recently used strings"
    )
    public static final String STRING_CACHE_SIZE = "stringCache.size";

    @Property(
            intValue = 64,
            label = "Template cache size (MB)",
            description = "Cache size for storing most recently used templates"
    )
    public static final String TEMPLATE_CACHE_SIZE = "templateCache.size";

    @Property(
            byteValue = MEMORY_THRESHOLD_DEFAULT,
            label = "Memory Multiplier",
            description = "TarMK compaction available memory multiplier needed to run compaction"
    )
    public static final String COMPACTION_MEMORY_THRESHOLD = "compaction.memoryThreshold";

    @Property(
            byteValue = GAIN_THRESHOLD_DEFAULT,
            label = "Compaction gain threshold",
            description = "TarMK compaction gain threshold. The gain estimation prevents compaction from running " +
                    "if the provided threshold is not met. Value represents a percentage so an input between 0 and 100 is expected."
    )
    public static final String COMPACTION_GAIN_THRESHOLD = "compaction.gainThreshold";

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
            boolValue = FORCE_AFTER_FAIL_DEFAULT,
            label = "Force Compaction",
            description = "Whether or not to force compact concurrent commits on top of already " +
                    " compacted commits after the maximum number of retries has been reached. " +
                    "Force committing tries to exclusively write lock the node store."
    )
    public static final String COMPACTION_FORCE_AFTER_FAIL = "compaction.forceAfterFail";

    public static final int COMPACTION_LOCK_WAIT_TIME_DEFAULT = 60;

    @Property(
            intValue = COMPACTION_LOCK_WAIT_TIME_DEFAULT,
            label = "Compaction Lock Wait Time",
            description = "Number of seconds to wait for the lock for committing compacted changes " +
                    "respectively to wait for the exclusive write lock for force committing."
    )
    public static final String COMPACTION_LOCK_WAIT_TIME = "compaction.lockWaitTime";

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

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String name;

    private FileStore store;

    private volatile SegmentNodeStore segmentNodeStore;

    private ObserverTracker observerTracker;

    private GCMonitorTracker gcMonitor;

    private ComponentContext context;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC, target = ONLY_STANDALONE_TARGET)
    private volatile BlobStore blobStore;

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    private ServiceRegistration storeRegistration;
    private ServiceRegistration providerRegistration;

    private final List<Registration> registrations = new ArrayList<>();
    private WhiteboardExecutor executor;
    private boolean customBlobStore;

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

    @Override
    protected SegmentNodeStore getNodeStore() {
        checkState(segmentNodeStore != null, "service must be activated when used");
        return segmentNodeStore;
    }

    @Activate
    public void activate(ComponentContext context) throws IOException {
        this.context = context;
        this.customBlobStore = Boolean.parseBoolean(property(CUSTOM_BLOB_STORE));

        if (blobStore == null && customBlobStore) {
            log.info("BlobStore use enabled. SegmentNodeStore would be initialized when BlobStore would be available");
        } else {
            registerNodeStore();
        }
    }

    protected void bindBlobStore(BlobStore blobStore) throws IOException {
        this.blobStore = blobStore;
        registerNodeStore();
    }

    protected void unbindBlobStore(BlobStore blobStore){
        this.blobStore = null;
        unregisterNodeStore();
    }

    @Deactivate
    public void deactivate() {
        unregisterNodeStore();

        synchronized (this) {
            if (observerTracker != null) {
                observerTracker.stop();
            }
            if (gcMonitor != null) {
                gcMonitor.stop();
            }
            segmentNodeStore = null;

            if (store != null) {
                store.close();
                store = null;
            }
        }
    }

    private synchronized void registerNodeStore() throws IOException {
        if (registerSegmentStore()) {
            if (toBoolean(property(STANDBY), false)) {
                return;
            }

            if (registerSegmentNodeStore()) {
                Dictionary<String, Object> props = new Hashtable<String, Object>();
                props.put(Constants.SERVICE_PID, SegmentNodeStore.class.getName());
                props.put("oak.nodestore.description", new String[]{"nodeStoreType=segment"});
                storeRegistration = context.getBundleContext().registerService(NodeStore.class.getName(), this, props);
            }
        }
    }

    private boolean registerSegmentStore() throws IOException {
        if (context == null) {
            log.info("Component still not activated. Ignoring the initialization call");
            return false;
        }

        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());

        // Listen for GCMonitor services

        gcMonitor = new GCMonitorTracker();
        gcMonitor.start(whiteboard);

        // Create the gc options
        SegmentGCOptions gcOptions = newGCOptions();

        // Build the FileStore
        FileStoreBuilder builder = fileStoreBuilder(getDirectory())
                .withSegmentCacheSize(getSegmentCacheSize())
                .withStringCacheSize(getStringCacheSize())
                .withTemplateCacheSize(getTemplateCacheSize())
                .withMaxFileSize(getMaxFileSize())
                .withMemoryMapping(getMode().equals("64"))
                .withGCMonitor(gcMonitor)
                .withStatisticsProvider(statisticsProvider)
                .withGCOptions(gcOptions);

        if (customBlobStore) {
            log.info("Initializing SegmentNodeStore with BlobStore [{}]", blobStore);
            builder.withBlobStore(blobStore);
        }

        store = builder.build();

        // Expose an MBean to provide information about the gc options

        registrations.add(registerMBean(
                whiteboard,
                SegmentRevisionGC.class,
                new SegmentRevisionGCMBean(gcOptions),
                SegmentRevisionGC.TYPE,
                "Segment node store gc options"
        ));

        // Expose stats about the segment cache

        CacheStatsMBean segmentCacheStats = store.getSegmentCacheStats();
        registrations.add(registerMBean(
                whiteboard,
                CacheStatsMBean.class,
                segmentCacheStats,
                CacheStats.TYPE,
                segmentCacheStats.getName()
        ));

        // Expose stats about the string and template caches

        CacheStatsMBean stringCacheStats = store.getStringCacheStats();
        registrations.add(registerMBean(
                whiteboard,
                CacheStatsMBean.class,
                stringCacheStats,CacheStats.TYPE,
                stringCacheStats.getName()
        ));

        CacheStatsMBean templateCacheStats = store.getTemplateCacheStats();
        registrations.add(registerMBean(
                whiteboard,
                CacheStatsMBean.class,
                templateCacheStats,CacheStats.TYPE,
                templateCacheStats.getName()
        ));

        CacheStatsMBean stringDeduplicationCacheStats = store.getStringDeduplicationCacheStats();
        if (stringDeduplicationCacheStats != null) {
            registrations.add(registerMBean(
            whiteboard,
            CacheStatsMBean.class,
            stringDeduplicationCacheStats,CacheStats.TYPE,
            stringDeduplicationCacheStats.getName()
            ));
        }

        CacheStatsMBean templateDeduplicationCacheStats = store.getTemplateDeduplicationCacheStats();
        if (templateDeduplicationCacheStats != null) {
            registrations.add(registerMBean(
            whiteboard,
            CacheStatsMBean.class,
            templateDeduplicationCacheStats,CacheStats.TYPE,
            templateDeduplicationCacheStats.getName()
            ));
        }

        // Listen for Executor services on the whiteboard

        executor = new WhiteboardExecutor();
        executor.start(whiteboard);

        // Expose an MBean to trigger garbage collection

        Runnable triggerGarbageCollection = new Runnable() {

            @Override
            public void run() {
                store.gc();
            }

        };

        registrations.add(registerMBean(
                whiteboard,
                RevisionGCMBean.class,
                new RevisionGC(triggerGarbageCollection, executor),
                RevisionGCMBean.TYPE,
                "Segment node store revision garbage collection"
        ));

        // Expose statistics about the FileStore

        registrations.add(registerMBean(
                whiteboard,
                FileStoreStatsMBean.class,
                store.getStats(),
                FileStoreStatsMBean.TYPE,
                "FileStore statistics"
        ));

        // Register a monitor for the garbage collection of the FileStore

        FileStoreGCMonitor fsgcm = new FileStoreGCMonitor(Clock.SIMPLE);

        registrations.add((new CompositeRegistration(
                whiteboard.register(GCMonitor.class, fsgcm, emptyMap()),
                registerMBean(
                        whiteboard,
                        GCMonitorMBean.class,
                        fsgcm,
                        GCMonitorMBean.TYPE,
                        "File Store garbage collection monitor"
                ),
                scheduleWithFixedDelay(whiteboard, fsgcm, 1)
        )));

        // Register a factory service to expose the FileStore

        providerRegistration = context.getBundleContext().registerService(SegmentStoreProvider.class.getName(), this, null);

        return true;
    }

    private SegmentGCOptions newGCOptions() {
        boolean pauseCompaction = toBoolean(property(PAUSE_COMPACTION), PAUSE_DEFAULT);
        int retryCount = toInteger(property(COMPACTION_RETRY_COUNT), RETRY_COUNT_DEFAULT);
        boolean forceAfterFail = toBoolean(property(COMPACTION_FORCE_AFTER_FAIL), FORCE_AFTER_FAIL_DEFAULT);
        final int lockWaitTime = toInteger(property(COMPACTION_LOCK_WAIT_TIME), COMPACTION_LOCK_WAIT_TIME_DEFAULT);

        byte memoryThreshold = getMemoryThreshold();
        byte gainThreshold = getGainThreshold();

        SegmentGCOptions segmentGCOptions = new SegmentGCOptions(
                pauseCompaction, memoryThreshold, gainThreshold, retryCount, forceAfterFail, lockWaitTime);
        segmentGCOptions.setForceAfterFail(forceAfterFail);
        return segmentGCOptions;
    }

    private boolean registerSegmentNodeStore() throws IOException {
        Dictionary<?, ?> properties = context.getProperties();
        name = String.valueOf(properties.get(NAME));

        final long blobGcMaxAgeInSecs = toLong(property(PROP_BLOB_GC_MAX_AGE), DEFAULT_BLOB_GC_MAX_AGE);

        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());

        segmentNodeStore = SegmentNodeStoreBuilders.builder(store).build();

        observerTracker = new ObserverTracker(segmentNodeStore);
        observerTracker.start(context.getBundleContext());

        registrations.add(registerMBean(whiteboard, CheckpointMBean.class, new SegmentCheckpointMBean(segmentNodeStore),
                CheckpointMBean.TYPE, "Segment node store checkpoint management"));

        // ensure a clusterId is initialized
        // and expose it as 'oak.clusterid' repository descriptor
        GenericDescriptors clusterIdDesc = new GenericDescriptors();
        clusterIdDesc.put(ClusterRepositoryInfo.OAK_CLUSTERID_REPOSITORY_DESCRIPTOR_KEY,
                new SimpleValueFactory().createValue(
                        ClusterRepositoryInfo.getOrCreateId(segmentNodeStore)), true, false);
        registrations.add(whiteboard.register(
                Descriptors.class,
                clusterIdDesc,
                Collections.emptyMap()
        ));

        // Register "discovery lite" descriptors
        registrations.add(whiteboard.register(
                Descriptors.class,
                new SegmentDiscoveryLiteDescriptors(segmentNodeStore),
                Collections.emptyMap()
        ));

        // If a shared data store register the repo id in the data store
        String repoId = "";
        if (SharedDataStoreUtils.isShared(blobStore)) {
            try {
                repoId = ClusterRepositoryInfo.getOrCreateId(segmentNodeStore);
                ((SharedDataStore) blobStore).addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                        SharedStoreRecordType.REPOSITORY.getNameFromId(repoId));
            } catch (Exception e) {
                throw new IOException("Could not register a unique repositoryId", e);
            }
        }

        if (store.getBlobStore() instanceof GarbageCollectableBlobStore) {
            BlobGarbageCollector gc = new MarkSweepGarbageCollector(
                    new SegmentBlobReferenceRetriever(store),
                    (GarbageCollectableBlobStore) store.getBlobStore(),
                    executor,
                    TimeUnit.SECONDS.toMillis(blobGcMaxAgeInSecs),
                    repoId
            );

            registrations.add(registerMBean(
                    whiteboard,
                    BlobGCMBean.class,
                    new BlobGC(gc, executor),
                    BlobGCMBean.TYPE,
                    "Segment node store blob garbage collection"
            ));
        }

        log.info("SegmentNodeStore initialized");
        return true;
    }

    private void unregisterNodeStore() {
        new CompositeRegistration(registrations).unregister();
        if (providerRegistration != null) {
            providerRegistration.unregister();
            providerRegistration = null;
        }
        if (storeRegistration != null) {
            storeRegistration.unregister();
            storeRegistration = null;
        }
        if (executor != null) {
            executor.stop();
            executor = null;
        }
    }

    private File getBaseDirectory() {
        String directory = property(DIRECTORY);

        if (directory != null) {
            return new File(directory);
        }

        return new File("tarmk");
    }

    private File getDirectory() {
        return new File(getBaseDirectory(), "segmentstore");
    }

    private String getMode() {
        String mode = property(MODE);

        if (mode != null) {
            return mode;
        }

        return System.getProperty(MODE, System.getProperty("sun.arch.data.model", "32"));
    }

    private String getCacheSize(String propertyName) {
        String cacheSize = property(propertyName);

        if (cacheSize != null) {
            return cacheSize;
        }

        return System.getProperty(propertyName);
    }

    private int getSegmentCacheSize() {
        return Integer.parseInt(getCacheSize(SEGMENT_CACHE_SIZE));
    }

    private int getStringCacheSize() {
        return Integer.parseInt(getCacheSize(STRING_CACHE_SIZE));
    }

    private int getTemplateCacheSize() {
        return Integer.parseInt(getCacheSize(TEMPLATE_CACHE_SIZE));
    }

    private String getMaxFileSizeProperty() {
        String size = property(SIZE);

        if (size != null) {
            return size;
        }

        return System.getProperty(SIZE, "256");
    }

    private int getMaxFileSize() {
        return Integer.parseInt(getMaxFileSizeProperty());
    }

    private byte getMemoryThreshold() {
        String mt = property(COMPACTION_MEMORY_THRESHOLD);

        if (mt == null) {
            return MEMORY_THRESHOLD_DEFAULT;
        }

        return Byte.valueOf(mt);
    }

    private byte getGainThreshold() {
        String gt = property(COMPACTION_GAIN_THRESHOLD);

        if (gt == null) {
            return GAIN_THRESHOLD_DEFAULT;
        }

        return Byte.valueOf(gt);
    }

    private String property(String name) {
        return lookupConfigurationThenFramework(context, name);
    }

    /**
     * needed for situations where you have to unwrap the
     * SegmentNodeStoreService, to get the SegmentStore, like the failover
     */
    @Override
    public SegmentStore getSegmentStore() {
        return store;
    }

    //------------------------------------------------------------< Observable >---

    @Override
    public Closeable addObserver(Observer observer) {
        return getNodeStore().addObserver(observer);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return name + ": " + segmentNodeStore;
    }
}
