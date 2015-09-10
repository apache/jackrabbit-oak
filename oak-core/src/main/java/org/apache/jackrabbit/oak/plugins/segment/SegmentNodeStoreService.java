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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toLong;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CLEANUP_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CLONE_BINARIES_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.FORCE_AFTER_FAIL_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.GAIN_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.MEMORY_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.PAUSE_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.PERSIST_COMPACTION_MAP_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.RETRY_COUNT_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.TIMESTAMP_DEFAULT;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
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
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategyMBean;
import org.apache.jackrabbit.oak.plugins.segment.compaction.DefaultCompactionStrategyMBean;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.Builder;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStoreGCMonitor;
import org.apache.jackrabbit.oak.plugins.segment.file.GCMonitorMBean;
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
        description = "NodeStore implementation based on Document model. For configuration option refer " +
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
            label = "Cache size (MB)",
            description = "Cache size for storing most recently used Segments"
    )
    public static final String CACHE = "cache";

    @Property(
            boolValue = CLONE_BINARIES_DEFAULT,
            label = "Clone Binaries",
            description = "Clone the binary segments while performing compaction"
    )
    public static final String COMPACTION_CLONE_BINARIES = "compaction.cloneBinaries";

    @Property(options = {
            @PropertyOption(name = "CLEAN_ALL", value = "CLEAN_ALL"),
            @PropertyOption(name = "CLEAN_NONE", value = "CLEAN_NONE"),
            @PropertyOption(name = "CLEAN_OLD", value = "CLEAN_OLD") },
            value = "CLEAN_OLD",
            label = "Cleanup Strategy",
            description = "Cleanup strategy used for live in memory segment references while performing cleanup. "+
                    "1. CLEAN_NONE: All in memory references are considered valid, " +
                    "2. CLEAN_OLD: Only in memory references older than a " +
                    "certain age are considered valid (compaction.cleanup.timestamp), " +
                    "3. CLEAN_ALL: None of the in memory references are considered valid"
    )
    public static final String COMPACTION_CLEANUP = "compaction.cleanup";

    @Property(
            longValue = TIMESTAMP_DEFAULT,
            label = "Reference expiry time (ms)",
            description = "Time interval in ms beyond which in memory segment references would be ignored " +
                    "while performing cleanup"
    )
    public static final String COMPACTION_CLEANUP_TIMESTAMP = "compaction.cleanup.timestamp";

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
                    "if the provided threshold is not met. Value represents a percentage so an input beween 0 and 100 is expected."
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
            boolValue = PERSIST_COMPACTION_MAP_DEFAULT,
            label = "Persist Compaction Map",
            description = "When enabled the compaction map would be persisted instead of being " +
                    "held in memory"
    )
    public static final String PERSIST_COMPACTION_MAP = "persistCompactionMap";

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

    private volatile SegmentNodeStore delegate;

    private ObserverTracker observerTracker;

    private GCMonitorTracker gcMonitor;

    private ComponentContext context;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC)
    private volatile BlobStore blobStore;

    private ServiceRegistration storeRegistration;
    private ServiceRegistration providerRegistration;
    private Registration checkpointRegistration;
    private Registration revisionGCRegistration;
    private Registration blobGCRegistration;
    private Registration compactionStrategyRegistration;
    private Registration segmentCacheMBean;
    private Registration stringCacheMBean;
    private Registration fsgcMonitorMBean;
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
        checkState(delegate != null, "service must be activated when used");
        return delegate;
    }

    @Activate
    private void activate(ComponentContext context) throws IOException {
        this.context = context;
        this.customBlobStore = Boolean.parseBoolean(property(CUSTOM_BLOB_STORE));

        if (blobStore == null && customBlobStore) {
            log.info("BlobStore use enabled. SegmentNodeStore would be initialized when BlobStore would be available");
        } else {
            registerNodeStore();
        }
    }

    public void registerNodeStore() throws IOException {
        if (registerSegmentStore()) {
            boolean standby = toBoolean(property(STANDBY), false);
            providerRegistration = context.getBundleContext().registerService(
                    SegmentStoreProvider.class.getName(), this, null);
            if (!standby) {
                Dictionary<String, Object> props = new Hashtable<String, Object>();
                props.put(Constants.SERVICE_PID, SegmentNodeStore.class.getName());
                props.put("oak.nodestore.description", new String[]{"nodeStoreType=segment"});
                storeRegistration = context.getBundleContext().registerService(
                        NodeStore.class.getName(), this, props);
            }
        }
    }

    public synchronized boolean registerSegmentStore() throws IOException {
        if (context == null) {
            log.info("Component still not activated. Ignoring the initialization call");
            return false;
        }
        Dictionary<?, ?> properties = context.getProperties();
        name = String.valueOf(properties.get(NAME));

        String directory = property(DIRECTORY);
        if (directory == null) {
            directory = "tarmk";
        }else{
            directory = FilenameUtils.concat(directory, "segmentstore");
        }

        String mode = property(MODE);
        if (mode == null) {
            mode = System.getProperty(MODE,
                    System.getProperty("sun.arch.data.model", "32"));
        }

        String size = property(SIZE);
        if (size == null) {
            size = System.getProperty(SIZE, "256");
        }

        String cache = property(CACHE);
        if (cache == null) {
            cache = System.getProperty(CACHE);
        }

        boolean pauseCompaction = toBoolean(property(PAUSE_COMPACTION),
                PAUSE_DEFAULT);
        boolean cloneBinaries = toBoolean(
                property(COMPACTION_CLONE_BINARIES),
                CLONE_BINARIES_DEFAULT);
        long cleanupTs = toLong(property(COMPACTION_CLEANUP_TIMESTAMP),
                TIMESTAMP_DEFAULT);
        int retryCount = toInteger(property(COMPACTION_RETRY_COUNT),
                RETRY_COUNT_DEFAULT);
        boolean forceCommit = toBoolean(property(COMPACTION_FORCE_AFTER_FAIL),
                FORCE_AFTER_FAIL_DEFAULT);
        final int lockWaitTime = toInteger(property(COMPACTION_LOCK_WAIT_TIME),
                COMPACTION_LOCK_WAIT_TIME_DEFAULT);
        boolean persistCompactionMap = toBoolean(property(PERSIST_COMPACTION_MAP),
                PERSIST_COMPACTION_MAP_DEFAULT);
        String cleanup = property(COMPACTION_CLEANUP);
        if (cleanup == null) {
            cleanup = CLEANUP_DEFAULT.toString();
        }

        String memoryThresholdS = property(COMPACTION_MEMORY_THRESHOLD);
        byte memoryThreshold = MEMORY_THRESHOLD_DEFAULT;
        if (memoryThresholdS != null) {
            memoryThreshold = Byte.valueOf(memoryThresholdS);
        }

        String gainThresholdS = property(COMPACTION_GAIN_THRESHOLD);
        byte gainThreshold = GAIN_THRESHOLD_DEFAULT;
        if (gainThresholdS != null) {
            gainThreshold = Byte.valueOf(gainThresholdS);
        }

        final long blobGcMaxAgeInSecs = toLong(property(PROP_BLOB_GC_MAX_AGE), DEFAULT_BLOB_GC_MAX_AGE);

        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        gcMonitor = new GCMonitorTracker();
        gcMonitor.start(whiteboard);
        Builder storeBuilder = FileStore.newFileStore(new File(directory))
                .withCacheSize(Integer.parseInt(cache))
                .withMaxFileSize(Integer.parseInt(size))
                .withMemoryMapping("64".equals(mode))
                .withGCMonitor(gcMonitor);
        if (customBlobStore) {
            log.info("Initializing SegmentNodeStore with BlobStore [{}]", blobStore);
            store = storeBuilder.withBlobStore(blobStore).create();
        } else {
            store = storeBuilder.create();
        }
        SegmentNodeStoreBuilder nodeStoreBuilder = SegmentNodeStore
                .newSegmentNodeStore(store);
        nodeStoreBuilder.withCompactionStrategy(pauseCompaction, cloneBinaries,
                cleanup, cleanupTs, memoryThreshold, lockWaitTime, retryCount,
                forceCommit, persistCompactionMap, gainThreshold);
        delegate = nodeStoreBuilder.create();

        CompactionStrategy compactionStrategy = nodeStoreBuilder
                .getCompactionStrategy();
        store.setCompactionStrategy(compactionStrategy);

        CacheStats segmentCacheStats = store.getTracker().getSegmentCacheStats();
        segmentCacheMBean = registerMBean(whiteboard, CacheStatsMBean.class,
                segmentCacheStats,
                CacheStats.TYPE, segmentCacheStats.getName());

        CacheStats stringCacheStats = store.getTracker().getStringCacheStats();
        if (stringCacheStats != null) {
            stringCacheMBean = registerMBean(whiteboard, CacheStatsMBean.class,
                    stringCacheStats,
                    CacheStats.TYPE, stringCacheStats.getName());
        }

        FileStoreGCMonitor fsgcMonitor = new FileStoreGCMonitor(Clock.SIMPLE);
        fsgcMonitorMBean = new CompositeRegistration(
                whiteboard.register(GCMonitor.class, fsgcMonitor, emptyMap()),
                registerMBean(whiteboard, GCMonitorMBean.class, fsgcMonitor, GCMonitorMBean.TYPE,
                        "File Store garbage collection monitor"),
                scheduleWithFixedDelay(whiteboard, fsgcMonitor, 1));

        observerTracker = new ObserverTracker(delegate);
        observerTracker.start(context.getBundleContext());

        executor = new WhiteboardExecutor();
        executor.start(whiteboard);

        checkpointRegistration = registerMBean(whiteboard, CheckpointMBean.class, new SegmentCheckpointMBean(delegate),
                CheckpointMBean.TYPE, "Segment node store checkpoint management");

        RevisionGC revisionGC = new RevisionGC(new Runnable() {
            @Override
            public void run() {
                store.gc();
            }
        }, executor);
        revisionGCRegistration = registerMBean(whiteboard, RevisionGCMBean.class, revisionGC,
                RevisionGCMBean.TYPE, "Segment node store revision garbage collection");

        // If a shared data store register the repo id in the data store
        if (SharedDataStoreUtils.isShared(blobStore)) {
            try {
                String repoId = ClusterRepositoryInfo.createId(delegate);
                ((SharedDataStore) blobStore).addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                    SharedStoreRecordType.REPOSITORY.getNameFromId(repoId));
            } catch (Exception e) {
                throw new IOException("Could not register a unique repositoryId", e);
            }
        }

        if (store.getBlobStore() instanceof GarbageCollectableBlobStore) {
            BlobGarbageCollector gc = new MarkSweepGarbageCollector(
                                                    new SegmentBlobReferenceRetriever(store.getTracker()),
                                                    (GarbageCollectableBlobStore) store.getBlobStore(),
                                                    executor, TimeUnit.SECONDS.toMillis(blobGcMaxAgeInSecs),
                                                    ClusterRepositoryInfo.getId(delegate));

            blobGCRegistration = registerMBean(whiteboard, BlobGCMBean.class, new BlobGC(gc, executor),
                    BlobGCMBean.TYPE, "Segment node store blob garbage collection");
        }

        compactionStrategyRegistration = registerMBean(whiteboard,
                CompactionStrategyMBean.class,
                new DefaultCompactionStrategyMBean(compactionStrategy),
                CompactionStrategyMBean.TYPE,
                "Segment node store compaction strategy settings");

        log.info("SegmentNodeStore initialized");
        return true;
    }

    private String property(String name) {
        return lookupConfigurationThenFramework(context, name);
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
            delegate = null;

            if (store != null) {
                store.close();
                store = null;
            }
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

    private void unregisterNodeStore() {
        if (segmentCacheMBean != null) {
            segmentCacheMBean.unregister();
            segmentCacheMBean = null;
        }
        if (stringCacheMBean != null) {
            stringCacheMBean.unregister();
            stringCacheMBean = null;
        }
        if(providerRegistration != null){
            providerRegistration.unregister();
            providerRegistration = null;
        }
        if(storeRegistration != null){
            storeRegistration.unregister();
            storeRegistration = null;
        }
        if (checkpointRegistration != null) {
            checkpointRegistration.unregister();
            checkpointRegistration = null;
        }
        if (revisionGCRegistration != null) {
            revisionGCRegistration.unregister();
            revisionGCRegistration = null;
        }
        if (blobGCRegistration != null) {
            blobGCRegistration.unregister();
            blobGCRegistration = null;
        }
        if (compactionStrategyRegistration != null) {
            compactionStrategyRegistration.unregister();
            compactionStrategyRegistration = null;
        }
        if (fsgcMonitorMBean != null) {
            fsgcMonitorMBean.unregister();
            fsgcMonitorMBean = null;
        }
        if (executor != null) {
            executor.stop();
            executor = null;
        }
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
        return name + ": " + delegate;
    }
}
