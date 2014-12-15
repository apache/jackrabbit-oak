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
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toLong;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CLEANUP_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CLONE_BINARIES_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.MEMORY_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.PAUSE_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.TIMESTAMP_DEFAULT;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.concurrent.Callable;

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
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategyMBean;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType;
import org.apache.jackrabbit.oak.plugins.segment.compaction.DefaultCompactionStrategyMBean;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;
import org.apache.jackrabbit.oak.spi.state.RevisionGC;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OSGi wrapper for the segment node store.
 */
@Component(policy = ConfigurationPolicy.REQUIRE)
public class SegmentNodeStoreService extends ProxyNodeStore
        implements Observable, SegmentStoreProvider {

    @Property(description="The unique name of this instance")
    public static final String NAME = "name";

    @Property(description="TarMK directory")
    public static final String DIRECTORY = "repository.home";

    @Property(description="TarMK mode (64 for memory mapping, 32 for normal file access)")
    public static final String MODE = "tarmk.mode";

    @Property(description="TarMK maximum file size (MB)", intValue=256)
    public static final String SIZE = "tarmk.size";

    @Property(description="Cache size (MB)", intValue=256)
    public static final String CACHE = "cache";

    @Property(description = "TarMK compaction clone binaries flag", boolValue = CLONE_BINARIES_DEFAULT)
    public static final String COMPACTION_CLONE_BINARIES = "compaction.cloneBinaries";

    @Property(options = {
            @PropertyOption(name = "CLEAN_ALL", value = "CLEAN_ALL"),
            @PropertyOption(name = "CLEAN_NONE", value = "CLEAN_NONE"),
            @PropertyOption(name = "CLEAN_OLD", value = "CLEAN_OLD") }, value = "CLEAN_OLD")
    public static final String COMPACTION_CLEANUP = "compaction.cleanup";

    @Property(description = "TarMK compaction strategy timestamp older (ms)", longValue = TIMESTAMP_DEFAULT)
    public static final String COMPACTION_CLEANUP_TIMESTAMP = "compaction.cleanup.timestamp";

    @Property(description = "TarMK compaction available memory multiplier needed to run compaction", byteValue = MEMORY_THRESHOLD_DEFAULT)
    public static final String COMPACTION_MEMORY_THRESHOLD = "compaction.memoryThreshold";

    @Property(description = "TarMK compaction paused flag", boolValue = PAUSE_DEFAULT)
    public static final String PAUSE_COMPACTION = "pauseCompaction";

    @Property(description = "Flag indicating that this component will not register as a NodeStore but just as a NodeStoreProvider", boolValue = false)
    public static final String STANDBY = "standby";

    /**
     * Boolean value indicating a blobStore is to be used
     */
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String name;

    private SegmentStore store;

    private SegmentNodeStore delegate;

    private ObserverTracker observerTracker;

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
    private WhiteboardExecutor executor;
    private boolean customBlobStore;

    @Override
    protected synchronized SegmentNodeStore getNodeStore() {
        checkState(delegate != null, "service must be activated when used");
        return delegate;
    }

    @Activate
    private void activate(ComponentContext context) throws IOException {
        this.context = context;
        this.customBlobStore = Boolean.parseBoolean(lookup(context, CUSTOM_BLOB_STORE));

        if (blobStore == null && customBlobStore) {
            log.info("BlobStore use enabled. SegmentNodeStore would be initialized when BlobStore would be available");
        } else {
            registerNodeStore();
        }
    }

    public void registerNodeStore() throws IOException {
        if (registerSegmentStore()) {
            Dictionary<String, String> props = new Hashtable<String, String>();
            props.put(Constants.SERVICE_PID, SegmentNodeStore.class.getName());

            boolean standby = toBoolean(lookup(context, STANDBY), false);
            providerRegistration = context.getBundleContext().registerService(
                    SegmentStoreProvider.class.getName(), this, props);
            if (!standby) {
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

        String directory = lookup(context, DIRECTORY);
        if (directory == null) {
            directory = "tarmk";
        }else{
            directory = FilenameUtils.concat(directory, "segmentstore");
        }

        String mode = lookup(context, MODE);
        if (mode == null) {
            mode = System.getProperty(MODE,
                    System.getProperty("sun.arch.data.model", "32"));
        }

        String size = lookup(context, SIZE);
        if (size == null) {
            size = System.getProperty(SIZE, "256");
        }

        boolean pauseCompaction = toBoolean(lookup(context, PAUSE_COMPACTION),
                PAUSE_DEFAULT);
        boolean cloneBinaries = toBoolean(
                lookup(context, COMPACTION_CLONE_BINARIES),
                CLONE_BINARIES_DEFAULT);
        long cleanupTs = toLong(lookup(context, COMPACTION_CLEANUP_TIMESTAMP),
                TIMESTAMP_DEFAULT);
        String cleanup = lookup(context, COMPACTION_CLEANUP);
        if (cleanup == null) {
            cleanup = CLEANUP_DEFAULT.toString();
        }

        String memoryThresholdS = lookup(context, COMPACTION_MEMORY_THRESHOLD);
        byte memoryThreshold = MEMORY_THRESHOLD_DEFAULT;
        if (memoryThresholdS != null) {
            memoryThreshold = Byte.valueOf(memoryThresholdS);
        }
        CompactionStrategy compactionStrategy = new CompactionStrategy(
                pauseCompaction, cloneBinaries, CleanupType.valueOf(cleanup), cleanupTs,
                memoryThreshold) {
            @Override
            public boolean compacted(Callable<Boolean> setHead) throws Exception {
                // Need to guard against concurrent commits to avoid
                // mixed segments. See OAK-2192.
                return delegate.locked(setHead);
            }
        };

        boolean memoryMapping = "64".equals(mode);
        int cacheSize = Integer.parseInt(size);
        if (customBlobStore) {
            log.info("Initializing SegmentNodeStore with BlobStore [{}]", blobStore);
            store = new FileStore(blobStore, new File(directory), cacheSize,
                    memoryMapping).setCompactionStrategy(compactionStrategy);
        } else {
            store = new FileStore(new File(directory), cacheSize, memoryMapping)
                    .setCompactionStrategy(compactionStrategy);
        }

        delegate = new SegmentNodeStore(store);
        observerTracker = new ObserverTracker(delegate);
        observerTracker.start(context.getBundleContext());

        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
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

        if (store.getBlobStore() instanceof GarbageCollectableBlobStore) {
            BlobGarbageCollector gc = new BlobGarbageCollector() {
                @Override
                public void collectGarbage() throws Exception {
                    MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector(
                            new SegmentBlobReferenceRetriever(store.getTracker()),
                            (GarbageCollectableBlobStore) store.getBlobStore(),
                            executor);
                    gc.collectGarbage();
                }
            };

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

    private static String lookup(ComponentContext context, String property) {
        if (context.getProperties().get(property) != null) {
            return context.getProperties().get(property).toString();
        }
        if (context.getBundleContext().getProperty(property) != null) {
            return context.getBundleContext().getProperty(property);
        }
        return null;
    }

    @Deactivate
    public synchronized void deactivate() {
        unregisterNodeStore();

        observerTracker.stop();
        delegate = null;

        store.close();
        store = null;
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
        if (executor != null) {
            executor.stop();
            executor = null;
        }
    }

    /**
     * needed for situations where you have to unwrap the
     * SegmentNodeStoreService, to get the SegmentStore, like the failover
     */
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
