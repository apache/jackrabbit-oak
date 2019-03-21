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

import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.isShared;
import static org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener.IGNORE_SNFE;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.RETAINED_GENERATIONS_DEFAULT;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.getOrCreateId;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Closer;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.api.jmx.FileStoreBackupRestoreMBean;
import org.apache.jackrabbit.oak.backup.impl.FileStoreBackupRestoreImpl;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.compaction.SegmentRevisionGC;
import org.apache.jackrabbit.oak.segment.compaction.SegmentRevisionGCMBean;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.FileStoreGCMonitor;
import org.apache.jackrabbit.oak.segment.file.FileStoreStatsMBean;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.MetricsIOMonitor;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.split.SplitPersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.spi.descriptors.GenericDescriptors;
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
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentNodeStoreRegistrar {

    static SegmentNodeStore registerSegmentNodeStore(Configuration cfg) throws IOException {
        return new SegmentNodeStoreRegistrar(cfg).register();
    }

    interface Configuration {

        boolean isPrimarySegmentStore();

        boolean isSecondarySegmentStore();

        boolean isStandbyInstance();

        String getRole();

        int getRetainedGenerations();

        int getDefaultRetainedGenerations();

        boolean getPauseCompaction();

        int getRetryCount();

        int getForceCompactionTimeout();

        long getSizeDeltaEstimation();

        int getMemoryThreshold();

        boolean getDisableEstimation();

        long getGCProcessLog();

        File getSegmentDirectory();

        File getSplitPersistenceDirectory();

        int getSegmentCacheSize();

        int getStringCacheSize();

        int getTemplateCacheSize();

        int getStringDeduplicationCacheSize();

        int getTemplateDeduplicationCacheSize();

        int getNodeDeduplicationCacheSize();

        int getMaxFileSize();

        boolean getMemoryMapping();

        boolean hasCustomBlobStore();

        boolean hasCustomSegmentStore();

        boolean hasSplitPersistence();

        boolean registerDescriptors();

        boolean dispatchChanges();

        String getRepositoryHome();

        long getBlobSnapshotInterval();

        long getBlobGcMaxAge();

        File getBackupDirectory();

        Whiteboard getWhiteboard();

        Closer getCloser();

        Logger getLogger();

        StatisticsProvider getStatisticsProvider();

        BlobStore getBlobStore();

        SegmentNodeStorePersistence getSegmentNodeStorePersistence();

        BundleContext getBundleContext();

    }

    private final Configuration cfg;

    private SegmentNodeStoreRegistrar(Configuration cfg) {
        this.cfg = cfg;
    }

    private SegmentNodeStore register() throws IOException {
        if (cfg.getBlobStore() == null && (cfg.hasCustomBlobStore() || cfg.isSecondarySegmentStore())) {
            cfg.getLogger().info("BlobStore enabled. SegmentNodeStore will be initialized once the blob store becomes available");
            return null;
        }

        if (cfg.getSegmentNodeStorePersistence() == null && cfg.hasCustomSegmentStore()) {
            cfg.getLogger().info("customSegmentStore enabled. SegmentNodeStore will be initialized once the custom segment store becomes available");
            return null;
        }

        // Listen for GCMonitor services
        GCMonitor gcMonitor = GCMonitor.EMPTY;

        if (cfg.isPrimarySegmentStore()) {
            GCMonitorTracker tracker = new GCMonitorTracker();
            tracker.start(cfg.getWhiteboard());
            registerCloseable(tracker);
            gcMonitor = tracker;
        }

        // Create the gc options
        if (cfg.getRetainedGenerations() != cfg.getDefaultRetainedGenerations()) {
            cfg.getLogger().warn(
                "The number of retained generations defaults to {} and can't be " +
                    "changed. This configuration option is considered deprecated " +
                    "and will be removed in the future.",
                RETAINED_GENERATIONS_DEFAULT
            );
        }
        SegmentGCOptions gcOptions = new SegmentGCOptions(cfg.getPauseCompaction(), cfg.getRetryCount(), cfg.getForceCompactionTimeout())
            .setGcSizeDeltaEstimation(cfg.getSizeDeltaEstimation())
            .setMemoryThreshold(cfg.getMemoryThreshold())
            .setEstimationDisabled(cfg.getDisableEstimation())
            .setGCLogInterval(cfg.getGCProcessLog());
        if (cfg.isStandbyInstance()) {
            gcOptions.setRetainedGenerations(1);
        }

        // Build the FileStore
        FileStoreBuilder builder = fileStoreBuilder(cfg.getSegmentDirectory())
            .withSegmentCacheSize(cfg.getSegmentCacheSize())
            .withStringCacheSize(cfg.getStringCacheSize())
            .withTemplateCacheSize(cfg.getTemplateCacheSize())
            .withStringDeduplicationCacheSize(cfg.getStringDeduplicationCacheSize())
            .withTemplateDeduplicationCacheSize(cfg.getTemplateDeduplicationCacheSize())
            .withNodeDeduplicationCacheSize(cfg.getNodeDeduplicationCacheSize())
            .withMaxFileSize(cfg.getMaxFileSize())
            .withMemoryMapping(cfg.getMemoryMapping())
            .withGCMonitor(gcMonitor)
            .withIOMonitor(new MetricsIOMonitor(cfg.getStatisticsProvider()))
            .withStatisticsProvider(cfg.getStatisticsProvider())
            .withGCOptions(gcOptions);

        if (cfg.hasCustomBlobStore() && cfg.getBlobStore() != null) {
            cfg.getLogger().info("Initializing SegmentNodeStore with BlobStore [{}]", cfg.getBlobStore());
            builder.withBlobStore(cfg.getBlobStore());
        }

        if (cfg.hasCustomSegmentStore() && cfg.getSegmentNodeStorePersistence() != null) {
            if (cfg.hasSplitPersistence()) {
                cfg.getLogger().info("Initializing SegmentNodeStore with custom persistence [{}] and local writes", cfg.getSegmentNodeStorePersistence());
                cfg.getSplitPersistenceDirectory().mkdirs();
                SegmentNodeStorePersistence roPersistence = cfg.getSegmentNodeStorePersistence();
                SegmentNodeStorePersistence rwPersistence = new TarPersistence(cfg.getSplitPersistenceDirectory());
                SegmentNodeStorePersistence persistence = new SplitPersistence(roPersistence, rwPersistence);
                builder.withCustomPersistence(persistence);
            } else {
                cfg.getLogger().info("Initializing SegmentNodeStore with custom persistence [{}]", cfg.getSegmentNodeStorePersistence());
                builder.withCustomPersistence(cfg.getSegmentNodeStorePersistence());
            }
        }

        if (cfg.isStandbyInstance()) {
            builder.withSnfeListener(IGNORE_SNFE);
            builder.withEagerSegmentCaching(true);
        }

        FileStore store;
        try {
            store = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            cfg.getLogger().error("The storage format is not compatible with this version of Oak Segment Tar", e);
            return null;
        }
        registerCloseable(store);

        // Listen for Executor services on the whiteboard

        WhiteboardExecutor executor = new WhiteboardExecutor();
        executor.start(cfg.getWhiteboard());
        registerCloseable(executor);

        // Expose stats about the segment cache

        CacheStatsMBean segmentCacheStats = store.getSegmentCacheStats();
        registerCloseable(registerMBean(
            CacheStatsMBean.class,
            segmentCacheStats,
            CacheStats.TYPE,
            segmentCacheStats.getName()
        ));

        // Expose stats about the string and template caches

        CacheStatsMBean stringCacheStats = store.getStringCacheStats();
        registerCloseable(registerMBean(
            CacheStatsMBean.class,
            stringCacheStats,
            CacheStats.TYPE,
            stringCacheStats.getName()
        ));

        CacheStatsMBean templateCacheStats = store.getTemplateCacheStats();
        registerCloseable(registerMBean(
            CacheStatsMBean.class,
            templateCacheStats,
            CacheStats.TYPE,
            templateCacheStats.getName()
        ));

        WriterCacheManager cacheManager = builder.getCacheManager();
        CacheStatsMBean stringDeduplicationCacheStats = cacheManager.getStringCacheStats();
        if (stringDeduplicationCacheStats != null) {
            registerCloseable(registerMBean(
                CacheStatsMBean.class,
                stringDeduplicationCacheStats,
                CacheStats.TYPE,
                stringDeduplicationCacheStats.getName()
            ));
        }

        CacheStatsMBean templateDeduplicationCacheStats = cacheManager.getTemplateCacheStats();
        if (templateDeduplicationCacheStats != null) {
            registerCloseable(registerMBean(
                CacheStatsMBean.class,
                templateDeduplicationCacheStats,
                CacheStats.TYPE,
                templateDeduplicationCacheStats.getName()
            ));
        }

        CacheStatsMBean nodeDeduplicationCacheStats = cacheManager.getNodeCacheStats();
        if (nodeDeduplicationCacheStats != null) {
            registerCloseable(registerMBean(
                CacheStatsMBean.class,
                nodeDeduplicationCacheStats,
                CacheStats.TYPE,
                nodeDeduplicationCacheStats.getName()
            ));
        }

        // Expose an MBean to managing and monitoring garbage collection

        FileStoreGCMonitor monitor = new FileStoreGCMonitor(Clock.SIMPLE);
        registerCloseable(register(
            GCMonitor.class,
            monitor
        ));
        if (!cfg.isStandbyInstance()) {
            registerCloseable(registerMBean(
                SegmentRevisionGC.class,
                new SegmentRevisionGCMBean(store, gcOptions, monitor),
                SegmentRevisionGC.TYPE,
                "Segment node store revision garbage collection"
            ));
        }

        registerCloseable(registerMBean(
            RevisionGCMBean.class,
            new RevisionGC(store.getGCRunner(), store::cancelGC, monitor::getStatus, executor),
            RevisionGCMBean.TYPE,
            "Revision garbage collection"
        ));

        // Expose statistics about the FileStore

        registerCloseable(registerMBean(
            FileStoreStatsMBean.class,
            store.getStats(),
            FileStoreStatsMBean.TYPE,
            "FileStore statistics"
        ));

        // register segment node store

        SegmentNodeStore.SegmentNodeStoreBuilder segmentNodeStoreBuilder = SegmentNodeStoreBuilders.builder(store).withStatisticsProvider(cfg.getStatisticsProvider());
        segmentNodeStoreBuilder.dispatchChanges(cfg.dispatchChanges());

        Logger log = LoggerFactory.getLogger(LoggingHook.class.getName() + ".writer");
        if (log.isTraceEnabled()) {
            segmentNodeStoreBuilder.withLoggingHook(log::trace);
        }

        SegmentNodeStore segmentNodeStore = segmentNodeStoreBuilder.build();

        if (cfg.isPrimarySegmentStore()) {
            ObserverTracker observerTracker = new ObserverTracker(segmentNodeStore);
            observerTracker.start(cfg.getBundleContext());
            registerCloseable(observerTracker);
        }

        if (cfg.isPrimarySegmentStore()) {
            registerCloseable(registerMBean(
                CheckpointMBean.class,
                new SegmentCheckpointMBean(segmentNodeStore),
                CheckpointMBean.TYPE,
                "Segment node store checkpoint management"
            ));
        }

        if (cfg.registerDescriptors()) {
            // ensure a clusterId is initialized
            // and expose it as 'oak.clusterid' repository descriptor
            GenericDescriptors clusterIdDesc = new GenericDescriptors();
            clusterIdDesc.put(
                ClusterRepositoryInfo.OAK_CLUSTERID_REPOSITORY_DESCRIPTOR_KEY,
                new SimpleValueFactory().createValue(getOrCreateId(segmentNodeStore)),
                true,
                false
            );
            registerCloseable(register(Descriptors.class, clusterIdDesc));
            // Register "discovery lite" descriptors
            registerCloseable(register(Descriptors.class, new SegmentDiscoveryLiteDescriptors(segmentNodeStore)));
        }

        // If a shared data store register the repo id in the data store
        if (!cfg.isSecondarySegmentStore() && cfg.hasCustomBlobStore() && isShared(cfg.getBlobStore())) {
            SharedDataStore sharedDataStore = (SharedDataStore) cfg.getBlobStore();
            try {
                sharedDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]), SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getNameFromId(getOrCreateId(segmentNodeStore)));
            } catch (Exception e) {
                throw new IOException("Could not register a unique repositoryId", e);
            }
            if (cfg.getBlobStore() instanceof BlobTrackingStore) {
                BlobTrackingStore trackingStore = (BlobTrackingStore) cfg.getBlobStore();
                if (trackingStore.getTracker() != null) {
                    trackingStore.getTracker().close();
                }
                trackingStore.addTracker(BlobIdTracker.build(cfg.getRepositoryHome(), getOrCreateId(segmentNodeStore), cfg.getBlobSnapshotInterval(), sharedDataStore));
            }
        }

        if (!cfg.isSecondarySegmentStore() && cfg.hasCustomBlobStore() && (cfg.getBlobStore() instanceof GarbageCollectableBlobStore)) {
            BlobGarbageCollector gc = new MarkSweepGarbageCollector(
                new SegmentBlobReferenceRetriever(store),
                (GarbageCollectableBlobStore) cfg.getBlobStore(),
                executor,
                TimeUnit.SECONDS.toMillis(cfg.getBlobGcMaxAge()),
                getOrCreateId(segmentNodeStore),
                cfg.getWhiteboard(),
                cfg.getStatisticsProvider()
            );
            registerCloseable(registerMBean(
                BlobGCMBean.class,
                new BlobGC(gc, executor),
                BlobGCMBean.TYPE,
                "Segment node store blob garbage collection"
            ));
        }

        // Expose an MBean for backup/restore operations

        registerCloseable(registerMBean(
            FileStoreBackupRestoreMBean.class,
            new FileStoreBackupRestoreImpl(
                segmentNodeStore,
                store.getRevisions(),
                store.getReader(),
                cfg.getBackupDirectory(),
                executor
            ),
            FileStoreBackupRestoreMBean.TYPE,
            "Segment node store backup/restore"
        ));

        // Expose statistics about the SegmentNodeStore

        registerCloseable(registerMBean(
            SegmentNodeStoreStatsMBean.class,
            segmentNodeStore.getStats(),
            SegmentNodeStoreStatsMBean.TYPE,
            "SegmentNodeStore statistics"
        ));

        if (cfg.isPrimarySegmentStore()) {
            cfg.getLogger().info("Primary SegmentNodeStore initialized");
        } else {
            cfg.getLogger().info("Secondary SegmentNodeStore initialized, role={}", cfg.getRole());
        }

        // Register a factory service to expose the FileStore
        registerCloseable(register(
            SegmentStoreProvider.class,
            new DefaultSegmentStoreProvider(store)
        ));

        if (cfg.isStandbyInstance()) {
            return segmentNodeStore;
        }

        if (cfg.isPrimarySegmentStore()) {
            Map<String, Object> props = new HashMap<>();
            props.put(Constants.SERVICE_PID, SegmentNodeStore.class.getName());
            props.put("oak.nodestore.description", new String[] {"nodeStoreType=segment"});
            registerCloseable(register(NodeStore.class, segmentNodeStore, props));
        }

        return segmentNodeStore;
    }

    private <T> Registration registerMBean(Class<T> clazz, T bean, String type, String name) {
        return registerMBean(clazz, bean, type, name, new HashMap<>());
    }

    private <T> Registration registerMBean(Class<T> clazz, T bean, String type, String name, Map<String, String> attributes) {
        return WhiteboardUtils.registerMBean(cfg.getWhiteboard(), clazz, bean, type, maybeAppendRole(name), maybePutRoleAttribute(attributes));
    }

    private <T> Registration register(Class<T> clazz, T service) {
        return register(clazz, service, new HashMap<>());
    }

    private <T> Registration register(Class<T> clazz, T service, Map<String, Object> properties) {
        return cfg.getWhiteboard().register(clazz, service, maybePutRoleProperty(properties));
    }

    private String maybeAppendRole(String name) {
        if (cfg.getRole() != null) {
            return name + " - " + cfg.getRole();
        }
        return name;
    }

    private String jmxRole() {
        return cfg.getRole().replaceAll(":", "-");
    }

    private Map<String, String> maybePutRoleAttribute(Map<String, String> attributes) {
        if (cfg.getRole() != null) {
            attributes.put("role", jmxRole());
        }
        return attributes;
    }

    private Map<String, Object> maybePutRoleProperty(Map<String, Object> attributes) {
        if (cfg.getRole() != null) {
            attributes.put("role", cfg.getRole());
        }
        return attributes;
    }

    private void registerCloseable(Closeable c) {
        cfg.getCloser().register(c);
    }

    private void registerCloseable(final AbstractServiceTracker<?> t) {
        registerCloseable((Closeable) t::stop);
    }

    private void registerCloseable(final Registration r) {
        registerCloseable((Closeable) r::unregister);
    }

    private void registerCloseable(final ObserverTracker t) {
        registerCloseable((Closeable) t::stop);
    }

}
