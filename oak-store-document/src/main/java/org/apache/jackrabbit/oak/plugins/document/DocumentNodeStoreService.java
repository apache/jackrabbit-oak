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
package org.apache.jackrabbit.oak.plugins.document;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toLong;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupFrameworkThenConfiguration;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_CACHE_SEGMENT_COUNT;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_CACHE_STACK_MOVE_DISTANCE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_CHILDREN_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_DIFF_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_MEMORY_CACHE_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_NODE_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.ONLY_STANDALONE_TARGET;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mongodb.MongoClientURI;

import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.api.jmx.PersistentCacheStatsMBean;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCacheStats;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStoreWrapper;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitorTracker;
import org.apache.jackrabbit.oak.spi.gc.LoggingGCMonitor;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.state.RevisionGC;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.spi.descriptors.GenericDescriptors;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.Designate;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The OSGi service to start/stop a DocumentNodeStore instance.
 */
@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        service = {})
@Designate(ocd = Configuration.class)
public class DocumentNodeStoreService {

    private static final long MB = 1024 * 1024;
    static final String DEFAULT_URI = "mongodb://localhost:27017/oak";
    static final int DEFAULT_CACHE = (int) (DEFAULT_MEMORY_CACHE_SIZE / MB);
    static final int DEFAULT_BLOB_CACHE_SIZE = 16;
    static final String DEFAULT_DB = "oak";
    static final boolean DEFAULT_SO_KEEP_ALIVE = false;
    static final String DEFAULT_PERSISTENT_CACHE = "cache,binary=0";
    static final String DEFAULT_JOURNAL_CACHE = "diff-cache";
    public static final String CONTINUOUS_RGC_EXPR = "*/5 * * * * ?";
    public static final String CLASSIC_RGC_EXPR = "0 0 2 * * ?";
    public static final long DEFAULT_RGC_TIME_LIMIT_SECS = 3*60*60; // default is 3 hours
    private static final String PREFIX = "oak.documentstore.";
    private static final String DESCRIPTION = "oak.nodestore.description";
    static final long DEFAULT_JOURNAL_GC_INTERVAL_MILLIS = 5*60*1000; // default is 5min
    static final long DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS = 24*60*60*1000; // default is 24hours
    private static final String DEFAULT_PROP_HOME = "./repository";
    static final long DEFAULT_MAX_REPLICATION_LAG = 6 * 60 * 60;
    static final boolean DEFAULT_BUNDLING_DISABLED = false;

    /**
     * Revisions older than this time would be garbage collected
     */
    static final long DEFAULT_VER_GC_MAX_AGE = 24 * 60 * 60; //TimeUnit.DAYS.toSeconds(1);


    /**
     * Blob modified before this time duration would be considered for Blob GC
     */
    static final long DEFAULT_BLOB_GC_MAX_AGE = 24 * 60 * 60;

    /**
     * Default interval for taking snapshots of locally tracked blob ids.
     */
    static final long DEFAULT_BLOB_SNAPSHOT_INTERVAL = 12 * 60 * 60;

    /**
     * Name of framework property to configure Mongo Connection URI
     */
    private static final String FWK_PROP_URI = "oak.mongo.uri";

    /**
     * Name of framework property to configure Mongo Database name
     * to use
     */
    private static final String FWK_PROP_DB = "oak.mongo.db";

    /**
     * Name of framework property to configure socket keep-alive for MongoDB
     */
    private static final String FWK_PROP_SO_KEEP_ALIVE = "oak.mongo.socketKeepAlive";


    // property name constants - values can come from framework properties or OSGi config
    private static final String PROP_URI = "mongouri";
    private static final String PROP_DB = "db";
    static final String PROP_SO_KEEP_ALIVE = "socketKeepAlive";
    private static final String PROP_CACHE = "cache";
    private static final String PROP_NODE_CACHE_PERCENTAGE = "nodeCachePercentage";
    private static final String PROP_PREV_DOC_CACHE_PERCENTAGE = "prevDocCachePercentage";
    private static final String PROP_CHILDREN_CACHE_PERCENTAGE = "childrenCachePercentage";
    private static final String PROP_DIFF_CACHE_PERCENTAGE = "diffCachePercentage";
    private static final String PROP_CACHE_SEGMENT_COUNT = "cacheSegmentCount";
    private static final String PROP_CACHE_STACK_MOVE_DISTANCE = "cacheStackMoveDistance";
    private static final String PROP_BLOB_CACHE_SIZE = "blobCacheSize";
    private static final String PROP_PERSISTENT_CACHE = "persistentCache";
    private static final String PROP_JOURNAL_CACHE = "journalCache";
    public static final String PROP_PREFETCH_EXTERNAL_CHANGES = "prefetchExternalChanges";
    private static final String PROP_JOURNAL_GC_MAX_AGE_MILLIS = "journalGCMaxAge";
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";
    public static final String PROP_VER_GC_MAX_AGE = "versionGcMaxAgeInSecs";
    public static final String PROP_VER_GC_EXPRESSION = "versionGCExpression";
    public static final String PROP_VER_GC_TIME_LIMIT = "versionGCTimeLimitInSecs";
    public static final String PROP_REV_RECOVERY_INTERVAL = "lastRevRecoveryJobIntervalInSecs";
    public static final String PROP_BLOB_GC_MAX_AGE = "blobGcMaxAgeInSecs";
    public static final String PROP_BLOB_SNAPSHOT_INTERVAL = "blobTrackSnapshotIntervalInSecs";
    private static final String PROP_HOME = "repository.home";
    public static final String PROP_REPLICATION_LAG = "maxReplicationLagInSecs";
    public static final String PROP_DS_TYPE = "documentStoreType";
    private static final String PROP_BUNDLING_DISABLED = "bundlingDisabled";
    public static final String PROP_UPDATE_LIMIT = "updateLimit";
    public static final String PROP_ROLE = "role";
    private static final String PROP_JOURNAL_GC_INTERVAL_MILLIS = "journalGCInterval";

    private static enum DocumentStoreType {
        MONGO, RDB;

        static DocumentStoreType fromString(String type) {
            if (type == null) {
                return MONGO;
            }
            return valueOf(type.toUpperCase(Locale.ROOT));
        }
    }

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private ServiceRegistration nodeStoreReg;
    private Closer closer;
    private WhiteboardExecutor executor;

    private volatile BlobStore blobStore;

    private volatile DataSource dataSource;

    private volatile DataSource blobDataSource;

    private volatile DocumentNodeStateCache nodeStateCache;

    private DocumentNodeStore nodeStore;
    private ObserverTracker observerTracker;
    private JournalPropertyHandlerFactory journalPropertyHandlerFactory = new JournalPropertyHandlerFactory();
    private ComponentContext context;
    private Whiteboard whiteboard;
    private long deactivationTimestamp = 0;
    private long maxReplicationLagInSecs;

    @Reference
    private StatisticsProvider statisticsProvider;

    private boolean customBlobStore;

    private ServiceRegistration blobStoreReg;

    private BlobStore defaultBlobStore;

    private volatile Configuration config;

    @Activate
    protected void activate(ComponentContext context, Configuration config) throws Exception {
        closer = Closer.create();
        this.config = config;
        this.context = context;
        whiteboard = new OsgiWhiteboard(context.getBundleContext());
        executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        maxReplicationLagInSecs = config.maxReplicationLagInSecs();
        customBlobStore = toBoolean(prop(CUSTOM_BLOB_STORE), false);
        documentStoreType = DocumentStoreType.fromString(config.documentStoreType());

        registerNodeStoreIfPossible();
    }

    private void registerNodeStoreIfPossible() throws IOException {
        // disallow attempts to restart (OAK-3420)
        if (deactivationTimestamp != 0) {
            log.info("DocumentNodeStore was already unregistered ({}ms ago)", System.currentTimeMillis() - deactivationTimestamp);
        } else if (context == null) {
            log.info("Component still not activated. Ignoring the initialization call");
        } else if (customBlobStore && blobStore == null) {
            log.info("Custom BlobStore use enabled. DocumentNodeStoreService would be initialized when "
                    + "BlobStore would be available");
        } else if (documentStoreType == DocumentStoreType.RDB && (dataSource == null || blobDataSource == null)) {
            log.info("DataSource use enabled. DocumentNodeStoreService would be initialized when "
                    + "DataSource would be available (currently available: nodes: {}, blobs: {})", dataSource, blobDataSource);
        } else {
            registerNodeStore();
        }
    }

    private void registerNodeStore() throws IOException {
        String uri = PropertiesUtil.toString(prop(PROP_URI, FWK_PROP_URI), DEFAULT_URI);
        String db = PropertiesUtil.toString(prop(PROP_DB, FWK_PROP_DB), DEFAULT_DB);
        boolean soKeepAlive = PropertiesUtil.toBoolean(prop(PROP_SO_KEEP_ALIVE, FWK_PROP_SO_KEEP_ALIVE), DEFAULT_SO_KEEP_ALIVE);

        int cacheSize = toInteger(prop(PROP_CACHE), DEFAULT_CACHE);
        int nodeCachePercentage = toInteger(prop(PROP_NODE_CACHE_PERCENTAGE), DEFAULT_NODE_CACHE_PERCENTAGE);
        int prevDocCachePercentage = toInteger(prop(PROP_PREV_DOC_CACHE_PERCENTAGE), DEFAULT_NODE_CACHE_PERCENTAGE);
        int childrenCachePercentage = toInteger(prop(PROP_CHILDREN_CACHE_PERCENTAGE), DEFAULT_CHILDREN_CACHE_PERCENTAGE);
        int diffCachePercentage = toInteger(prop(PROP_DIFF_CACHE_PERCENTAGE), DEFAULT_DIFF_CACHE_PERCENTAGE);
        int blobCacheSize = toInteger(prop(PROP_BLOB_CACHE_SIZE), DEFAULT_BLOB_CACHE_SIZE);
        String persistentCache = getPath(PROP_PERSISTENT_CACHE, DEFAULT_PERSISTENT_CACHE);
        String journalCache = getPath(PROP_JOURNAL_CACHE, DEFAULT_JOURNAL_CACHE);
        int cacheSegmentCount = toInteger(prop(PROP_CACHE_SEGMENT_COUNT), DEFAULT_CACHE_SEGMENT_COUNT);
        int cacheStackMoveDistance = toInteger(prop(PROP_CACHE_STACK_MOVE_DISTANCE), DEFAULT_CACHE_STACK_MOVE_DISTANCE);
        boolean bundlingDisabled = toBoolean(prop(PROP_BUNDLING_DISABLED), DEFAULT_BUNDLING_DISABLED);
        boolean prefetchExternalChanges = toBoolean(prop(PROP_PREFETCH_EXTERNAL_CHANGES), false);
        int updateLimit = toInteger(prop(PROP_UPDATE_LIMIT), DocumentMK.UPDATE_LIMIT);
        long journalGCMaxAge = toLong(context.getProperties().get(PROP_JOURNAL_GC_MAX_AGE_MILLIS), DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS);
        DocumentMK.Builder mkBuilder =
                new DocumentMK.Builder().
                setStatisticsProvider(statisticsProvider).
                memoryCacheSize(cacheSize * MB).
                memoryCacheDistribution(
                        nodeCachePercentage,
                        prevDocCachePercentage,
                        childrenCachePercentage, 
                        diffCachePercentage).
                setCacheSegmentCount(cacheSegmentCount).
                setCacheStackMoveDistance(cacheStackMoveDistance).
                setBundlingDisabled(bundlingDisabled).
                        setJournalPropertyHandlerFactory(journalPropertyHandlerFactory).
                setLeaseCheck(!ClusterNodeInfo.DEFAULT_LEASE_CHECK_DISABLED /* OAK-2739: enabled by default */).
                setLeaseFailureHandler(new LeaseFailureHandler() {
                    
                    @Override
                    public void handleLeaseFailure() {
                        try {
                            // plan A: try stopping oak-core
                            log.error("handleLeaseFailure: stopping oak-core...");
                            Bundle bundle = context.getBundleContext().getBundle();
                            bundle.stop(Bundle.STOP_TRANSIENT);
                            log.error("handleLeaseFailure: stopped oak-core.");
                            // plan A worked, perfect!
                        } catch (BundleException e) {
                            log.error("handleLeaseFailure: exception while stopping oak-core: "+e, e);
                            // plan B: stop only DocumentNodeStoreService (to stop the background threads)
                            log.error("handleLeaseFailure: stopping DocumentNodeStoreService...");
                            context.disableComponent(DocumentNodeStoreService.class.getName());
                            log.error("handleLeaseFailure: stopped DocumentNodeStoreService");
                            // plan B succeeded.
                        }
                    }
                }).
                setPrefetchExternalChanges(prefetchExternalChanges).
                setUpdateLimit(updateLimit).
                setJournalGCMaxAge(journalGCMaxAge).
                setNodeCachePredicate(createCachePredicate());

        if (!Strings.isNullOrEmpty(persistentCache)) {
            mkBuilder.setPersistentCache(persistentCache);
        }
        if (!Strings.isNullOrEmpty(journalCache)) {
            mkBuilder.setJournalCache(journalCache);
        }

        boolean wrappingCustomBlobStore = customBlobStore && blobStore instanceof BlobStoreWrapper;

        //Set blobstore before setting the DB
        if (customBlobStore && !wrappingCustomBlobStore) {
            checkNotNull(blobStore, "Use of custom BlobStore enabled via  [%s] but blobStore reference not " +
                    "initialized", CUSTOM_BLOB_STORE);
            mkBuilder.setBlobStore(blobStore);
        }

        if (documentStoreType == DocumentStoreType.RDB) {
            checkNotNull(dataSource, "DataStore type set [%s] but DataSource reference not initialized", PROP_DS_TYPE);
            if (!customBlobStore) {
                checkNotNull(blobDataSource, "DataStore type set [%s] but BlobDataSource reference not initialized", PROP_DS_TYPE);
                mkBuilder.setRDBConnection(dataSource, blobDataSource);
                log.info("Connected to datasources {} {}", dataSource, blobDataSource);
            } else {
                if (blobDataSource != null && blobDataSource != dataSource) {
                    log.info("Ignoring blobDataSource {} as custom blob store takes precedence.", blobDataSource);
                }
                mkBuilder.setRDBConnection(dataSource);
                log.info("Connected to datasource {}", dataSource);
            }
        } else {
            MongoClientURI mongoURI = new MongoClientURI(uri);

            if (log.isInfoEnabled()) {
                // Take care around not logging the uri directly as it
                // might contain passwords
                log.info("Starting DocumentNodeStore with host={}, db={}, cache size (MB)={}, persistentCache={}, " +
                                "journalCache={}, blobCacheSize (MB)={}, maxReplicationLagInSecs={}",
                        mongoURI.getHosts(), db, cacheSize, persistentCache,
                        journalCache, blobCacheSize, maxReplicationLagInSecs);
                log.info("Mongo Connection details {}", MongoConnection.toString(mongoURI.getOptions()));
            }

            mkBuilder.setMaxReplicationLag(maxReplicationLagInSecs, TimeUnit.SECONDS);
            mkBuilder.setSocketKeepAlive(soKeepAlive);
            mkBuilder.setMongoDB(uri, db, blobCacheSize);

            log.info("Connected to database '{}'", db);
        }

        if (!customBlobStore){
            defaultBlobStore = mkBuilder.getBlobStore();
            log.info("Registering the BlobStore with ServiceRegistry");
            blobStoreReg = context.getBundleContext().registerService(BlobStore.class.getName(),
                    defaultBlobStore , null);
        }

        //Set wrapping blob store after setting the DB
        if (wrappingCustomBlobStore) {
            ((BlobStoreWrapper) blobStore).setBlobStore(mkBuilder.getBlobStore());
            mkBuilder.setBlobStore(blobStore);
        }

        mkBuilder.setExecutor(executor);

        // attach GCMonitor
        final GCMonitorTracker gcMonitor = new GCMonitorTracker();
        gcMonitor.start(whiteboard);
        closer.register(asCloseable(gcMonitor));
        Logger vgcLogger = LoggerFactory.getLogger(VersionGarbageCollector.class);
        GCMonitor loggingGCMonitor;
        if (isContinuousRevisionGC()) {
            // log less chatty with continuous RevisionGC
            loggingGCMonitor = new QuietGCMonitor(vgcLogger);
        } else {
            loggingGCMonitor = new LoggingGCMonitor(vgcLogger);
        }
        mkBuilder.setGCMonitor(new DelegatingGCMonitor(
                newArrayList(gcMonitor, loggingGCMonitor)));

        nodeStore = mkBuilder.getNodeStore();

        // ensure a clusterId is initialized 
        // and expose it as 'oak.clusterid' repository descriptor
        GenericDescriptors clusterIdDesc = new GenericDescriptors();
        clusterIdDesc.put(ClusterRepositoryInfo.OAK_CLUSTERID_REPOSITORY_DESCRIPTOR_KEY, 
                new SimpleValueFactory().createValue(
                        ClusterRepositoryInfo.getOrCreateId(nodeStore)), true, false);
        whiteboard.register(Descriptors.class, clusterIdDesc, Collections.emptyMap());
        
        // If a shared data store register the repo id in the data store
        if (SharedDataStoreUtils.isShared(blobStore)) {
            String repoId = null;
            try {
                repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
                ((SharedDataStore) blobStore).addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                    SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getNameFromId(repoId));
            } catch (Exception e) {
                throw new IOException("Could not register a unique repositoryId", e);
            }

            if (blobStore instanceof BlobTrackingStore) {
                final long trackSnapshotInterval = toLong(prop(PROP_BLOB_SNAPSHOT_INTERVAL),
                    DEFAULT_BLOB_SNAPSHOT_INTERVAL);
                String root = getRepositoryHome();

                BlobTrackingStore trackingStore = (BlobTrackingStore) blobStore;
                if (trackingStore.getTracker() != null) {
                    trackingStore.getTracker().close();
                }
                ((BlobTrackingStore) blobStore).addTracker(
                    new BlobIdTracker(root, repoId, trackSnapshotInterval, (SharedDataStore)
                        blobStore));
            }
        }


        registerJMXBeans(nodeStore, mkBuilder);
        registerLastRevRecoveryJob(nodeStore);
        registerJournalGC(nodeStore);
        registerVersionGCJob(nodeStore);

        if (!isNodeStoreProvider()) {
            observerTracker = new ObserverTracker(nodeStore);
            observerTracker.start(context.getBundleContext());
        }
        journalPropertyHandlerFactory.start(whiteboard);

        DocumentStore ds = nodeStore.getDocumentStore();

        // OAK-2682: time difference detection applied at startup with a default
        // max time diff of 2000 millis (2sec)
        final long maxDiff = Long.parseLong(System.getProperty("oak.documentMK.maxServerTimeDiffMillis", "2000"));
        try {
            if (maxDiff>=0) {
                final long timeDiff = ds.determineServerTimeDifferenceMillis();
                log.info("registerNodeStore: server time difference: {}ms (max allowed: {}ms)", timeDiff, maxDiff);
                if (Math.abs(timeDiff) > maxDiff) {
                    throw new AssertionError("Server clock seems off (" + timeDiff + "ms) by more than configured amount ("
                            + maxDiff + "ms)");
                }
            }
        } catch (RuntimeException e) { // no checked exception
            // in case of a RuntimeException, just log but continue
            log.warn("registerNodeStore: got RuntimeException while trying to determine time difference to server: " + e, e);
        }

        String[] serviceClasses;
        if (isNodeStoreProvider()) {
            registerNodeStoreProvider(nodeStore);
            serviceClasses = new String[]{
                    DocumentNodeStore.class.getName(),
                    Clusterable.class.getName()
            };
        } else {
            serviceClasses = new String[]{
                    NodeStore.class.getName(),
                    DocumentNodeStore.class.getName(),
                    Clusterable.class.getName()
            };
        }

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, DocumentNodeStore.class.getName());
        props.put(DESCRIPTION, getMetadata(ds));
        // OAK-2844: in order to allow DocumentDiscoveryLiteService to directly
        // require a service DocumentNodeStore (instead of having to do an 'instanceof')
        // the registration is now done for both NodeStore and DocumentNodeStore here.
        nodeStoreReg = context.getBundleContext().registerService(
            serviceClasses,
            nodeStore, props);
    }

    private Predicate<String> createCachePredicate() {
        if (config.persistentCacheIncludes().length == 0) {
            return Predicates.alwaysTrue();
        }
        if (Arrays.equals(config.persistentCacheIncludes(), new String[]{"/"})) {
            return Predicates.alwaysTrue();
        }

        Set<String> paths = new HashSet<>();
        for (String p : config.persistentCacheIncludes()) {
            p = p != null ? Strings.emptyToNull(p.trim()) : null;
            if (p != null) {
                paths.add(p);
            }
        }
        PathFilter pf = new PathFilter(paths, emptyList());
        log.info("Configuring persistent cache to only cache nodes under paths {}", paths);
        return path -> path != null && pf.filter(path) == PathFilter.Result.INCLUDE;
    }

    private boolean isNodeStoreProvider() {
        return prop(PROP_ROLE) != null;
    }

    private boolean isContinuousRevisionGC() {
        String expr = getVersionGCExpression();
        String[] elements = expr.split("\\s");
        // simple heuristic to determine if revision GC runs 'frequently'
        return elements.length >= 6 && elements[1].equals("*");
    }

    private String getVersionGCExpression() {
        String defaultExpr;
        if (DocumentStoreType.fromString(config.documentStoreType()) == DocumentStoreType.MONGO) {
            defaultExpr = CONTINUOUS_RGC_EXPR;
        } else {
            defaultExpr = "";
        }
        String expr = PropertiesUtil.toString(prop(PROP_VER_GC_EXPRESSION), "");
        if (expr.isEmpty()) {
            expr = defaultExpr;
        }
        // validate expression
        try {
            if (!expr.isEmpty()) {
                new CronExpression(expr);
            }
        } catch (ParseException e) {
            log.warn("Invalid cron expression, falling back to default '" + defaultExpr + "'", e);
            expr = defaultExpr;
        }
        return expr;
    }

    private void registerNodeStoreProvider(final NodeStore ns) {
        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(NodeStoreProvider.ROLE, prop(PROP_ROLE));
        nodeStoreReg = context.getBundleContext().registerService(NodeStoreProvider.class.getName(), new NodeStoreProvider() {
                    @Override
                    public NodeStore getNodeStore() {
                        return ns;
                    }
                },
                props);
        log.info("Registered NodeStoreProvider backed by DocumentNodeStore");
    }

    @Deactivate
    protected void deactivate() {
        if (observerTracker != null) {
            observerTracker.stop();
        }

        if (journalPropertyHandlerFactory != null){
            journalPropertyHandlerFactory.stop();
        }

        unregisterNodeStore();
    }

    @Reference(name = "blobStore",
            cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            target = ONLY_STANDALONE_TARGET
    )
    @SuppressWarnings("UnusedDeclaration")
    protected void bindBlobStore(BlobStore blobStore) throws IOException {
        if (defaultBlobStore == blobStore){
            return;
        }
        log.info("Initializing DocumentNodeStore with BlobStore [{}]", blobStore);
        this.blobStore = blobStore;
        registerNodeStoreIfPossible();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindBlobStore(BlobStore blobStore) {
        if (defaultBlobStore == blobStore){
            return;
        }
        this.blobStore = null;
        unregisterNodeStore();
    }

    @Reference(name = "dataSource",
            cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            target = "(datasource.name=oak)"
    )
    @SuppressWarnings("UnusedDeclaration")
    protected void bindDataSource(DataSource dataSource) throws IOException {
        if (this.dataSource != null) {
            log.info("Ignoring bindDataSource [{}] because dataSource [{}] is already bound", dataSource, this.dataSource);
        } else {
            log.info("Initializing DocumentNodeStore with dataSource [{}]", dataSource);
            this.dataSource = dataSource;
            registerNodeStoreIfPossible();
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindDataSource(DataSource dataSource) {
        if (this.dataSource != dataSource) {
            log.info("Ignoring unbindDataSource [{}] because dataSource is bound to [{}]", dataSource, this.dataSource);
        } else {
            log.info("Unregistering DocumentNodeStore because dataSource [{}] was unbound", dataSource);
            this.dataSource = null;
            unregisterNodeStore();
        }
    }


    private DocumentStoreType documentStoreType;

    @Reference(name = "blobDataSource",
            cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            target = "(datasource.name=oak)"
    )
    @SuppressWarnings("UnusedDeclaration")
    protected void bindBlobDataSource(DataSource dataSource) throws IOException {
        if (this.blobDataSource != null) {
            log.info("Ignoring bindBlobDataSource [{}] because blobDataSource [{}] is already bound", dataSource,
                    this.blobDataSource);
        } else {
            log.info("Initializing DocumentNodeStore with blobDataSource [{}]", dataSource);
            this.blobDataSource = dataSource;
            registerNodeStoreIfPossible();
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindBlobDataSource(DataSource dataSource) {
        if (this.blobDataSource != dataSource) {
            log.info("Ignoring unbindBlobDataSource [{}] because dataSource is bound to [{}]", dataSource, this.blobDataSource);
        } else {
            log.info("Unregistering DocumentNodeStore because blobDataSource [{}] was unbound", dataSource);
            this.blobDataSource = null;
            unregisterNodeStore();
        }
    }

    @Reference(name = "nodeStateCache",
            cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC
    )
    @SuppressWarnings("UnusedDeclaration")
    protected void bindNodeStateCache(DocumentNodeStateCache nodeStateCache) throws IOException {
       if (nodeStore != null){
           log.info("Registered DocumentNodeStateCache [{}] with DocumentNodeStore", nodeStateCache);
           nodeStore.setNodeStateCache(nodeStateCache);
       }
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindNodeStateCache(DocumentNodeStateCache nodeStateCache) {
        if (nodeStore != null){
            nodeStore.setNodeStateCache(DocumentNodeStateCache.NOOP);
        }
    }

    private void unregisterNodeStore() {
        deactivationTimestamp = System.currentTimeMillis();

        closeQuietly(closer);

        if (nodeStoreReg != null) {
            nodeStoreReg.unregister();
            nodeStoreReg = null;
        }

        //If we exposed our BlobStore then unregister it *after*
        //NodeStore service. This ensures that if any other component
        //like SecondaryStoreCache depends on this then it remains active
        //untill DocumentNodeStore get deactivated
        if (blobStoreReg != null){
            blobStoreReg.unregister();
            blobStoreReg = null;
        }

        if (nodeStore != null) {
            nodeStore.dispose();
            nodeStore = null;
        }

        if (executor != null) {
            executor.stop();
            executor = null;
        }
    }

    private void registerJMXBeans(final DocumentNodeStore store, DocumentMK.Builder mkBuilder) throws
            IOException {
        addRegistration(
                registerMBean(whiteboard,
                        CacheStatsMBean.class,
                        store.getNodeCacheStats(),
                        CacheStatsMBean.TYPE,
                        store.getNodeCacheStats().getName()));
        addRegistration(
                registerMBean(whiteboard,
                        CacheStatsMBean.class,
                        store.getNodeChildrenCacheStats(),
                        CacheStatsMBean.TYPE,
                        store.getNodeChildrenCacheStats().getName())
        );
        for (CacheStats cs : store.getDiffCacheStats()) {
            addRegistration(
                    registerMBean(whiteboard,
                            CacheStatsMBean.class, cs,
                            CacheStatsMBean.TYPE, cs.getName()));
        }
        DocumentStore ds = store.getDocumentStore();
        if (ds.getCacheStats() != null) {
            for (CacheStats cacheStats : ds.getCacheStats()) {
                addRegistration(
                        registerMBean(whiteboard,
                                CacheStatsMBean.class,
                                cacheStats,
                                CacheStatsMBean.TYPE,
                                cacheStats.getName())
                );
            }
        }

        addRegistration(
                registerMBean(whiteboard,
                        CheckpointMBean.class,
                        new DocumentCheckpointMBean(store),
                        CheckpointMBean.TYPE,
                        "Document node store checkpoint management")
        );

        addRegistration(
                registerMBean(whiteboard,
                        DocumentNodeStoreMBean.class,
                        store.getMBean(),
                        DocumentNodeStoreMBean.TYPE,
                        "Document node store management")
        );

        if (mkBuilder.getBlobStoreCacheStats() != null) {
            addRegistration(
                    registerMBean(whiteboard,
                            CacheStatsMBean.class,
                            mkBuilder.getBlobStoreCacheStats(),
                            CacheStatsMBean.TYPE,
                            mkBuilder.getBlobStoreCacheStats().getName())
            );
        }

        if (mkBuilder.getDocumentStoreStatsCollector() instanceof DocumentStoreStatsMBean) {
            addRegistration(
                    registerMBean(whiteboard,
                            DocumentStoreStatsMBean.class,
                            (DocumentStoreStatsMBean) mkBuilder.getDocumentStoreStatsCollector(),
                            DocumentStoreStatsMBean.TYPE,
                            "DocumentStore Statistics")
            );
        }

        // register persistent cache stats
        Map<CacheType, PersistentCacheStats> persistenceCacheStats = mkBuilder.getPersistenceCacheStats();
        for (PersistentCacheStats pcs: persistenceCacheStats.values()) {
            addRegistration(
                    registerMBean(whiteboard,
                            PersistentCacheStatsMBean.class,
                            pcs,
                            PersistentCacheStatsMBean.TYPE,
                            pcs.getName())
            );
        }


        final long versionGcMaxAgeInSecs = toLong(prop(PROP_VER_GC_MAX_AGE), DEFAULT_VER_GC_MAX_AGE);
        final long blobGcMaxAgeInSecs = toLong(prop(PROP_BLOB_GC_MAX_AGE), DEFAULT_BLOB_GC_MAX_AGE);

        if (store.getBlobStore() instanceof GarbageCollectableBlobStore) {
            BlobGarbageCollector gc = store.createBlobGarbageCollector(blobGcMaxAgeInSecs, 
                                                        ClusterRepositoryInfo.getOrCreateId(nodeStore),
                                                        whiteboard);
            addRegistration(registerMBean(whiteboard, BlobGCMBean.class, new BlobGC(gc, executor),
                    BlobGCMBean.TYPE, "Document node store blob garbage collection"));
        }

        Runnable startGC = new RevisionGCJob(store, versionGcMaxAgeInSecs, 0);
        Runnable cancelGC = () -> store.getVersionGarbageCollector().cancel();
        Supplier<String> status = () -> store.getVersionGarbageCollector().getStatus();
        RevisionGC revisionGC = new RevisionGC(startGC, cancelGC, status, executor);
        addRegistration(registerMBean(whiteboard, RevisionGCMBean.class, revisionGC,
                RevisionGCMBean.TYPE, "Document node store revision garbage collection"));

        addRegistration(registerMBean(whiteboard, RevisionGCStatsMBean.class,
                store.getVersionGarbageCollector().getRevisionGCStats(),
                RevisionGCStatsMBean.TYPE,
                "Document node store revision garbage collection statistics"));

        BlobStoreStats blobStoreStats = mkBuilder.getBlobStoreStats();
        if (!customBlobStore && blobStoreStats != null) {
            addRegistration(registerMBean(whiteboard,
                    BlobStoreStatsMBean.class,
                    blobStoreStats,
                    BlobStoreStatsMBean.TYPE,
                    ds.getClass().getSimpleName()));
        }

        if (!mkBuilder.isBundlingDisabled()){
            addRegistration(registerMBean(whiteboard,
                    BackgroundObserverMBean.class,
                    store.getBundlingConfigHandler().getMBean(),
                    BackgroundObserverMBean.TYPE,
                    "BundlingConfigObserver"));
        }
    }

    private void registerLastRevRecoveryJob(final DocumentNodeStore nodeStore) {
        long leaseTime = toLong(context.getProperties().get(PROP_REV_RECOVERY_INTERVAL),
                ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);
        addRegistration(scheduleWithFixedDelay(whiteboard,
                new LastRevRecoveryJob(nodeStore), TimeUnit.MILLISECONDS.toSeconds(leaseTime),
                false/*runOnSingleClusterNode*/, true /*use dedicated pool*/));
    }

    private void registerJournalGC(final DocumentNodeStore nodeStore) {
        long journalGCInterval = toLong(context.getProperties().get(PROP_JOURNAL_GC_INTERVAL_MILLIS),
                DEFAULT_JOURNAL_GC_INTERVAL_MILLIS);
        addRegistration(scheduleWithFixedDelay(whiteboard,
                new JournalGCJob(nodeStore),
                jobPropertiesFor(JournalGCJob.class),
                TimeUnit.MILLISECONDS.toSeconds(journalGCInterval),
                true/*runOnSingleClusterNode*/, true /*use dedicated pool*/));
    }

    private void registerVersionGCJob(final DocumentNodeStore nodeStore) {
        String expr = getVersionGCExpression();
        if (expr.isEmpty()) {
            return;
        }
        Map<String, Object> props = jobPropertiesFor(RevisionGCJob.class);
        props.put("scheduler.expression", expr);
        long versionGcMaxAgeInSecs = toLong(prop(PROP_VER_GC_MAX_AGE), DEFAULT_VER_GC_MAX_AGE);
        long versionGCTimeLimitInSecs = toLong(prop(PROP_VER_GC_TIME_LIMIT), DEFAULT_RGC_TIME_LIMIT_SECS);
        addRegistration(scheduleWithFixedDelay(whiteboard,
                new RevisionGCJob(nodeStore, versionGcMaxAgeInSecs,
                        versionGCTimeLimitInSecs),
                props, MODIFIED_IN_SECS_RESOLUTION, true, true));
    }

    private String prop(String propName) {
        return prop(propName, PREFIX + propName);
    }

    private String prop(String propName, String fwkPropName) {
        return lookupFrameworkThenConfiguration(context, propName, fwkPropName);
    }

    private String getPath(String propName, String defaultValue) {
        String path = PropertiesUtil.toString(prop(propName), defaultValue);
        if (Strings.isNullOrEmpty(path)) {
            return path;
        }
        if ("-".equals(path)) {
            // disable this path configuration
            return "";
        }
        // resolve as relative to repository.home
        return FilenameUtils.concat(getRepositoryHome(), path);
    }

    private String getRepositoryHome() {
        String repoHome = prop(PROP_HOME, PROP_HOME);
        if (Strings.isNullOrEmpty(repoHome)) {
            repoHome = DEFAULT_PROP_HOME;
        }
        return repoHome;
    }

    private static String[] getMetadata(DocumentStore ds) {
        Map<String, String> meta = new HashMap<String, String>(ds.getMetadata());
        meta.put("nodeStoreType", "document");
        String[] result = new String[meta.size()];
        int i = 0;
        for (Map.Entry<String, String> e : meta.entrySet()) {
            result[i++] = e.getKey() + "=" + e.getValue();
        }
        return result;
    }

    private void addRegistration(@Nonnull Registration reg) {
        closer.register(asCloseable(reg));
    }

    private static Closeable asCloseable(@Nonnull final Registration reg) {
        checkNotNull(reg);
        return new Closeable() {
            @Override
            public void close() throws IOException {
                reg.unregister();
            }
        };
    }

    private static Closeable asCloseable(@Nonnull final AbstractServiceTracker t) {
        checkNotNull(t);
        return new Closeable() {
            @Override
            public void close() throws IOException {
                t.stop();
            }
        };
    }

    static final class RevisionGCJob implements Runnable, Supplier<String> {

        // log as VersionGarbageCollector
        private static final Logger LOGGER = LoggerFactory.getLogger(VersionGarbageCollector.class);

        // log every hour
        private static final long LOG_INTERVAL = TimeUnit.HOURS.toMillis(1);

        private final DocumentNodeStore nodeStore;
        private final long versionGCMaxAgeInSecs;
        private final long versionGCTimeLimitInSecs;
        private volatile Object lastResult = "";
        private long lastLogTime;
        private VersionGCStats stats;

        RevisionGCJob(DocumentNodeStore ns,
                      long versionGcMaxAgeInSecs,
                      long versionGCTimeLimitInSecs) {
            this.nodeStore = ns;
            this.versionGCMaxAgeInSecs = versionGcMaxAgeInSecs;
            this.versionGCTimeLimitInSecs = versionGCTimeLimitInSecs;
            resetStats();
        }

        @Override
        public void run() {
            VersionGarbageCollector gc = nodeStore.getVersionGarbageCollector();
            gc.setOptions(gc.getOptions().withMaxDuration(TimeUnit.SECONDS, versionGCTimeLimitInSecs));
            try {
                VersionGCStats s = gc.gc(versionGCMaxAgeInSecs, TimeUnit.SECONDS);
                stats.addRun(s);
                lastResult = s.toString();
            } catch (Exception e) {
                lastResult = e;
                LOGGER.warn("Error occurred while executing the Version Garbage Collector", e);
            } finally {
                maybeLogStats();
            }
        }

        /**
         * Returns the result of the last revision GC run. This method throws
         * an {@link UncheckedExecutionException} if the last run failed with an
         * exception.
         *
         * @return result of the last revision GC run.
         */
        @Override
        public String get() throws UncheckedExecutionException {
            if (lastResult instanceof Exception) {
                throw new UncheckedExecutionException((Exception) lastResult);
            }
            return String.valueOf(lastResult);
        }

        private void resetStats() {
            lastLogTime = nodeStore.getClock().getTime();
            stats = new VersionGCStats();
        }

        private void maybeLogStats() {
            if (nodeStore.getClock().getTime() > lastLogTime + LOG_INTERVAL) {
                LOGGER.info("Garbage collector stats since {}: {}",
                        Utils.timestampToString(lastLogTime), stats);
                resetStats();
            }
        }
    }

    private static final class JournalGCJob implements Runnable {

        private final DocumentNodeStore nodeStore;

        JournalGCJob(DocumentNodeStore ns) {
            this.nodeStore = ns;
        }

        @Override
        public void run() {
            nodeStore.getJournalGarbageCollector().gc();
        }
    }

    private static final class LastRevRecoveryJob implements Runnable {

        private final DocumentNodeStore nodeStore;

        LastRevRecoveryJob(DocumentNodeStore ns) {
            this.nodeStore = ns;
        }

        @Override
        public void run() {
            nodeStore.getLastRevRecoveryAgent().performRecoveryIfNeeded();
        }
    }

    private static Map<String, Object> jobPropertiesFor(Class clazz) {
        Map<String, Object> props = new HashMap<>();
        props.put("scheduler.name", clazz.getName());
        return props;
    }
}
