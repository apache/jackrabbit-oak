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
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toLong;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupFrameworkThenConfiguration;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_CHILDREN_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_DIFF_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_DOC_CHILDREN_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_NODE_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.ONLY_STANDALONE_TARGET;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.mongodb.MongoClientURI;

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
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.identifier.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStoreWrapper;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.RevisionGC;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The OSGi service to start/stop a DocumentNodeStore instance.
 */
@Component(policy = ConfigurationPolicy.REQUIRE,
        metatype = true,
        label = "Apache Jackrabbit Oak Document NodeStore Service",
        description = "NodeStore implementation based on Document model. For configuration option refer " +
                "to http://jackrabbit.apache.org/oak/docs/osgi_config.html#DocumentNodeStore. Note that for system " +
                "stability purpose it is advisable to not change these settings at runtime. Instead the config change " +
                "should be done via file system based config file and this view should ONLY be used to determine which " +
                "options are supported"
)
public class DocumentNodeStoreService {
    private static final String DEFAULT_URI = "mongodb://localhost:27017/oak";
    private static final int DEFAULT_CACHE = 256;
    private static final int DEFAULT_BLOB_CACHE_SIZE = 16;
    private static final String DEFAULT_DB = "oak";
    private static final String DEFAULT_PERSISTENT_CACHE = "";
    private static final int DEFAULT_CACHE_SEGMENT_COUNT = 16;
    private static final int DEFAULT_CACHE_STACK_MOVE_DISTANCE = 16;
    private static final String PREFIX = "oak.documentstore.";
    private static final String DESCRIPTION = "oak.nodestore.description";

    /**
     * Name of framework property to configure Mongo Connection URI
     */
    private static final String FWK_PROP_URI = "oak.mongo.uri";

    /**
     * Name of framework property to configure Mongo Database name
     * to use
     */
    private static final String FWK_PROP_DB = "oak.mongo.db";

    @Property(value = DEFAULT_URI,
            label = "Mongo URI",
            description = "Mongo connection URI used to connect to Mongo. Refer to " +
                    "http://docs.mongodb.org/manual/reference/connection-string/ for details. Note that this value " +
                    "can be overridden via framework property 'oak.mongo.uri'"
    )
    private static final String PROP_URI = "mongouri";

    @Property(value = DEFAULT_DB,
            label = "Mongo DB name",
            description = "Name of the database in Mongo. Note that this value " +
                    "can be overridden via framework property 'oak.mongo.db'"
    )
    private static final String PROP_DB = "db";

    @Property(intValue = DEFAULT_CACHE,
            label = "Cache Size (in MB)",
            description = "Cache size in MB. This is distributed among various caches used in DocumentNodeStore"
    )
    private static final String PROP_CACHE = "cache";
    
    @Property(intValue = DEFAULT_NODE_CACHE_PERCENTAGE,
            label = "NodeState Cache",
            description = "Percentage of cache to be allocated towards Node cache"
    )
    private static final String PROP_NODE_CACHE_PERCENTAGE = "nodeCachePercentage";
    
    @Property(intValue = DocumentMK.Builder.DEFAULT_CHILDREN_CACHE_PERCENTAGE,
            label = "NodeState Children Cache",
            description = "Percentage of cache to be allocated towards Children cache"
    )
    private static final String PROP_CHILDREN_CACHE_PERCENTAGE = "childrenCachePercentage";
    
    @Property(intValue = DocumentMK.Builder.DEFAULT_DIFF_CACHE_PERCENTAGE,
            label = "Diff Cache",
            description = "Percentage of cache to be allocated towards Diff cache"
    )
    private static final String PROP_DIFF_CACHE_PERCENTAGE = "diffCachePercentage";
    
    @Property(intValue = DocumentMK.Builder.DEFAULT_DOC_CHILDREN_CACHE_PERCENTAGE,
            label = "Document Children Cache",
            description = "Percentage of cache to be allocated towards Document children cache"
    )
    private static final String PROP_DOC_CHILDREN_CACHE_PERCENTAGE = "docChildrenCachePercentage";

    @Property(intValue = DocumentMK.Builder.DEFAULT_CACHE_SEGMENT_COUNT,
            label = "LIRS Cache Segment Count",
            description = "The number of segments in the LIRS cache " + 
                    "(default 16, a higher count means higher concurrency " + 
                    "but slightly lower cache hit rate)"
    )
    private static final String PROP_CACHE_SEGMENT_COUNT = "cacheSegmentCount";

    @Property(intValue = DocumentMK.Builder.DEFAULT_CACHE_STACK_MOVE_DISTANCE,
            label = "LIRS Cache Stack Move Distance",
            description = "The delay to move entries to the head of the queue " + 
                    "in the LIRS cache " +
                    "(default 16, a higher value means higher concurrency " + 
                    "but slightly lower cache hit rate)"
    )
    private static final String PROP_CACHE_STACK_MOVE_DISTANCE = "cacheStackMoveDistance";

    @Property(intValue =  DEFAULT_BLOB_CACHE_SIZE,
            label = "Blob Cache Size (in MB)",
            description = "Cache size to store blobs in memory. Used only with default BlobStore " +
                    "(as per DocumentStore type)"
    )
    private static final String PROP_BLOB_CACHE_SIZE = "blobCacheSize";

    @Property(value = DEFAULT_PERSISTENT_CACHE,
            label = "Persistent Cache Config",
            description = "Configuration for enabling Persistent cache. By default it is not enabled. Refer to " +
                    "http://jackrabbit.apache.org/oak/docs/nodestore/persistent-cache.html for various options"
    )
    private static final String PROP_PERSISTENT_CACHE = "persistentCache";

    @Property(boolValue = false,
            label = "Custom BlobStore",
            description = "Boolean value indicating that a custom BlobStore is to be used. " +
                    "By default, for MongoDB, MongoBlobStore is used; for RDB, RDBBlobStore is used."
    )
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    private static final long DEFAULT_JOURNAL_GC_INTERVAL_MILLIS = 5*60*1000; // default is 5min
    @Property(longValue = DEFAULT_JOURNAL_GC_INTERVAL_MILLIS,
            label = "Journal Garbage Collection Interval (millis)",
            description = "Long value indicating interval (in milliseconds) with which the "
                    + "journal (for external changes) is cleaned up. Default is " + DEFAULT_JOURNAL_GC_INTERVAL_MILLIS
    )
    private static final String PROP_JOURNAL_GC_INTERVAL_MILLIS = "journalGCInterval";
    
    private static final long DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS = 6*60*60*1000; // default is 6hours
    @Property(longValue = DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS,
            label = "Maximum Age of Journal Entries (millis)",
            description = "Long value indicating max age (in milliseconds) that "
                    + "journal (for external changes) entries are kept (older ones are candidates for gc). "
                    + "Default is " + DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS
    )
    private static final String PROP_JOURNAL_GC_MAX_AGE_MILLIS = "journalGCMaxAge";

    private static final long MB = 1024 * 1024;

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

    private ServiceRegistration reg;
    private final List<Registration> registrations = new ArrayList<Registration>();
    private WhiteboardExecutor executor;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC, target = ONLY_STANDALONE_TARGET)
    private volatile BlobStore blobStore;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC,
            target = "(datasource.name=oak)"
    )
    private volatile DataSource dataSource;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC,
            target = "(datasource.name=oak)"
    )
    private volatile DataSource blobDataSource;

    private DocumentMK mk;
    private ObserverTracker observerTracker;
    private ComponentContext context;
    private Whiteboard whiteboard;
    private long deactivationTimestamp = 0;


    /**
     * Revisions older than this time would be garbage collected
     */
    private static final long DEFAULT_VER_GC_MAX_AGE = 24 * 60 * 60; //TimeUnit.DAYS.toSeconds(1);
    @Property (longValue = DEFAULT_VER_GC_MAX_AGE,
            label = "Version GC Max Age (in secs)",
            description = "Version Garbage Collector (GC) logic will only consider those deleted for GC which " +
                    "are not accessed recently (currentTime - lastModifiedTime > versionGcMaxAgeInSecs). For " +
                    "example as per default only those document which have been *marked* deleted 24 hrs ago will be " +
                    "considered for GC. This also applies how older revision of live document are GC."
    )
    public static final String PROP_VER_GC_MAX_AGE = "versionGcMaxAgeInSecs";

    public static final String PROP_REV_RECOVERY_INTERVAL = "lastRevRecoveryJobIntervalInSecs";

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

    private static final long DEFAULT_MAX_REPLICATION_LAG = 6 * 60 * 60;
    @Property(longValue = DEFAULT_MAX_REPLICATION_LAG,
            label = "Max Replication Lag (in secs)",
            description = "Value in seconds. Determines the duration beyond which it can be safely assumed " +
                    "that the state on the secondaries is consistent with the primary, and it is safe to read from them"
    )
    public static final String PROP_REPLICATION_LAG = "maxReplicationLagInSecs";
    private long maxReplicationLagInSecs = DEFAULT_MAX_REPLICATION_LAG;

    @Property(options = {
                @PropertyOption(name = "MONGO", value = "MONGO"),
                @PropertyOption(name = "RDB", value = "RDB")
            },
            value = "MONGO",
            label = "DocumentStore Type",
            description = "Type of DocumentStore to use for persistence. Defaults to MONGO"
    )
    public static final String PROP_DS_TYPE = "documentStoreType";
    private DocumentStoreType documentStoreType;

    private boolean customBlobStore;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) throws Exception {
        this.context = context;
        whiteboard = new OsgiWhiteboard(context.getBundleContext());
        executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        maxReplicationLagInSecs = toLong(config.get(PROP_REPLICATION_LAG), DEFAULT_MAX_REPLICATION_LAG);
        customBlobStore = toBoolean(prop(CUSTOM_BLOB_STORE), false);
        documentStoreType = DocumentStoreType.fromString(PropertiesUtil.toString(config.get(PROP_DS_TYPE), "MONGO"));

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

        int cacheSize = toInteger(prop(PROP_CACHE), DEFAULT_CACHE);
        int nodeCachePercentage = toInteger(prop(PROP_NODE_CACHE_PERCENTAGE), DEFAULT_NODE_CACHE_PERCENTAGE);
        int childrenCachePercentage = toInteger(prop(PROP_CHILDREN_CACHE_PERCENTAGE), DEFAULT_CHILDREN_CACHE_PERCENTAGE);
        int docChildrenCachePercentage = toInteger(prop(PROP_DOC_CHILDREN_CACHE_PERCENTAGE), DEFAULT_DOC_CHILDREN_CACHE_PERCENTAGE);
        int diffCachePercentage = toInteger(prop(PROP_DIFF_CACHE_PERCENTAGE), DEFAULT_DIFF_CACHE_PERCENTAGE);
        int blobCacheSize = toInteger(prop(PROP_BLOB_CACHE_SIZE), DEFAULT_BLOB_CACHE_SIZE);
        String persistentCache = PropertiesUtil.toString(prop(PROP_PERSISTENT_CACHE), DEFAULT_PERSISTENT_CACHE);
        int cacheSegmentCount = toInteger(prop(PROP_CACHE_SEGMENT_COUNT), DEFAULT_CACHE_SEGMENT_COUNT);
        int cacheStackMoveDistance = toInteger(prop(PROP_CACHE_STACK_MOVE_DISTANCE), DEFAULT_CACHE_STACK_MOVE_DISTANCE);

        DocumentMK.Builder mkBuilder =
                new DocumentMK.Builder().
                memoryCacheSize(cacheSize * MB).
                memoryCacheDistribution(
                        nodeCachePercentage, 
                        childrenCachePercentage, 
                        docChildrenCachePercentage, 
                        diffCachePercentage).
                setCacheSegmentCount(cacheSegmentCount).
                setCacheStackMoveDistance(cacheStackMoveDistance).
                setLeaseCheck(true /* OAK-2739: enabled by default */).
                setLeaseFailureHandler(new LeaseFailureHandler() {
                    
                    @Override
                    public void handleLeaseFailure() {
                        try {
                            // plan A: try stopping oak-core
                            log.error("handleLeaseFailure: stopping oak-core...");
                            Bundle bundle = context.getBundleContext().getBundle();
                            bundle.stop();
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
                });

        if (persistentCache != null && persistentCache.length() > 0) {
            mkBuilder.setPersistentCache(persistentCache);
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
                                "blobCacheSize (MB)={}, maxReplicationLagInSecs={}",
                        mongoURI.getHosts(), db, cacheSize, persistentCache, blobCacheSize, maxReplicationLagInSecs);
                log.info("Mongo Connection details {}", MongoConnection.toString(mongoURI.getOptions()));
            }

            mkBuilder.setMaxReplicationLag(maxReplicationLagInSecs, TimeUnit.SECONDS);
            mkBuilder.setMongoDB(uri, db, blobCacheSize);

            log.info("Connected to database '{}'", db);
        }

        //Set wrapping blob store after setting the DB
        if (wrappingCustomBlobStore) {
            ((BlobStoreWrapper) blobStore).setBlobStore(mkBuilder.getBlobStore());
            mkBuilder.setBlobStore(blobStore);
        }

        mkBuilder.setExecutor(executor);
        mk = mkBuilder.open();

        // If a shared data store register the repo id in the data store
        if (SharedDataStoreUtils.isShared(blobStore)) {
            try {
                String repoId = ClusterRepositoryInfo.createId(mk.getNodeStore());
                ((SharedDataStore) blobStore).addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                    SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getNameFromId(repoId));
            } catch (Exception e) {
                throw new IOException("Could not register a unique repositoryId", e);
            }
        }

        registerJMXBeans(mk.getNodeStore());
        registerLastRevRecoveryJob(mk.getNodeStore());
        registerJournalGC(mk.getNodeStore());

        NodeStore store;
        DocumentNodeStore mns = mk.getNodeStore();
        store = mns;
        observerTracker = new ObserverTracker(mns);

        observerTracker.start(context.getBundleContext());

        DocumentStore ds = mk.getDocumentStore();

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

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, DocumentNodeStore.class.getName());
        props.put(DESCRIPTION, getMetadata(ds));
        // OAK-2844: in order to allow DocumentDiscoveryLiteService to directly
        // require a service DocumentNodeStore (instead of having to do an 'instanceof')
        // the registration is now done for both NodeStore and DocumentNodeStore here.
        reg = context.getBundleContext().registerService(new String[]{NodeStore.class.getName(), DocumentNodeStore.class.getName()}, store, props);
    }

    @Deactivate
    protected void deactivate() {
        if (observerTracker != null) {
            observerTracker.stop();
        }

        unregisterNodeStore();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindBlobStore(BlobStore blobStore) throws IOException {
        log.info("Initializing DocumentNodeStore with BlobStore [{}]", blobStore);
        this.blobStore = blobStore;
        registerNodeStoreIfPossible();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindBlobStore(BlobStore blobStore) {
        this.blobStore = null;
        unregisterNodeStore();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindDataSource(DataSource dataSource) throws IOException {
        log.info("Initializing DocumentNodeStore with dataSource [{}]", dataSource);
        this.dataSource = dataSource;
        registerNodeStoreIfPossible();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindDataSource(DataSource dataSource) {
        this.dataSource = null;
        unregisterNodeStore();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindBlobDataSource(DataSource dataSource) throws IOException {
        log.info("Initializing DocumentNodeStore with blobDataSource [{}]", dataSource);
        this.blobDataSource = dataSource;
        registerNodeStoreIfPossible();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindBlobDataSource(DataSource dataSource) {
        this.blobDataSource = null;
        unregisterNodeStore();
    }

    private void unregisterNodeStore() {
        deactivationTimestamp = System.currentTimeMillis();

        for (Registration r : registrations) {
            r.unregister();
        }
        registrations.clear();

        if (reg != null) {
            reg.unregister();
            reg = null;
        }

        if (mk != null) {
            mk.dispose();
            mk = null;
        }

        if (executor != null) {
            executor.stop();
            executor = null;
        }
    }

    private void registerJMXBeans(final DocumentNodeStore store) throws IOException {
        registrations.add(
                registerMBean(whiteboard,
                        CacheStatsMBean.class,
                        store.getNodeCacheStats(),
                        CacheStatsMBean.TYPE,
                        store.getNodeCacheStats().getName()));
        registrations.add(
                registerMBean(whiteboard,
                        CacheStatsMBean.class,
                        store.getNodeChildrenCacheStats(),
                        CacheStatsMBean.TYPE,
                        store.getNodeChildrenCacheStats().getName())
        );
        registrations.add(
                registerMBean(whiteboard,
                        CacheStatsMBean.class,
                        store.getDocChildrenCacheStats(),
                        CacheStatsMBean.TYPE,
                        store.getDocChildrenCacheStats().getName())
        );
        for (CacheStats cs : store.getDiffCacheStats()) {
            registrations.add(
                    registerMBean(whiteboard,
                            CacheStatsMBean.class, cs,
                            CacheStatsMBean.TYPE, cs.getName()));
        }
        DocumentStore ds = store.getDocumentStore();
        if (ds.getCacheStats() != null) {
            registrations.add(
                    registerMBean(whiteboard,
                            CacheStatsMBean.class,
                            ds.getCacheStats(),
                            CacheStatsMBean.TYPE,
                            ds.getCacheStats().getName())
            );
        }

        registrations.add(
                registerMBean(whiteboard,
                        CheckpointMBean.class,
                        new DocumentCheckpointMBean(store),
                        CheckpointMBean.TYPE,
                        "Document node store checkpoint management")
        );

        registrations.add(
                registerMBean(whiteboard,
                        DocumentNodeStoreMBean.class,
                        store.getMBean(),
                        DocumentNodeStoreMBean.TYPE,
                        "Document node store management")
        );

        final long versionGcMaxAgeInSecs = toLong(prop(PROP_VER_GC_MAX_AGE), DEFAULT_VER_GC_MAX_AGE);
        final long blobGcMaxAgeInSecs = toLong(prop(PROP_BLOB_GC_MAX_AGE), DEFAULT_BLOB_GC_MAX_AGE);

        if (store.getBlobStore() instanceof GarbageCollectableBlobStore) {
            BlobGarbageCollector gc = store.createBlobGarbageCollector(blobGcMaxAgeInSecs, 
                                                        ClusterRepositoryInfo.getId(mk.getNodeStore()));
            registrations.add(registerMBean(whiteboard, BlobGCMBean.class, new BlobGC(gc, executor),
                    BlobGCMBean.TYPE, "Document node store blob garbage collection"));
        }

        RevisionGC revisionGC = new RevisionGC(new Runnable() {
            @Override
            public void run() {
                try {
                    store.getVersionGarbageCollector().gc(versionGcMaxAgeInSecs, TimeUnit.SECONDS);
                } catch (IOException e) {
                    log.warn("Error occurred while executing the Version Garbage Collector", e);
                }
            }
        }, executor);
        registrations.add(registerMBean(whiteboard, RevisionGCMBean.class, revisionGC,
                RevisionGCMBean.TYPE, "Document node store revision garbage collection"));

        //TODO Register JMX bean for Off Heap Cache stats
    }

    private void registerLastRevRecoveryJob(final DocumentNodeStore nodeStore) {
        long leaseTime = toLong(context.getProperties().get(PROP_REV_RECOVERY_INTERVAL),
                ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS);
        Runnable recoverJob = new Runnable() {
            @Override
            public void run() {
                nodeStore.getLastRevRecoveryAgent().performRecoveryIfNeeded();
            }
        };
        registrations.add(WhiteboardUtils.scheduleWithFixedDelay(whiteboard,
                recoverJob, TimeUnit.MILLISECONDS.toSeconds(leaseTime)));
    }

    private void registerJournalGC(final DocumentNodeStore nodeStore) {
        long journalGCInterval = toLong(context.getProperties().get(PROP_JOURNAL_GC_INTERVAL_MILLIS),
                DEFAULT_JOURNAL_GC_INTERVAL_MILLIS);
        final long journalGCMaxAge = toLong(context.getProperties().get(PROP_JOURNAL_GC_MAX_AGE_MILLIS),
                DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS);
        Runnable journalGCJob = new Runnable() {

            @Override
            public void run() {
                nodeStore.getJournalGarbageCollector().gc(journalGCMaxAge, TimeUnit.MILLISECONDS);
            }

        };
        registrations.add(WhiteboardUtils.scheduleWithFixedDelay(whiteboard,
                journalGCJob, TimeUnit.MILLISECONDS.toSeconds(journalGCInterval), true/*runOnSingleClusterNode*/));
    }

    private Object prop(String propName) {
        return prop(propName, PREFIX + propName);
    }

    private Object prop(String propName, String fwkPropName) {
        return lookupFrameworkThenConfiguration(context, propName, fwkPropName);
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
}
