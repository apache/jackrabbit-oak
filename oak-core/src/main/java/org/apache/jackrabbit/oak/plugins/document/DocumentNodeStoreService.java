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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.RevisionGC;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toLong;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_CHILDREN_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_DIFF_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder.DEFAULT_NODE_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

/**
 * The OSGi service to start/stop a DocumentNodeStore instance.
 */
@Component(policy = ConfigurationPolicy.REQUIRE)
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

    //DocumentMK would be done away with so better not
    //to expose this setting in config ui
    @Property(boolValue = false, propertyPrivate = true)
    private static final String PROP_USE_MK = "useMK";

    @Property(value = DEFAULT_URI)
    private static final String PROP_URI = "mongouri";

    @Property(value = DEFAULT_DB)
    private static final String PROP_DB = "db";

    @Property(intValue = DEFAULT_CACHE)
    private static final String PROP_CACHE = "cache";
    
    @Property(intValue = DEFAULT_NODE_CACHE_PERCENTAGE)
    private static final String PROP_NODE_CACHE_PERCENTAGE = "nodeCachePercentage";
    
    @Property(intValue = DEFAULT_CHILDREN_CACHE_PERCENTAGE)
    private static final String PROP_CHILDREN_CACHE_PERCENTAGE = "childrenCachePercentage";
    
    @Property(intValue = DEFAULT_DIFF_CACHE_PERCENTAGE)
    private static final String PROP_DIFF_CACHE_PERCENTAGE = "diffCachePercentage";
    
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

    @Property(intValue =  DEFAULT_BLOB_CACHE_SIZE)
    private static final String PROP_BLOB_CACHE_SIZE = "blobCacheSize";

    @Property(value =  DEFAULT_PERSISTENT_CACHE)
    private static final String PROP_PERSISTENT_CACHE = "persistentCache";

    /**
     * Boolean value indicating a blobStore is to be used
     */
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
            return valueOf(type.toUpperCase());
        }
    }

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private ServiceRegistration reg;
    private final List<Registration> registrations = new ArrayList<Registration>();
    private WhiteboardExecutor executor;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC)
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
    private static final long DEFAULT_VER_GC_MAX_AGE = TimeUnit.DAYS.toSeconds(1);
    public static final String PROP_VER_GC_MAX_AGE = "versionGcMaxAgeInSecs";

    public static final String PROP_REV_RECOVERY_INTERVAL = "lastRevRecoveryJobIntervalInSecs";

    /**
     * Blob modified before this time duration would be considered for Blob GC
     */
    private static final long DEFAULT_BLOB_GC_MAX_AGE = TimeUnit.HOURS.toSeconds(24);
	public static final String PROP_BLOB_GC_MAX_AGE = "blobGcMaxAgeInSecs";

    private static final long DEFAULT_MAX_REPLICATION_LAG = TimeUnit.HOURS.toSeconds(6);
    public static final String PROP_REPLICATION_LAG = "maxReplicationLagInSecs";
    private long maxReplicationLagInSecs = DEFAULT_MAX_REPLICATION_LAG;

    /**
     * Specifies the type of DocumentStore MONGO, RDB
     */
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
        int diffCachePercentage = toInteger(prop(PROP_DIFF_CACHE_PERCENTAGE), DEFAULT_DIFF_CACHE_PERCENTAGE);
        int blobCacheSize = toInteger(prop(PROP_BLOB_CACHE_SIZE), DEFAULT_BLOB_CACHE_SIZE);
        String persistentCache = PropertiesUtil.toString(prop(PROP_PERSISTENT_CACHE), DEFAULT_PERSISTENT_CACHE);
        int cacheSegmentCount = toInteger(prop(PROP_CACHE_SEGMENT_COUNT), DEFAULT_CACHE_SEGMENT_COUNT);
        int cacheStackMoveDistance = toInteger(prop(PROP_CACHE_STACK_MOVE_DISTANCE), DEFAULT_CACHE_STACK_MOVE_DISTANCE);
        boolean useMK = toBoolean(context.getProperties().get(PROP_USE_MK), false);

        DocumentMK.Builder mkBuilder =
                new DocumentMK.Builder().
                memoryCacheSize(cacheSize * MB).
                memoryCacheDistribution(
                        nodeCachePercentage, 
                        childrenCachePercentage, 
                        diffCachePercentage).
                setCacheSegmentCount(cacheSegmentCount).
                setCacheStackMoveDistance(cacheStackMoveDistance);

        if (persistentCache != null && persistentCache.length() > 0) {
            mkBuilder.setPersistentCache(persistentCache);
        }

        //Set blobstore before setting the DB
        if (customBlobStore) {
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
            MongoClientOptions.Builder builder = MongoConnection.getDefaultBuilder();
            MongoClientURI mongoURI = new MongoClientURI(uri, builder);

            if (log.isInfoEnabled()) {
                // Take care around not logging the uri directly as it
                // might contain passwords
                String type = useMK ? "MK" : "NodeStore";
                log.info("Starting Document{} with host={}, db={}, cache size (MB)={}, " +
                                "blobCacheSize (MB)={}, maxReplicationLagInSecs={}",
                        type, mongoURI.getHosts(), db, cacheSize, blobCacheSize, maxReplicationLagInSecs);
                log.info("Mongo Connection details {}", MongoConnection.toString(mongoURI.getOptions()));
            }

            MongoClient client = new MongoClient(mongoURI);
            DB mongoDB = client.getDB(db);

            mkBuilder.setMaxReplicationLag(maxReplicationLagInSecs, TimeUnit.SECONDS);
            mkBuilder.setMongoDB(mongoDB, blobCacheSize);

            log.info("Connected to database {}", mongoDB);
        }

        mkBuilder.setExecutor(executor);
        mk = mkBuilder.open();

        registerJMXBeans(mk.getNodeStore());
        registerLastRevRecoveryJob(mk.getNodeStore());
        registerJournalGC(mk.getNodeStore());

        NodeStore store;
        if (useMK) {
            KernelNodeStore kns = new KernelNodeStore(mk);
            store = kns;
            observerTracker = new ObserverTracker(kns);
        } else {
            DocumentNodeStore mns = mk.getNodeStore();
            store = mns;
            observerTracker = new ObserverTracker(mns);
        }

        observerTracker.start(context.getBundleContext());

        DocumentStore ds = mk.getDocumentStore();

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, DocumentNodeStore.class.getName());
        props.put(DESCRIPTION, getMetadata(ds));
        reg = context.getBundleContext().registerService(NodeStore.class.getName(), store, props);
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
                        DocumentNodeStoreMBean.class,
                        store.getMBean(),
                        DocumentNodeStoreMBean.TYPE,
                        "Document node store management")
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

        final long versionGcMaxAgeInSecs = toLong(prop(PROP_VER_GC_MAX_AGE), DEFAULT_VER_GC_MAX_AGE);
        final long blobGcMaxAgeInSecs = toLong(prop(PROP_BLOB_GC_MAX_AGE), DEFAULT_BLOB_GC_MAX_AGE);

        if (store.getBlobStore() instanceof GarbageCollectableBlobStore) {
            BlobGarbageCollector gc = new BlobGarbageCollector() {
                @Override
                public void collectGarbage() throws Exception {
                    store.createBlobGarbageCollector(blobGcMaxAgeInSecs).collectGarbage();
                }
            };
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
                recoverJob, TimeUnit.MILLISECONDS.toSeconds(leaseTime),
                false/*runOnSingleClusterNode*/, true /*use dedicated pool*/));
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
                journalGCJob, TimeUnit.MILLISECONDS.toSeconds(journalGCInterval),
                true/*runOnSingleClusterNode*/, true /*use dedicated pool*/));
    }

    private Object prop(String propName) {
        return prop(propName, PREFIX + propName);
    }

    private Object prop(String propName, String fwkPropName) {
        //Prefer framework property first
        Object value = context.getBundleContext().getProperty(fwkPropName);
        if (value != null) {
            return value;
        }

        //Fallback to one from config
        return context.getProperties().get(propName);
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
