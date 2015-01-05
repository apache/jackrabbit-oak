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
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
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
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobGC;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
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

/**
 * The OSGi service to start/stop a DocumentNodeStore instance.
 */
@Component(policy = ConfigurationPolicy.REQUIRE)
public class DocumentNodeStoreService {
    private static final String DEFAULT_URI = "mongodb://localhost:27017/oak";
    private static final int DEFAULT_CACHE = 256;
    private static final int DEFAULT_OFF_HEAP_CACHE = 0;
    private static final int DEFAULT_CHANGES_SIZE = 256;
    private static final int DEFAULT_BLOB_CACHE_SIZE = 16;
    private static final String DEFAULT_DB = "oak";
    private static final String DEFAULT_PERSISTENT_CACHE = "";
    private static final String PREFIX = "oak.documentstore.";

    /**
     * Name of framework property to configure Mongo Connection URI
     */
    private static final String FWK_PROP_URI = "oak.mongo.uri";

    /**
     * Name of framework property to configure Mongo Database name
     * to use
     */
    private static final String FWK_PROP_DB = "oak.mongo.db";

    @Property(value = DEFAULT_URI)
    private static final String PROP_URI = "mongouri";

    @Property(value = DEFAULT_DB)
    private static final String PROP_DB = "db";

    @Property(intValue = DEFAULT_CACHE)
    private static final String PROP_CACHE = "cache";

    @Property(intValue = DEFAULT_OFF_HEAP_CACHE)
    private static final String PROP_OFF_HEAP_CACHE = "offHeapCache";

    @Property(intValue =  DEFAULT_CHANGES_SIZE)
    private static final String PROP_CHANGES_SIZE = "changesSize";

    @Property(intValue =  DEFAULT_BLOB_CACHE_SIZE)
    private static final String PROP_BLOB_CACHE_SIZE = "blobCacheSize";
    
    @Property(value =  DEFAULT_PERSISTENT_CACHE)
    private static final String PROP_PERSISTENT_CACHE = "persistentCache";

    /**
     * Boolean value indicating a blobStore is to be used
     */
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    /**
     * Boolean value indicating a different DataSource has to be used for
     * BlobStore
     */
    public static final String CUSTOM_BLOB_DATA_SOURCE = "customBlobDataSource";

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


    /**
     * Revisions older than this time would be garbage collected
     */
    private static final long DEFAULT_VER_GC_MAX_AGE = TimeUnit.DAYS.toSeconds(1);
    public static final String PROP_VER_GC_MAX_AGE = "versionGcMaxAgeInSecs";
    private long versionGcMaxAgeInSecs = DEFAULT_VER_GC_MAX_AGE;

    public static final String PROP_REV_RECOVERY_INTERVAL = "lastRevRecoveryJobIntervalInSecs";

    /**
     * Blob modified before this time duration would be considered for Blob GC
     */
    private static final long DEFAULT_BLOB_GC_MAX_AGE = TimeUnit.HOURS.toSeconds(24);
    public static final String PROP_BLOB_GC_MAX_AGE = "blobGcMaxAgeInSecs";
    private long blobGcMaxAgeInSecs = DEFAULT_BLOB_GC_MAX_AGE;

    private static final long DEFAULT_MAX_REPLICATION_LAG = TimeUnit.HOURS.toSeconds(6);
    public static final String PROP_REPLICATION_LAG = "maxReplicationLagInSecs";
    private long maxReplicationLagInSecs = DEFAULT_MAX_REPLICATION_LAG;

    /**
     * Specifies the type of DocumentStore MONGO, RDB
     */
    public static final String PROP_DS_TYPE = "documentStoreType";
    private DocumentStoreType documentStoreType;

    private boolean customBlobStore;
    private boolean customBlobDataSource;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) throws Exception {
        this.context = context;
        whiteboard = new OsgiWhiteboard(context.getBundleContext());
        executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        maxReplicationLagInSecs = toLong(config.get(PROP_REPLICATION_LAG), DEFAULT_MAX_REPLICATION_LAG);
        customBlobStore = toBoolean(prop(CUSTOM_BLOB_STORE), false);
        customBlobDataSource = toBoolean(prop(CUSTOM_BLOB_DATA_SOURCE), false);
        documentStoreType = DocumentStoreType.fromString(PropertiesUtil.toString(config.get(PROP_DS_TYPE), "MONGO"));

        modified(config);
        registerNodeStoreIfPossible();
    }

    private void registerNodeStoreIfPossible() throws IOException {
        if (context == null) {
            log.info("Component still not activated. Ignoring the initialization call");
        } else if (customBlobStore && blobStore == null) {
            log.info("BlobStore use enabled. DocumentNodeStoreService would be initialized when "
                    + "BlobStore would be available");
        } else if (documentStoreType == DocumentStoreType.RDB
                && (dataSource == null || (customBlobDataSource && blobDataSource == null))) {
            log.info("DataSource use enabled. DocumentNodeStoreService would be initialized when "
                    + "DataSource would be available");
        } else {
            registerNodeStore();
        }
    }

    private void registerNodeStore() throws IOException {
        String uri = PropertiesUtil.toString(prop(PROP_URI, FWK_PROP_URI), DEFAULT_URI);
        String db = PropertiesUtil.toString(prop(PROP_DB, FWK_PROP_DB), DEFAULT_DB);

        int offHeapCache = toInteger(prop(PROP_OFF_HEAP_CACHE), DEFAULT_OFF_HEAP_CACHE);
        int cacheSize = toInteger(prop(PROP_CACHE), DEFAULT_CACHE);
        int changesSize = toInteger(prop(PROP_CHANGES_SIZE), DEFAULT_CHANGES_SIZE);
        int blobCacheSize = toInteger(prop(PROP_BLOB_CACHE_SIZE), DEFAULT_BLOB_CACHE_SIZE);
        String persistentCache = PropertiesUtil.toString(prop(PROP_PERSISTENT_CACHE), DEFAULT_PERSISTENT_CACHE);

        DocumentMK.Builder mkBuilder =
                new DocumentMK.Builder().
                memoryCacheSize(cacheSize * MB).
                offHeapCacheSize(offHeapCache * MB);
        
        if (persistentCache != null && persistentCache.length() > 0) {
            mkBuilder.setPersistentCache(persistentCache);
        }

        //Set blobstore before setting the DB
        if (customBlobStore) {
            checkNotNull(blobStore, "Use of custom BlobStore enabled via  [%s] but blobStore reference not " +
                    "initialized", CUSTOM_BLOB_STORE);
            mkBuilder.setBlobStore(blobStore);
        }

        if (documentStoreType == DocumentStoreType.RDB){
            checkNotNull(dataSource, "DataStore type set [%s] but DataSource reference not initialized", PROP_DS_TYPE);
            if(customBlobDataSource){
                checkNotNull(blobDataSource, "DataStore type set [%s] and BlobStore is configured to use different " +
                        "DataSource via [%s] but BlobDataSource reference not initialized", PROP_DS_TYPE, CUSTOM_BLOB_DATA_SOURCE);
                mkBuilder.setRDBConnection(dataSource, blobDataSource);
                log.info("Connected to datasources {} {}", dataSource, blobDataSource);
            } else {
                mkBuilder.setRDBConnection(dataSource);
                log.info("Connected to datasource {}", dataSource);
            }
        } else {
            MongoClientOptions.Builder builder = MongoConnection.getDefaultBuilder();
            MongoClientURI mongoURI = new MongoClientURI(uri, builder);

            if (log.isInfoEnabled()) {
                // Take care around not logging the uri directly as it
                // might contain passwords
                log.info("Starting DocumentNodeStore with host={}, db={}, cache size (MB)={}, Off Heap Cache size (MB)={}, " +
                                "'changes' collection size (MB)={}, blobCacheSize (MB)={}, maxReplicationLagInSecs={}",
                        mongoURI.getHosts(), db, cacheSize, offHeapCache, changesSize, blobCacheSize, maxReplicationLagInSecs);
                log.info("Mongo Connection details {}", MongoConnection.toString(mongoURI.getOptions()));
            }

            MongoClient client = new MongoClient(mongoURI);
            DB mongoDB = client.getDB(db);

            mkBuilder.setMaxReplicationLag(maxReplicationLagInSecs, TimeUnit.SECONDS);
            mkBuilder.setMongoDB(mongoDB, changesSize, blobCacheSize);

            log.info("Connected to database {}", mongoDB);
        }

        mkBuilder.setExecutor(executor);
        mk = mkBuilder.open();

        registerJMXBeans(mk.getNodeStore());
        registerLastRevRecoveryJob(mk.getNodeStore());

        NodeStore store;
        DocumentNodeStore mns = mk.getNodeStore();
        store = mns;
        observerTracker = new ObserverTracker(mns);

        observerTracker.start(context.getBundleContext());

        Dictionary<String, String> props = new Hashtable<String, String>();
        props.put(Constants.SERVICE_PID, DocumentNodeStore.class.getName());
        reg = context.getBundleContext().registerService(NodeStore.class.getName(), store, props);
    }

    /**
     * At runtime DocumentNodeStore only pickup modification of certain properties
     */
    @Modified
    protected void modified(Map<String, ?> config){
        versionGcMaxAgeInSecs = toLong(config.get(PROP_VER_GC_MAX_AGE), DEFAULT_VER_GC_MAX_AGE);
        blobGcMaxAgeInSecs = toLong(config.get(PROP_BLOB_GC_MAX_AGE), DEFAULT_BLOB_GC_MAX_AGE);
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
        this.blobDataSource = dataSource;
        registerNodeStoreIfPossible();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindBlobDataSource(DataSource dataSource) {
        this.blobDataSource = null;
        unregisterNodeStore();
    }

    private void unregisterNodeStore() {
        for (Registration r : registrations) {
            r.unregister();
        }

        if (reg != null) {
            reg.unregister();
        }

        if (mk != null) {
            mk.dispose();
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

        DiffCache cl = store.getDiffCache();
        if (cl instanceof MemoryDiffCache) {
            MemoryDiffCache mcl = (MemoryDiffCache) cl;
            registrations.add(
                    registerMBean(whiteboard,
                            CacheStatsMBean.class,
                            mcl.getDiffCacheStats(),
                            CacheStatsMBean.TYPE,
                            mcl.getDiffCacheStats().getName()));
        }

        DocumentStore ds = store.getDocumentStore();
        if (ds instanceof CachingDocumentStore) {
            CachingDocumentStore cds = (CachingDocumentStore) ds;
            registrations.add(
                    registerMBean(whiteboard,
                            CacheStatsMBean.class,
                            cds.getCacheStats(),
                            CacheStatsMBean.TYPE,
                            cds.getCacheStats().getName())
            );
        }

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
                store.getVersionGarbageCollector().gc(versionGcMaxAgeInSecs, TimeUnit.SECONDS);
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
}
