/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.io.Closeables;
import org.apache.jackrabbit.guava.common.util.concurrent.AtomicDouble;
import com.mongodb.Block;
import com.mongodb.DBObject;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.WriteError;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;

import com.mongodb.client.model.CreateCollectionOptions;

import org.apache.jackrabbit.guava.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreStatsCollector;
import org.apache.jackrabbit.oak.plugins.document.JournalEntry;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.Throttler;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheChangesTracker;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.cache.ModificationStamp;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.bson.BSONException;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import static java.util.Objects.isNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Maps.filterKeys;
import static org.apache.jackrabbit.guava.common.collect.Sets.difference;
import static com.mongodb.client.model.Projections.include;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.asDocumentStoreException;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.DELETED_ONCE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NULL;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_MAX_REV_TIME_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_TYPE;
import static org.apache.jackrabbit.oak.plugins.document.Throttler.NO_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition.newEqualsCondition;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoThrottlerFactory.exponentialThrottler;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.createIndex;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.createPartialIndex;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.getDocumentStoreExceptionTypeFor;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.hasIndex;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isThrottlingEnabled;

/**
 * A document store that uses MongoDB as the backend.
 */
public class MongoDocumentStore implements DocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStore.class);
    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(MongoDocumentStore.class.getName()
                    + ".perf"));

    private static final Bson BY_ID_ASC = new BasicDBObject(Document.ID, 1);

    private static final String OPLOG_RS = "oplog.rs";

    /**
     * The threshold value <b>(in hours)</b> after which the document store should start (if enabled) throttling.
     * Default is 2 hours.
     * <p>
     * For mongo based document store this value is threshold for the oplog replication window.
     */
    public static final int DEFAULT_THROTTLING_THRESHOLD = Integer.getInteger("oak.mongo.throttlingThreshold", 2);
    /**
     * The default throttling time (in millis) when throttling is enabled. This is the time for
     * which we block any data modification operation when system has been throttled.
     */
    public static final long DEFAULT_THROTTLING_TIME_MS = Long.getLong("oak.mongo.throttlingTime", 20);
    /**
     * nodeNameLimit for node name based on Mongo Version
     */
    private final int nodeNameLimit;

    /**
     * throttler for mongo document store
     */
    private Throttler throttler = NO_THROTTLING;

    enum DocumentReadPreference {
        PRIMARY,
        PREFER_PRIMARY,
        PREFER_SECONDARY,
        PREFER_SECONDARY_IF_OLD_ENOUGH
    }

    public static final int IN_CLAUSE_BATCH_SIZE = 500;

    /**
     * A conflicting ID assignment on insert. Used by
     * {@link #sendBulkUpdate(Collection, java.util.Collection, Map)} for
     * {@code UpdateOp} with {@code isNew == false}. An upsert with this
     * operation will always fail when there is no existing document.
     */
    private static final Map CONFLICT_ON_INSERT = new BasicDBObject(
            "$setOnInsert",
            new BasicDBObject(Document.ID, "a").append(Document.ID, "b")
    ).toMap();

    private MongoCollection<BasicDBObject> nodes;
    private final MongoCollection<BasicDBObject> clusterNodes;
    private final MongoCollection<BasicDBObject> settings;
    private final MongoCollection<BasicDBObject> journal;

    private final MongoDBConnection connection;
    private final MongoDBConnection clusterNodesConnection;
    private final Map<String, String> mongoStorageOptions = new HashMap<>();

    private final NodeDocumentCache nodesCache;

    private final NodeDocumentLocks nodeLocks;

    private Clock clock = Clock.SIMPLE;

    private final long maxReplicationLagMillis;

    private final AtomicLong mongoWriteExceptions = new AtomicLong();

    /**
     * Duration in seconds under which queries would use index on _modified field
     * If set to -1 then modifiedTime index would not be used.
     * <p>
     * Default is 60 seconds.
     */
    private final long maxDeltaForModTimeIdxSecs =
            Long.getLong("oak.mongo.maxDeltaForModTimeIdxSecs", 60);

    /**
     * Disables the index hint sent to MongoDB.
     * This overrides {@link #maxDeltaForModTimeIdxSecs}.
     */
    private final boolean disableIndexHint =
            Boolean.getBoolean("oak.mongo.disableIndexHint");

    /**
     * Duration in milliseconds after which a mongo query will be terminated.
     * <p>
     * If this value is -1 no timeout is being set at all, if it is 1 or greater
     * this translated to MongoDB's maxTimeNS being set accordingly.
     * <p>
     * Default is 60'000 (one minute).
     * See: http://mongodb.github.io/node-mongodb-native/driver-articles/anintroductionto1_4_and_2_6.html#maxtimems
     */
    private final long maxQueryTimeMS =
            Long.getLong("oak.mongo.maxQueryTimeMS", TimeUnit.MINUTES.toMillis(1));

    /**
     * The number of documents to put into one bulk update.
     * <p>
     * Default is 30.
     */
    private int bulkSize =
            Integer.getInteger("oak.mongo.bulkSize", 30);

    /**
     * How many times should be the bulk update request retries in case of
     * a conflict.
     * <p>
     * Default is 0 (no retries).
     */
    private int bulkRetries =
            Integer.getInteger("oak.mongo.bulkRetries", 0);

    /**
     * How many times a query to MongoDB should be retried when it fails with a
     * MongoException.
     */
    private final int queryRetries =
            Integer.getInteger("oak.mongo.queryRetries", 2);

    /**
     * Acceptable replication lag of secondaries in milliseconds. Reads are
     * directed to the primary if the estimated replication lag is higher than
     * this value.
     */
    private final int acceptableLagMillis =
            Integer.getInteger("oak.mongo.acceptableLagMillis", 5000);

    /**
     * The minimal number of documents to prefetch.
     */
    private final int minPrefetch =
            Integer.getInteger("oak.mongo.minPrefetch", 5);

    /**
     * Feature flag for use of MongoDB client sessions.
     */
    private final boolean useClientSession;

    private String lastReadWriteMode;

    private final Map<String, String> metadata;

    private DocumentStoreStatsCollector stats;

    /**
     * An updater instance to periodically updates mongo oplog window
     */
    private MongoDocumentStoreThrottlingMetricsUpdater throttlingMetricsUpdater;

    private boolean hasModifiedIdCompoundIndex = true;

    private static final Key KEY_MODIFIED = new Key(MODIFIED_IN_SECS, null);

    private final boolean readOnly;

    @Override
    public int getNodeNameLimit() {
        return nodeNameLimit;
    }

    /**
     * Return the {@link Throttler} for the underlying store
     * Default is no throttling
     *
     * @return throttler for document store
     */
    @Override
    public Throttler throttler() {
        return throttler;
    }

    public MongoDocumentStore(MongoClient connection, MongoDatabase db,
                              MongoDocumentNodeStoreBuilderBase<?> builder) {
        this.readOnly = builder.getReadOnlyMode();
        MongoStatus status = builder.getMongoStatus();
        if (status == null) {
            status = new MongoStatus(connection, db.getName());
        }
        status.checkVersion();
        metadata = Map.of(
                "type", "mongo",
                "version", status.getVersion());

        this.nodeNameLimit = MongoUtils.getNodeNameLimit(status);
        this.connection = new MongoDBConnection(connection, db, status, builder.getMongoClock());
        this.clusterNodesConnection = getOrCreateClusterNodesConnection(builder);
        stats = builder.getDocumentStoreStatsCollector();
        nodes = this.connection.getCollection(Collection.NODES.toString());
        clusterNodes = this.clusterNodesConnection.getCollection(Collection.CLUSTER_NODES.toString());
        settings = this.connection.getCollection(Collection.SETTINGS.toString());
        journal = this.connection.getCollection(Collection.JOURNAL.toString());
        initializeMongoStorageOptions(builder);

        maxReplicationLagMillis = builder.getMaxReplicationLagMillis();

        useClientSession = !builder.isClientSessionDisabled()
                && Boolean.parseBoolean(System.getProperty("oak.mongo.clientSession", "true"));

        if (!readOnly) {
            ensureIndexes(db, status);
        }

        this.nodeLocks = new StripedNodeDocumentLocks();
        this.nodesCache = builder.buildNodeDocumentCache(this, nodeLocks);

        // if throttling is enabled
        final boolean throttlingEnabled = isThrottlingEnabled(builder);
        if (throttlingEnabled) {
            MongoDatabase localDb = connection.getDatabase("local");
            Optional<String> ol = StreamSupport.stream(localDb.listCollectionNames().spliterator(), false)
                    .filter(s -> Objects.equals(OPLOG_RS, s)).findFirst();

            if (ol.isPresent()) {
                // oplog window based on current oplog filling rate
                final AtomicDouble oplogWindow = new AtomicDouble(MAX_VALUE);
                throttler = exponentialThrottler(DEFAULT_THROTTLING_THRESHOLD, oplogWindow, DEFAULT_THROTTLING_TIME_MS);
                throttlingMetricsUpdater = new MongoDocumentStoreThrottlingMetricsUpdater(localDb, oplogWindow);
                throttlingMetricsUpdater.scheduleUpdateMetrics();
                LOG.info("Started MongoDB throttling metrics with threshold {}, throttling time {}",
                        DEFAULT_THROTTLING_THRESHOLD, DEFAULT_THROTTLING_TIME_MS);
            } else {
                LOG.warn("Connected to MongoDB with replication not detected and hence oplog based throttling is not supported");
            }
        }

        LOG.info("Connected to MongoDB {} with maxReplicationLagMillis {}, " +
                "maxDeltaForModTimeIdxSecs {}, disableIndexHint {}, " +
                "leaseSocketTimeout {}, clientSessionSupported {}, " +
                "clientSessionInUse {}, {}, serverStatus {}, throttlingSupported {}",
                status.getVersion(), maxReplicationLagMillis,
                maxDeltaForModTimeIdxSecs, disableIndexHint,
                builder.getLeaseSocketTimeout(),
                status.isClientSessionSupported(), useClientSession,
                db.getWriteConcern(), status.getServerDetails(), throttlingEnabled);
    }

    // constructs storage options from config
    private void initializeMongoStorageOptions(MongoDocumentNodeStoreBuilderBase<?> builder) {
        if (builder.getCollectionCompressionType() != null) {
            this.mongoStorageOptions.put(MongoDBConfig.COLLECTION_COMPRESSION_TYPE, builder.getCollectionCompressionType());
        }
    }

    @NotNull
    private MongoDBConnection getOrCreateClusterNodesConnection(@NotNull MongoDocumentNodeStoreBuilderBase<?> builder) {
        MongoDBConnection mc;
        int leaseSocketTimeout = builder.getLeaseSocketTimeout();
        if (leaseSocketTimeout > 0) {
            mc = builder.createMongoDBClient(leaseSocketTimeout);
        } else {
            // use same connection
            mc = connection;
        }
        return mc;
    }

    private void ensureIndexes(@NotNull MongoDatabase db, @NotNull MongoStatus mongoStatus) {
        // reading documents in the nodes collection and checking
        // existing indexes is performed against the MongoDB primary
        // this ensures the information is up-to-date and accurate
        boolean emptyNodesCollection = execute(session -> MongoUtils.isCollectionEmpty(nodes, session), Collection.NODES);
        createCollection(db, Collection.NODES.toString(), mongoStatus);
        // compound index on _modified and _id
        if (emptyNodesCollection) {
            // this is an empty store, create a compound index
            // on _modified and _id (OAK-3071)

            createIndex(nodes, new String[]{NodeDocument.MODIFIED_IN_SECS, Document.ID},
                    new boolean[]{true, true}, false, false);
        } else if (!hasIndex(nodes.withReadPreference(ReadPreference.primary()),
                NodeDocument.MODIFIED_IN_SECS, Document.ID)) {
            hasModifiedIdCompoundIndex = false;
            LOG.warn("Detected an upgrade from Oak version <= 1.2. For optimal " +
                    "performance it is recommended to create a compound index " +
                    "for the 'nodes' collection on {_modified:1, _id:1}.");
        }

        // index on the _bin flag to faster access nodes with binaries for GC
        createIndex(nodes, NodeDocument.HAS_BINARY_FLAG, true, false, true);

        // index on _deleted for fast lookup of potentially garbage
        // depending on the MongoDB version, create a partial index
        if (emptyNodesCollection) {
            if (mongoStatus.isVersion(3, 2)) {
                createPartialIndex(nodes, new String[]{DELETED_ONCE, MODIFIED_IN_SECS},
                        new boolean[]{true, true}, "{" + DELETED_ONCE + ":true}");
            } else {
                createIndex(nodes, NodeDocument.DELETED_ONCE, true, false, true);
            }
        } else if (!hasIndex(nodes.withReadPreference(ReadPreference.primary()),
                DELETED_ONCE, MODIFIED_IN_SECS)) {
            LOG.warn("Detected an upgrade from Oak version <= 1.6. For optimal " +
                    "Revision GC performance it is recommended to create a " +
                    "partial index for the 'nodes' collection on " +
                    "{_deletedOnce:1, _modified:1} with a partialFilterExpression " +
                    "{_deletedOnce:true}. Partial indexes require MongoDB 3.2 " +
                    "or higher.");
        }

        // compound index on _sdType and _sdMaxRevTime
        if (emptyNodesCollection) {
            // this is an empty store, create compound index
            // on _sdType and _sdMaxRevTime (OAK-6129)
            createIndex(nodes, new String[]{SD_TYPE, SD_MAX_REV_TIME_IN_SECS},
                    new boolean[]{true, true}, false, true);
        } else if (!hasIndex(nodes.withReadPreference(ReadPreference.primary()),
                SD_TYPE, SD_MAX_REV_TIME_IN_SECS)) {
            LOG.warn("Detected an upgrade from Oak version <= 1.6. For optimal " +
                    "Revision GC performance it is recommended to create a " +
                    "sparse compound index for the 'nodes' collection on " +
                    "{_sdType:1, _sdMaxRevTime:1}.");
        }

        // index on _modified for journal entries
        createIndex(journal, JournalEntry.MODIFIED, true, false, false);
    }

    private void createCollection(MongoDatabase db, String collectionName, MongoStatus mongoStatus) {
        CreateCollectionOptions options = new CreateCollectionOptions();

        if (mongoStatus.isVersion(4, 2)) {
            options.storageEngineOptions(MongoDBConfig.getCollectionStorageOptions(mongoStorageOptions));
            if (!Iterables.tryFind(db.listCollectionNames(), s -> Objects.equals(collectionName, s)).isPresent()) {
                db.createCollection(collectionName, options);
                LOG.info("Creating Collection {}, with collection storage options", collectionName);
            }
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        // TODO should not be needed, but it seems
        // oak-jcr doesn't call dispose()
        dispose();
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        InvalidationResult result = new InvalidationResult();
        for (CacheValue key : nodesCache.keys()) {
            result.invalidationCount++;
            invalidateCache(Collection.NODES, key.toString());
        }
        return result;
    }

    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        LOG.debug("invalidateCache: start");
        final InvalidationResult result = new InvalidationResult();
        int size  = 0;

        final Iterator<String> it = keys.iterator();
        while(it.hasNext()) {
            // read chunks of documents only
            final List<String> ids = new ArrayList<String>(IN_CLAUSE_BATCH_SIZE);
            while(it.hasNext() && ids.size() < IN_CLAUSE_BATCH_SIZE) {
                final String id = it.next();
                if (nodesCache.getIfPresent(id) != null) {
                    // only add those that we actually do have cached
                    ids.add(id);
                }
            }
            size += ids.size();
            if (LOG.isTraceEnabled()) {
                LOG.trace("invalidateCache: batch size: {} of total so far {}",
                        ids.size(), size);
            }

            Map<String, ModificationStamp> modStamps = getModStamps(ids);
            result.queryCount++;

            int invalidated = nodesCache.invalidateOutdated(modStamps);
            for (String id : filter(ids, x -> !modStamps.keySet().contains(x))) {
                nodesCache.invalidate(id);
                invalidated++;
            }
            result.cacheEntriesProcessedCount += ids.size();
            result.invalidationCount += invalidated;
            result.upToDateCount += ids.size() - invalidated;
        }

        result.cacheSize = size;
        LOG.trace("invalidateCache: end. total: {}", size);
        return result;
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        if (collection == Collection.NODES) {
            nodesCache.invalidate(key);
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        final long start = PERFLOG.start();
        final T result = find(collection, key, true, -1);
        PERFLOG.end(start, 1, "find: preferCached=true, key={}", key);
        return result;
    }

    @Override
    public <T extends Document> T find(final Collection<T> collection,
                                       final String key,
                                       int maxCacheAge) {
        final long start = PERFLOG.start();
        final T result = find(collection, key, false, maxCacheAge);
        PERFLOG.end(start, 1, "find: preferCached=false, key={}", key);
        return result;
    }

    @SuppressWarnings("unchecked")
    private <T extends Document> T find(final Collection<T> collection,
                                       final String key,
                                       boolean preferCached,
                                       final int maxCacheAge) {
        if (collection != Collection.NODES) {
            DocumentReadPreference readPref = DocumentReadPreference.PRIMARY;
            if (withClientSession()) {
                readPref = getDefaultReadPreference(collection);
            }
            return findUncachedWithRetry(collection, key, readPref);
        }
        NodeDocument doc;
        if (maxCacheAge > 0 || preferCached) {
            // first try without lock
            doc = nodesCache.getIfPresent(key);
            if (doc != null) {
                if (preferCached ||
                        getTime() - doc.getCreated() < maxCacheAge) {
                    stats.doneFindCached(collection, key);
                    if (doc == NodeDocument.NULL) {
                        return null;
                    }
                    return (T) doc;
                }
            }
        }
        Throwable t;
        try {
            Lock lock = nodeLocks.acquire(key);
            try {
                if (maxCacheAge > 0 || preferCached) {
                    // try again some other thread may have populated
                    // the cache by now
                    doc = nodesCache.getIfPresent(key);
                    if (doc != null) {
                        if (preferCached ||
                                getTime() - doc.getCreated() < maxCacheAge) {
                            stats.doneFindCached(collection, key);
                            if (doc == NodeDocument.NULL) {
                                return null;
                            }
                            return (T) doc;
                        }
                    }
                }
                final NodeDocument d = (NodeDocument) findUncachedWithRetry(
                        collection, key,
                        getReadPreference(maxCacheAge));
                invalidateCache(collection, key);
                doc = nodesCache.get(key, new Callable<NodeDocument>() {
                    @Override
                    public NodeDocument call() throws Exception {
                        return d == null ? NodeDocument.NULL : d;
                    }
                });
            } finally {
                lock.unlock();
            }
            if (doc == NodeDocument.NULL) {
                return null;
            } else {
                return (T) doc;
            }
        } catch (UncheckedExecutionException e) {
            t = e.getCause();
        } catch (ExecutionException e) {
            t = e.getCause();
        } catch (RuntimeException e) {
            t = e;
        }
        throw handleException(t, collection, key);
    }

    /**
     * Finds a document and performs a number of retries if the read fails with
     * an exception.
     *
     * @param collection the collection to read from.
     * @param key the key of the document to find.
     * @param docReadPref the read preference.
     * @param <T> the document type of the given collection.
     * @return the document or {@code null} if the document doesn't exist.
     */
    @Nullable
    private <T extends Document> T findUncachedWithRetry(
            Collection<T> collection, String key,
            DocumentReadPreference docReadPref) {
        if (key.equals("0:/")) {
            LOG.trace("root node");
        }
        int numAttempts = queryRetries + 1;
        MongoException ex = null;
        for (int i = 0; i < numAttempts; i++) {
            if (i > 0) {
                LOG.warn("Retrying read of " + key);
            }
            try {
                return findUncached(collection, key, docReadPref);
            } catch (MongoException e) {
                ex = e;
                LOG.warn("findUncachedWithRetry : read fails with an exception" + e, e);
            }
        }
        if (ex != null) {
            throw handleException(ex, collection, key);
        } else {
            // impossible to get here
            throw new IllegalStateException();
        }
    }

    @Nullable
    protected <T extends Document> T findUncached(Collection<T> collection, String key, DocumentReadPreference docReadPref) {
        log("findUncached", key, docReadPref);
        final Stopwatch watch = startWatch();
        boolean isSlaveOk = false;
        boolean docFound = true;
        try {
            ReadPreference readPreference = getMongoReadPreference(collection, null, docReadPref);
            MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection, readPreference);

            if(readPreference.isSlaveOk()){
                LOG.trace("Routing call to secondary for fetching [{}]", key);
                isSlaveOk = true;
            }

            List<BasicDBObject> result = new ArrayList<>(1);
            execute(session -> {
                if (session != null) {
                    dbCollection.find(session, getByKeyQuery(key)).into(result);
                } else {
                    dbCollection.find(getByKeyQuery(key)).into(result);
                }
                return null;
            }, collection);

            if(result.isEmpty()) {
                docFound = false;
                return null;
            }
            T doc = convertFromDBObject(collection, result.get(0));
            if (doc != null) {
                doc.seal();
            }
            return doc;
        } finally {
            stats.doneFindUncached(watch.elapsed(TimeUnit.NANOSECONDS), collection, key, docFound, isSlaveOk);
        }
    }

    @NotNull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection,
                                String fromKey,
                                String toKey,
                                int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @NotNull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              String indexedProperty,
                                              long startValue,
                                              int limit) {
        return query(collection, fromKey, toKey, indexedProperty, startValue, limit, emptyList());
    }

    @NotNull
    @Override
    public <T extends Document> List<T> query(final Collection<T> collection,
                                              final String fromKey,
                                              final String toKey,
                                              final String indexedProperty,
                                              final long startValue,
                                              final int limit,
                                              final List<String> projection) throws DocumentStoreException {
        return queryWithRetry(collection, fromKey, toKey, indexedProperty, startValue, limit, projection, maxQueryTimeMS);
    }

    /**
     * Queries for documents and performs a number of retries if the read fails
     * with an exception.
     */
    @NotNull
    private <T extends Document> List<T> queryWithRetry(Collection<T> collection,
                                                        String fromKey,
                                                        String toKey,
                                                        String indexedProperty,
                                                        long startValue,
                                                        int limit,
                                                        List<String> projection,
                                                        long maxQueryTime) {
        int numAttempts = queryRetries + 1;
        MongoException ex = null;
        for (int i = 0; i < numAttempts; i++) {
            if (i > 0) {
                LOG.warn("Retrying query, fromKey={}, toKey={}", fromKey, toKey);
            }
            try {
                return queryInternal(collection, fromKey, toKey,
                        indexedProperty, startValue, limit, projection, maxQueryTime);
            } catch (MongoException e) {
                ex = e;
            }
        }
        if (ex != null) {
            throw handleException(ex, collection, List.of(fromKey, toKey));
        } else {
            // impossible to get here
            throw new IllegalStateException();
        }
    }

    @SuppressWarnings("unchecked")
    @NotNull
    protected <T extends Document> List<T> queryInternal(Collection<T> collection,
                                                         String fromKey,
                                                         String toKey,
                                                         String indexedProperty,
                                                         long startValue,
                                                         int limit,
                                                         List<String> projection,
                                                         long maxQueryTime) {
        log("query", fromKey, toKey, indexedProperty, startValue, limit);

        List<Bson> clauses = new ArrayList<>();
        clauses.add(Filters.gt(Document.ID, fromKey));
        clauses.add(Filters.lt(Document.ID, toKey));

        Bson hint;
        if (NodeDocument.MODIFIED_IN_SECS.equals(indexedProperty)
                && canUseModifiedTimeIdx(startValue)) {
            hint = new BasicDBObject(NodeDocument.MODIFIED_IN_SECS, 1);
        } else {
            hint = new BasicDBObject(NodeDocument.ID, 1);
        }

        if (indexedProperty != null) {
            if (NodeDocument.DELETED_ONCE.equals(indexedProperty)) {
                if (startValue != 1) {
                    throw new DocumentStoreException(
                            "unsupported value for property " + 
                                    NodeDocument.DELETED_ONCE);
                }
                clauses.add(Filters.eq(indexedProperty, true));
            } else {
                clauses.add(Filters.gte(indexedProperty, startValue));
            }
        }
        Bson query = Filters.and(clauses);
        String parentId = Utils.getParentIdFromLowerLimit(fromKey);
        long lockTime = -1;
        final Stopwatch watch = startWatch();

        boolean isSlaveOk = false;
        int resultSize = 0;
        CacheChangesTracker cacheChangesTracker = null;
        if (parentId != null && collection == Collection.NODES && (projection == null || projection.isEmpty())) {
            cacheChangesTracker = nodesCache.registerTracker(fromKey, toKey);
        }
        try {
            ReadPreference readPreference =
                    getMongoReadPreference(collection, parentId, getDefaultReadPreference(collection));

            if(readPreference.isSlaveOk()){
                isSlaveOk = true;
                LOG.trace("Routing call to secondary for fetching children from [{}] to [{}]", fromKey, toKey);
            }

            List<T> list = new ArrayList<T>();
            MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection, readPreference);
            execute(session -> {
                FindIterable<BasicDBObject> result;
                if (session != null) {
                    result = dbCollection.find(session, query);
                } else {
                    result = dbCollection.find(query);
                }

                if (projection != null && !projection.isEmpty()) {
                    result.projection(include(projection));
                }

                result.sort(BY_ID_ASC);
                if (limit >= 0) {
                    result.limit(limit);
                }
                if (!disableIndexHint && !hasModifiedIdCompoundIndex) {
                    result.hint(hint);
                }
                if (maxQueryTime > 0) {
                    // OAK-2614: set maxTime if maxQueryTimeMS > 0
                    result.maxTime(maxQueryTime, TimeUnit.MILLISECONDS);
                }

                try (MongoCursor<BasicDBObject> cursor = result.iterator()) {
                    for (int i = 0; i < limit && cursor.hasNext(); i++) {
                        BasicDBObject o = cursor.next();
                        T doc = convertFromDBObject(collection, o);
                        list.add(doc);
                    }
                }
                return null;
            }, collection);
            resultSize = list.size();

            if (cacheChangesTracker != null) {
                nodesCache.putNonConflictingDocs(cacheChangesTracker, (List<NodeDocument>) list);
            }

            return list;
        } finally {
            if (cacheChangesTracker != null) {
                cacheChangesTracker.close();
            }
            stats.doneQuery(watch.elapsed(TimeUnit.NANOSECONDS), collection, fromKey, toKey,
                    indexedProperty != null , resultSize, lockTime, isSlaveOk);
        }
    }

    boolean canUseModifiedTimeIdx(long modifiedTimeInSecs) {
        if (maxDeltaForModTimeIdxSecs < 0) {
            return false;
        }
        return (NodeDocument.getModifiedInSecs(getTime()) - modifiedTimeInSecs) <= maxDeltaForModTimeIdxSecs;
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        log("remove", key);
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        Stopwatch watch = startWatch();
        try {
            execute(session -> {
                Bson filter = getByKeyQuery(key);
                if (session != null) {
                    dbCollection.deleteOne(session, filter);
                } else {
                    dbCollection.deleteOne(filter);
                }
                return null;
            }, collection);
        } catch (Exception e) {
            throw DocumentStoreException.convert(e, "Remove failed for " + key);
        } finally {
            invalidateCache(collection, key);
            stats.doneRemove(watch.elapsed(TimeUnit.NANOSECONDS), collection, 1);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        log("remove", keys);
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        Stopwatch watch = startWatch();
        try {
            for(List<String> keyBatch : CollectionUtils.partitionList(keys, IN_CLAUSE_BATCH_SIZE)){
                Bson query = Filters.in(Document.ID, keyBatch);
                try {
                    execute(session -> {
                        if (session != null) {
                            dbCollection.deleteMany(session, query);
                        } else {
                            dbCollection.deleteMany(query);
                        }
                        return null;
                    }, collection);
                } catch (Exception e) {
                    throw DocumentStoreException.convert(e, "Remove failed for " + keyBatch);
                } finally {
                    if (collection == Collection.NODES) {
                        for (String key : keyBatch) {
                            invalidateCache(collection, key);
                        }
                    }
                }
            }
        } finally {
            stats.doneRemove(watch.elapsed(TimeUnit.NANOSECONDS), collection, keys.size());
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Long> toRemove) {
        log("remove", toRemove);
        int num = 0;
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        Stopwatch watch = startWatch();
        try {
            List<String> batchIds = new ArrayList<>();
            List<Bson> batch = new ArrayList<>();
            Iterator<Entry<String, Long>> it = toRemove.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Long> entry = it.next();
                Condition c = newEqualsCondition(entry.getValue());
                Bson clause = createQueryForUpdate(entry.getKey(),
                        Collections.singletonMap(KEY_MODIFIED, c));
                batchIds.add(entry.getKey());
                batch.add(clause);
                if (!it.hasNext() || batch.size() == IN_CLAUSE_BATCH_SIZE) {
                    Bson query = Filters.or(batch);
                    try {
                        num += execute(session -> {
                            DeleteResult result;
                            if (session != null) {
                                result = dbCollection.deleteMany(session, query);
                            } else {
                                result = dbCollection.deleteMany(query);
                            }
                            return result.getDeletedCount();
                        }, collection);
                    } catch (Exception e) {
                        throw DocumentStoreException.convert(e, "Remove failed for " + batch);
                    } finally {
                        if (collection == Collection.NODES) {
                            invalidateCache(batchIds);
                        }
                    }
                    batchIds.clear();
                    batch.clear();
                }
            }
        } finally {
            stats.doneRemove(watch.elapsed(TimeUnit.NANOSECONDS), collection, num);
        }
        return num;
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                    String indexedProperty, long startValue, long endValue)
            throws DocumentStoreException {
        log("remove", collection, indexedProperty, startValue, endValue);
        int num = 0;
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        Stopwatch watch = startWatch();
        try {
            Bson query = Filters.and(
                    Filters.gt(indexedProperty, startValue),
                    Filters.lt(indexedProperty, endValue)
            );
            try {
                num = (int) Math.min(execute((DocumentStoreCallable<Long>) session -> {
                    DeleteResult result;
                    if (session != null) {
                        result = dbCollection.deleteMany(session, query);
                    } else {
                        result = dbCollection.deleteMany(query);
                    }
                    return result.getDeletedCount();
                }, collection), MAX_VALUE);
            } catch (Exception e) {
                throw DocumentStoreException.convert(e, "Remove failed for " + collection + ": " +
                    indexedProperty + " in (" + startValue + ", " + endValue + ")");
            } finally {
                if (collection == Collection.NODES) {
                    // this method is currently being used only for Journal collection while GC.
                    // But, to keep sanctity of the API, we need to acknowledge that Nodes collection
                    // could've been used. But, in this signature, there's no useful way to invalidate
                    // cache.
                    // So, we use the hammer for this task
                    invalidateCache();
                }
            }
        } finally {
            stats.doneRemove(watch.elapsed(TimeUnit.NANOSECONDS), collection, num);
        }
        return num;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private <T extends Document> T findAndModify(Collection<T> collection,
                                                 UpdateOp updateOp,
                                                 boolean upsert,
                                                 boolean checkConditions) {
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        // make sure we don't modify the original updateOp
        updateOp = updateOp.copy();
        Bson update = createUpdate(updateOp, !upsert);

        Lock lock = null;
        if (collection == Collection.NODES) {
            lock = nodeLocks.acquire(updateOp.getId());
        }
        final Stopwatch watch = startWatch();
        boolean newEntry = false;

        try {
            // get modCount of cached document
            Long modCount = null;
            T cachedDoc = null;
            if (collection == Collection.NODES) {
                cachedDoc = (T) nodesCache.getIfPresent(updateOp.getId());
                if (cachedDoc != null) {
                    modCount = cachedDoc.getModCount();
                }
            }

            // perform a conditional update with limited result
            // if we have a matching modCount
            if (modCount != null) {
                // only perform the conditional update when there are
                // no conditions and the check is OK. this avoid an
                // unnecessary call when the conditions do not match
                if (!checkConditions || UpdateUtils.checkConditions(cachedDoc, updateOp.getConditions())) {
                    // below condition may overwrite a user supplied condition
                    // on _modCount. This is fine, because the conditions were
                    // already checked against the cached document with the
                    // matching _modCount value. There is no need to check the
                    // user supplied condition on _modCount again on the server
                    Bson query = Filters.and(
                            createQueryForUpdate(updateOp.getId(), updateOp.getConditions()),
                            Filters.eq(Document.MOD_COUNT, modCount)
                    );

                    UpdateResult result = execute(session -> {
                        if (session != null) {
                            return dbCollection.updateOne(session, query, update);
                        } else {
                            return dbCollection.updateOne(query, update);
                        }
                    }, collection);
                    if (result.getModifiedCount() > 0) {
                        // success, update cached document
                        if (collection == Collection.NODES) {
                            NodeDocument newDoc = (NodeDocument) applyChanges(collection, cachedDoc, updateOp);
                            nodesCache.put(newDoc);
                        }
                        // return previously cached document
                        return cachedDoc;
                    }
                }
            }

            // conditional update failed or not possible
            // perform operation and get complete document
            Bson query = createQueryForUpdate(updateOp.getId(), updateOp.getConditions());
            FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
                    .returnDocument(ReturnDocument.BEFORE).upsert(upsert);
            BasicDBObject oldNode = execute(session -> {
                if (session != null) {
                    return dbCollection.findOneAndUpdate(session, query, update, options);
                } else {
                    return dbCollection.findOneAndUpdate(query, update, options);
                }
            }, collection);

            if (oldNode == null && upsert) {
                newEntry = true;
            }

            if (checkConditions && oldNode == null) {
                return null;
            }

            T oldDoc = convertFromDBObject(collection, oldNode);
            if (oldDoc != null) {
                if (collection == Collection.NODES) {
                    NodeDocument newDoc = (NodeDocument) applyChanges(collection, oldDoc, updateOp);
                    nodesCache.put(newDoc);
                }
                oldDoc.seal();
            } else if (upsert) {
                if (collection == Collection.NODES) {
                    NodeDocument doc = (NodeDocument) collection.newDocument(this);
                    UpdateUtils.applyChanges(doc, updateOp);
                    nodesCache.putIfAbsent(doc);
                }
            } else {
                // updateOp without conditions and not an upsert
                // this means the document does not exist
                if (collection == Collection.NODES) {
                    nodesCache.invalidate(updateOp.getId());
                }
            }
            return oldDoc;
        } catch (MongoWriteException e) {
            WriteError werr = e.getError();
            LOG.error("Failed to update the document with Id={} with MongoWriteException message = '{}'. Document statistics: {}.",
                    updateOp.getId(), werr.getMessage(), produceDiagnostics(collection, updateOp.getId()), e);
            throw handleException(e, collection, updateOp.getId());
        } catch (MongoCommandException e) {
            LOG.error("Failed to update the document with Id={} with MongoCommandException message ='{}'. ",
                    updateOp.getId(), e.getMessage());
            throw handleException(e, collection, updateOp.getId());
        } catch (Exception e) {
            throw handleException(e, collection, updateOp.getId());
        } finally {
            if (lock != null) {
                lock.unlock();
            }
            stats.doneFindAndModify(watch.elapsed(TimeUnit.NANOSECONDS), collection, updateOp.getId(),
                    newEntry, true, 0);
        }
    }

    private <T extends Document> String produceDiagnostics(Collection<T> col, String id) {
        StringBuilder t = new StringBuilder();

        try {
            T doc = find(col, id);
            if (doc != null) {
                t.append("_id: " + doc.getId() + ", _modCount: " + doc.getModCount() + ", memory: " + doc.getMemory());
                t.append("; Contents: ");
                t.append(Utils.mapEntryDiagnostics(doc.entrySet()));
            }
        } catch (Throwable thisIsBestEffort) {
            t.append(thisIsBestEffort.getMessage());
        }

        return t.toString();
    }

    /**
     * Try to apply all the {@link UpdateOp}s with at least MongoDB requests as
     * possible. The return value is the list of the old documents (before
     * applying changes). The mechanism is as follows:
     *
     * <ol>
     * <li>For each UpdateOp try to read the assigned document from the cache.
     *     Add them to {@code oldDocs}.</li>
     * <li>Prepare a list of all UpdateOps that doesn't have their documents and
     *     read them in one find() call. Add results to {@code oldDocs}.</li>
     * <li>Prepare a bulk update. For each remaining UpdateOp add following
     *     operation:
     *   <ul>
     *   <li>Find document with the same id and the same mod_count as in the
     *       {@code oldDocs}.</li>
     *   <li>Apply changes from the UpdateOps.</li>
     *   </ul>
     * </li>
     * <li>Execute the bulk update.</li>
     * </ol>
     *
     * If some other process modifies the target documents between points 2 and
     * 3, the mod_count will be increased as well and the bulk update will fail
     * for the concurrently modified docs. The method will then remove the
     * failed documents from the {@code oldDocs} and restart the process from
     * point 2. It will stop after 3rd iteration.
     *
     * @see #createOrUpdate(Collection, List)
     */
    @Override
    @NotNull
    public <T extends Document> List<T> findAndUpdate(final @NotNull Collection<T> collection, final @NotNull List<UpdateOp> updateOps) {
        log("findAndUpdate", updateOps);
        final Map<String, UpdateOp> operationsToCover = new LinkedHashMap<>(updateOps.size());
        final List<UpdateOp> duplicates = new ArrayList<>();
        final Map<UpdateOp, T> results = new LinkedHashMap<>(updateOps.size());

        final Stopwatch watch = startWatch();
        int retryCount = 0;
        try {
            for (UpdateOp updateOp : updateOps) {
                UpdateOp clone = updateOp.copy();
                if (operationsToCover.containsKey(updateOp.getId())) {
                    duplicates.add(clone);
                } else {
                    operationsToCover.put(updateOp.getId(), clone);
                }
                results.put(clone, null);
            }

            Map<String, T> oldDocs = new HashMap<>(updateOps.size());
            if (collection == NODES) {
                oldDocs.putAll((Map<String, T>) getCachedNodes(operationsToCover.keySet()));
            }

            for (int i = 0; i <= bulkRetries; i++) {
                retryCount = i;
                if (operationsToCover.size() <= 2) {
                    // bulkUpdate() method invokes Mongo twice, so sending 2 updates
                    // in bulk mode wouldn't result in any performance gain
                    break;
                }
                for (List<UpdateOp> partition : CollectionUtils.partitionList(new ArrayList<>(operationsToCover.values()), bulkSize)) {
                    Map<UpdateOp, T> successfulUpdates = bulkModify(collection, partition, oldDocs);
                    results.putAll(successfulUpdates);
                    operationsToCover.values().removeAll(successfulUpdates.keySet());
                }
            }

            // if there are some changes left, we'll apply them one after another i.e. failed ones
            final Iterator<UpdateOp> it = Iterators.concat(operationsToCover.values().iterator(), duplicates.iterator());
            while (it.hasNext()) {
                UpdateOp op = it.next();
                it.remove();
                results.put(op, findAndUpdate(collection, op));
            }
        } catch (MongoException e) {
            throw handleException(e, collection, Iterables.transform(updateOps, UpdateOp::getId));
        } finally {
            stats.doneFindAndModify(watch.elapsed(NANOSECONDS), collection, updateOps.stream().map(UpdateOp::getId).collect(toList()),
                    true, retryCount);
        }
        final List<T> resultList = new ArrayList<>(results.values());
        log("findAndUpdate returns", resultList);
        return resultList;
    }

    @Nullable
    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws DocumentStoreException {
        log("createOrUpdate", update);
        UpdateUtils.assertUnconditional(update);
        T doc = findAndModify(collection, update, update.isNew(), false);
        log("createOrUpdate returns ", doc);
        return doc;
    }

    /**
     * Try to apply all the {@link UpdateOp}s with at least MongoDB requests as
     * possible. The return value is the list of the old documents (before
     * applying changes). The mechanism is as follows:
     *
     * <ol>
     * <li>For each UpdateOp try to read the assigned document from the cache.
     *     Add them to {@code oldDocs}.</li>
     * <li>Prepare a list of all UpdateOps that doesn't have their documents and
     *     read them in one find() call. Add results to {@code oldDocs}.</li>
     * <li>Prepare a bulk update. For each remaining UpdateOp add following
     *     operation:
     *   <ul>
     *   <li>Find document with the same id and the same mod_count as in the
     *       {@code oldDocs}.</li>
     *   <li>Apply changes from the UpdateOps.</li>
     *   </ul>
     * </li>
     * <li>Execute the bulk update.</li>
     * </ol>
     *
     * If some other process modifies the target documents between points 2 and
     * 3, the mod_count will be increased as well and the bulk update will fail
     * for the concurrently modified docs. The method will then remove the
     * failed documents from the {@code oldDocs} and restart the process from
     * point 2. It will stop after 3rd iteration.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
        log("createOrUpdate", updateOps);

        Map<String, UpdateOp> operationsToCover = new LinkedHashMap<String, UpdateOp>();
        List<UpdateOp> duplicates = new ArrayList<UpdateOp>();
        Map<UpdateOp, T> results = new LinkedHashMap<UpdateOp, T>();

        final Stopwatch watch = startWatch();
        try {
            for (UpdateOp updateOp : updateOps) {
                UpdateUtils.assertUnconditional(updateOp);
                UpdateOp clone = updateOp.copy();
                if (operationsToCover.containsKey(updateOp.getId())) {
                    duplicates.add(clone);
                } else {
                    operationsToCover.put(updateOp.getId(), clone);
                }
                results.put(clone, null);
            }

            Map<String, T> oldDocs = new HashMap<String, T>();
            if (collection == Collection.NODES) {
                oldDocs.putAll((Map<String, T>) getCachedNodes(operationsToCover.keySet()));
            }

            for (int i = 0; i <= bulkRetries; i++) {
                if (operationsToCover.size() <= 2) {
                    // bulkUpdate() method invokes Mongo twice, so sending 2 updates
                    // in bulk mode wouldn't result in any performance gain
                    break;
                }
                for (List<UpdateOp> partition : CollectionUtils.partitionList(new ArrayList<>(operationsToCover.values()), bulkSize)) {
                    Map<UpdateOp, T> successfulUpdates = bulkUpdate(collection, partition, oldDocs);
                    results.putAll(successfulUpdates);
                    operationsToCover.values().removeAll(successfulUpdates.keySet());
                }
            }

            // if there are some changes left, we'll apply them one after another
            Iterator<UpdateOp> it = Iterators.concat(operationsToCover.values().iterator(), duplicates.iterator());
            while (it.hasNext()) {
                UpdateOp op = it.next();
                it.remove();
                T oldDoc = createOrUpdate(collection, op);
                if (oldDoc != null) {
                    results.put(op, oldDoc);
                }
            }
        } catch (MongoException e) {
            throw handleException(e, collection, Iterables.transform(updateOps,
                    input -> input.getId()));
        } finally {
            stats.doneCreateOrUpdate(watch.elapsed(TimeUnit.NANOSECONDS),
                    collection, Lists.transform(updateOps, input -> input.getId()));
        }
        List<T> resultList = new ArrayList<T>(results.values());
        log("createOrUpdate returns", resultList);
        return resultList;
    }

    private Map<String, NodeDocument> getCachedNodes(Set<String> keys) {
        Map<String, NodeDocument> nodes = new HashMap<String, NodeDocument>();
        for (String key : keys) {
            NodeDocument cached = nodesCache.getIfPresent(key);
            if (cached != null) {
                nodes.put(key, cached);
            }
        }
        return nodes;
    }

    @NotNull
    private <T extends Document> Map<UpdateOp, T> bulkModify(final Collection<T> collection, final List<UpdateOp> updateOps,
                                                             final Map<String, T> oldDocs) {
        Map<String, UpdateOp> bulkOperations = createMap(updateOps);
        Set<String> lackingDocs = difference(bulkOperations.keySet(), oldDocs.keySet());
        oldDocs.putAll(findDocuments(collection, lackingDocs));

        CacheChangesTracker tracker = null;
        if (collection == NODES) {
            tracker = nodesCache.registerTracker(bulkOperations.keySet());
        }

        try {
            final BulkRequestResult bulkResult = sendBulkRequest(collection, bulkOperations.values(), oldDocs, false);
            final Set<String> potentiallyUpdatedDocsSet = difference(bulkOperations.keySet(), bulkResult.failedUpdates);

            final Map<String, T> updatedDocsMap = new HashMap<>(potentiallyUpdatedDocsSet.size());

            final List<NodeDocument> docsToCache = new ArrayList<>();

            if (bulkResult.modifiedCount == potentiallyUpdatedDocsSet.size()) {
                // all documents had been updated, now we can simply
                // apply the update op on oldDocs and update the cache
                potentiallyUpdatedDocsSet.forEach(key -> {
                    T oldDoc = oldDocs.get(key);
                    if (isNull(oldDoc) || oldDoc == NULL) {
                        log("Skipping updating doc cache for ", key);
                        return;
                    }
                    T newDoc = applyChanges(collection, oldDoc, bulkOperations.get(key));
                    updatedDocsMap.put(newDoc.getId(), newDoc);
                    if (collection == NODES) {
                        docsToCache.add((NodeDocument) newDoc);
                    }
                });
            } else {
                // some documents might have not been updated, lets fetch them from database
                // and found out which had not been updated
                updatedDocsMap.putAll(findDocuments(collection, potentiallyUpdatedDocsSet));

                updatedDocsMap.forEach((key, value) -> {
                    T oldDoc = oldDocs.get(key);
                    if (isNull(oldDoc) || oldDoc == NULL || Objects.equals(oldDoc.getModCount(), value.getModCount())) {
                        // simply ignore updating the document cache in case
                        // 1. oldDoc is null
                        // 2. document didn't get updated i.e. modCount is same after update operation
                        log("Skipping updating doc cache for ", key);
                        return;
                    }
                    if (collection == NODES) {
                        docsToCache.add((NodeDocument) applyChanges(collection, oldDoc, bulkOperations.get(key)));
                    }
                });
            }

            // updates nodesCache if
            if (collection == NODES && docsToCache.size() > 0) {
                nodesCache.putNonConflictingDocs(tracker, docsToCache);
            }
            oldDocs.keySet().removeAll(bulkResult.failedUpdates);

            final Map<UpdateOp, T> result = new HashMap<>(oldDocs.size());

            // document might have been updated, if updated then add oldDoc else add null to result
            bulkOperations.entrySet().stream().filter(e -> !bulkResult.failedUpdates.contains(e.getKey())).forEach(e -> {
                T updated = updatedDocsMap.get(e.getKey());
                T oldDoc = oldDocs.get(e.getKey());
                if (oldDoc == null || oldDoc == NULL || Objects.equals(oldDoc.getModCount(), updated.getModCount())) {
                    // add null value in result cause, either this document didn't exist
                    // at time of modify operation, and we didn't anything for it
                    // or oldDoc is present and modCount is same,
                    // so document had not been updated.
                    log(" didn't get updated, returning null.", e.getKey());
                    result.put(e.getValue(), null);
                } else {
                    // document had been updated, seal the document and add in the result map
                    oldDoc.seal();
                    result.put(e.getValue(), oldDoc);
                }
            });
            return result;
        } finally {
            if (tracker != null) {
                tracker.close();
            }
        }
    }

    private <T extends Document> Map<UpdateOp, T> bulkUpdate(Collection<T> collection,
                                                             List<UpdateOp> updateOperations,
                                                             Map<String, T> oldDocs) {
        Map<String, UpdateOp> bulkOperations = createMap(updateOperations);
        Set<String> lackingDocs = difference(bulkOperations.keySet(), oldDocs.keySet());
        oldDocs.putAll(findDocuments(collection, lackingDocs));

        CacheChangesTracker tracker = null;
        if (collection == Collection.NODES) {
            tracker = nodesCache.registerTracker(bulkOperations.keySet());
        }

        try {
            BulkRequestResult bulkResult = sendBulkRequest(collection, bulkOperations.values(), oldDocs, true);

            if (collection == Collection.NODES) {
                List<NodeDocument> docsToCache = new ArrayList<NodeDocument>();
                for (UpdateOp op : filterKeys(bulkOperations, x -> bulkResult.upserts.contains(x)).values()) {
                    NodeDocument doc = Collection.NODES.newDocument(this);
                    UpdateUtils.applyChanges(doc, op);
                    docsToCache.add(doc);
                }

                for (String key : difference(bulkOperations.keySet(), bulkResult.failedUpdates)) {
                    T oldDoc = oldDocs.get(key);
                    if (oldDoc != null && oldDoc != NodeDocument.NULL) {
                        NodeDocument newDoc = (NodeDocument) applyChanges(collection, oldDoc, bulkOperations.get(key));
                        docsToCache.add(newDoc);
                    }
                }

                nodesCache.putNonConflictingDocs(tracker, docsToCache);
            }
            oldDocs.keySet().removeAll(bulkResult.failedUpdates);

            Map<UpdateOp, T> result = new HashMap<UpdateOp, T>();
            for (Entry<String, UpdateOp> entry : bulkOperations.entrySet()) {
                if (bulkResult.failedUpdates.contains(entry.getKey())) {
                    continue;
                } else if (bulkResult.upserts.contains(entry.getKey())) {
                    result.put(entry.getValue(), null);
                } else {
                    result.put(entry.getValue(), oldDocs.get(entry.getKey()));
                }
            }
            return result;
        } finally {
            if (tracker != null) {
                tracker.close();
            }
        }
    }

    private static Map<String, UpdateOp> createMap(List<UpdateOp> updateOps) {
        return Maps.uniqueIndex(updateOps, input -> input.getId());
    }

    private <T extends Document> Map<String, T> findDocuments(Collection<T> collection, Set<String> keys) {
        try {
            Map<String, T> docs = new HashMap<String, T>();
            if (!keys.isEmpty()) {
                List<Bson> conditions = new ArrayList<>(keys.size());
                for (String key : keys) {
                    conditions.add(getByKeyQuery(key));
                }
                MongoCollection<BasicDBObject> dbCollection;
                if (secondariesWithinAcceptableLag()) {
                    dbCollection = getDBCollection(collection);
                } else {
                    lagTooHigh();
                    dbCollection = getDBCollection(collection).withReadPreference(ReadPreference.primary());
                }
                execute(session -> {
                    FindIterable<BasicDBObject> cursor;
                    if (session != null) {
                        cursor = dbCollection.find(session, Filters.or(conditions));
                    } else {
                        cursor = dbCollection.find(Filters.or(conditions));
                    }
                    for (BasicDBObject doc : cursor) {
                        T foundDoc = convertFromDBObject(collection, doc);
                        docs.put(foundDoc.getId(), foundDoc);
                    }
                    return null;
                }, collection);
            }
            return docs;
        } catch (BSONException ex) {
            // TODO: refactor, see OAK-10650
            LOG.error("trying bulk find, retrying one-by-one", ex);
            return findDocumentsOneByOne(collection, keys);
        }
    }

    // variant of findDocuments that avoids BSON exception by iterating instead of doing a bulk operation
    private <T extends Document> Map<String, T> findDocumentsOneByOne(Collection<T> collection, Set<String> keys) {
        Map<String, T> docs = new HashMap<String, T>();
        for (String key : keys) {
            Bson condition = getByKeyQuery(key);

            MongoCollection<BasicDBObject> dbCollection;
            if (secondariesWithinAcceptableLag()) {
                dbCollection = getDBCollection(collection);
            } else {
                lagTooHigh();
                dbCollection = getDBCollection(collection).withReadPreference(ReadPreference.primary());
            }
            execute(session -> {
                FindIterable<BasicDBObject> cursor;
                if (session != null) {
                    cursor = dbCollection.find(session, condition);
                } else {
                    cursor = dbCollection.find(condition);
                }
                for (BasicDBObject doc : cursor) {
                    T foundDoc = convertFromDBObject(collection, doc);
                    docs.put(foundDoc.getId(), foundDoc);
                }
                return null;
            }, collection);
        }

        return docs;
    }

    @NotNull
    private <T extends Document> BulkRequestResult sendBulkRequest(final Collection<T> collection,
                                                                   final java.util.Collection<UpdateOp> updateOps,
                                                                   final Map<String, T> oldDocs,
                                                                   final boolean isUpsert) {
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        List<WriteModel<BasicDBObject>> writes = new ArrayList<>(updateOps.size());
        String[] bulkIds = new String[updateOps.size()];
        int i = 0;
        for (UpdateOp updateOp : updateOps) {
            String id = updateOp.getId();
            Bson query = createQueryForUpdate(id, updateOp.getConditions());
            // fail on insert when isNew == false OR isUpsert == false
            boolean failInsert = !(isUpsert && updateOp.isNew());
            T oldDoc = oldDocs.get(id);
            if (oldDoc == null || oldDoc == NodeDocument.NULL) {
                query = Filters.and(query, Filters.exists(Document.MOD_COUNT, false));
            } else {
                query = Filters.and(query, Filters.eq(Document.MOD_COUNT, oldDoc.getModCount()));
            }
            writes.add(new UpdateOneModel<>(query, createUpdate(updateOp, failInsert), new UpdateOptions().upsert(isUpsert))
            );
            bulkIds[i++] = id;
        }

        BulkWriteResult bulkResult;
        Set<String> failedUpdates = new HashSet<String>();
        Set<String> upserts = new HashSet<String>();
        BulkWriteOptions options = new BulkWriteOptions().ordered(false);
        try {
            bulkResult = execute(session -> {
                if (session != null) {
                    return dbCollection.bulkWrite(session, writes, options);
                } else {
                    return dbCollection.bulkWrite(writes, options);
                }
            }, collection);
        } catch (MongoBulkWriteException e) {
            bulkResult = e.getWriteResult();
            for (BulkWriteError err : e.getWriteErrors()) {
                failedUpdates.add(bulkIds[err.getIndex()]);
            }
        }
        for (BulkWriteUpsert upsert : bulkResult.getUpserts()) {
            upserts.add(bulkIds[upsert.getIndex()]);
        }
        return new BulkRequestResult(failedUpdates, upserts, bulkResult.getModifiedCount());
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update)
            throws DocumentStoreException {
        log("findAndUpdate", update);
        T doc = findAndModify(collection, update, false, true);
        log("findAndUpdate returns ", doc);
        return doc;
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        log("create", updateOps);
        List<T> docs = new ArrayList<T>();
        List<BasicDBObject> inserts = new ArrayList<>(updateOps.size());
        List<String> ids = new ArrayList<>(updateOps.size());

        for (UpdateOp update : updateOps) {
            BasicDBObject doc = new BasicDBObject();
            inserts.add(doc);
            doc.put(Document.ID, update.getId());
            UpdateUtils.assertUnconditional(update);
            T target = collection.newDocument(this);
            UpdateUtils.applyChanges(target, update);
            docs.add(target);
            ids.add(update.getId());
            for (Entry<Key, Operation> entry : update.getChanges().entrySet()) {
                Key k = entry.getKey();
                Operation op = entry.getValue();
                switch (op.type) {
                    case SET:
                    case MAX:
                    case INCREMENT: {
                        doc.put(k.toString(), op.value);
                        break;
                    }
                    case SET_MAP_ENTRY: {
                        Revision r = k.getRevision();
                        if (r == null) {
                            throw new IllegalStateException("SET_MAP_ENTRY must not have null revision");
                        }
                        BasicDBObject value = (BasicDBObject) doc.get(k.getName());
                        if (value == null) {
                            value = new BasicDBObject();
                            doc.put(k.getName(), value);
                        }
                        value.put(r.toString(), op.value);
                        break;
                    }
                    case REMOVE:
                    case REMOVE_MAP_ENTRY:
                        // nothing to do for new entries
                        break;
                }
            }
            if (!doc.containsField(Document.MOD_COUNT)) {
                doc.put(Document.MOD_COUNT, 1L);
                target.put(Document.MOD_COUNT, 1L);
            }
        }

        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        final Stopwatch watch = startWatch();
        boolean insertSuccess = false;
        try {
            try {
                execute(session -> {
                    if (session != null) {
                        dbCollection.insertMany(session, inserts);
                    } else {
                        dbCollection.insertMany(inserts);
                    }
                    return null;
                }, collection);
                if (collection == Collection.NODES) {
                    for (T doc : docs) {
                        nodesCache.putIfAbsent((NodeDocument) doc);
                    }
                }
                insertSuccess = true;
                return true;
            } catch (BsonMaximumSizeExceededException e) {
                for (T doc : docs) {
                    LOG.error("Failed to create one of the documents " +
                                    "with BsonMaximumSizeExceededException message = '{}'. " +
                                    "The document id={} has estimated size={} in VM.",
                                    e.getMessage(), doc.getId(), doc.getMemory());
                }
                return false;
            } catch (MongoException e) {
                LOG.warn("Encountered MongoException while inserting documents: {} - exception: {}",
                        ids, e.getMessage());
                return false;
            }
        } finally {
            stats.doneCreate(watch.elapsed(TimeUnit.NANOSECONDS), collection, ids, insertSuccess);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Document> void prefetch(Collection<T> collection,
                                              Iterable<String> keysToPrefetch) {
        log("prefetch", keysToPrefetch);

        Set<String> keys = new HashSet<>();
        for (String k : keysToPrefetch) {
            if (nodesCache.getIfPresent(k) == null) {
                keys.add(k);
            }
        }
        if (keys.size() < minPrefetch) {
            return;
        }

        final Stopwatch watch = startWatch();
        List<String> resultKeys = new ArrayList<>(keys.size());
        CacheChangesTracker tracker = null;
        if (collection == Collection.NODES) {
            // keys set is modified later. create a copy of the keys set
            // owned by the cache changes tracker.
            tracker = nodesCache.registerTracker(new HashSet<>(keys));
        }
        Throwable t;
        try {
            ReadPreference readPreference = getMongoReadPreference(collection, null, getDefaultReadPreference(collection));
            MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection, readPreference);

            if (readPreference.isSlaveOk()) {
                LOG.trace("Routing call to secondary for prefetching [{}]", keys);
            }

            List<BasicDBObject> result = new ArrayList<>(keys.size());
            execute(session -> {
                final Bson query = Filters.in(Document.ID, keys);
                if (session != null) {
                    dbCollection.find(session, query).into(result);
                } else {
                    dbCollection.find(query).into(result);
                }
                return null;
            }, collection);

            List<T> docs = new ArrayList<>(keys.size());
            for (BasicDBObject dbObject : result) {
                final T d = convertFromDBObject(collection, dbObject);
                if (d == null) {
                    continue;
                }
                d.seal();
                String key = String.valueOf(d.get(Document.ID));
                resultKeys.add(key);
                keys.remove(key);
                docs.add(d);
            }

            if (tracker != null) {
                nodesCache.putNonConflictingDocs(tracker, (List<NodeDocument>) docs);

                // documents for remaining ids in keys do not exist
                for (String id : keys) {
                    Lock lock = nodeLocks.acquire(id);
                    try {
                        // load NULL document into cache unless it may have
                        // been affected by another concurrent operation
                        if (!tracker.mightBeenAffected(id)) {
                            nodesCache.get(id, () -> NULL);
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
            return;
        } catch (UncheckedExecutionException | ExecutionException e) {
            t = e.getCause();
        } catch (RuntimeException e) {
            t = e;
        } finally {
            if (tracker != null) {
                tracker.close();
            }
            stats.donePrefetch(watch.elapsed(TimeUnit.NANOSECONDS), collection, resultKeys);
        }
        throw handleException(t, collection, keysToPrefetch);
    }

    /**
     * Returns the {@link Document#MOD_COUNT} and
     * {@link NodeDocument#MODIFIED_IN_SECS} values of the documents with the
     * given {@code keys}. The returned map will only contain entries for
     * existing documents. The default value is -1 if the document does not have
     * a modCount field. The same applies to the modified field.
     *
     * @param keys the keys of the documents.
     * @return map with key to modification stamp mapping.
     * @throws MongoException if the call fails
     */
    @NotNull
    private Map<String, ModificationStamp> getModStamps(Iterable<String> keys)
            throws MongoException {
        // Fetch only the modCount and id
        final BasicDBObject fields = new BasicDBObject(Document.ID, 1);
        fields.put(Document.MOD_COUNT, 1);
        fields.put(NodeDocument.MODIFIED_IN_SECS, 1);

        Map<String, ModificationStamp> modCounts = new HashMap<>();

        nodes.withReadPreference(ReadPreference.primary())
                .find(Filters.in(Document.ID, keys)).projection(fields)
                .forEach((Block<BasicDBObject>) obj -> {
                    String id = (String) obj.get(Document.ID);
                    Long modCount = Utils.asLong((Number) obj.get(Document.MOD_COUNT));
                    if (modCount == null) {
                        modCount = -1L;
                    }
                    Long modified = Utils.asLong((Number) obj.get(NodeDocument.MODIFIED_IN_SECS));
                    if (modified == null) {
                        modified = -1L;
                    }
                    modCounts.put(id, new ModificationStamp(modCount, modified));
                });
        return modCounts;
    }

    DocumentReadPreference getReadPreference(int maxCacheAge) {
        if (withClientSession()) {
            return DocumentReadPreference.PREFER_SECONDARY;
        } else if(maxCacheAge >= 0 && maxCacheAge < maxReplicationLagMillis) {
            return DocumentReadPreference.PRIMARY;
        } else if(maxCacheAge == MAX_VALUE){
            return DocumentReadPreference.PREFER_SECONDARY;
        } else {
           return DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH;
        }
    }

    DocumentReadPreference getDefaultReadPreference(Collection col) {
        DocumentReadPreference preference = DocumentReadPreference.PRIMARY;
        if (withClientSession()) {
            preference = DocumentReadPreference.PREFER_SECONDARY;
        } else if (col == Collection.NODES) {
            preference = DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH;
        }
        return preference;
    }

    <T extends Document> ReadPreference getMongoReadPreference(@NotNull Collection<T> collection,
                                                               @Nullable String parentId,
                                                               @NotNull DocumentReadPreference preference) {
        switch(preference){
            case PRIMARY:
                return ReadPreference.primary();
            case PREFER_PRIMARY :
                return ReadPreference.primaryPreferred();
            case PREFER_SECONDARY :
                if (!withClientSession() || secondariesWithinAcceptableLag()) {
                    return getConfiguredReadPreference(collection);
                } else {
                    lagTooHigh();
                    return ReadPreference.primary();
                }
            case PREFER_SECONDARY_IF_OLD_ENOUGH:
                if(collection != Collection.NODES){
                    return ReadPreference.primary();
                }

                boolean secondarySafe;
                if (withClientSession() && secondariesWithinAcceptableLag()) {
                    secondarySafe = true;
                } else {
                   // This is not quite accurate, because ancestors
                    // are updated in a background thread (_lastRev). We
                    // will need to revise this for low maxReplicationLagMillis
                    // values
                    long replicationSafeLimit = getTime() - maxReplicationLagMillis;

                    if (parentId == null) {
                        secondarySafe = false;
                    } else {
                        //If parent has been modified loooong time back then there children
                        //would also have not be modified. In that case we can read from secondary
                        NodeDocument cachedDoc = getIfCached(Collection.NODES, parentId);
                        secondarySafe = cachedDoc != null && !cachedDoc.hasBeenModifiedSince(replicationSafeLimit);
                    }
                }

                ReadPreference readPreference;
                if (secondarySafe) {
                    readPreference = getConfiguredReadPreference(collection);
                } else {
                    readPreference = ReadPreference.primary();
                }

                return readPreference;
            default:
                throw new IllegalArgumentException("Unsupported usage " + preference);
        }
    }

    /**
     * Retrieves the ReadPreference specified for the Mongo DB in use irrespective of
     * DBCollection. Depending on deployments the user can tweak the default references
     * to read from secondary and in that also tag secondaries
     *
     * @return db level ReadPreference
     */
    <T extends Document> ReadPreference getConfiguredReadPreference(Collection<T> collection){
        return getDBCollection(collection).getReadPreference();
    }

    @Nullable
    protected <T extends Document> T convertFromDBObject(@NotNull Collection<T> collection,
                                                         @Nullable DBObject n) {
        T copy = null;
        if (n != null) {
            copy = collection.newDocument(this);
            for (String key : n.keySet()) {
                Object o = n.get(key);
                if (o instanceof String) {
                    copy.put(key, o);
                } else if (o instanceof Number &&
                        (NodeDocument.MODIFIED_IN_SECS.equals(key) || Document.MOD_COUNT.equals(key))) {
                    copy.put(key, Utils.asLong((Number) o));
                } else if (o instanceof Long) {
                    copy.put(key, o);
                } else if (o instanceof Integer) {
                    copy.put(key, o);
                } else if (o instanceof Boolean) {
                    copy.put(key, o);
                } else if (o instanceof BasicDBObject) {
                    copy.put(key, convertMongoMap((BasicDBObject) o));
                }
            }
        }
        return copy;
    }

    @NotNull
    private Map<Revision, Object> convertMongoMap(@NotNull BasicDBObject obj) {
        Map<Revision, Object> map = new TreeMap<Revision, Object>(StableRevisionComparator.REVERSE);
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            map.put(Revision.fromString(entry.getKey()), entry.getValue());
        }
        return map;
    }

    <T extends Document> MongoCollection<BasicDBObject> getDBCollection(Collection<T> collection) {
        if (collection == Collection.NODES) {
            return nodes;
        } else if (collection == Collection.CLUSTER_NODES) {
            return clusterNodes;
        } else if (collection == Collection.SETTINGS) {
            return settings;
        } else if (collection == Collection.JOURNAL) {
            return journal;
        } else {
            throw new IllegalArgumentException(
                    "Unknown collection: " + collection.toString());
        }
    }

    <T extends Document> MongoCollection<BasicDBObject> getDBCollection(Collection<T> collection,
                                                                        ReadPreference readPreference) {
        return getDBCollection(collection).withReadPreference(readPreference);
    }

    MongoDatabase getDatabase() {
        return connection.getDatabase();
    }

    MongoClient getClient() {
        return connection.getClient();
    }

    private static Bson getByKeyQuery(String key) {
        return Filters.eq(Document.ID, key);
    }

    @Override
    public void dispose() {
        connection.close();
        if (clusterNodesConnection != connection) {
            clusterNodesConnection.close();
        }
        try {
            Closeables.close(throttlingMetricsUpdater, false);
        } catch (IOException e) {
            LOG.warn("Error occurred while closing throttlingMetricsUpdater", e);
        }
        try {
            nodesCache.close();
        } catch (IOException e) {
            LOG.warn("Error occurred while closing nodes cache", e);
        }
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        return nodesCache.getCacheStats();
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @NotNull
    @Override
    public Map<String, String> getStats() {
        Map<String, String> builder = new HashMap<>();
        List<MongoCollection<?>> all = ImmutableList.of(nodes, clusterNodes, settings, journal);
        all.forEach(c -> toMapBuilder(builder,
                connection.getDatabase().runCommand(
                    new BasicDBObject("collStats", c.getNamespace().getCollectionName()),
                        BasicDBObject.class),
                c.getNamespace().getCollectionName()));
        return Collections.unmodifiableMap(builder);
    }

    long getMaxDeltaForModTimeIdxSecs() {
        return maxDeltaForModTimeIdxSecs;
    }

    boolean getDisableIndexHint() {
        return disableIndexHint;
    }

    private static void log(String message, Object... args) {
        if (LOG.isDebugEnabled()) {
            String argList = Arrays.toString(args);
            if (argList.length() > 10000) {
                argList = argList.length() + ": " + argList;
            }
            LOG.debug(message + argList);
        }
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        if (collection != Collection.NODES) {
            return null;
        }
        @SuppressWarnings("unchecked")
        T doc = (T) nodesCache.getIfPresent(key);
        if (doc == NodeDocument.NULL) {
            doc = null;
        }
        return doc;
    }

    @NotNull
    private static Bson createQueryForUpdate(String key,
                                             Map<Key, Condition> conditions) {
        Bson query = getByKeyQuery(key);
        if (conditions.isEmpty()) {
            // special case when there are no conditions
            return query;
        }
        List<Bson> conditionList = new ArrayList<>(conditions.size() + 1);
        conditionList.add(query);
        for (Entry<Key, Condition> entry : conditions.entrySet()) {
            Key k = entry.getKey();
            Condition c = entry.getValue();
            switch (c.type) {
                case EXISTS:
                    conditionList.add(Filters.exists(k.toString(), Boolean.TRUE.equals(c.value)));
                    break;
                case EQUALS:
                    conditionList.add(Filters.eq(k.toString(), c.value));
                    break;
                case NOTEQUALS:
                    conditionList.add(Filters.ne(k.toString(), c.value));
                    break;
            }
        }
        return Filters.and(conditionList);
    }

    /**
     * Creates a MongoDB update object from the given UpdateOp.
     *
     * @param updateOp the update op.
     * @param failOnInsert whether to create an update that will fail on insert.
     * @return the DBObject.
     */
    private static BasicDBObject createUpdate(UpdateOp updateOp,
                                              boolean failOnInsert) {
        BasicDBObject setUpdates = new BasicDBObject();
        BasicDBObject maxUpdates = new BasicDBObject();
        BasicDBObject incUpdates = new BasicDBObject();
        BasicDBObject unsetUpdates = new BasicDBObject();
        // always increment modCount
        updateOp.increment(Document.MOD_COUNT, 1);

        // other updates
        for (Entry<Key, Operation> entry : updateOp.getChanges().entrySet()) {
            Key k = entry.getKey();
            Operation op = entry.getValue();
            switch (op.type) {
                case SET:
                case SET_MAP_ENTRY: {
                    setUpdates.append(k.toString(), op.value);
                    break;
                }
                case MAX: {
                    maxUpdates.append(k.toString(), op.value);
                    break;
                }
                case INCREMENT: {
                    incUpdates.append(k.toString(), op.value);
                    break;
                }
                case REMOVE:
                case REMOVE_MAP_ENTRY: {
                    unsetUpdates.append(k.toString(), "1");
                    break;
                }
            }
        }

        BasicDBObject update = new BasicDBObject();
        if (!setUpdates.isEmpty()) {
            update.append("$set", setUpdates);
        }
        if (!maxUpdates.isEmpty()) {
            update.append("$max", maxUpdates);
        }
        if (!incUpdates.isEmpty()) {
            update.append("$inc", incUpdates);
        }
        if (!unsetUpdates.isEmpty()) {
            update.append("$unset", unsetUpdates);
        }

        if (failOnInsert) {
            update.putAll(CONFLICT_ON_INSERT);
        }

        return update;
    }

    @NotNull
    private <T extends Document> T applyChanges(Collection<T> collection, T oldDoc, UpdateOp update) {
        T doc = collection.newDocument(this);
        oldDoc.deepCopy(doc);
        UpdateUtils.applyChanges(doc, update);
        doc.seal();
        return doc;
    }

    private Stopwatch startWatch() {
        return Stopwatch.createStarted();
    }


    @Override
    public void setReadWriteMode(String readWriteMode) {
        if (readWriteMode == null || readWriteMode.equals(lastReadWriteMode)) {
            return;
        }
        lastReadWriteMode = readWriteMode;
        try {
            String rwModeUri = readWriteMode;
            if(!readWriteMode.startsWith("mongodb://")){
                rwModeUri = String.format("mongodb://localhost/?%s", readWriteMode);
            }
            MongoClientURI uri = new MongoClientURI(rwModeUri);
            ReadPreference readPref = uri.getOptions().getReadPreference();

            if (!readPref.equals(nodes.getReadPreference())) {
                nodes = nodes.withReadPreference(readPref);
                LOG.info("Using ReadPreference {} ", readPref);
            }

            WriteConcern writeConcern = uri.getOptions().getWriteConcern();
            if (!writeConcern.equals(nodes.getWriteConcern())) {
                nodes = nodes.withWriteConcern(writeConcern);
                LOG.info("Using WriteConcern " + writeConcern);
            }
        } catch (Exception e) {
            LOG.error("Error setting readWriteMode " + readWriteMode, e);
        }
    }

    private long getTime() {
        return clock.getTime();
    }

    void setClock(Clock clock) {
        this.clock = clock;
    }

    NodeDocumentCache getNodeDocumentCache() {
        return nodesCache;
    }

    public void setStatsCollector(DocumentStoreStatsCollector stats) {
        this.stats = stats;
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        // the assumption is that the network delay from this instance
        // to the server, and from the server back to this instance
        // are (more or less) equal.
        // taking this assumption into account allows to remove
        // the network delays from the picture: the difference
        // between end and start time is exactly this network
        // delay (plus some server time, but that's neglected).
        // so if the clocks are in perfect sync and the above
        // mentioned assumption holds, then the server time should
        // be exactly at the midPoint between start and end.
        // this should allow a more accurate picture of the diff.
        final long start = System.currentTimeMillis();
        // assumption here: server returns UTC - ie the returned
        // date object is correctly taking care of time zones.
        final BasicDBObject isMaster;
        try {
            isMaster = getDatabase().runCommand(new BasicDBObject("isMaster", 1), BasicDBObject.class);
            if (isMaster == null) {
                // OAK-4107 / OAK-4515 : extra safety
                LOG.warn("determineServerTimeDifferenceMillis: db.isMaster returned null - cannot determine time difference - assuming 0ms.");
                return 0;
            }
            final Date serverLocalTime = isMaster.getDate("localTime");
            if (serverLocalTime == null) {
                // OAK-4107 / OAK-4515 : looks like this can happen - at least
                // has been seen once on mongo 3.0.9
                // let's handle this gently and issue a log.warn
                // instead of throwing a NPE
                LOG.warn("determineServerTimeDifferenceMillis: db.isMaster.localTime returned null - cannot determine time difference - assuming 0ms.");
                return 0;
            }
            final long end = System.currentTimeMillis();

            final long midPoint = (start + end) / 2;
            final long serverLocalTimeMillis = serverLocalTime.getTime();

            // the difference should be
            // * positive when local instance is ahead
            // * and negative when the local instance is behind
            final long diff = midPoint - serverLocalTimeMillis;

            return diff;
        } catch (Exception e) {
            LOG.warn("determineServerTimeDifferenceMillis: db.isMaster failed with exception - assuming 0ms. "
                            + "(Result details: server exception=" + e + ", server error message=" + e.getMessage() + ")", e);
        }
        return 0;
    }

    private <T extends Document> DocumentStoreException handleException(Throwable ex,
                                                                        Collection<T> collection,
                                                                        Iterable<String> ids) {
        if (collection == Collection.NODES) {
            for (String id : ids) {
                invalidateCache(collection, id);
            }
        }

        if (ex instanceof MongoWriteException) {
            mongoWriteExceptions.incrementAndGet();
        }

        return asDocumentStoreException(ex.getMessage(), ex,
                getDocumentStoreExceptionTypeFor(ex), ids);
    }

    public long getAmountOfMongoWriteExceptions() {
        return mongoWriteExceptions.get();
    }

    private <T extends Document> DocumentStoreException handleException(Throwable ex,
                                                                        Collection<T> collection,
                                                                        String id) {
        return handleException(ex, collection, Collections.singleton(id));
    }

    private static void toMapBuilder(Map<String, String> builder,
                                     BasicDBObject stats,
                                     String prefix) {
        stats.forEach((k, v) -> {
            // exclude some verbose internals and status
            if (!k.equals("wiredTiger") && !k.equals("indexDetails") && !k.equals("ok")) {
                String key = prefix + "." + k;
                if (v instanceof BasicDBObject) {
                    toMapBuilder(builder, (BasicDBObject) v, key);
                } else {
                    builder.put(key, String.valueOf(v));
                }
            }
        });
    }

    private boolean withClientSession() {
        return connection.getStatus().isClientSessionSupported() && useClientSession;
    }

    private boolean secondariesWithinAcceptableLag() {
        return getClient().getReplicaSetStatus() == null
                || connection.getStatus().getReplicaSetLagEstimate() < acceptableLagMillis;
    }

    private void lagTooHigh() {
        LOG.debug("Read from secondary is preferred but replication lag is too high. Directing read to primary.");
    }

    /**
     * Execute a callable with an optional {@link ClientSession}. A client
     * session is passed to {@link DocumentStoreCallable#call(ClientSession)} if
     * the connected MongoDB servers support client sessions, otherwise the
     * session is {@code null}. The client session must only be used within
     * the scope of the {@link DocumentStoreCallable#call(ClientSession)}.
     * 
     * @param callable the callable.
     * @param collection the collection this callable operates on.
     * @param <T> the return type of the callable.
     * @return the result of the callable.
     * @throws DocumentStoreException if the callable throws an exception.
     */
    private <T> T execute(@NotNull DocumentStoreCallable<T> callable,
                          @NotNull Collection<?> collection)
            throws DocumentStoreException {
        T result;
        if (withClientSession()) {
            try (ClientSession session = createClientSession(collection)) {
                result = callable.call(session);
            }
        } else {
            result = callable.call(null);
        }
        return result;
    }

    private ClientSession createClientSession(Collection<?> collection) {
        MongoDBConnection dbConnection;
        if (Collection.CLUSTER_NODES == collection) {
            dbConnection = clusterNodesConnection;
        } else {
            dbConnection = connection;
        }
        return dbConnection.createClientSession();
    }

    interface DocumentStoreCallable<T> {

        T call(@Nullable ClientSession session) throws DocumentStoreException;
    }

    private static class BulkRequestResult {

        private final Set<String> failedUpdates;

        private final Set<String> upserts;
        private final int modifiedCount;

        private BulkRequestResult(Set<String> failedUpdates, Set<String> upserts, int modifiedCount) {
            this.failedUpdates = failedUpdates;
            this.upserts = upserts;
            this.modifiedCount = modifiedCount;
        }
    }

    private static class InvalidationResult implements CacheInvalidationStats {
        int invalidationCount;
        int upToDateCount;
        int cacheSize;
        int queryCount;
        int cacheEntriesProcessedCount;

        @Override
        public String toString() {
            return "InvalidationResult{" +
                    "invalidationCount=" + invalidationCount +
                    ", upToDateCount=" + upToDateCount +
                    ", cacheSize=" + cacheSize +
                    ", queryCount=" + queryCount +
                    ", cacheEntriesProcessedCount=" + cacheEntriesProcessedCount +
                    '}';
        }

        @Override
        public String summaryReport() {
            return toString();
        }
    }
}
