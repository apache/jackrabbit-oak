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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mongodb.Block;
import com.mongodb.DBObject;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreStatsCollector;
import org.apache.jackrabbit.oak.plugins.document.JournalEntry;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionListener;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheChangesTracker;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.cache.ModificationStamp;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.LocalChanges;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
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
import com.mongodb.client.result.UpdateResult;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Sets.difference;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.asDocumentStoreException;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.DELETED_ONCE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_MAX_REV_TIME_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_TYPE;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition.newEqualsCondition;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.createIndex;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.createPartialIndex;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.getDocumentStoreExceptionTypeFor;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.hasIndex;

/**
 * A document store that uses MongoDB as the backend.
 */
public class MongoDocumentStore implements DocumentStore, RevisionListener {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStore.class);
    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(MongoDocumentStore.class.getName()
                    + ".perf"));

    private static final Bson BY_ID_ASC = new BasicDBObject(Document.ID, 1);

    enum DocumentReadPreference {
        PRIMARY,
        PREFER_PRIMARY,
        PREFER_SECONDARY,
        PREFER_SECONDARY_IF_OLD_ENOUGH
    }

    public static final int IN_CLAUSE_BATCH_SIZE = 500;

    private MongoCollection<BasicDBObject> nodes;
    private final MongoCollection<BasicDBObject> clusterNodes;
    private final MongoCollection<BasicDBObject> settings;
    private final MongoCollection<BasicDBObject> journal;

    private final MongoClient client;
    private final MongoDatabase db;

    private final NodeDocumentCache nodesCache;

    private final NodeDocumentLocks nodeLocks;

    private Clock clock = Clock.SIMPLE;

    private ReplicaSetInfo replicaInfo;

    private RevisionVector mostRecentAccessedRevisions;

    LocalChanges localChanges;

    private final long maxReplicationLagMillis;

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
     * How often in milliseconds the MongoDocumentStore should estimate the
     * replication lag.
     * <p>
     * Default is 60'000 (one minute).
     */
    private long estimationPullFrequencyMS =
            Long.getLong("oak.mongo.estimationPullFrequencyMS", TimeUnit.SECONDS.toMillis(5));

    /**
     * Fallback to the old secondary-routing strategy. Setting this to true
     * disables the optimisation introduced in the OAK-3865.
     * <p>
     * Default is false.
     */
    private boolean fallbackSecondaryStrategy =
            Boolean.getBoolean("oak.mongo.fallbackSecondaryStrategy");

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

    private String lastReadWriteMode;

    private final Map<String, String> metadata;

    private DocumentStoreStatsCollector stats;

    private boolean hasModifiedIdCompoundIndex = true;

    private static final Key KEY_MODIFIED = new Key(MODIFIED_IN_SECS, null);

    private final boolean readOnly;

    public MongoDocumentStore(MongoClient client, String dbName,
                              MongoDocumentNodeStoreBuilderBase<?> builder) {
        this.readOnly = builder.getReadOnlyMode();
        MongoStatus mongoStatus = builder.getMongoStatus();
        if (mongoStatus == null) {
            mongoStatus = new MongoStatus(client, dbName);
        }
        mongoStatus.checkVersion();
        metadata = ImmutableMap.<String,String>builder()
                .put("type", "mongo")
                .put("version", mongoStatus.getVersion())
                .build();

        this.client = client;
        this.db = client.getDatabase(dbName);
        stats = builder.getDocumentStoreStatsCollector();
        nodes = db.getCollection(Collection.NODES.toString(), BasicDBObject.class);
        clusterNodes = db.getCollection(Collection.CLUSTER_NODES.toString(), BasicDBObject.class);
        settings = db.getCollection(Collection.SETTINGS.toString(), BasicDBObject.class);
        journal = db.getCollection(Collection.JOURNAL.toString(), BasicDBObject.class);

        maxReplicationLagMillis = builder.getMaxReplicationLagMillis();

        if (fallbackSecondaryStrategy) {
            replicaInfo = null;
            localChanges = null;
        } else {
            replicaInfo = new ReplicaSetInfo(clock, client, dbName, builder.getMongoUri(), estimationPullFrequencyMS, maxReplicationLagMillis, builder.getExecutor());
            Thread replicaInfoThread = new Thread(replicaInfo, "MongoDocumentStore replica set info provider");
            replicaInfoThread.setDaemon(true);
            replicaInfoThread.start();
        }

        // indexes:
        // the _id field is the primary key, so we don't need to define it

        long initialDocsCount = nodes.count();
        // compound index on _modified and _id
        if (initialDocsCount == 0) {
            // this is an empty store, create a compound index
            // on _modified and _id (OAK-3071)
            createIndex(nodes, new String[]{NodeDocument.MODIFIED_IN_SECS, Document.ID},
                    new boolean[]{true, true}, false, false);
        } else if (!hasIndex(nodes, NodeDocument.MODIFIED_IN_SECS, Document.ID)) {
            hasModifiedIdCompoundIndex = false;
            if (!builder.getReadOnlyMode()) {
                LOG.warn("Detected an upgrade from Oak version <= 1.2. For optimal " +
                        "performance it is recommended to create a compound index " +
                        "for the 'nodes' collection on {_modified:1, _id:1}.");
            }
        }

        // index on the _bin flag to faster access nodes with binaries for GC
        createIndex(nodes, NodeDocument.HAS_BINARY_FLAG, true, false, true);

        // index on _deleted for fast lookup of potentially garbage
        // depending on the MongoDB version, create a partial index
        if (initialDocsCount == 0) {
            if (mongoStatus.isVersion(3, 2)) {
                createPartialIndex(nodes, new String[]{DELETED_ONCE, MODIFIED_IN_SECS},
                        new boolean[]{true, true}, "{" + DELETED_ONCE + ":true}");
            } else {
                createIndex(nodes, NodeDocument.DELETED_ONCE, true, false, true);
            }
        } else if (!hasIndex(nodes, DELETED_ONCE, MODIFIED_IN_SECS)) {
            if (!builder.getReadOnlyMode()) {
                LOG.warn("Detected an upgrade from Oak version <= 1.6. For optimal " +
                        "Revision GC performance it is recommended to create a " +
                        "partial index for the 'nodes' collection on " +
                        "{_deletedOnce:1, _modified:1} with a partialFilterExpression " +
                        "{_deletedOnce:true}. Partial indexes require MongoDB 3.2 " +
                        "or higher.");
            }
        }

        // compound index on _sdType and _sdMaxRevTime
        if (initialDocsCount == 0) {
            // this is an empty store, create compound index
            // on _sdType and _sdMaxRevTime (OAK-6129)
            createIndex(nodes, new String[]{SD_TYPE, SD_MAX_REV_TIME_IN_SECS},
                    new boolean[]{true, true}, false, true);
        } else if (!hasIndex(nodes, SD_TYPE, SD_MAX_REV_TIME_IN_SECS)) {
            if (!builder.getReadOnlyMode()) {
                LOG.warn("Detected an upgrade from Oak version <= 1.6. For optimal " +
                        "Revision GC performance it is recommended to create a " +
                        "sparse compound index for the 'nodes' collection on " +
                        "{_sdType:1, _sdMaxRevTime:1}.");
            }
        }

        // index on _modified for journal entries
        createIndex(journal, JournalEntry.MODIFIED, true, false, false);

        this.nodeLocks = new StripedNodeDocumentLocks();
        this.nodesCache = builder.buildNodeDocumentCache(this, nodeLocks);

        LOG.info("Connected to MongoDB {} with maxReplicationLagMillis {}, " +
                "maxDeltaForModTimeIdxSecs {}, disableIndexHint {}, " +
                "{}, serverStatus {}",
                mongoStatus.getVersion(), maxReplicationLagMillis, maxDeltaForModTimeIdxSecs,
                disableIndexHint, db.getWriteConcern(),
                mongoStatus.getServerDetails());
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
            for (String id : filter(ids, not(in(modStamps.keySet())))) {
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
            return findUncachedWithRetry(collection, key,
                    DocumentReadPreference.PRIMARY);
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
    @CheckForNull
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
            }
        }
        if (ex != null) {
            throw handleException(ex, collection, key);
        } else {
            // impossible to get here
            throw new IllegalStateException();
        }
    }

    @CheckForNull
    protected <T extends Document> T findUncached(Collection<T> collection, String key, DocumentReadPreference docReadPref) {
        log("findUncached", key, docReadPref);
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        final Stopwatch watch = startWatch();
        boolean isSlaveOk = false;
        boolean docFound = true;
        try {
            ReadPreference readPreference = getMongoReadPreference(collection, null, key, docReadPref);

            if(readPreference.isSlaveOk()){
                LOG.trace("Routing call to secondary for fetching [{}]", key);
                isSlaveOk = true;
            }

            List<BasicDBObject> result = new ArrayList<>(1);
            dbCollection.withReadPreference(readPreference).find(getByKeyQuery(key)).into(result);

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

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection,
                                String fromKey,
                                String toKey,
                                int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              String indexedProperty,
                                              long startValue,
                                              int limit) {
        return queryWithRetry(collection, fromKey, toKey, indexedProperty,
                startValue, limit, maxQueryTimeMS);
    }

    /**
     * Queries for documents and performs a number of retries if the read fails
     * with an exception.
     */
    @Nonnull
    private <T extends Document> List<T> queryWithRetry(Collection<T> collection,
                                                        String fromKey,
                                                        String toKey,
                                                        String indexedProperty,
                                                        long startValue,
                                                        int limit,
                                                        long maxQueryTime) {
        int numAttempts = queryRetries + 1;
        MongoException ex = null;
        for (int i = 0; i < numAttempts; i++) {
            if (i > 0) {
                LOG.warn("Retrying query, fromKey={}, toKey={}", fromKey, toKey);
            }
            try {
                return queryInternal(collection, fromKey, toKey,
                        indexedProperty, startValue, limit, maxQueryTime);
            } catch (MongoException e) {
                ex = e;
            }
        }
        if (ex != null) {
            throw handleException(ex, collection, Lists.newArrayList(fromKey, toKey));
        } else {
            // impossible to get here
            throw new IllegalStateException();
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    protected <T extends Document> List<T> queryInternal(Collection<T> collection,
                                                         String fromKey,
                                                         String toKey,
                                                         String indexedProperty,
                                                         long startValue,
                                                         int limit,
                                                         long maxQueryTime) {
        log("query", fromKey, toKey, indexedProperty, startValue, limit);
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);

        List<Bson> clauses = new ArrayList<>();
        clauses.add(Filters.gt(Document.ID, fromKey));
        clauses.add(Filters.lt(Document.ID, toKey));

        Bson hint = new BasicDBObject(NodeDocument.ID, 1);

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

                if (NodeDocument.MODIFIED_IN_SECS.equals(indexedProperty)
                        && canUseModifiedTimeIdx(startValue)) {
                    hint = new BasicDBObject(NodeDocument.MODIFIED_IN_SECS, -1);
                }
            }
        }
        Bson query = Filters.and(clauses);
        String parentId = Utils.getParentIdFromLowerLimit(fromKey);
        long lockTime = -1;
        final Stopwatch watch  = startWatch();

        boolean isSlaveOk = false;
        int resultSize = 0;
        CacheChangesTracker cacheChangesTracker = null;
        if (parentId != null && collection == Collection.NODES) {
            cacheChangesTracker = nodesCache.registerTracker(fromKey, toKey);
        }
        try {
            ReadPreference readPreference =
                    getMongoReadPreference(collection, parentId, null, getDefaultReadPreference(collection));

            if(readPreference.isSlaveOk()){
                isSlaveOk = true;
                LOG.trace("Routing call to secondary for fetching children from [{}] to [{}]", fromKey, toKey);
            }
            FindIterable<BasicDBObject> result = dbCollection
                    .withReadPreference(readPreference).find(query).sort(BY_ID_ASC);
            if (limit >= 0) {
                result.limit(limit);
            }
            if (!disableIndexHint && !hasModifiedIdCompoundIndex) {
                result.modifiers(new BasicDBObject("$hint", hint));
            }
            if (maxQueryTime > 0) {
                // OAK-2614: set maxTime if maxQueryTimeMS > 0
                result.maxTime(maxQueryTime, TimeUnit.MILLISECONDS);
            }

            List<T> list;
            try (MongoCursor<BasicDBObject> cursor = result.iterator()) {
                list = new ArrayList<T>();
                for (int i = 0; i < limit && cursor.hasNext(); i++) {
                    BasicDBObject o = cursor.next();
                    T doc = convertFromDBObject(collection, o);
                    list.add(doc);
                }
                resultSize = list.size();
            }

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
            dbCollection.deleteOne(getByKeyQuery(key));
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
            for(List<String> keyBatch : Lists.partition(keys, IN_CLAUSE_BATCH_SIZE)){
                Bson query = Filters.in(Document.ID, keyBatch);
                try {
                    dbCollection.deleteMany(query);
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
            List<String> batchIds = Lists.newArrayList();
            List<Bson> batch = Lists.newArrayList();
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
                        num += dbCollection.deleteMany(query).getDeletedCount();
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
                num = (int) Math.min(dbCollection.deleteMany(query).getDeletedCount(), Integer.MAX_VALUE);
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
    @CheckForNull
    private <T extends Document> T findAndModify(Collection<T> collection,
                                                 UpdateOp updateOp,
                                                 boolean upsert,
                                                 boolean checkConditions) {
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        // make sure we don't modify the original updateOp
        updateOp = updateOp.copy();
        Bson update = createUpdate(updateOp, false);

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
                    Bson query = createQueryForUpdate(updateOp.getId(),
                            updateOp.getConditions());
                    // below condition may overwrite a user supplied condition
                    // on _modCount. This is fine, because the conditions were
                    // already checked against the cached document with the
                    // matching _modCount value. There is no need to check the
                    // user supplied condition on _modCount again on the server
                    query = Filters.and(query, Filters.eq(Document.MOD_COUNT, modCount));

                    UpdateResult result = dbCollection.updateOne(query, update);
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
            BasicDBObject oldNode = dbCollection.findOneAndUpdate(query, update, options);

            if (oldNode == null){
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
                    updateLocalChanges(newDoc);
                }
                oldDoc.seal();
            } else if (upsert) {
                if (collection == Collection.NODES) {
                    NodeDocument doc = (NodeDocument) collection.newDocument(this);
                    UpdateUtils.applyChanges(doc, updateOp);
                    nodesCache.putIfAbsent(doc);
                    updateLocalChanges(doc);
                }
            } else {
                // updateOp without conditions and not an upsert
                // this means the document does not exist
            }
            return oldDoc;
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

    @CheckForNull
    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws DocumentStoreException {
        log("createOrUpdate", update);
        UpdateUtils.assertUnconditional(update);
        T doc = findAndModify(collection, update, true, false);
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
    @CheckForNull
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
                for (List<UpdateOp> partition : Lists.partition(Lists.newArrayList(operationsToCover.values()), bulkSize)) {
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
                    new Function<UpdateOp, String>() {
                @Override
                public String apply(UpdateOp input) {
                    return input.getId();
                }
            }));
        } finally {
            stats.doneCreateOrUpdate(watch.elapsed(TimeUnit.NANOSECONDS),
                    collection, Lists.transform(updateOps, new Function<UpdateOp, String>() {
                @Override
                public String apply(UpdateOp input) {
                    return input.getId();
                }
            }));
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
            BulkUpdateResult bulkResult = sendBulkUpdate(collection, bulkOperations.values(), oldDocs);

            if (collection == Collection.NODES) {
                List<NodeDocument> docsToCache = new ArrayList<NodeDocument>();
                for (UpdateOp op : filterKeys(bulkOperations, in(bulkResult.upserts)).values()) {
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

                for (NodeDocument doc : docsToCache) {
                    updateLocalChanges(doc);
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
        return Maps.uniqueIndex(updateOps, new Function<UpdateOp, String>() {
            @Override
            public String apply(UpdateOp input) {
                return input.getId();
            }
        });
    }

    private <T extends Document> Map<String, T> findDocuments(Collection<T> collection, Set<String> keys) {
        Map<String, T> docs = new HashMap<String, T>();
        if (!keys.isEmpty()) {
            List<Bson> conditions = new ArrayList<>(keys.size());
            for (String key : keys) {
                conditions.add(getByKeyQuery(key));
            }

            FindIterable<BasicDBObject> cursor = getDBCollection(collection)
                    .find(Filters.or(conditions));
            for (BasicDBObject doc : cursor) {
                T foundDoc = convertFromDBObject(collection, doc);
                docs.put(foundDoc.getId(), foundDoc);
            }
        }
        return docs;
    }

    private <T extends Document> BulkUpdateResult sendBulkUpdate(Collection<T> collection,
            java.util.Collection<UpdateOp> updateOps, Map<String, T> oldDocs) {
        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        List<WriteModel<BasicDBObject>> writes = new ArrayList<>(updateOps.size());
        String[] bulkIds = new String[updateOps.size()];
        int i = 0;
        for (UpdateOp updateOp : updateOps) {
            String id = updateOp.getId();
            Bson query = createQueryForUpdate(id, updateOp.getConditions());
            T oldDoc = oldDocs.get(id);
            if (oldDoc == null || oldDoc == NodeDocument.NULL) {
                query = Filters.and(query, Filters.exists(Document.MOD_COUNT, false));
                writes.add(new UpdateOneModel<>(query, createUpdate(updateOp, true), new UpdateOptions().upsert(true)));
            } else {
                query = Filters.and(query, Filters.eq(Document.MOD_COUNT, oldDoc.getModCount()));
                writes.add(new UpdateOneModel<>(query, createUpdate(updateOp, false), new UpdateOptions().upsert(true)));
            }
            bulkIds[i++] = id;
        }

        BulkWriteResult bulkResult;
        Set<String> failedUpdates = new HashSet<String>();
        Set<String> upserts = new HashSet<String>();
        try {
            bulkResult = dbCollection.bulkWrite(writes,
                    new BulkWriteOptions().ordered(false));
        } catch (MongoBulkWriteException e) {
            bulkResult = e.getWriteResult();
            for (BulkWriteError err : e.getWriteErrors()) {
                failedUpdates.add(bulkIds[err.getIndex()]);
            }
        }
        for (BulkWriteUpsert upsert : bulkResult.getUpserts()) {
            upserts.add(bulkIds[upsert.getIndex()]);
        }
        return new BulkUpdateResult(failedUpdates, upserts);
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
            if (!doc.containsKey(Document.MOD_COUNT)) {
                doc.put(Document.MOD_COUNT, 1L);
                target.put(Document.MOD_COUNT, 1L);
            }
        }

        MongoCollection<BasicDBObject> dbCollection = getDBCollection(collection);
        final Stopwatch watch = startWatch();
        boolean insertSuccess = false;
        try {
            try {
                dbCollection.insertMany(inserts);
                if (collection == Collection.NODES) {
                    for (T doc : docs) {
                        nodesCache.putIfAbsent((NodeDocument) doc);
                        updateLocalChanges((NodeDocument) doc);
                    }
                }
                insertSuccess = true;
                return true;
            } catch (MongoException e) {
                return false;
            }
        } finally {
            stats.doneCreate(watch.elapsed(TimeUnit.NANOSECONDS), collection, ids, insertSuccess);
        }
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
    @Nonnull
    private Map<String, ModificationStamp> getModStamps(Iterable<String> keys)
            throws MongoException {
        // Fetch only the modCount and id
        final BasicDBObject fields = new BasicDBObject(Document.ID, 1);
        fields.put(Document.MOD_COUNT, 1);
        fields.put(NodeDocument.MODIFIED_IN_SECS, 1);

        Map<String, ModificationStamp> modCounts = Maps.newHashMap();

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

    DocumentReadPreference getReadPreference(int maxCacheAge){
        long lag = fallbackSecondaryStrategy ? maxReplicationLagMillis : replicaInfo.getLag();
        if(maxCacheAge >= 0 && maxCacheAge < lag) {
            return DocumentReadPreference.PRIMARY;
        } else if(maxCacheAge == Integer.MAX_VALUE){
            return DocumentReadPreference.PREFER_SECONDARY;
        } else {
           return DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH;
        }
    }

    DocumentReadPreference getDefaultReadPreference(Collection col){
        return col == Collection.NODES ? DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH : DocumentReadPreference.PRIMARY;
    }

    <T extends Document> ReadPreference getMongoReadPreference(@Nonnull Collection<T> collection,
                                                               @Nullable String parentId,
                                                               @Nullable String documentId,
                                                               @Nonnull DocumentReadPreference preference) {
        switch(preference){
            case PRIMARY:
                return ReadPreference.primary();
            case PREFER_PRIMARY :
                return ReadPreference.primaryPreferred();
            case PREFER_SECONDARY :
                return getConfiguredReadPreference(collection);
            case PREFER_SECONDARY_IF_OLD_ENOUGH:
                if(collection != Collection.NODES){
                    return ReadPreference.primary();
                }

                boolean secondarySafe;
                if (fallbackSecondaryStrategy) {
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
                        NodeDocument cachedDoc = nodesCache.getIfPresent(parentId);
                        secondarySafe = cachedDoc != null && !cachedDoc.hasBeenModifiedSince(replicationSafeLimit);
                    }
                } else if (localChanges != null) {
                    secondarySafe = true;
                    secondarySafe &= collection == Collection.NODES;
                    secondarySafe &= documentId == null || !localChanges.mayContain(documentId);
                    secondarySafe &= parentId == null || !localChanges.mayContainChildrenOf(parentId);
                    secondarySafe &= mostRecentAccessedRevisions == null || replicaInfo.isMoreRecentThan(mostRecentAccessedRevisions);
                } else { // localChanges not initialized yet
                    secondarySafe = false;
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

    @CheckForNull
    protected <T extends Document> T convertFromDBObject(@Nonnull Collection<T> collection,
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

    @Nonnull
    private Map<Revision, Object> convertMongoMap(@Nonnull BasicDBObject obj) {
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

    MongoDatabase getDatabase() {
        return db;
    }

    MongoClient getClient() {
        return client;
    }

    private static Bson getByKeyQuery(String key) {
        return Filters.eq(Document.ID, key);
    }

    @Override
    public void dispose() {
        if (replicaInfo != null) {
            replicaInfo.stop();
        }
        client.close();
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

    @Nonnull
    @Override
    public Map<String, String> getStats() {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        List<MongoCollection<?>> all = ImmutableList.of(nodes, clusterNodes, settings, journal);
        all.forEach(c -> toMapBuilder(builder,
                db.runCommand(
                    new BasicDBObject("collStats", c.getNamespace().getCollectionName()),
                        BasicDBObject.class),
                c.getNamespace().getCollectionName()));
        return builder.build();
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

    @Nonnull
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
     * @param includeId whether to include the SET id operation
     * @return the DBObject.
     */
    @Nonnull
    private static BasicDBObject createUpdate(UpdateOp updateOp, boolean includeId) {
        BasicDBObject setUpdates = new BasicDBObject();
        BasicDBObject maxUpdates = new BasicDBObject();
        BasicDBObject incUpdates = new BasicDBObject();
        BasicDBObject unsetUpdates = new BasicDBObject();

        // always increment modCount
        updateOp.increment(Document.MOD_COUNT, 1);
        if (includeId) {
            setUpdates.append(Document.ID, updateOp.getId());
        }

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

        return update;
    }

    @Nonnull
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

    void setReplicaInfo(ReplicaSetInfo replicaInfo) {
        if (this.replicaInfo != null) {
            this.replicaInfo.stop();
        }
        this.replicaInfo = replicaInfo;
        this.replicaInfo.addListener(localChanges);
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
            isMaster = db.runCommand(new BasicDBObject("isMaster", 1), BasicDBObject.class);
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

    @Override
    public synchronized void updateAccessedRevision(RevisionVector revisions, int clusterId) {
        if (localChanges == null && replicaInfo != null) {
            localChanges = new LocalChanges(clusterId);
            replicaInfo.addListener(localChanges);
        }

        RevisionVector previousValue = mostRecentAccessedRevisions;
        if (mostRecentAccessedRevisions == null) {
            mostRecentAccessedRevisions = revisions;
        } else {
            mostRecentAccessedRevisions = mostRecentAccessedRevisions.pmax(revisions);
        }
        if (LOG.isDebugEnabled() && !mostRecentAccessedRevisions.equals(previousValue)) {
            LOG.debug("Most recent accessed revisions: {}", mostRecentAccessedRevisions);
        }
    }

    private void updateLocalChanges(NodeDocument doc) {
        if (localChanges != null) {
            localChanges.add(doc.getId(), Revision.getCurrentTimestamp());
        }
    }

    private <T extends Document> DocumentStoreException handleException(Throwable ex,
                                                                        Collection<T> collection,
                                                                        Iterable<String> ids) {
        if (collection == Collection.NODES) {
            for (String id : ids) {
                invalidateCache(collection, id);
            }
        }
        return asDocumentStoreException(ex.getMessage(), ex,
                getDocumentStoreExceptionTypeFor(ex), ids);
    }

    private <T extends Document> DocumentStoreException handleException(Throwable ex,
                                                                        Collection<T> collection,
                                                                        String id) {
        return handleException(ex, collection, Collections.singleton(id));
    }

    private static void toMapBuilder(ImmutableMap.Builder<String, String> builder,
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

    private static class BulkUpdateResult {

        private final Set<String> failedUpdates;

        private final Set<String> upserts;

        private BulkUpdateResult(Set<String> failedUpdates, Set<String> upserts) {
            this.failedUpdates = failedUpdates;
            this.upserts = upserts;
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
