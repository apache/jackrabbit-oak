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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.QueryOperators;
import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.JournalEntry;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.locks.TreeNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A document store that uses MongoDB as the backend.
 */
public class MongoDocumentStore implements DocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStore.class);
    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(MongoDocumentStore.class.getName()
                    + ".perf"));

    private static final DBObject BY_ID_ASC = new BasicDBObject(Document.ID, 1);

    enum DocumentReadPreference {
        PRIMARY,
        PREFER_PRIMARY,
        PREFER_SECONDARY,
        PREFER_SECONDARY_IF_OLD_ENOUGH
    }

    public static final int IN_CLAUSE_BATCH_SIZE = 500;

    private final DBCollection nodes;
    private final DBCollection clusterNodes;
    private final DBCollection settings;
    private final DBCollection journal;

    private final DB db;

    private final NodeDocumentCache nodesCache;

    private final TreeNodeDocumentLocks nodeLocks;

    private Clock clock = Clock.SIMPLE;

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
     * Duration in milliseconds after a mongo query with an additional
     * constraint (e.g. _modified) on the NODES collection times out and is
     * executed again without holding a {@link TreeLock} and without updating
     * the cache with data retrieved from MongoDB.
     * <p>
     * Default is 3000 (three seconds).
     */
    private long maxLockedQueryTimeMS =
            Long.getLong("oak.mongo.maxLockedQueryTimeMS", TimeUnit.SECONDS.toMillis(3));

    private String lastReadWriteMode;

    private final Map<String, String> metadata;

    public MongoDocumentStore(DB db, DocumentMK.Builder builder) {
        String version = checkVersion(db);
        metadata = ImmutableMap.<String,String>builder()
                .put("type", "mongo")
                .put("version", version)
                .build();

        this.db = db;
        nodes = db.getCollection(Collection.NODES.toString());
        clusterNodes = db.getCollection(Collection.CLUSTER_NODES.toString());
        settings = db.getCollection(Collection.SETTINGS.toString());
        journal = db.getCollection(Collection.JOURNAL.toString());

        maxReplicationLagMillis = builder.getMaxReplicationLagMillis();

        // indexes:
        // the _id field is the primary key, so we don't need to define it
        DBObject index = new BasicDBObject();
        // modification time (descending)
        index.put(NodeDocument.MODIFIED_IN_SECS, -1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        nodes.createIndex(index, options);

        // index on the _bin flag to faster access nodes with binaries for GC
        index = new BasicDBObject();
        index.put(NodeDocument.HAS_BINARY_FLAG, 1);
        options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        options.put("sparse", Boolean.TRUE);
        this.nodes.createIndex(index, options);

        index = new BasicDBObject();
        index.put(NodeDocument.DELETED_ONCE, 1);
        options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        options.put("sparse", Boolean.TRUE);
        this.nodes.createIndex(index, options);

        index = new BasicDBObject();
        index.put(NodeDocument.SD_TYPE, 1);
        options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        options.put("sparse", Boolean.TRUE);
        this.nodes.createIndex(index, options);

        index = new BasicDBObject();
        index.put(JournalEntry.MODIFIED, 1);
        options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        this.journal.createIndex(index, options);

        this.nodeLocks = new TreeNodeDocumentLocks();
        this.nodesCache = builder.buildNodeDocumentCache(this, nodeLocks);

        LOG.info("Configuration maxReplicationLagMillis {}, " +
                "maxDeltaForModTimeIdxSecs {}, disableIndexHint {}, {}",
                maxReplicationLagMillis, maxDeltaForModTimeIdxSecs,
                disableIndexHint, db.getWriteConcern());
    }

    private static String checkVersion(DB db) {
        String version = db.command("buildInfo").getString("version");
        Matcher m = Pattern.compile("^(\\d+)\\.(\\d+)\\..*").matcher(version);
        if (!m.matches()) {
            throw new IllegalArgumentException("Malformed MongoDB version: " + version);
        }
        int major = Integer.parseInt(m.group(1));
        int minor = Integer.parseInt(m.group(2));
        if (major > 2) {
            return version;
        }
        if (minor < 6) {
            String msg = "MongoDB version 2.6.0 or higher required. " +
                    "Currently connected to a MongoDB with version: " + version;
            throw new RuntimeException(msg);
        }

        return version;
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
        for (CacheValue key : nodesCache.asMap().keySet()) {
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
            
            QueryBuilder query = QueryBuilder.start(Document.ID).in(ids);
            // Fetch only the modCount and id
            final BasicDBObject fields = new BasicDBObject(Document.ID, 1);
            fields.put(Document.MOD_COUNT, 1);
            
            DBCursor cursor = nodes.find(query.get(), fields);
            cursor.setReadPreference(ReadPreference.primary());
            result.queryCount++;

            Map<String, Number> modCounts = new HashMap<String, Number>();
            for (DBObject obj : cursor) {
                String id = (String) obj.get(Document.ID);
                Number modCount = (Number) obj.get(Document.MOD_COUNT);
                modCounts.put(id, modCount);
            }

            int invalidated = nodesCache.invalidateOutdated(modCounts);
            result.cacheEntriesProcessedCount += modCounts.size();
            result.invalidationCount += invalidated;
            result.upToDateCount = modCounts.size() - invalidated;
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
                    DocumentReadPreference.PRIMARY, 2);
        }
        NodeDocument doc;
        if (maxCacheAge > 0 || preferCached) {
            // first try without lock
            doc = nodesCache.getIfPresent(key);
            if (doc != null) {
                if (preferCached ||
                        getTime() - doc.getCreated() < maxCacheAge) {
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
                            if (doc == NodeDocument.NULL) {
                                return null;
                            }
                            return (T) doc;
                        }
                    }
                }
                final NodeDocument d = (NodeDocument) findUncachedWithRetry(
                        collection, key,
                        getReadPreference(maxCacheAge), 2);
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
        throw new DocumentStoreException("Failed to load document with " + key, t);
    }

    /**
     * Finds a document and performs a number of retries if the read fails with
     * an exception.
     *
     * @param collection the collection to read from.
     * @param key the key of the document to find.
     * @param docReadPref the read preference.
     * @param retries the number of retries. Must not be negative.
     * @param <T> the document type of the given collection.
     * @return the document or {@code null} if the document doesn't exist.
     */
    @CheckForNull
    private <T extends Document> T findUncachedWithRetry(
            Collection<T> collection, String key,
            DocumentReadPreference docReadPref,
            int retries) {
        checkArgument(retries >= 0, "retries must not be negative");
        if (key.equals("0:/")) {
            LOG.trace("root node");
        }
        int numAttempts = retries + 1;
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
            throw ex;
        } else {
            // impossible to get here
            throw new IllegalStateException();
        }
    }

    @CheckForNull
    protected <T extends Document> T findUncached(Collection<T> collection, String key, DocumentReadPreference docReadPref) {
        log("findUncached", key, docReadPref);
        DBCollection dbCollection = getDBCollection(collection);
        final long start = PERFLOG.start();
        boolean isSlaveOk = false;
        try {
            ReadPreference readPreference = getMongoReadPreference(collection, Utils.getParentId(key), docReadPref);

            if(readPreference.isSlaveOk()){
                LOG.trace("Routing call to secondary for fetching [{}]", key);
                isSlaveOk = true;
            }

            DBObject obj = dbCollection.findOne(getByKeyQuery(key).get(), null, null, readPreference);

            if (obj == null
                    && readPreference.isSlaveOk()) {
                //In case secondary read preference is used and node is not found
                //then check with primary again as it might happen that node document has not been
                //replicated. This is required for case like SplitDocument where the SplitDoc is fetched with
                //maxCacheAge == Integer.MAX_VALUE which results in readPreference of secondary.
                //In such a case we know that document with such an id must exist
                //but possibly dut to replication lag it has not reached to secondary. So in that case read again
                //from primary
                obj = dbCollection.findOne(getByKeyQuery(key).get(), null, null, ReadPreference.primary());
            }
            if(obj == null){
                return null;
            }
            T doc = convertFromDBObject(collection, obj);
            if (doc != null) {
                doc.seal();
            }
            return doc;
        } finally {
            PERFLOG.end(start, 1, "findUncached on key={}, isSlaveOk={}", key,
                    isSlaveOk);
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
        boolean withLock = true;
        if (collection == Collection.NODES && indexedProperty != null) {
            long maxQueryTime;
            if (maxQueryTimeMS > 0) {
                maxQueryTime = Math.min(maxQueryTimeMS, maxLockedQueryTimeMS);
            } else {
                maxQueryTime = maxLockedQueryTimeMS;
            }
            try {
                return queryInternal(collection, fromKey, toKey, indexedProperty,
                        startValue, limit, maxQueryTime, true);
            } catch (MongoExecutionTimeoutException e) {
                LOG.info("query timed out after {} milliseconds and will be retried without lock {}",
                        maxQueryTime, Lists.newArrayList(fromKey, toKey, indexedProperty, startValue, limit));
                withLock = false;
            }
        }
        return queryInternal(collection, fromKey, toKey, indexedProperty,
                startValue, limit, maxQueryTimeMS, withLock);
    }

    @Nonnull
    <T extends Document> List<T> queryInternal(Collection<T> collection,
                                                       String fromKey,
                                                       String toKey,
                                                       String indexedProperty,
                                                       long startValue,
                                                       int limit,
                                                       long maxQueryTime,
                                                       boolean withLock) {
        log("query", fromKey, toKey, indexedProperty, startValue, limit);
        DBCollection dbCollection = getDBCollection(collection);
        QueryBuilder queryBuilder = QueryBuilder.start(Document.ID);
        queryBuilder.greaterThan(fromKey);
        queryBuilder.lessThan(toKey);

        DBObject hint = new BasicDBObject(NodeDocument.ID, 1);

        if (indexedProperty != null) {
            if (NodeDocument.DELETED_ONCE.equals(indexedProperty)) {
                if (startValue != 1) {
                    throw new DocumentStoreException(
                            "unsupported value for property " + 
                                    NodeDocument.DELETED_ONCE);
                }
                queryBuilder.and(indexedProperty);
                queryBuilder.is(true);
            } else {
                queryBuilder.and(indexedProperty);
                queryBuilder.greaterThanEquals(startValue);

                if (NodeDocument.MODIFIED_IN_SECS.equals(indexedProperty)
                        && canUseModifiedTimeIdx(startValue)) {
                    hint = new BasicDBObject(NodeDocument.MODIFIED_IN_SECS, -1);
                }
            }
        }
        DBObject query = queryBuilder.get();
        String parentId = Utils.getParentIdFromLowerLimit(fromKey);
        long lockTime = -1;
        final long start = PERFLOG.start();
        Lock lock = withLock ? nodeLocks.acquireExclusive(parentId != null ? parentId : "") : null;
        try {
            if (start != -1) {
                lockTime = System.currentTimeMillis() - start;
            }
            DBCursor cursor = dbCollection.find(query).sort(BY_ID_ASC);
            if (!disableIndexHint) {
                cursor.hint(hint);
            }
            if (maxQueryTime > 0) {
                // OAK-2614: set maxTime if maxQueryTimeMS > 0
                cursor.maxTime(maxQueryTime, TimeUnit.MILLISECONDS);
            }
            ReadPreference readPreference =
                    getMongoReadPreference(collection, parentId, getDefaultReadPreference(collection));

            if(readPreference.isSlaveOk()){
                LOG.trace("Routing call to secondary for fetching children from [{}] to [{}]", fromKey, toKey);
            }

            cursor.setReadPreference(readPreference);

            List<T> list;
            try {
                list = new ArrayList<T>();
                for (int i = 0; i < limit && cursor.hasNext(); i++) {
                    DBObject o = cursor.next();
                    T doc = convertFromDBObject(collection, o);
                    if (collection == Collection.NODES
                            && doc != null
                            && lock != null) {
                        nodesCache.putIfNewer((NodeDocument) doc);
                    }
                    list.add(doc);
                }
            } finally {
                cursor.close();
            }
            return list;
        } finally {
            if (lock != null) {
                lock.unlock();
            }
            PERFLOG.end(start, 1, "query for children from [{}] to [{}], lock:{}", fromKey, toKey, lockTime);
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
        DBCollection dbCollection = getDBCollection(collection);
        long start = PERFLOG.start();
        try {
            dbCollection.remove(getByKeyQuery(key).get());
        } catch (Exception e) {
            throw DocumentStoreException.convert(e, "Remove failed for " + key);
        } finally {
            invalidateCache(collection, key);
            PERFLOG.end(start, 1, "remove key={}", key);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        log("remove", keys);
        DBCollection dbCollection = getDBCollection(collection);
        long start = PERFLOG.start();
        try {
            for(List<String> keyBatch : Lists.partition(keys, IN_CLAUSE_BATCH_SIZE)){
                DBObject query = QueryBuilder.start(Document.ID).in(keyBatch).get();
                try {
                    dbCollection.remove(query);
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
            PERFLOG.end(start, 1, "remove keys={}", keys);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           Map<String, Map<Key, Condition>> toRemove) {
        log("remove", toRemove);
        int num = 0;
        DBCollection dbCollection = getDBCollection(collection);
        long start = PERFLOG.start();
        try {
            List<String> batchIds = Lists.newArrayList();
            List<DBObject> batch = Lists.newArrayList();
            Iterator<Entry<String, Map<Key, Condition>>> it = toRemove.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Map<Key, Condition>> entry = it.next();
                QueryBuilder query = createQueryForUpdate(
                        entry.getKey(), entry.getValue());
                batchIds.add(entry.getKey());
                batch.add(query.get());
                if (!it.hasNext() || batch.size() == IN_CLAUSE_BATCH_SIZE) {
                    DBObject q = new BasicDBObject();
                    q.put(QueryOperators.OR, batch);
                    try {
                        num += dbCollection.remove(q).getN();
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
            PERFLOG.end(start, 1, "remove keys={}", toRemove);
        }
        return num;
    }

    @SuppressWarnings("unchecked")
    @CheckForNull
    private <T extends Document> T findAndModify(Collection<T> collection,
                                                 UpdateOp updateOp,
                                                 boolean upsert,
                                                 boolean checkConditions) {
        DBCollection dbCollection = getDBCollection(collection);
        // make sure we don't modify the original updateOp
        updateOp = updateOp.copy();
        DBObject update = createUpdate(updateOp);

        Lock lock = null;
        if (collection == Collection.NODES) {
            lock = nodeLocks.acquire(updateOp.getId());
        }
        final long start = PERFLOG.start();
        try {
            // get modCount of cached document
            Number modCount = null;
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

                QueryBuilder query = createQueryForUpdate(updateOp.getId(),
                        updateOp.getConditions());
                query.and(Document.MOD_COUNT).is(modCount);

                WriteResult result = dbCollection.update(query.get(), update);
                if (result.getN() > 0) {
                    // success, update cached document
                    if (collection == Collection.NODES) {
                        NodeDocument newDoc = (NodeDocument) applyChanges(collection, cachedDoc, updateOp);
                        nodesCache.put(newDoc);
                    }
                    // return previously cached document
                    return cachedDoc;
                }
            }

            // conditional update failed or not possible
            // perform operation and get complete document
            QueryBuilder query = createQueryForUpdate(updateOp.getId(), updateOp.getConditions());
            DBObject oldNode = dbCollection.findAndModify(query.get(), null, null /*sort*/, false /*remove*/, update, false /*returnNew*/, upsert);
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
            }
            return oldDoc;
        } catch (Exception e) {
            throw DocumentStoreException.convert(e);
        } finally {
            if (lock != null) {
                lock.unlock();
            }
            PERFLOG.end(start, 1, "findAndModify [{}]", updateOp.getId());
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
        DBObject[] inserts = new DBObject[updateOps.size()];

        for (int i = 0; i < updateOps.size(); i++) {
            inserts[i] = new BasicDBObject();
            UpdateOp update = updateOps.get(i);
            UpdateUtils.assertUnconditional(update);
            T target = collection.newDocument(this);
            UpdateUtils.applyChanges(target, update);
            docs.add(target);
            for (Entry<Key, Operation> entry : update.getChanges().entrySet()) {
                Key k = entry.getKey();
                Operation op = entry.getValue();
                switch (op.type) {
                    case SET:
                    case MAX:
                    case INCREMENT: {
                        inserts[i].put(k.toString(), op.value);
                        break;
                    }
                    case SET_MAP_ENTRY: {
                        Revision r = k.getRevision();
                        if (r == null) {
                            throw new IllegalStateException(
                                    "SET_MAP_ENTRY must not have null revision");
                        }
                        DBObject value = (DBObject) inserts[i].get(k.getName());
                        if (value == null) {
                            value = new RevisionEntry(r, op.value);
                            inserts[i].put(k.getName(), value);
                        } else if (value.keySet().size() == 1) {
                            String key = value.keySet().iterator().next();
                            Object val = value.get(key);
                            value = new BasicDBObject(key, val);
                            value.put(r.toString(), op.value);
                            inserts[i].put(k.getName(), value);
                        } else {
                            value.put(r.toString(), op.value);
                        }
                        break;
                    }
                    case REMOVE_MAP_ENTRY:
                        // nothing to do for new entries
                        break;
                }
            }
            if (!inserts[i].containsField(Document.MOD_COUNT)) {
                inserts[i].put(Document.MOD_COUNT, 1L);
                target.put(Document.MOD_COUNT, 1L);
            }
        }

        DBCollection dbCollection = getDBCollection(collection);
        final long start = PERFLOG.start();
        try {
            try {
                dbCollection.insert(inserts);
                if (collection == Collection.NODES) {
                    for (T doc : docs) {
                        nodesCache.putIfAbsent((NodeDocument) doc);
                    }
                }
                return true;
            } catch (MongoException e) {
                return false;
            }
        } finally {
            PERFLOG.end(start, 1, "create");
        }
    }

    @Override
    public <T extends Document> void update(Collection<T> collection,
                                            List<String> keys,
                                            UpdateOp updateOp) {
        log("update", keys, updateOp);
        UpdateUtils.assertUnconditional(updateOp);
        DBCollection dbCollection = getDBCollection(collection);
        QueryBuilder query = QueryBuilder.start(Document.ID).in(keys);
        // make sure we don't modify the original updateOp
        updateOp = updateOp.copy();
        DBObject update = createUpdate(updateOp);
        final long start = PERFLOG.start();
        try {
            Map<String, NodeDocument> cachedDocs = Collections.emptyMap();
            if (collection == Collection.NODES) {
                cachedDocs = Maps.newHashMap();
                for (String key : keys) {
                    cachedDocs.put(key, nodesCache.getIfPresent(key));
                }
            }
            try {
                dbCollection.update(query.get(), update, false, true);
                // update cache
                for (Entry<String, NodeDocument> entry : cachedDocs.entrySet()) {
                    // the cachedDocs is not empty, so the collection = NODES
                    Lock lock = nodeLocks.acquire(entry.getKey());
                    try {
                        if (entry.getValue() == null || entry.getValue() == NodeDocument.NULL) {
                            // make sure concurrently loaded document is
                            // invalidated
                            nodesCache.invalidate(entry.getKey());
                        } else {
                            NodeDocument newDoc = applyChanges(Collection.NODES, entry.getValue(), updateOp.shallowCopy(entry.getKey()));
                            nodesCache.replaceCachedDocument(entry.getValue(), newDoc);
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (MongoException e) {
                throw DocumentStoreException.convert(e);
            }
        } finally {
            PERFLOG.end(start, 1, "update");
        }
    }

    DocumentReadPreference getReadPreference(int maxCacheAge){
        if(maxCacheAge >= 0 && maxCacheAge < maxReplicationLagMillis) {
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

    <T extends Document> ReadPreference getMongoReadPreference(Collection<T> collection,
                                                               String parentId,
                                                               DocumentReadPreference preference) {
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

                // read from primary unless parent has not been modified
                // within replication lag period
                ReadPreference readPreference = ReadPreference.primary();
                if (parentId != null) {
                    long replicationSafeLimit = getTime() - maxReplicationLagMillis;
                    NodeDocument cachedDoc = nodesCache.getIfPresent(parentId);
                    // FIXME: this is not quite accurate, because ancestors
                    // are updated in a background thread (_lastRev). We
                    // will need to revise this for low maxReplicationLagMillis
                    // values
                    if (cachedDoc != null && !cachedDoc.hasBeenModifiedSince(replicationSafeLimit)) {

                        //If parent has been modified loooong time back then there children
                        //would also have not be modified. In that case we can read from secondary
                        readPreference = getConfiguredReadPreference(collection);
                    }
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
    ReadPreference getConfiguredReadPreference(Collection collection){
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

    <T extends Document> DBCollection getDBCollection(Collection<T> collection) {
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

    private static QueryBuilder getByKeyQuery(String key) {
        return QueryBuilder.start(Document.ID).is(key);
    }

    @Override
    public void dispose() {
        nodes.getDB().getMongo().close();
        try {
            nodesCache.close();
        } catch (IOException e) {
            LOG.warn("Error occurred while closing Off Heap Cache", e);
        }
    }

    @Override
    public CacheStats getCacheStats() {
        return nodesCache.getCacheStats();
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
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
        return doc;
    }

    @Nonnull
    private static QueryBuilder createQueryForUpdate(String key,
                                                     Map<Key, Condition> conditions) {
        QueryBuilder query = getByKeyQuery(key);

        for (Entry<Key, Condition> entry : conditions.entrySet()) {
            Key k = entry.getKey();
            Condition c = entry.getValue();
            switch (c.type) {
                case EXISTS:
                    query.and(k.toString()).exists(c.value);
                    break;
                case EQUALS:
                    query.and(k.toString()).is(c.value);
                    break;
                case NOTEQUALS:
                    query.and(k.toString()).notEquals(c.value);
                    break;
            }
        }

        return query;
    }

    /**
     * Creates a MongoDB update object from the given UpdateOp.
     *
     * @param updateOp the update op.
     * @return the DBObject.
     */
    @Nonnull
    private static DBObject createUpdate(UpdateOp updateOp) {
        BasicDBObject setUpdates = new BasicDBObject();
        BasicDBObject maxUpdates = new BasicDBObject();
        BasicDBObject incUpdates = new BasicDBObject();
        BasicDBObject unsetUpdates = new BasicDBObject();

        // always increment modCount
        updateOp.increment(Document.MOD_COUNT, 1);

        // other updates
        for (Entry<Key, Operation> entry : updateOp.getChanges().entrySet()) {
            Key k = entry.getKey();
            if (k.getName().equals(Document.ID)) {
                // avoid exception "Mod on _id not allowed"
                continue;
            }
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

    @CheckForNull
    private <T extends Document> T applyChanges(Collection<T> collection, T oldDoc, UpdateOp update) {
        T doc = collection.newDocument(this);
        oldDoc.deepCopy(doc);
        UpdateUtils.applyChanges(doc, update);
        doc.seal();
        return doc;
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
                nodes.setReadPreference(readPref);
                LOG.info("Using ReadPreference {} ",readPref);
            }

            WriteConcern writeConcern = uri.getOptions().getWriteConcern();
            if (!writeConcern.equals(nodes.getWriteConcern())) {
                nodes.setWriteConcern(writeConcern);
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

    void setMaxLockedQueryTimeMS(long maxLockedQueryTimeMS) {
        this.maxLockedQueryTimeMS = maxLockedQueryTimeMS;
    }

    long getLockAcquisitionCount() {
        return nodeLocks.getLockAcquisitionCount();
    }

    NodeDocumentCache getNodeDocumentCache() {
        return nodesCache;
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
        final Date serverLocalTime = db.command("serverStatus").getDate("localTime");
        final long end = System.currentTimeMillis();

        final long midPoint = (start + end) / 2;
        final long serverLocalTimeMillis = serverLocalTime.getTime();

        // the difference should be
        // * positive when local instance is ahead
        // * and negative when the local instance is behind
        final long diff = midPoint - serverLocalTimeMillis;

        return diff;
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
