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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.CachedNodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.UpdateUtils;
import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.cache.ForwardingListener;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocOffHeapCache;
import org.apache.jackrabbit.oak.plugins.document.cache.OffHeapCache;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Striped;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A document store that uses MongoDB as the backend.
 */
public class MongoDocumentStore implements CachingDocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStore.class);

    private static final boolean LOG_TIME = false;

    private static final DBObject BY_ID_ASC = new BasicDBObject(Document.ID, 1);

    static enum DocumentReadPreference {
        PRIMARY,
        PREFER_PRIMARY,
        PREFER_SECONDARY,
        PREFER_SECONDARY_IF_OLD_ENOUGH
    }

    public static final int IN_CLAUSE_BATCH_SIZE = 500;

    private final DBCollection nodes;
    private final DBCollection clusterNodes;
    private final DBCollection settings;

    /**
     * The sum of all milliseconds this class waited for MongoDB.
     */
    private long timeSum;

    private final Cache<CacheValue, NodeDocument> nodesCache;
    private final CacheStats cacheStats;

    /**
     * Locks to ensure cache consistency on reads, writes and invalidation.
     */
    private final Striped<Lock> locks = Striped.lock(128);

    /**
     * ReadWriteLocks to synchronize cache access when child documents are
     * requested from MongoDB and put into the cache. Accessing a single
     * document in the cache will acquire a read (shared) lock for the parent
     * key in addition to the lock (from {@link #locks}) for the individual
     * document. Reading multiple sibling documents will acquire a write
     * (exclusive) lock for the parent key. See OAK-1897.
     */
    private final Striped<ReadWriteLock> parentLocks = Striped.readWriteLock(64);

    /**
     * Comparator for maps with {@link Revision} keys. The maps are ordered
     * descending, newest revisions first!
     */
    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    private Clock clock = Clock.SIMPLE;

    private final long maxReplicationLagMillis;

    private String lastReadWriteMode;

    public MongoDocumentStore(DB db, DocumentMK.Builder builder) {
        checkVersion(db);
        nodes = db.getCollection(
                Collection.NODES.toString());
        clusterNodes = db.getCollection(
                Collection.CLUSTER_NODES.toString());
        settings = db.getCollection(
                Collection.SETTINGS.toString());

        maxReplicationLagMillis = builder.getMaxReplicationLagMillis();

        // indexes:
        // the _id field is the primary key, so we don't need to define it
        DBObject index = new BasicDBObject();
        // modification time (descending)
        index.put(NodeDocument.MODIFIED_IN_SECS, -1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        nodes.ensureIndex(index, options);

        // index on the _bin flag to faster access nodes with binaries for GC
        index = new BasicDBObject();
        index.put(NodeDocument.HAS_BINARY_FLAG, 1);
        options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        options.put("sparse", Boolean.TRUE);
        this.nodes.ensureIndex(index, options);

        index = new BasicDBObject();
        index.put(NodeDocument.DELETED_ONCE, 1);
        options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        options.put("sparse", Boolean.TRUE);
        this.nodes.ensureIndex(index, options);

        index = new BasicDBObject();
        index.put(NodeDocument.SD_TYPE, 1);
        options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        options.put("sparse", Boolean.TRUE);
        this.nodes.ensureIndex(index, options);


        // TODO expire entries if the parent was changed
        if (builder.useOffHeapCache()) {
            nodesCache = createOffHeapCache(builder);
        } else {
            nodesCache = builder.buildCache(builder.getDocumentCacheSize());
        }

        cacheStats = new CacheStats(nodesCache, "Document-Documents", builder.getWeigher(),
                builder.getDocumentCacheSize());
    }

    private static void checkVersion(DB db) {
        String version = db.command("buildInfo").getString("version");
        Matcher m = Pattern.compile("^(\\d+)\\.(\\d+)\\..*").matcher(version);
        if (!m.matches()) {
            throw new IllegalArgumentException("Malformed MongoDB version: " + version);
        }
        int major = Integer.parseInt(m.group(1));
        int minor = Integer.parseInt(m.group(2));
        if (major > 2) {
            return;
        }
        if (minor < 6) {
            String msg = "MongoDB version 2.6.0 or higher required. " +
                    "Currently connected to a MongoDB with version: " + version;
            throw new RuntimeException(msg);
        }
    }

    private Cache<CacheValue, NodeDocument> createOffHeapCache(
            DocumentMK.Builder builder) {
        ForwardingListener<CacheValue, NodeDocument> listener = ForwardingListener.newInstance();

        Cache<CacheValue, NodeDocument> primaryCache = CacheBuilder.newBuilder()
                .weigher(builder.getWeigher())
                .maximumWeight(builder.getDocumentCacheSize())
                .removalListener(listener)
                .recordStats()
                .build();

        return new NodeDocOffHeapCache(primaryCache, listener, builder, this);
    }

    private static long start() {
        return LOG_TIME ? System.currentTimeMillis() : 0;
    }

    private void end(String message, long start) {
        if (LOG_TIME) {
            long t = System.currentTimeMillis() - start;
            if (t > 0) {
                LOG.debug(message + ": " + t);
            }
            timeSum += t;
        }
    }

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        // TODO should not be needed, but it seems
        // oak-jcr doesn't call dispose()
        dispose();
    }

    @Override
    public void invalidateCache() {
        //TODO Check if we should use LinearInvalidator for small cache sizes as
        //that would lead to lesser number of queries
        CacheInvalidator.createHierarchicalInvalidator(this).invalidateCache();
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        if (collection == Collection.NODES) {
            TreeLock lock = acquire(key);
            try {
                nodesCache.invalidate(new StringValue(key));
            } finally {
                lock.unlock();
            }
        }
    }

    public <T extends Document> void invalidateCache(Collection<T> collection, List<String> keys) {
        for(String key : keys){
            invalidateCache(collection, key);
        }
    }


    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return find(collection, key, true, -1);
    }

    @Override
    public <T extends Document> T find(final Collection<T> collection,
                                       final String key,
                                       int maxCacheAge) {
        return find(collection, key, false, maxCacheAge);
    }

    @SuppressWarnings("unchecked")
    private <T extends Document> T find(final Collection<T> collection,
                                       final String key,
                                       boolean preferCached,
                                       final int maxCacheAge) {
        if (collection != Collection.NODES) {
            return findUncached(collection, key, DocumentReadPreference.PRIMARY);
        }
        CacheValue cacheKey = new StringValue(key);
        NodeDocument doc;
        if (maxCacheAge > 0 || preferCached) {
            // first try without lock
            doc = nodesCache.getIfPresent(cacheKey);
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
            TreeLock lock = acquire(key);
            try {
                if (maxCacheAge == 0) {
                    invalidateCache(collection, key);
                }
                while (true) {
                    doc = nodesCache.get(cacheKey, new Callable<NodeDocument>() {
                        @Override
                        public NodeDocument call() throws Exception {
                            NodeDocument doc = (NodeDocument) findUncached(collection, key, getReadPreference(maxCacheAge));
                            if (doc == null) {
                                doc = NodeDocument.NULL;
                            }
                            return doc;
                        }
                    });
                    if (maxCacheAge == 0 || preferCached) {
                        break;
                    }
                    if (getTime() - doc.getCreated() < maxCacheAge) {
                        break;
                    }
                    // too old: invalidate, try again
                    invalidateCache(collection, key);
                }
            } finally {
                lock.unlock();
            }
            if (doc == NodeDocument.NULL) {
                return null;
            } else {
                return (T) doc;
            }
        } catch (ExecutionException e) {
            t = e.getCause();
        }
        throw new DocumentStoreException("Failed to load document with " + key, t);
    }

    @CheckForNull
    private <T extends Document> T findUncached(Collection<T> collection, String key, DocumentReadPreference docReadPref) {
        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            ReadPreference readPreference = getMongoReadPreference(collection, Utils.getParentId(key), docReadPref);

            if(readPreference.isSlaveOk()){
                LOG.trace("Routing call to secondary for fetching [{}]", key);
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
            end("findUncached", start);
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
        log("query", fromKey, toKey, limit);
        DBCollection dbCollection = getDBCollection(collection);
        QueryBuilder queryBuilder = QueryBuilder.start(Document.ID);
        queryBuilder.greaterThan(fromKey);
        queryBuilder.lessThan(toKey);
        if (indexedProperty != null) {
            queryBuilder.and(indexedProperty);
            queryBuilder.greaterThanEquals(startValue);
        }
        DBObject query = queryBuilder.get();
        String parentId = Utils.getParentIdFromLowerLimit(fromKey);
        TreeLock lock = acquireExclusive(parentId != null ? parentId : "");
        long start = start();
        try {
            DBCursor cursor = dbCollection.find(query).sort(BY_ID_ASC);
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
                    if (collection == Collection.NODES && doc != null) {
                        doc.seal();
                        String id = doc.getId();
                        CacheValue cacheKey = new StringValue(id);
                        // do not overwrite document in cache if the
                        // existing one in the cache is newer
                        NodeDocument cached = nodesCache.getIfPresent(cacheKey);
                        if (cached != null && cached != NodeDocument.NULL) {
                            // check mod count
                            Number cachedModCount = cached.getModCount();
                            Number modCount = doc.getModCount();
                            if (cachedModCount == null || modCount == null) {
                                throw new IllegalStateException(
                                        "Missing " + Document.MOD_COUNT);
                            }
                            if (modCount.longValue() > cachedModCount.longValue()) {
                                nodesCache.put(cacheKey, (NodeDocument) doc);
                            }
                        } else {
                            nodesCache.put(cacheKey, (NodeDocument) doc);
                        }
                    }
                    list.add(doc);
                }
            } finally {
                cursor.close();
            }
            return list;
        } finally {
            lock.unlock();
            end("query", start);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        log("remove", key);
        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            WriteResult writeResult = dbCollection.remove(getByKeyQuery(key).get(), WriteConcern.SAFE);
            invalidateCache(collection, key);
            if (writeResult.getError() != null) {
                throw new DocumentStoreException("Remove failed: " + writeResult.getError());
            }
        } finally {
            end("remove", start);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        DBCollection dbCollection = getDBCollection(collection);
        for(List<String> keyBatch : Lists.partition(keys, IN_CLAUSE_BATCH_SIZE)){
            DBObject query = QueryBuilder.start(Document.ID).in(keyBatch).get();
            WriteResult writeResult = dbCollection.remove(query, WriteConcern.SAFE);
            invalidateCache(collection, keyBatch);
            if (writeResult.getError() != null) {
                throw new DocumentStoreException("Remove failed: " + writeResult.getError());
            }
        }

    }

    @CheckForNull
    private <T extends Document> T findAndModify(Collection<T> collection,
                                                 UpdateOp updateOp,
                                                 boolean upsert,
                                                 boolean checkConditions) {
        DBCollection dbCollection = getDBCollection(collection);
        // make sure we don't modify the original updateOp
        updateOp = updateOp.copy();
        DBObject update = createUpdate(updateOp);

        TreeLock lock = acquire(updateOp.getId());
        long start = start();
        try {
            // get modCount of cached document
            Number modCount = null;
            T cachedDoc = null;
            if (collection == Collection.NODES) {
                @SuppressWarnings("unchecked")
                T doc = (T) nodesCache.getIfPresent(new StringValue(updateOp.getId()));
                cachedDoc = doc;
                if (cachedDoc != null) {
                    modCount = cachedDoc.getModCount();
                }
            }

            // perform a conditional update with limited result
            // if we have a matching modCount
            if (modCount != null) {
                QueryBuilder query = createQueryForUpdate(updateOp, checkConditions);
                query.and(Document.MOD_COUNT).is(modCount);
                DBObject fields = new BasicDBObject();
                // return _id only
                fields.put("_id", 1);

                DBObject oldNode = dbCollection.findAndModify(query.get(), fields,
                        null /*sort*/, false /*remove*/, update, false /*returnNew*/,
                        false /*upsert*/);
                if (oldNode != null) {
                    // success, update cached document
                    applyToCache(collection, cachedDoc, updateOp);
                    // return previously cached document
                    return cachedDoc;
                }
            }

            // conditional update failed or not possible
            // perform operation and get complete document
            QueryBuilder query = createQueryForUpdate(updateOp, checkConditions);
            DBObject oldNode = dbCollection.findAndModify(query.get(), null,
                    null /*sort*/, false /*remove*/, update, false /*returnNew*/,
                    upsert);
            if (checkConditions && oldNode == null) {
                return null;
            }
            T oldDoc = convertFromDBObject(collection, oldNode);
            applyToCache(collection, oldDoc, updateOp);
            if (oldDoc != null) {
                oldDoc.seal();
            }
            return oldDoc;
        } catch (Exception e) {
            throw DocumentStoreException.convert(e);
        } finally {
            lock.unlock();
            end("findAndModify", start);
        }
    }

    @CheckForNull
    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws DocumentStoreException {
        log("createOrUpdate", update);
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
            T target = collection.newDocument(this);
            UpdateUtils.applyChanges(target, update, comparator);
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
                        DBObject value = new RevisionEntry(r, op.value);
                        inserts[i].put(k.getName(), value);
                        break;
                    }
                    case REMOVE_MAP_ENTRY:
                        // nothing to do for new entries
                        break;
                    case CONTAINS_MAP_ENTRY:
                        // no effect
                        break;
                }
            }
            if (!inserts[i].containsField(Document.MOD_COUNT)) {
                inserts[i].put(Document.MOD_COUNT, 1L);
                target.put(Document.MOD_COUNT, 1L);
            }
        }

        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            try {
                WriteResult writeResult = dbCollection.insert(inserts, WriteConcern.SAFE);
                if (writeResult.getError() != null) {
                    return false;
                }
                if (collection == Collection.NODES) {
                    for (T doc : docs) {
                        TreeLock lock = acquire(doc.getId());
                        try {
                            addToCache((NodeDocument) doc);
                        } finally {
                            lock.unlock();
                        }
                    }
                }
                return true;
            } catch (MongoException e) {
                return false;
            }
        } finally {
            end("create", start);
        }
    }

    @Override
    public <T extends Document> void update(Collection<T> collection,
                                            List<String> keys,
                                            UpdateOp updateOp) {
        DBCollection dbCollection = getDBCollection(collection);
        QueryBuilder query = QueryBuilder.start(Document.ID).in(keys);
        // make sure we don't modify the original updateOp
        updateOp = updateOp.copy();
        DBObject update = createUpdate(updateOp);
        long start = start();
        try {
            Map<String, NodeDocument> cachedDocs = Collections.emptyMap();
            if (collection == Collection.NODES) {
                cachedDocs = Maps.newHashMap();
                for (String key : keys) {
                    cachedDocs.put(key, nodesCache.getIfPresent(new StringValue(key)));
                }
            }
            try {
                WriteResult writeResult = dbCollection.update(query.get(), update, false, true, WriteConcern.SAFE);
                if (writeResult.getError() != null) {
                    throw new DocumentStoreException("Update failed: " + writeResult.getError());
                }
                if (collection == Collection.NODES) {
                    // update cache
                    for (Entry<String, NodeDocument> entry : cachedDocs.entrySet()) {
                        TreeLock lock = acquire(entry.getKey());
                        try {
                            if (entry.getValue() == null) {
                                // make sure concurrently loaded document is invalidated
                                nodesCache.invalidate(new StringValue(entry.getKey()));
                            } else {
                                applyToCache(Collection.NODES, entry.getValue(),
                                        updateOp.shallowCopy(entry.getKey()));
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            } catch (MongoException e) {
                throw DocumentStoreException.convert(e);
            }
        } finally {
            end("update", start);
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

                //Default to primary preferred such that in case primary is being elected
                //we can still read from secondary
                //TODO REVIEW Would that be safe
                ReadPreference readPreference = ReadPreference.primaryPreferred();
                if (parentId != null) {
                    long replicationSafeLimit = getTime() - maxReplicationLagMillis;
                    NodeDocument cachedDoc = (NodeDocument) getIfCached(collection, parentId);
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
        Map<Revision, Object> map = new TreeMap<Revision, Object>(comparator);
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
        }else {
            throw new IllegalArgumentException(
                    "Unknown collection: " + collection.toString());
        }
    }

    private static QueryBuilder getByKeyQuery(String key) {
        return QueryBuilder.start(Document.ID).is(key);
    }

    @Override
    public void dispose() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("MongoDB time: " + timeSum);
        }
        nodes.getDB().getMongo().close();

        if (nodesCache instanceof Closeable) {
            try {
                ((Closeable) nodesCache).close();
            } catch (IOException e) {

                LOG.warn("Error occurred while closing Off Heap Cache", e);
            }
        }
    }

    @Override
    public CacheStats getCacheStats() {
        return cacheStats;
    }

    Iterable<? extends Map.Entry<CacheValue, ? extends CachedNodeDocument>> getCacheEntries() {
        if (nodesCache instanceof OffHeapCache) {
            return Iterables.concat(nodesCache.asMap().entrySet(),
                    ((OffHeapCache) nodesCache).offHeapEntriesMap().entrySet());
        }
        return nodesCache.asMap().entrySet();
    }

    CachedNodeDocument getCachedNodeDoc(String id) {
        if (nodesCache instanceof OffHeapCache) {
            return ((OffHeapCache) nodesCache).getCachedDocument(id);
        }

        return nodesCache.getIfPresent(new StringValue(id));
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
        T doc = (T) nodesCache.getIfPresent(new StringValue(key));
        return doc;
    }

    /**
     * Applies an update to the nodes cache. This method does not acquire
     * a lock for the document. The caller must ensure it holds a lock for
     * the updated document. See striped {@link #locks}.
     *
     * @param <T> the document type.
     * @param collection the document collection.
     * @param oldDoc the old document or <code>null</code> if the update is for
     *               a new document (insert).
     * @param updateOp the update operation.
     */
    private <T extends Document> void applyToCache(@Nonnull Collection<T> collection,
                                                   @Nullable T oldDoc,
                                                   @Nonnull UpdateOp updateOp) {
        // cache the new document
        if (collection == Collection.NODES) {
            CacheValue key = new StringValue(updateOp.getId());
            NodeDocument newDoc = (NodeDocument) collection.newDocument(this);
            if (oldDoc != null) {
                // we can only update the cache based on the oldDoc if we
                // still have the oldDoc in the cache, otherwise we may
                // update the cache with an outdated document
                NodeDocument cached = nodesCache.getIfPresent(key);
                if (cached == null) {
                    // cannot use oldDoc to update cache
                    return;
                }
                oldDoc.deepCopy(newDoc);
            }
            UpdateUtils.applyChanges(newDoc, updateOp, comparator);
            newDoc.seal();

            NodeDocument cached = addToCache(newDoc);
            if (cached == newDoc) {
                // successful
                return;
            }
            if (oldDoc == null) {
                // this is an insert and some other thread was quicker
                // loading it into the cache -> return now
                return;
            }
            // this is an update (oldDoc != null)
            if (Objects.equal(cached.getModCount(), oldDoc.getModCount())) {
                nodesCache.put(key, newDoc);
            } else {
                // the cache entry was modified by some other thread in
                // the meantime. the updated cache entry may or may not
                // include this update. we cannot just apply our update
                // on top of the cached entry.
                // therefore we must invalidate the cache entry
                nodesCache.invalidate(key);
            }
        }
    }

    /**
     * Adds a document to the {@link #nodesCache} iff there is no document
     * in the cache with the document key. This method does not acquire a lock
     * from {@link #locks}! The caller must ensure a lock is held for the
     * given document.
     *
     * @param doc the document to add to the cache.
     * @return either the given <code>doc</code> or the document already present
     *          in the cache.
     */
    @Nonnull
    private NodeDocument addToCache(@Nonnull final NodeDocument doc) {
        if (doc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        doc.seal();
        // make sure we only cache the document if it wasn't
        // changed and cached by some other thread in the
        // meantime. That is, use get() with a Callable,
        // which is only used when the document isn't there
        try {
            CacheValue key = new StringValue(doc.getId());
            for (;;) {
                NodeDocument cached = nodesCache.get(key,
                        new Callable<NodeDocument>() {
                    @Override
                    public NodeDocument call() {
                        return doc;
                    }
                });
                if (cached != NodeDocument.NULL) {
                    return cached;
                } else {
                    nodesCache.invalidate(key);
                }
            }
        } catch (ExecutionException e) {
            // will never happen because call() just returns
            // the already available doc
            throw new IllegalStateException(e);
        }
    }

    @Nonnull
    private static QueryBuilder createQueryForUpdate(UpdateOp updateOp,
                                              boolean checkConditions) {
        QueryBuilder query = getByKeyQuery(updateOp.getId());

        for (Entry<Key, Operation> entry : updateOp.getChanges().entrySet()) {
            Key k = entry.getKey();
            Operation op = entry.getValue();
            switch (op.type) {
                case CONTAINS_MAP_ENTRY: {
                    if (checkConditions) {
                        query.and(k.toString()).exists(op.value);
                    }
                    break;
                }
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

    /**
     * Returns the parent id for the given id. An empty String is returned if
     * the given value is the id of the root document or the id for a long path.
     *
     * @param id an id for a document.
     * @return the id of the parent document or the empty String.
     */
    @Nonnull
    private static String getParentId(@Nonnull String id) {
        String parentId = Utils.getParentId(checkNotNull(id));
        if (parentId == null) {
            parentId = "";
        }
        return parentId;
    }

    /**
     * Acquires a log for the given key. The returned tree lock will also hold
     * a shared lock on the parent key.
     *
     * @param key a key.
     * @return the acquired lock for the given key.
     */
    private TreeLock acquire(String key) {
        return TreeLock.shared(parentLocks.get(getParentId(key)), locks.get(key));
    }

    /**
     * Acquires an exclusive lock on the given parent key. Use this method to
     * block cache access for child keys of the given parent key.
     *
     * @param parentKey the parent key.
     * @return the acquired lock for the given parent key.
     */
    private TreeLock acquireExclusive(String parentKey) {
        return TreeLock.exclusive(parentLocks.get(parentKey));
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

    private final static class TreeLock {

        private final Lock parentLock;
        private final Lock lock;

        private TreeLock(Lock parentLock, Lock lock) {
            this.parentLock = parentLock;
            this.lock = lock;
        }

        static TreeLock shared(ReadWriteLock parentLock, Lock lock) {
            return new TreeLock(parentLock.readLock(), lock).lock();
        }

        static TreeLock exclusive(ReadWriteLock parentLock) {
            return new TreeLock(parentLock.writeLock(), null).lock();
        }

        private TreeLock lock() {
            parentLock.lock();
            if (lock != null) {
                lock.lock();
            }
            return this;
        }

        private void unlock() {
            if (lock != null) {
                lock.unlock();
            }
            parentLock.unlock();
        }
    }
}