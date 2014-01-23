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
package org.apache.jackrabbit.oak.plugins.mongomk;

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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Splitter;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.mongomk.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.mongomk.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.mongomk.cache.ForwardingListener;
import org.apache.jackrabbit.oak.plugins.mongomk.cache.NodeDocOffHeapCache;
import org.apache.jackrabbit.oak.plugins.mongomk.cache.OffHeapCache;
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
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * A document store that uses MongoDB as the backend.
 */
public class MongoDocumentStore implements DocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStore.class);
    
    private static final boolean LOG_TIME = false;

    private static final DBObject BY_ID_ASC = new BasicDBObject(Document.ID, 1);

    private final DBCollection nodes;
    private final DBCollection clusterNodes;
    
    /**
     * The sum of all milliseconds this class waited for MongoDB.
     */
    private long timeSum;
    
    private final Cache<String, NodeDocument> nodesCache;
    private final CacheStats cacheStats;

    /**
     * Locks to ensure cache consistency on reads, writes and invalidation.
     */
    private final Striped<Lock> locks = Striped.lock(64);

    /**
     * Comparator for maps with {@link Revision} keys. The maps are ordered
     * descending, newest revisions first!
     */
    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    private String lastReadWriteMode;

    public MongoDocumentStore(DB db, MongoMK.Builder builder) {
        nodes = db.getCollection(
                Collection.NODES.toString());
        clusterNodes = db.getCollection(
                Collection.CLUSTER_NODES.toString());

        // indexes:
        // the _id field is the primary key, so we don't need to define it
        DBObject index = new BasicDBObject();
        // modification time (descending)
        index.put(NodeDocument.MODIFIED, -1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.FALSE);
        nodes.ensureIndex(index, options);

        // TODO expire entries if the parent was changed
        if(builder.useOffHeapCache()){
            nodesCache = createOffHeapCache(builder);
        }else{
            nodesCache = builder.buildCache(builder.getDocumentCacheSize());
        }

        cacheStats = new CacheStats(nodesCache, "MongoMk-Documents", builder.getWeigher(),
                builder.getDocumentCacheSize());
    }

    private Cache<String , NodeDocument> createOffHeapCache(MongoMK.Builder builder){
        ForwardingListener<String , NodeDocument> listener = ForwardingListener.newInstance();

        Cache<String,NodeDocument> primaryCache = CacheBuilder.newBuilder()
                .weigher(builder.getWeigher())
                .maximumWeight(builder.getDocumentCacheSize())
                .removalListener(listener)
                .recordStats()
                .build();

        Cache<String,NodeDocument> cache =
                new NodeDocOffHeapCache( primaryCache, listener, builder, this );
        return cache;
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
            Lock lock = getAndLock(key);
            try {
                nodesCache.invalidate(key);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return find(collection, key, Integer.MAX_VALUE);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T extends Document> T find(final Collection<T> collection,
                                       final String key,
                                       int maxCacheAge) {
        if (collection != Collection.NODES) {
            return findUncached(collection, key);
        }
        NodeDocument doc;
        if (maxCacheAge > 0) {
            // first try without lock
            doc = nodesCache.getIfPresent(key);
            if (doc != null) {
                if (maxCacheAge == Integer.MAX_VALUE ||
                        System.currentTimeMillis() - doc.getCreated() < maxCacheAge) {
                    if (doc == NodeDocument.NULL) {
                        return null;
                    }
                    return (T) doc;
                }
            }
        }
        try {
            Lock lock = getAndLock(key);
            try {
                if (maxCacheAge == 0) {
                    invalidateCache(collection, key);
                }
                while (true) {
                    doc = nodesCache.get(key, new Callable<NodeDocument>() {
                        @Override
                        public NodeDocument call() throws Exception {
                            NodeDocument doc = (NodeDocument) findUncached(collection, key);
                            if (doc == null) {
                                doc = NodeDocument.NULL;
                            }
                            return doc;
                        }
                    });
                    if (maxCacheAge == 0 || maxCacheAge == Integer.MAX_VALUE) {
                        break;
                    }
                    if (System.currentTimeMillis() - doc.getCreated() < maxCacheAge) {
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
            throw new IllegalStateException("Failed to load document with " + key, e);
        }
    }
    
    @CheckForNull
    <T extends Document> T findUncached(Collection<T> collection, String key) {
        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            DBObject obj = dbCollection.findOne(getByKeyQuery(key).get());
            if (obj == null) {
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
        long start = start();
        try {
            DBCursor cursor = dbCollection.find(query).sort(BY_ID_ASC);
            List<T> list = new ArrayList<T>();
            for (int i = 0; i < limit && cursor.hasNext(); i++) {
                DBObject o = cursor.next();
                T doc = convertFromDBObject(collection, o);
                if (collection == Collection.NODES && doc != null) {
                    doc.seal();
                    String id = doc.getId();
                    Lock lock = getAndLock(id);
                    try {
                        // do not overwrite document in cache if the
                        // existing one in the cache is newer
                        NodeDocument cached = nodesCache.getIfPresent(id);
                        if (cached != null && cached != NodeDocument.NULL) {
                            // check mod count
                            Number cachedModCount = cached.getModCount();
                            Number modCount = doc.getModCount();
                            if (cachedModCount == null || modCount == null) {
                                throw new IllegalStateException(
                                        "Missing " + Document.MOD_COUNT);
                            }
                            if (modCount.longValue() > cachedModCount.longValue()) {
                                nodesCache.put(id, (NodeDocument) doc);
                            }
                        } else {
                            nodesCache.put(id, (NodeDocument) doc);
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                list.add(doc);
            }
            return list;
        } finally {
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
                throw new MicroKernelException("Remove failed: " + writeResult.getError());
            }
        } finally {
            end("remove", start);
        }
    }

    @CheckForNull
    private <T extends Document> T findAndModify(Collection<T> collection,
                                                 UpdateOp updateOp,
                                                 boolean upsert,
                                                 boolean checkConditions) {
        DBCollection dbCollection = getDBCollection(collection);
        DBObject update = createUpdate(updateOp);

        Lock lock = getAndLock(updateOp.getId());
        long start = start();
        try {
            // get modCount of cached document
            Number modCount = null;
            T cachedDoc = null;
            if (collection == Collection.NODES) {
                @SuppressWarnings("unchecked")
                T doc = (T) nodesCache.getIfPresent(updateOp.getId());
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
            return oldDoc;
        } catch (Exception e) {
            throw new MicroKernelException(e);
        } finally {
            lock.unlock();
            end("findAndModify", start);
        }
    }

    @CheckForNull
    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException {
        log("createOrUpdate", update);
        T doc = findAndModify(collection, update, true, false);
        log("createOrUpdate returns ", doc);
        return doc;
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException {
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
            update.increment(Document.MOD_COUNT, 1);
            T target = collection.newDocument(this);
            UpdateUtils.applyChanges(target, update, comparator);
            docs.add(target);
            for (Entry<Key, Operation> entry : update.getChanges().entrySet()) {
                Key k = entry.getKey();
                Operation op = entry.getValue();
                switch (op.type) {
                    case SET:
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
                        Lock lock = getAndLock(doc.getId());
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
        DBObject update = createUpdate(updateOp);
        long start = start();
        try {
            Map<String, NodeDocument> cachedDocs = Collections.emptyMap();
            if (collection == Collection.NODES) {
                cachedDocs = Maps.newHashMap();
                for (String key : keys) {
                    cachedDocs.put(key, nodesCache.getIfPresent(key));
                }
            }
            try {
                WriteResult writeResult = dbCollection.update(query.get(), update, false, true, WriteConcern.SAFE);
                if (writeResult.getError() != null) {
                    throw new MicroKernelException("Update failed: " + writeResult.getError());
                }
                // update cache
                for (Entry<String, NodeDocument> entry : cachedDocs.entrySet()) {
                    Lock lock = getAndLock(entry.getKey());
                    try {
                        if (entry.getValue() == null) {
                            // make sure concurrently loaded document is invalidated
                            nodesCache.invalidate(entry.getKey());
                        } else {
                            applyToCache(Collection.NODES, entry.getValue(),
                                    new UpdateOp(entry.getKey(), updateOp));
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (MongoException e) {
                throw new MicroKernelException(e);
            }
        } finally {
            end("update", start);
        }
    }

    @CheckForNull
    <T extends Document> T convertFromDBObject(@Nonnull Collection<T> collection,
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("MongoDB time: " + timeSum);
        }
        nodes.getDB().getMongo().close();

        if(nodesCache instanceof Closeable){
            try {
                ((Closeable)nodesCache).close();
            } catch (IOException e) {

                LOG.warn("Error occurred while closing Off Heap Cache",e);
            }
        }
    }

    public CacheStats getCacheStats() {
        return cacheStats;
    }

    Iterable<? extends Map.Entry<String, ? extends CachedNodeDocument>> getCacheEntries() {
        if(nodesCache instanceof OffHeapCache){
            return Iterables.concat(nodesCache.asMap().entrySet(),
                    ((OffHeapCache)nodesCache).offHeapEntriesMap().entrySet());
        }
        return nodesCache.asMap().entrySet();
    }

    CachedNodeDocument getCachedNodeDoc(String id){
        if(nodesCache instanceof OffHeapCache){
            return  ((OffHeapCache) nodesCache).getCachedDocument(id);
        }

        return nodesCache.getIfPresent(id);
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
            NodeDocument newDoc = (NodeDocument) collection.newDocument(this);
            if (oldDoc != null) {
                oldDoc.deepCopy(newDoc);
                oldDoc.seal();
            }
            String key = updateOp.getId();
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
            for (;;) {
                NodeDocument cached = nodesCache.get(doc.getId(),
                        new Callable<NodeDocument>() {
                    @Override
                    public NodeDocument call() {
                        return doc;
                    }
                });
                if (cached != NodeDocument.NULL) {
                    return cached;
                } else {
                    nodesCache.invalidate(doc.getId());
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
        QueryBuilder query = getByKeyQuery(updateOp.id);

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
                case SET: {
                    setUpdates.append(k.toString(), op.value);
                    break;
                }
                case INCREMENT: {
                    incUpdates.append(k.toString(), op.value);
                    break;
                }
                case SET_MAP_ENTRY: {
                    setUpdates.append(k.toString(), op.value);
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
        if (!incUpdates.isEmpty()) {
            update.append("$inc", incUpdates);
        }
        if (!unsetUpdates.isEmpty()) {
            update.append("$unset", unsetUpdates);
        }

        return update;
    }

    private Lock getAndLock(String key) {
        Lock l = locks.get(key);
        l.lock();
        return l;
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        if (readWriteMode == null || readWriteMode.equals(lastReadWriteMode)) {
            return;
        }
        lastReadWriteMode = readWriteMode;
        try {
            Map<String, String> map = Splitter.on(", ").withKeyValueSeparator(":").split(readWriteMode);
            String read = map.get("read");
            if (read != null) {
                ReadPreference readPref = ReadPreference.valueOf(read);
                if (!readPref.equals(nodes.getReadPreference())) {
                    nodes.setReadPreference(readPref);
                    LOG.info("Using ReadPreference " + readPref);
                }
            } 
            String write = map.get("write");
            if (write != null) {
                WriteConcern writeConcern = WriteConcern.valueOf(write);
                if (!writeConcern.equals(nodes.getWriteConcern())) {
                    nodes.setWriteConcern(writeConcern);
                    LOG.info("Using WriteConcern " + writeConcern);
                }
            }
        } catch (Exception e) {
            LOG.error("Error setting readWriteMode " + readWriteMode, e);
        }
    }
}