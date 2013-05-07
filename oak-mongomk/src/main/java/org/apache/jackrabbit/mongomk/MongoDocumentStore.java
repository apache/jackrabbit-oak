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
package org.apache.jackrabbit.mongomk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mongomk.UpdateOp.Operation;
import org.apache.jackrabbit.mongomk.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * A document store that uses MongoDB as the backend.
 */
public class MongoDocumentStore implements DocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStore.class);
    
    /**
     * The number of documents to cache.
     */
    private static final int CACHE_DOCUMENTS = Integer.getInteger("oak.mongoMK.cacheDocs", 20 * 1024);

    private static final boolean LOG_TIME = false;

    private final DBCollection nodes;
    private final DBCollection clusterNodes;
    
    /**
     * The sum of all milliseconds this class waited for MongoDB.
     */
    private long timeSum;
    
    private final Cache<String, CachedDocument> nodesCache;

    public MongoDocumentStore(DB db) {
        nodes = db.getCollection(
                Collection.NODES.toString());
        clusterNodes = db.getCollection(
                Collection.CLUSTER_NODES.toString());
        
        // the _id field is the primary key, so we don't need to define it
        // the following code is just a template in case we need more indexes
        // DBObject index = new BasicDBObject();
        // index.put(KEY_PATH, 1L);
        // DBObject options = new BasicDBObject();
        // options.put("unique", Boolean.TRUE);
        // nodesCollection.ensureIndex(index, options);

        // TODO expire entries if the parent was changed
        nodesCache = CacheBuilder.newBuilder()
                .maximumSize(CACHE_DOCUMENTS)
                .build();
        
    }
    
    private static long start() {
        return LOG_TIME ? System.currentTimeMillis() : 0;
    }
    
    private void end(long start) {
        if (LOG_TIME) {
            timeSum += System.currentTimeMillis() - start;
        }
    }
    
    public void finalize() throws Throwable {
        super.finalize();
        // TODO should not be needed, but it seems
        // oak-jcr doesn't call dispose()
        dispose();
    }
    
    @Override
    public void invalidateCache() {
        nodesCache.invalidateAll();
    }
    
    @Override
    public void invalidateCache(Collection collection, String key) {
        if (collection == Collection.NODES) {
            nodesCache.invalidate(key);
        }
    }

    public Map<String, Object> find(Collection collection, String key) {
        return find(collection, key, Integer.MAX_VALUE);
    }
    
    @Override
    public Map<String, Object> find(final Collection collection, final String key, int maxCacheAge) {
        if (collection != Collection.NODES) {
            return findUncached(collection, key);
        }
        try {
            CachedDocument doc;
            if (maxCacheAge == 0) {
                nodesCache.invalidate(key);
            }
            while (true) {
                doc = nodesCache.get(key, new Callable<CachedDocument>() {
                    @Override
                    public CachedDocument call() throws Exception {
                        Map<String, Object> map = findUncached(collection, key);
                        return new CachedDocument(map);
                    }
                });
                if (maxCacheAge == 0 || maxCacheAge == Integer.MAX_VALUE) {
                    break;
                }
                if (System.currentTimeMillis() - doc.time < maxCacheAge) {
                    break;
                }
                // too old: invalidate, try again
                nodesCache.invalidate(key);
            }
            return doc.value;
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to load document with " + key, e);
        }
    }
    
    Map<String, Object> findUncached(Collection collection, String key) {
        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            DBObject doc = dbCollection.findOne(getByKeyQuery(key).get());
            if (doc == null) {
                return null;
            }
            return convertFromDBObject(doc);
        } finally {
            end(start);
        }
    }
    
    @Nonnull
    @Override
    public List<Map<String, Object>> query(Collection collection,
            String fromKey, String toKey, int limit) {
        log("query", fromKey, toKey, limit);
        DBCollection dbCollection = getDBCollection(collection);
        QueryBuilder queryBuilder = QueryBuilder.start(UpdateOp.ID);
        queryBuilder.greaterThanEquals(fromKey);
        queryBuilder.lessThan(toKey);
        DBObject query = queryBuilder.get();
        long start = start();
        try {
            DBCursor cursor = dbCollection.find(query);
            List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
            for (int i = 0; i < limit && cursor.hasNext(); i++) {
                DBObject o = cursor.next();
                Map<String, Object> map = convertFromDBObject(o);
                if (collection == Collection.NODES) {
                    String key = (String) map.get(UpdateOp.ID);
                    nodesCache.put(key, new CachedDocument(map));
                }
                list.add(map);
            }
            return list;
        } finally {
            end(start);
        }
    }

    @Override
    public void remove(Collection collection, String key) {
        log("remove", key);        
        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            if (collection == Collection.NODES) {
                nodesCache.invalidate(key);
            }
            WriteResult writeResult = dbCollection.remove(getByKeyQuery(key).get(), WriteConcern.SAFE);
            if (writeResult.getError() != null) {
                throw new MicroKernelException("Remove failed: " + writeResult.getError());
            }
        } finally {
            end(start);
        }
    }

    @CheckForNull
    private Map<String, Object> findAndModify(Collection collection,
                                               UpdateOp updateOp,
                                               boolean upsert,
                                               boolean checkConditions) {
        DBCollection dbCollection = getDBCollection(collection);
        QueryBuilder query = getByKeyQuery(updateOp.key);

        BasicDBObject setUpdates = new BasicDBObject();
        BasicDBObject incUpdates = new BasicDBObject();
        BasicDBObject unsetUpdates = new BasicDBObject();

        for (Entry<String, Operation> entry : updateOp.changes.entrySet()) {
            String k = entry.getKey();
            if (k.equals(UpdateOp.ID)) {
                // avoid exception "Mod on _id not allowed"
                continue;
            }
            Operation op = entry.getValue();
            switch (op.type) {
                case SET: {
                    setUpdates.append(k, op.value);
                    break;
                }
                case INCREMENT: {
                    incUpdates.append(k, op.value);
                    break;
                }
                case SET_MAP_ENTRY: {
                    setUpdates.append(k, op.value);
                    break;
                }
                case REMOVE_MAP_ENTRY: {
                    unsetUpdates.append(k, "1");
                    break;
                }
                case SET_MAP: {
                    String[] kv = k.split("\\.");
                    BasicDBObject sub = new BasicDBObject();
                    sub.put(kv[1], op.value);
                    setUpdates.append(kv[0], sub);
                    break;
                }
                case CONTAINS_MAP_ENTRY: {
                    if (checkConditions) {
                        query.and(k).exists(op.value);
                    }
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

        // dbCollection.update(query, update, true /*upsert*/, false /*multi*/,
        //         WriteConcern.SAFE);
        // return null;

        long start = start();
        try {
            DBObject oldNode = dbCollection.findAndModify(query.get(), null /*fields*/,
                    null /*sort*/, false /*remove*/, update, false /*returnNew*/,
                    upsert /*upsert*/);
            if (checkConditions && oldNode == null) {
                return null;
            }
            Map<String, Object> map = convertFromDBObject(oldNode);
            
            // cache the new document
            if (collection == Collection.NODES) {
                Map<String, Object> newMap = Utils.newMap();
                Utils.deepCopyMap(map, newMap);
                String key = updateOp.getKey();
                MemoryDocumentStore.applyChanges(newMap, updateOp);
                nodesCache.put(key, new CachedDocument(newMap));
            }
            
            return map;
        } catch (Exception e) {
            throw new MicroKernelException(e);
        } finally {
            end(start);
        }
    }

    @Nonnull
    @Override
    public Map<String, Object> createOrUpdate(Collection collection,
                                              UpdateOp update)
            throws MicroKernelException {
        log("createOrUpdate", update);
        Map<String, Object> map = findAndModify(collection, update, true, false);
        log("createOrUpdate returns ", map);
        return map;
    }

    @Override
    public Map<String, Object> findAndUpdate(Collection collection,
                                             UpdateOp update)
            throws MicroKernelException {
        log("findAndUpdate", update);
        Map<String, Object> map = findAndModify(collection, update, false, true);
        log("findAndUpdate returns ", map);
        return map;
    }

    @Override
    public boolean create(Collection collection, List<UpdateOp> updateOps) {
        log("create", updateOps);       
        ArrayList<Map<String, Object>> maps = new ArrayList<Map<String, Object>>();
        DBObject[] inserts = new DBObject[updateOps.size()];

        for (int i = 0; i < updateOps.size(); i++) {
            inserts[i] = new BasicDBObject();
            UpdateOp update = updateOps.get(i);
            Map<String, Object> target = Utils.newMap();
            MemoryDocumentStore.applyChanges(target, update);
            maps.add(target);
            for (Entry<String, Operation> entry : update.changes.entrySet()) {
                String k = entry.getKey();
                Operation op = entry.getValue();
                switch (op.type) {
                    case SET:
                    case INCREMENT: {
                        inserts[i].put(k, op.value);
                        break;
                    }
                    case SET_MAP:
                    case SET_MAP_ENTRY: {
                        String[] kv = k.split("\\.");
                        DBObject value = new BasicDBObject(kv[1], op.value);
                        inserts[i].put(kv[0], value);
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
                    for (Map<String, Object> map : maps) {
                        String id = (String) map.get(UpdateOp.ID);
                        nodesCache.put(id, new CachedDocument(map));
                    }
                }
                return true;
            } catch (MongoException e) {
                return false;
            }
        } finally {
            end(start);
        }        
    }

    private static Map<String, Object> convertFromDBObject(DBObject n) {
        Map<String, Object> copy = Utils.newMap();
        if (n != null) {
            for (String key : n.keySet()) {
                Object o = n.get(key);
                if (o instanceof String) {
                    copy.put(key, o);
                } else if (o instanceof Long) {
                    copy.put(key, o);
                } else if (o instanceof BasicDBObject) {
                    copy.put(key, o);
                }
            }
        }
        return copy;
    }

    private DBCollection getDBCollection(Collection collection) {
        switch (collection) {
            case NODES:
                return nodes;
            case CLUSTER_NODES:
                return clusterNodes;
            default:
                throw new IllegalArgumentException(collection.name());
        }
    }

    private static QueryBuilder getByKeyQuery(String key) {
        return QueryBuilder.start(UpdateOp.ID).is(key);
    }
    
    @Override
    public void dispose() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("MongoDB time: " + timeSum);
        }
        nodes.getDB().getMongo().close();
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
    
    /**
     * A cache entry.
     */
    static class CachedDocument {
        final long time = System.currentTimeMillis();
        final Map<String, Object> value;
        CachedDocument(Map<String, Object> value) {
            this.value = value;
        }
    }

    @Override
    public boolean isCached(Collection collection, String key) {
        if (collection != Collection.NODES) {
            return false;
        }
        return nodesCache.getIfPresent(key) != null;
    }

}