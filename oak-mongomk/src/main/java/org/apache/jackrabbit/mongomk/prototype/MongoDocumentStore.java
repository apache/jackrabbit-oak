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
package org.apache.jackrabbit.mongomk.prototype;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mongomk.prototype.UpdateOp.Operation;
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

    private static final boolean LOG_TIME = false;

    private final DBCollection nodesCollection;
    
    private long time;
    
    private final Cache<String, CachedDocument> cache;

    public MongoDocumentStore(DB db) {
        nodesCollection = db.getCollection(Collection.NODES.toString());
        
        // the _id field is the primary key, so we don't need to define it
        // the following code is just a template in case we need more indexes
        // DBObject index = new BasicDBObject();
        // index.put(KEY_PATH, 1L);
        // DBObject options = new BasicDBObject();
        // options.put("unique", Boolean.TRUE);
        // nodesCollection.ensureIndex(index, options);

        // TODO expire entries if the parent was changed
        cache = CacheBuilder.newBuilder()
                .maximumSize(MongoMK.CACHE_DOCUMENTS)
                .build();
        
    }
    
    private static long start() {
        return LOG_TIME ? System.currentTimeMillis() : 0;
    }
    
    private void end(long start) {
        if (LOG_TIME) {
            time += System.currentTimeMillis() - start;
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
        cache.invalidateAll();
    }

    public Map<String, Object> find(Collection collection, String path) {
        return find(collection, path, Integer.MAX_VALUE);
    }
    
    @Override
    public Map<String, Object> find(final Collection collection, final String path, int maxCacheAge) {
        try {
            CachedDocument doc;
            while (true) {
                doc = cache.get(path, new Callable<CachedDocument>() {
                    @Override
                    public CachedDocument call() throws Exception {
                        Map<String, Object> map = findUncached(collection, path);
                        return new CachedDocument(map);
                    }
                });
                if (maxCacheAge == Integer.MAX_VALUE) {
                    break;
                }
                if (System.currentTimeMillis() - doc.time < maxCacheAge) {
                    break;
                }
                // too old: invalidate, try again
                cache.invalidate(path);
            }
            return doc.value;
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to load node " + path, e);
        }
    }
    
    public Map<String, Object> findUncached(Collection collection, String path) {
        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            DBObject doc = dbCollection.findOne(getByPathQuery(path));
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
                String key = (String) map.get(UpdateOp.ID);
                cache.put(key, new CachedDocument(map));
                list.add(map);
            }
            return list;
        } finally {
            end(start);
        }
    }

    @Override
    public void remove(Collection collection, String path) {
        log("remove", path);        
        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            cache.invalidate(path);
            WriteResult writeResult = dbCollection.remove(getByPathQuery(path), WriteConcern.SAFE);
            if (writeResult.getError() != null) {
                throw new MicroKernelException("Remove failed: " + writeResult.getError());
            }
        } finally {
            end(start);
        }
    }

    @Nonnull
    @Override
    public Map<String, Object> createOrUpdate(Collection collection, UpdateOp updateOp) {
        log("createOrUpdate", updateOp);        
        DBCollection dbCollection = getDBCollection(collection);

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
                case ADD_MAP_ENTRY: {
                    setUpdates.append(k, op.value);
                    break;
                }
                case REMOVE_MAP_ENTRY: {
                    unsetUpdates.append(k, "1");
                    break;
                }
                case SET_MAP_ENTRY: {
                    String[] kv = k.split("\\.");
                    BasicDBObject sub = new BasicDBObject();
                    sub.put(kv[1], op.value);
                    setUpdates.append(kv[0], sub);
                    break;
                }
            }
        }

        DBObject query = getByPathQuery(updateOp.key);
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
            DBObject oldNode = dbCollection.findAndModify(query, null /*fields*/,
                    null /*sort*/, false /*remove*/, update, false /*returnNew*/,
                    true /*upsert*/);
            Map<String, Object> map = convertFromDBObject(oldNode);
            
            // cache the new document
            Map<String, Object> newMap = Utils.newMap();
            Utils.deepCopyMap(map, newMap);
            String key = updateOp.getKey();
            MemoryDocumentStore.applyChanges(newMap, updateOp);
            cache.put(key, new CachedDocument(newMap));
            
            log("createOrUpdate returns ", map);
            return map;
        } catch (Exception e) {
            throw new MicroKernelException(e);
        } finally {
            end(start);
        }
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
                    case SET: {
                        inserts[i].put(k, op.value);
                        break;
                    }
                    case INCREMENT: {
                        inserts[i].put(k, op.value);
                        break;
                    }
                    case SET_MAP_ENTRY:
                    case ADD_MAP_ENTRY: {
                        String[] kv = k.split("\\.");
                        DBObject value = new BasicDBObject(kv[1], op.value);
                        inserts[i].put(kv[0], value);
                        break;
                    }
                    case REMOVE_MAP_ENTRY: {
                        // nothing to do for new entries
                    }
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
                for (Map<String, Object> map : maps) {
                    String id = (String) map.get(UpdateOp.ID);
                    cache.put(id, new CachedDocument(map));
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
                return nodesCollection;
            default:
                throw new IllegalArgumentException(collection.name());
        }
    }

    private static DBObject getByPathQuery(String path) {
        return QueryBuilder.start(UpdateOp.ID).is(path).get();
    }
    
    @Override
    public void dispose() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("MongoDB time: " + time);
        }
        nodesCollection.getDB().getMongo().close();
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

}