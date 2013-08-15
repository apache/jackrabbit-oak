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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.mongomk.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
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

    private final DBCollection nodes;
    private final DBCollection clusterNodes;
    
    /**
     * The sum of all milliseconds this class waited for MongoDB.
     */
    private long timeSum;
    
    private final Cache<String, NodeDocument> nodesCache;
    private final CacheStats cacheStats;

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
        nodesCache = builder.buildCache(builder.getDocumentCacheSize());
        cacheStats = new CacheStats(nodesCache, "MongoMk-Documents", builder.getWeigher(),
                builder.getDocumentCacheSize());
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
        nodesCache.invalidateAll();
    }
    
    @Override
    public void invalidateCache(Collection collection, String key) {
        if (collection == Collection.NODES) {
            nodesCache.invalidate(key);
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return find(collection, key, Integer.MAX_VALUE);
    }
    
    @Override
    public <T extends Document> T find(final Collection<T> collection,
                                       final String key,
                                       int maxCacheAge) {
        if (collection != Collection.NODES) {
            return findUncached(collection, key);
        }
        try {
            NodeDocument doc;
            if (maxCacheAge == 0) {
                nodesCache.invalidate(key);
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
                nodesCache.invalidate(key);
            }
            if (doc == NodeDocument.NULL) {
                return null;
            } else {
                //noinspection unchecked
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
            DBObject doc = dbCollection.findOne(getByKeyQuery(key).get());
            if (doc == null) {
                return null;
            }
            return convertFromDBObject(collection, doc);
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
        queryBuilder.greaterThanEquals(fromKey);
        queryBuilder.lessThan(toKey);
        if (indexedProperty != null) {
            queryBuilder.and(indexedProperty);
            queryBuilder.greaterThanEquals(startValue);
        }
        DBObject query = queryBuilder.get();
        long start = start();
        try {
            DBCursor cursor = dbCollection.find(query);
            List<T> list = new ArrayList<T>();
            for (int i = 0; i < limit && cursor.hasNext(); i++) {
                DBObject o = cursor.next();
                T doc = convertFromDBObject(collection, o);
                if (collection == Collection.NODES) {
                    nodesCache.put(doc.getId(), (NodeDocument) doc);
                }
                list.add(doc);
            }
            return list;
        } finally {
            end("query", start);
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
            end("remove", start);
        }
    }

    @CheckForNull
    private <T extends Document> T findAndModify(Collection<T> collection,
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
            if (k.equals(Document.ID)) {
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
            T doc = convertFromDBObject(collection, oldNode);
            
            // cache the new document
            if (collection == Collection.NODES) {
                T newDoc = collection.newDocument();
                Utils.deepCopyMap(doc, newDoc);
                String key = updateOp.getKey();
                MemoryDocumentStore.applyChanges(newDoc, updateOp);
                nodesCache.put(key, (NodeDocument) newDoc);
            }
            
            return doc;
        } catch (Exception e) {
            throw new MicroKernelException(e);
        } finally {
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
            T target = collection.newDocument();
            MemoryDocumentStore.applyChanges(target, update);
            docs.add(target);
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
                    for (T doc : docs) {
                        nodesCache.put(doc.getId(), (NodeDocument) doc);
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

    private static <T extends Document> T convertFromDBObject(Collection<T> collection,
                                                              DBObject n) {
        T copy = collection.newDocument();
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
    }

    public CacheStats getCacheStats() {
        return cacheStats;
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
    public boolean isCached(Collection collection, String key) {
        if (collection != Collection.NODES) {
            return false;
        }
        return nodesCache.getIfPresent(key) != null;
    }

}