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

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mongomk.prototype.UpdateOp.Operation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class MongoDocumentStore implements DocumentStore {

    public static final String KEY_PATH = "_id";

    private final DBCollection nodesCollection;
    
    private final boolean LOG = false;
    private final boolean LOG_TIME = true;
    private long time;

    public MongoDocumentStore(DB db) {
        nodesCollection = db.getCollection(Collection.NODES.toString());
        ensureIndex();
    }
    
    private long start() {
        return LOG_TIME ? System.currentTimeMillis() : 0;
    }
    
    private void end(long start) {
        if (LOG_TIME) {
            time += System.currentTimeMillis() - start;
        }
    }
    
    public void finalize() {
        // TODO should not be needed, but it seems
        // oak-jcr doesn't call dispose()
        dispose();
    }

    @Override
    public Map<String, Object> find(Collection collection, String path) {
        log("find", path);
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
    
    @Override
    public List<Map<String, Object>> query(Collection collection,
            String fromKey, String toKey, int limit) {
        log("query", fromKey, toKey, limit);
        DBCollection dbCollection = getDBCollection(collection);
        QueryBuilder queryBuilder = QueryBuilder.start(KEY_PATH);
        queryBuilder.greaterThanEquals(fromKey);
        queryBuilder.lessThan(toKey);
        DBObject query = queryBuilder.get();
        long start = start();
        try {
            DBCursor cursor = dbCollection.find(query);
            List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
            for (int i=0; i<limit && cursor.hasNext(); i++) {
                DBObject o = cursor.next();
                Map<String, Object> map = convertFromDBObject(o);
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
            WriteResult writeResult = dbCollection.remove(getByPathQuery(path), WriteConcern.SAFE);
            if (writeResult.getError() != null) {
                throw new MicroKernelException("Remove failed: " + writeResult.getError());
            }
        } finally {
            end(start);
        }
    }

    @Override
    public Map<String, Object> createOrUpdate(Collection collection, UpdateOp updateOp) {
        log("createOrUpdate", updateOp);        
        DBCollection dbCollection = getDBCollection(collection);

        BasicDBObject setUpdates = new BasicDBObject();
        BasicDBObject incUpdates = new BasicDBObject();

        for (Entry<String, Operation> entry : updateOp.changes.entrySet()) {
            String k = entry.getKey();
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
                    setUpdates.append(k + "." + op.subKey.toString(), op.value.toString());
                    break;
                }
                case REMOVE_MAP_ENTRY: {
                    // TODO
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

//        dbCollection.update(query, update, true /*upsert*/, false /*multi*/,
//                WriteConcern.SAFE);
//        return null;

        long start = start();
        try {
            DBObject oldNode = dbCollection.findAndModify(query, null /*fields*/,
                    null /*sort*/, false /*remove*/, update, false /*returnNew*/,
                    true /*upsert*/);
            return convertFromDBObject(oldNode);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        } finally {
            end(start);
        }
    }

    @Override
    public void create(Collection collection, List<UpdateOp> updateOps) {
        log("create", updateOps);        
        DBObject[] inserts = new DBObject[updateOps.size()];

        for (int i = 0; i < updateOps.size(); i++) {
            inserts[i] = new BasicDBObject();
            for (Entry<String, Operation> entry : updateOps.get(i).changes.entrySet()) {
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
                    case ADD_MAP_ENTRY: {
                        DBObject value = new BasicDBObject(op.subKey.toString(), op.value.toString());
                        inserts[i].put(k, value);
                        break;
                    }
                    case REMOVE_MAP_ENTRY: {
                        // TODO
                        break;
                    }
                }
            }
        }

        DBCollection dbCollection = getDBCollection(collection);
        long start = start();
        try {
            WriteResult writeResult = dbCollection.insert(inserts, WriteConcern.SAFE);
            if (writeResult.getError() != null) {
                throw new MicroKernelException("Batch create failed: " + writeResult.getError());
            }
        } finally {
            end(start);
        }        
    }

    private void ensureIndex() {
        // the _id field is the primary key, so we don't need to define it
        // the following code is just a template in case we need more indexes
        // DBObject index = new BasicDBObject();
        // index.put(KEY_PATH, 1L);
        // DBObject options = new BasicDBObject();
        // options.put("unique", Boolean.TRUE);
        // nodesCollection.ensureIndex(index, options);
    }

    private static Map<String, Object> convertFromDBObject(DBObject n) {
        Map<String, Object> copy = Utils.newMap();
        if (n != null) {
            synchronized (n) {
                for (String key : n.keySet()) {
                    copy.put(key, n.get(key));
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
        return QueryBuilder.start(KEY_PATH).is(path).get();
    }
    
    @Override
    public void dispose() {
        if (LOG_TIME) {
            System.out.println("MongoDB time: " + time);
        }
        nodesCollection.getDB().getMongo().close();
    }
    
    private void log(Object... args) {
        if (LOG) {
            System.out.println(Arrays.toString(args));
        }
    }

}