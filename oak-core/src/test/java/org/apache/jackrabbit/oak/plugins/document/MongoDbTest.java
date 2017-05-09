/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;

/**
 * Test directly ran against MongoDB.
 */
public class MongoDbTest {

    @Test
    @Ignore
    public void manyChildNodes() {
        DB db = MongoUtils.getConnection().getDB();
        MongoUtils.dropCollections(db);
        DBCollection nodes = db.getCollection(Collection.NODES.toString());
        DBObject index = new BasicDBObject();
        // modification time (descending)
        index.put("_mod", -1L);
        // and then id (ascending)
        index.put("_id", 1L);
        DBObject options = new BasicDBObject();
        // options.put("unique", Boolean.TRUE);
        nodes.createIndex(index, options);

        // index on (_id, _mod):
        // Query plan: { "cursor" : "BtreeCursor _id_1__mod_-1" ,
        // "isMultiKey" : false , "n" : 2000 , "nscannedObjects" : 2000 ,
        // "nscanned" : 954647 , "nscannedObjectsAllPlans" : 1907080 ,
        // "nscannedAllPlans" : 2859727 , "scanAndOrder" : false ,
        // "indexOnly" : true , "nYields" : 5 , "nChunkSkips" : 0 ,
        // "millis" : 5112 ,...
        // Time: 2229 ms
        // Count: 2000

        // index on (_mod, _id)
        // Query plan: { "cursor" : "BtreeCursor _mod_-1__id_1" ,
        // "isMultiKey" : false , "n" : 2000 , "nscannedObjects" : 2000 ,
        // "nscanned" : 2000 , "nscannedObjectsAllPlans" : 2203 ,
        // "nscannedAllPlans" : 2203 , "scanAndOrder" : false ,
        // "indexOnly" : true , "nYields" : 0 , "nChunkSkips" : 0 ,
        // "millis" : 3 ,...
        // Time: 43 ms
        // Count: 2000

        int children = 1000000;
        int perInsert = 1000;
        int group = 0;
        String parent = "/parent/node/abc";
        for (int i = 0; i < children;) {
            DBObject[] inserts = new DBObject[perInsert];
            group++;
            for (int j = 0; j < perInsert; j++, i++) {
                BasicDBObject doc = new BasicDBObject();
                inserts[j] = doc;
                doc.put("_id", parent + "/node" + i);
                doc.put("_mod", group);
            }
            nodes.insert(inserts, WriteConcern.SAFE);
            log("inserted " + i + "/" + children);
        }
        QueryBuilder queryBuilder = QueryBuilder.start("_mod");
        queryBuilder.greaterThanEquals(group - 1);
        queryBuilder.and("_id").greaterThan(parent + "/");
        queryBuilder.and("_id").lessThanEquals(parent + "0");
        DBObject query = queryBuilder.get();
        BasicDBObject keys = new BasicDBObject();
        keys.put("_id", 1);
        DBCursor cursor = nodes.find(query, keys);
        int count = 0;
        log("Query plan: " + cursor.explain());
        long time = System.currentTimeMillis();
        while (cursor.hasNext()) {
            DBObject obj = cursor.next();
            // dummy read operation (to ensure we did get the data)
            obj.get("_id");
            count++;
            // log(" read " + obj);
        }
        time = System.currentTimeMillis() - time;
        log("Time: " + time + " ms");
        log("Count: " + count);
        db.getMongo().close();
    }

    @Test
    @Ignore
    public void updateDocument() {
        DB db = MongoUtils.getConnection().getDB();
        MongoUtils.dropCollections(db);
        DBCollection nodes = db.getCollection(Collection.NODES.toString());
        DBObject index = new BasicDBObject();
        // modification time (descending)
        index.put("_mod", -1L);
        // and then id (ascending)
        index.put("_id", 1L);
        DBObject options = new BasicDBObject();
        // options.put("unique", Boolean.TRUE);
        nodes.createIndex(index, options);

        long time;
        time = System.currentTimeMillis();

        int nodeCount = 4500;
        String parent = "/parent/node/abc";
        DBObject[] inserts = new DBObject[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            BasicDBObject doc = new BasicDBObject();
            inserts[i] = doc;
            doc.put("_id", parent + "/node" + i);
            doc.put("_mod", 0);
            doc.put("_counter", 0);
            doc.put("x", 10);
        }
        nodes.insert(inserts, WriteConcern.SAFE);

        time = System.currentTimeMillis() - time;
        System.out.println("insert: " + time);
        time = System.currentTimeMillis();

        for (int i = 0; i < nodeCount; i++) {
            QueryBuilder queryBuilder = QueryBuilder.start(Document.ID).is(parent + "/node" + i);
            DBObject fields = new BasicDBObject();
            // return _id only
            fields.put("_id", 1);
            DBObject query = queryBuilder.get();

            BasicDBObject setUpdates = new BasicDBObject();
            BasicDBObject incUpdates = new BasicDBObject();
            BasicDBObject unsetUpdates = new BasicDBObject();

            setUpdates.append("_mod", i);
            incUpdates.append("_counter", 1);
            unsetUpdates.append("x", "1");

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

            // 1087 ms (upsert true+false, returnNew = false)
            // 1100 ms (returnNew = true)
//            DBObject oldNode =
            nodes.findAndModify(query, fields,
                    null /*sort*/, false /*remove*/, update, false /*returnNew*/,
                    true /*upsert*/);

            // 250 ms WriteConcern.NORMAL, NONE
            // 891 ms WriteConvern.SAFE
            // > 10 s WriteConcern.JOURNAL_SAFE, FSYNC_SAFE

//            WriteResult result =
//            nodes.update(query, update, /* upsert */ true, /* multi */ false,
//                    WriteConcern.NORMAL);


        }

        time = System.currentTimeMillis() - time;
        System.out.println("update: " + time);
        time = System.currentTimeMillis();

        db.getMongo().close();
    }

    private static void log(String msg) {
        System.out.println(msg);
    }

}
