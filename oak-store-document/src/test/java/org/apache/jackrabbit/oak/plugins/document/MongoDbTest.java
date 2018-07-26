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

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.bson.conversions.Bson;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;

/**
 * Test directly ran against MongoDB.
 */
public class MongoDbTest {

    @Test
    @Ignore
    public void manyChildNodes() {
        MongoConnection c = MongoUtils.getConnection();
        MongoDatabase db = c.getDatabase();
        MongoUtils.dropCollections(c.getDatabase());
        MongoCollection<BasicDBObject> nodes = db.getCollection(Collection.NODES.toString(), BasicDBObject.class);
        BasicDBObject index = new BasicDBObject();
        // modification time (descending)
        index.put("_mod", -1L);
        // and then id (ascending)
        index.put("_id", 1L);
        IndexOptions options = new IndexOptions();
        // options.unique(true);
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
            List<BasicDBObject> inserts = new ArrayList<>();
            group++;
            for (int j = 0; j < perInsert; j++, i++) {
                BasicDBObject doc = new BasicDBObject();
                inserts.add(doc);
                doc.put("_id", parent + "/node" + i);
                doc.put("_mod", group);
            }
            nodes.insertMany(inserts);
            log("inserted " + i + "/" + children);
        }
        Bson query = Filters.and(
                Filters.gte("_mod", group - 1),
                Filters.gt("_id", parent + "/"),
                Filters.lte("_id", parent + "0")
        );
        BasicDBObject keys = new BasicDBObject();
        keys.put("_id", 1);
        FindIterable<BasicDBObject> cursor = nodes.find(query).projection(keys);
        int count = 0;
        log("Query plan: " + explain(nodes, query));
        long time = System.currentTimeMillis();
        for (BasicDBObject obj : cursor) {
            // dummy read operation (to ensure we did get the data)
            obj.get("_id");
            count++;
            // log(" read " + obj);
        }
        time = System.currentTimeMillis() - time;
        log("Time: " + time + " ms");
        log("Count: " + count);
        c.close();
    }

    @Test
    @Ignore
    public void updateDocument() {
        MongoConnection c = MongoUtils.getConnection();
        MongoDatabase db = c.getDatabase();
        MongoUtils.dropCollections(c.getDatabase());
        MongoCollection<BasicDBObject> nodes = db.getCollection(Collection.NODES.toString(), BasicDBObject.class);
        BasicDBObject index = new BasicDBObject();
        // modification time (descending)
        index.put("_mod", -1L);
        // and then id (ascending)
        index.put("_id", 1L);
        IndexOptions options = new IndexOptions();
        // options.unique(true);
        nodes.createIndex(index, options);

        long time;
        time = System.currentTimeMillis();

        int nodeCount = 4500;
        String parent = "/parent/node/abc";
        List<BasicDBObject> inserts = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            BasicDBObject doc = new BasicDBObject();
            inserts.add(doc);
            doc.put("_id", parent + "/node" + i);
            doc.put("_mod", 0);
            doc.put("_counter", 0);
            doc.put("x", 10);
        }
        nodes.insertMany(inserts);

        time = System.currentTimeMillis() - time;
        System.out.println("insert: " + time);
        time = System.currentTimeMillis();

        for (int i = 0; i < nodeCount; i++) {
            BasicDBObject fields = new BasicDBObject();
            // return _id only
            fields.put("_id", 1);
            FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions()
                    .projection(fields).upsert(true);

            BasicDBObject query = new BasicDBObject(Document.ID, parent + "/node" + i);

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
            nodes.findOneAndUpdate(query, update, updateOptions);

            // 250 ms WriteConcern.NORMAL, NONE
            // 891 ms WriteConcern.SAFE
            // > 10 s WriteConcern.JOURNAL_SAFE, FSYNC_SAFE

//            WriteResult result =
//            nodes.update(query, update, /* upsert */ true, /* multi */ false,
//                    WriteConcern.NORMAL);


        }

        time = System.currentTimeMillis() - time;
        System.out.println("update: " + time);
        time = System.currentTimeMillis();

        c.close();
    }

    private static BasicDBObject explain(MongoCollection<BasicDBObject> collection,
                                         Bson query) {
        return collection.find(query).modifiers(new BasicDBObject("$explain", true)).first();
    }

    private static void log(String msg) {
        System.out.println(msg);
    }

}
