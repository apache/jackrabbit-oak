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
package org.apache.jackrabbit.mongomk;

import org.apache.jackrabbit.mongomk.DocumentStore.Collection;
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
        nodes.ensureIndex(index, options);  
        
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
    
    private static void log(String msg) {
        System.out.println(msg);
    }
    
}
