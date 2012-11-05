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
package org.apache.jackrabbit.mongomk.action;

import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.model.SyncMongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * An action for reading and incrementing the head revision id.
 */
public class ReadAndIncHeadRevisionAction extends BaseAction<SyncMongo> {

    /**
     * Constructs a new {@code ReadAndIncHeadRevisionQuery}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     */
    public ReadAndIncHeadRevisionAction(MongoConnection mongoConnection) {
        super(mongoConnection);
    }

    @Override
    public SyncMongo execute() throws Exception {
        DBObject query = new BasicDBObject();
        DBObject inc = new BasicDBObject(SyncMongo.KEY_NEXT_REVISION_ID, 1L);
        DBObject update = new BasicDBObject("$inc", inc);
        DBCollection headCollection = mongoConnection.getSyncCollection();

        DBObject dbObject = headCollection.findAndModify(query, null, null, false, update, true, false);
        // Not sure why but sometimes dbObject is null. Simply retry for now.
        while (dbObject == null) {
            dbObject = headCollection.findAndModify(query, null, null, false, update, true, false);
        }
        return SyncMongo.fromDBObject(dbObject);
    }
}
