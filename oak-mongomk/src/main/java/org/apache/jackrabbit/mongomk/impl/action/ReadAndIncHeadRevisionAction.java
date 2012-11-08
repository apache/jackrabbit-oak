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
package org.apache.jackrabbit.mongomk.impl.action;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.model.MongoSync;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * An action for reading and incrementing the head revision id.
 */
public class ReadAndIncHeadRevisionAction extends BaseAction<MongoSync> {

    /**
     * Constructs a new {@code ReadAndIncHeadRevisionQuery}.
     *
     * @param nodeStore Node store.
     */
    public ReadAndIncHeadRevisionAction(MongoNodeStore nodeStore) {
        super(nodeStore);
    }

    @Override
    public MongoSync execute() throws Exception {
        DBObject query = new BasicDBObject();
        DBObject inc = new BasicDBObject(MongoSync.KEY_NEXT_REVISION_ID, 1L);
        DBObject update = new BasicDBObject("$inc", inc);
        DBCollection headCollection = nodeStore.getSyncCollection();

        DBObject dbObject = headCollection.findAndModify(query, null, null, false, update, true, false);
        // FIXME - Not sure why but sometimes dbObject is null. Simply retry for now.
        while (dbObject == null) {
            dbObject = headCollection.findAndModify(query, null, null, false, update, true, false);
        }
        return MongoSync.fromDBObject(dbObject);
    }
}
