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
import com.mongodb.QueryBuilder;

/**
 * An action for saving and setting the head revision id.
 */
public class SaveAndSetHeadRevisionAction extends BaseAction<SyncMongo> {

    private final long newHeadRevision;
    private final long oldHeadRevision;

    /**
     * Constructs a new {@code SaveAndSetHeadRevisionAction}.
     *
     * @param mongoConnection  The {@link MongoConnection}.
     * @param oldHeadRevision
     * @param newHeadRevision
     */
    public SaveAndSetHeadRevisionAction(MongoConnection mongoConnection,
            long oldHeadRevision, long newHeadRevision) {
        super(mongoConnection);
        this.oldHeadRevision = oldHeadRevision;
        this.newHeadRevision = newHeadRevision;
    }

    @Override
    public SyncMongo execute() throws Exception {
        DBCollection headCollection = mongoConnection.getSyncCollection();
        DBObject query = QueryBuilder.start(SyncMongo.KEY_HEAD_REVISION_ID).is(oldHeadRevision).get();
        DBObject update = new BasicDBObject("$set", new BasicDBObject(SyncMongo.KEY_HEAD_REVISION_ID, newHeadRevision));
        DBObject dbObject = headCollection.findAndModify(query, null, null, false, update, true, false);
        return SyncMongo.fromDBObject(dbObject);
    }
}
