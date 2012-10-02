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
package org.apache.jackrabbit.mongomk.query;

import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.model.HeadMongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An query for saveing and setting the head revision id.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class SaveAndSetHeadRevisionQuery extends AbstractQuery<HeadMongo> {

    private final long newHeadRevision;
    private final long oldHeadRevision;

    /**
     * Constructs a new {@code SaveAndSetHeadRevisionQuery}.
     *
     * @param mongoConnection
     *            The {@link MongoConnection}.
     * @param oldHeadRevision
     * @param newHeadRevision
     */
    public SaveAndSetHeadRevisionQuery(MongoConnection mongoConnection, long oldHeadRevision, long newHeadRevision) {
        super(mongoConnection);

        this.oldHeadRevision = oldHeadRevision;
        this.newHeadRevision = newHeadRevision;
    }

    @Override
    public HeadMongo execute() throws Exception {
        HeadMongo headMongo = null;

        DBCollection headCollection = mongoConnection.getHeadCollection();
        DBObject query = QueryBuilder.start(HeadMongo.KEY_HEAD_REVISION_ID).is(oldHeadRevision).get();
        DBObject update = new BasicDBObject("$set", new BasicDBObject(HeadMongo.KEY_HEAD_REVISION_ID, newHeadRevision));

        DBObject dbObject = headCollection.findAndModify(query, null, null, false, update, true, false);
        if (dbObject != null) {
            headMongo = HeadMongo.fromDBObject(dbObject);
        }

        return headMongo;
    }
}
