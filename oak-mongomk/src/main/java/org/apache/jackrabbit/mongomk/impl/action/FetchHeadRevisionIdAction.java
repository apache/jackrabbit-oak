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
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.mongomk.impl.model.MongoSync;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An action for fetching the head revision.
 */
public class FetchHeadRevisionIdAction extends BaseAction<Long> {

    private final String branchId;

    /**
     * Constructs a new {@code FetchHeadRevisionIdAction}.
     *
     * @param nodeStore Node store.
     */
    public FetchHeadRevisionIdAction(MongoNodeStore nodeStore) {
        this(nodeStore, null);
    }

    /**
     * Constructs a new {@code FetchHeadRevisionIdAction}.
     *
     * @param nodeStore Node store.
     * @param branchId Branch id.
     */
    public FetchHeadRevisionIdAction(MongoNodeStore nodeStore, String branchId) {
        super(nodeStore);
        this.branchId = branchId;
    }

    @Override
    public Long execute() throws Exception {
        DBCollection headCollection = nodeStore.getSyncCollection();
        MongoSync syncMongo = (MongoSync)headCollection.findOne();
        long headRevisionId = syncMongo.getHeadRevisionId();

        DBCollection collection = nodeStore.getCommitCollection();
        QueryBuilder qb = QueryBuilder.start(MongoCommit.KEY_FAILED).notEquals(Boolean.TRUE)
                .and(MongoCommit.KEY_REVISION_ID).lessThanEquals(headRevisionId);
        if (branchId == null) {
            qb = qb.and(new BasicDBObject(MongoNode.KEY_BRANCH_ID, new BasicDBObject("$exists", false)));
        } else {
            qb = qb.and(MongoNode.KEY_BRANCH_ID).is(branchId);
        }

        DBObject query = qb.get();
        DBObject fields = new BasicDBObject(MongoCommit.KEY_REVISION_ID, 1);
        DBObject orderBy = new BasicDBObject(MongoCommit.KEY_REVISION_ID, -1);
        MongoCommit commit = (MongoCommit)collection.findOne(query, fields, orderBy);
        return commit != null? commit.getRevisionId() : headRevisionId;
    }
}
