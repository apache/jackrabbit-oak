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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An action for fetching the base (trunk) revision id that the branch is based on.
 */
public class FetchBranchBaseRevisionIdAction extends BaseAction<Long> {

    private final String branchId;

    /**
     * Constructs a new {@code FetchBranchBaseRevisionIdAction}.
     *
     * @param nodeStore Node store.
     * @param branchId The branch id. It should not be null.
     */
    public FetchBranchBaseRevisionIdAction(MongoNodeStore nodeStore, String branchId) {
        super(nodeStore);
        this.branchId = branchId;
    }

    @Override
    public Long execute() {
        if (branchId == null) {
            throw new IllegalArgumentException("Branch id cannot be null");
        }

        DBCollection commitCollection = nodeStore.getCommitCollection();
        QueryBuilder queryBuilder = QueryBuilder.start(MongoCommit.KEY_FAILED)
                .notEquals(Boolean.TRUE)
                .and(MongoCommit.KEY_BRANCH_ID).is(branchId);
        DBObject query = queryBuilder.get();

        BasicDBObject filter = new BasicDBObject();
        filter.put(MongoCommit.KEY_BASE_REVISION_ID, 1);

        BasicDBObject orderBy = new BasicDBObject(MongoCommit.KEY_BASE_REVISION_ID, 1);

        DBCursor dbCursor = commitCollection.find(query, filter).sort(orderBy).limit(1);
        if (dbCursor.hasNext()) {
            MongoCommit commitMongo = (MongoCommit)dbCursor.next();
            return commitMongo.getBaseRevisionId();
        }
        return 0L;
    }
}
