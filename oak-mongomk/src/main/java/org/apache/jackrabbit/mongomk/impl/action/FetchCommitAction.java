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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An action for fetching a commit. An exception is thrown if a commit with the
 * revision id does not exist.
 */
public class FetchCommitAction extends BaseAction<MongoCommit> {

    private static final Logger LOG = LoggerFactory.getLogger(FetchCommitAction.class);

    private final long revisionId;

    /**
     * Constructs a new {@link FetchCommitAction}
     *
     * @param nodeStore Node store.
     * @param revisionId Revision id.
     */
    public FetchCommitAction(MongoNodeStore nodeStore, long revisionId) {
        super(nodeStore);
        this.revisionId = revisionId;
    }

    @Override
    public MongoCommit execute() throws Exception {
        DBCollection commitCollection = nodeStore.getCommitCollection();
        DBObject query = QueryBuilder.start(MongoCommit.KEY_FAILED).notEquals(Boolean.TRUE)
                .and(MongoCommit.KEY_REVISION_ID).is(revisionId)
                .get();

        LOG.debug(String.format("Executing query: %s", query));

        DBObject dbObject = commitCollection.findOne(query);
        if (dbObject == null) {
            throw new Exception(String.format("Commit with revision %d could not be found", revisionId));
        }
        return (MongoCommit)dbObject;
    }
}