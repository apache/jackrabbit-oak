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
import org.apache.jackrabbit.mongomk.impl.model.MongoSync;

import com.mongodb.DBCollection;

/**
 * An action for fetching the head revision.
 */
public class FetchHeadRevisionIdAction extends BaseAction<Long> {

    private boolean includeBranchCommits = true;

    /**
     * Constructs a new {@code FetchHeadRevisionIdAction}.
     *
     * @param nodeStore Node store.
     */
    public FetchHeadRevisionIdAction(MongoNodeStore nodeStore) {
        super(nodeStore);
    }

    /**
     * Sets whether the branch commits are included in the query.
     *
     * @param includeBranchCommits Whether the branch commits are included.
     */
    public void includeBranchCommits(boolean includeBranchCommits) {
        this.includeBranchCommits = includeBranchCommits;
    }

    @Override
    public Long execute() throws Exception {
        DBCollection headCollection = nodeStore.getSyncCollection();
        MongoSync syncMongo = (MongoSync)headCollection.findOne();
        long headRevisionId = syncMongo.getHeadRevisionId();
        if (includeBranchCommits) {
            return headRevisionId;
        }

        // Otherwise, find the first revision id that's not part of a branch.
        long revisionId = headRevisionId;
        while (true) {
            MongoCommit commitMongo = new FetchCommitAction(nodeStore, revisionId).execute();
            if (commitMongo.getBranchId() == null) {
                return revisionId;
            }
            revisionId = commitMongo.getBaseRevisionId();
        }
    }
}
