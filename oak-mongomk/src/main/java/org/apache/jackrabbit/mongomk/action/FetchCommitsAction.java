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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An action for fetching valid commits.
 */
public class FetchCommitsAction extends AbstractAction<List<CommitMongo>> {

    private static final int LIMITLESS = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FetchCommitsAction.class);

    private long fromRevisionId = -1;
    private long toRevisionId = -1;
    private int maxEntries = LIMITLESS;
    private boolean includeBranchCommits = true;

    /**
     * Constructs a new {@link FetchCommitsAction}
     *
     * @param mongoConnection Mongo connection.
     * @param toRevisionId To revision id.
     */
    public FetchCommitsAction(MongoConnection mongoConnection) {
        this(mongoConnection, -1L, -1L);
    }

    /**
     * Constructs a new {@link FetchCommitsAction}
     *
     * @param mongoConnection Mongo connection.
     * @param toRevisionId To revision id.
     */
    public FetchCommitsAction(MongoConnection mongoConnection, long toRevisionId) {
        this(mongoConnection, -1L, toRevisionId);
    }

    /**
     * Constructs a new {@link FetchCommitsAction}
     *
     * @param mongoConnection Mongo connection.
     * @param fromRevisionId From revision id.
     * @param toRevisionId To revision id.
     */
    public FetchCommitsAction(MongoConnection mongoConnection, long fromRevisionId,
            long toRevisionId) {
        super(mongoConnection);
        this.fromRevisionId = fromRevisionId;
        this.toRevisionId = toRevisionId;
    }

    /**
     * Sets the max number of entries that should be fetched.
     *
     * @param maxEntries The max number of entries.
     */
    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
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
    public List<CommitMongo> execute() {
        if (maxEntries == 0) {
            return Collections.emptyList();
        }
        DBCursor dbCursor = fetchListOfValidCommits();
        return convertToCommits(dbCursor);
    }

    private DBCursor fetchListOfValidCommits() {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        QueryBuilder queryBuilder = QueryBuilder.start(CommitMongo.KEY_FAILED).notEquals(Boolean.TRUE);
        if (toRevisionId > -1) {
            queryBuilder = queryBuilder.and(CommitMongo.KEY_REVISION_ID).lessThanEquals(toRevisionId);
        }

        if (!includeBranchCommits) {
            queryBuilder = queryBuilder.and(new BasicDBObject(NodeMongo.KEY_BRANCH_ID,
                    new BasicDBObject("$exists", false)));
        }

        DBObject query = queryBuilder.get();

        LOG.debug(String.format("Executing query: %s", query));

        return maxEntries > 0? commitCollection.find(query).limit(maxEntries) : commitCollection.find(query);
    }

    private List<CommitMongo> convertToCommits(DBCursor dbCursor) {
        Map<Long, CommitMongo> commits = new HashMap<Long, CommitMongo>();
        while (dbCursor.hasNext()) {
            CommitMongo commitMongo = (CommitMongo) dbCursor.next();
            commits.put(commitMongo.getRevisionId(), commitMongo);
        }

        List<CommitMongo> validCommits = new LinkedList<CommitMongo>();
        if (commits.isEmpty()) {
            return validCommits;
        }

        Set<Long> revisions = commits.keySet();
        long currentRevision = (toRevisionId != -1 && revisions.contains(toRevisionId)) ?
                toRevisionId : Collections.max(revisions);

        while (true) {
            CommitMongo commitMongo = commits.get(currentRevision);
            if (commitMongo == null) {
                break;
            }
            validCommits.add(commitMongo);
            long baseRevision = commitMongo.getBaseRevId();
            if (currentRevision == 0L || baseRevision < fromRevisionId) {
                break;
            }
            currentRevision = baseRevision;
        }

        LOG.debug(String.format("Found list of valid revisions for max revision %s: %s",
                toRevisionId, validCommits));

        return validCommits;
    }
}
