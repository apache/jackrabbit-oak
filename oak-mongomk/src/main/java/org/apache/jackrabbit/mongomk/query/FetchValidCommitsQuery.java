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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.log4j.Logger;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An query for fetching valid commits.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class FetchValidCommitsQuery extends AbstractQuery<List<CommitMongo>> {

    private static final int LIMITLESS = 0;
    private static final Logger LOG = Logger.getLogger(FetchValidCommitsQuery.class);

    private final String fromRevisionId;
    private String toRevisionId;
    private int maxEntries = LIMITLESS;

    /**
     * Constructs a new {link FetchValidCommitsQuery}.
     *
     * @param mongoConnection Mongo connection.
     * @param fromRevisionId From revision id.
     */
    public FetchValidCommitsQuery(MongoConnection mongoConnection, String toRevisionId) {
        this(mongoConnection, null, toRevisionId);
    }

    /**
     * Constructs a new {@link FetchValidCommitsQuery}
     *
     * @param mongoConnection Mongo connection.
     * @param fromRevisionId From revision id.
     * @param toRevisionId To revision id.
     */
    public FetchValidCommitsQuery(MongoConnection mongoConnection, String fromRevisionId,
            String toRevisionId) {
        super(mongoConnection);
        this.fromRevisionId = fromRevisionId;
        this.toRevisionId = toRevisionId;
    }

    /**
     * FIXME - Maybe this should be removed.
     *
     * Constructs a new {@link FetchValidCommitsQuery}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param maxEntries Max number of entries that should be fetched.
     */
    public FetchValidCommitsQuery(MongoConnection mongoConnection, int maxEntries) {
        super(mongoConnection);
        fromRevisionId = null;
        toRevisionId = String.valueOf(Integer.MAX_VALUE);
        this.maxEntries = maxEntries;
    }

    @Override
    public List<CommitMongo> execute() {
        DBCursor dbCursor = fetchListOfValidCommits();
        List<CommitMongo> commits = convertToCommits(dbCursor);

        return commits;
    }

    private List<CommitMongo> convertToCommits(DBCursor dbCursor) {
        Map<Long, CommitMongo> revisions = new HashMap<Long, CommitMongo>();
        while (dbCursor.hasNext()) {
            CommitMongo commitMongo = (CommitMongo) dbCursor.next();
            revisions.put(commitMongo.getRevisionId(), commitMongo);
        }

        List<CommitMongo> validCommits = new LinkedList<CommitMongo>();
        if (revisions.isEmpty()) {
            return validCommits;
        }

        Long currentRevision = MongoUtil.toMongoRepresentation(toRevisionId);
        if (!revisions.containsKey(currentRevision)) {
            currentRevision = Collections.max(revisions.keySet());
        }

        while (true) {
            CommitMongo commitMongo = revisions.get(currentRevision);
            validCommits.add(commitMongo);
            Long baseRevision = commitMongo.getBaseRevisionId();
            Long fromRevision = MongoUtil.toMongoRepresentation(fromRevisionId);
            if ((currentRevision == 0L) || (baseRevision == null || baseRevision < fromRevision)) {
                break;
            }

            currentRevision = baseRevision;
        }

        LOG.debug(String.format("Found list of valid revisions for max revision %s: %s", toRevisionId, validCommits));

        return validCommits;
    }

    private DBCursor fetchListOfValidCommits() {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject query = QueryBuilder.start(CommitMongo.KEY_FAILED).notEquals(Boolean.TRUE)
                .and(CommitMongo.KEY_REVISION_ID).lessThanEquals(MongoUtil.toMongoRepresentation(toRevisionId))
                .get();

        LOG.debug(String.format("Executing query: %s", query));

        return commitCollection.find(query).limit(maxEntries);
    }
}
