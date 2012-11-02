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

import java.util.List;
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
 * A query for fetching nodes under certain paths.
 */
public class FetchNodesForPathsQuery extends AbstractQuery<List<NodeMongo>> {

    private static final Logger LOG = LoggerFactory.getLogger(FetchNodesForPathsQuery.class);

    private final Set<String> paths;
    private final long revisionId;

    private String branchId;

    /**
     * Constructs a new {@code FetchNodesForRevisionQuery}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param paths The paths to fetch.
     * @param revisionId The revision id.
     * @param branchId
     */
    public FetchNodesForPathsQuery(MongoConnection mongoConnection,
            Set<String> paths, long revisionId) {
        super(mongoConnection);
        this.paths = paths;
        this.revisionId = revisionId;
    }

    /**
     * Sets the branchId for the query.
     *
     * @param branchId Branch id.
     */
    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    @Override
    public List<NodeMongo> execute() {
        DBCursor dbCursor = performQuery();
        FetchCommitsQuery query = new FetchCommitsQuery(mongoConnection, revisionId);
        List<CommitMongo> validCommits = query.execute();
        return QueryUtils.getMostRecentValidNodes(dbCursor, validCommits);
    }

    private DBCursor performQuery() {
        QueryBuilder queryBuilder = QueryBuilder.start(NodeMongo.KEY_PATH).in(paths)
                .and(NodeMongo.KEY_REVISION_ID).lessThanEquals(revisionId);

        if (branchId == null) {
            DBObject query = new BasicDBObject(NodeMongo.KEY_BRANCH_ID, new BasicDBObject("$exists", false));
            queryBuilder = queryBuilder.and(query);
        } else {
            queryBuilder = queryBuilder.and(NodeMongo.KEY_BRANCH_ID).is(branchId);
        }

        DBObject query = queryBuilder.get();
        LOG.debug(String.format("Executing query: %s", query));

        DBCollection nodeCollection = mongoConnection.getNodeCollection();
        return nodeCollection.find(query);
    }
}