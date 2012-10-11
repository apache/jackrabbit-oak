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
import java.util.regex.Pattern;

import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * FIXME - Create a constructor with required parameters and handle optional
 * parameters with setters.

 * A query for fetching nodes by path and depth.
 */
public class FetchNodesByPathAndDepthQuery extends AbstractQuery<List<NodeMongo>> {

    private static final Logger LOG = LoggerFactory.getLogger(FetchNodesByPathAndDepthQuery.class);

    private final int depth;
    private final String path;
    private final long revisionId;

    private String branchId;

    /**
     * Constructs a new {@code FetchNodesByPathAndDepthQuery}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param path The path.
     * @param revisionId The revision id.
     * @param depth The depth.
     */
    public FetchNodesByPathAndDepthQuery(MongoConnection mongoConnection, String path,
            long revisionId, int depth) {
        super(mongoConnection);
        this.path = path;
        this.revisionId = revisionId;
        this.depth = depth;
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
        List<Long> validRevisions = new FetchValidRevisionsQuery(mongoConnection, revisionId).execute();
        return QueryUtils.getMostRecentValidNodes(dbCursor, validRevisions);
    }

    private DBCursor performQuery() {
        Pattern pattern = createPrefixRegExp();
        QueryBuilder queryBuilder = QueryBuilder.start(NodeMongo.KEY_PATH).regex(pattern);
        if (revisionId > 0) {
            queryBuilder = queryBuilder.and(NodeMongo.KEY_REVISION_ID).lessThanEquals(revisionId);
        }

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

    private Pattern createPrefixRegExp() {
        StringBuilder sb = new StringBuilder();

        if (depth < 0) {
            sb.append("^");
            sb.append(path);
        }
        if (depth == 0) {
            sb.append("^");
            sb.append(path);
            sb.append("$");
        } else if (depth > 0) {
            sb.append("^");
            if (!"/".equals(path)) {
                sb.append(path);
            }
            sb.append("(/[^/]*)");
            sb.append("{0,");
            sb.append(depth);
            sb.append("}$");
        }

        Pattern pattern = Pattern.compile(sb.toString());

        return pattern;
    }
}