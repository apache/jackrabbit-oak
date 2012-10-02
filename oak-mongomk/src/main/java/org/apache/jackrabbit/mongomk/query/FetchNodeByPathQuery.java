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

import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * A query for fetching a node by a given path.
 */
public class FetchNodeByPathQuery extends AbstractQuery<NodeMongo> {

    private static final Logger LOG = LoggerFactory.getLogger(FetchNodesByPathAndDepthQuery.class);

    private final String path;
    private final long revisionId;
    private boolean fetchAllProperties;

    /**
     * Constructs a new {@code FetchNodeByPathQuery} with path and zero revision id.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param path The path.
     */
    public FetchNodeByPathQuery(MongoConnection mongoConnection, String path) {
        this(mongoConnection, path, 0);
    }

    /**
     * Constructs a new {@code FetchNodeByPathQuery} with path and revision id.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param path The path.
     * @param revisionId The revision id.
     */
    public FetchNodeByPathQuery(MongoConnection mongoConnection, String path,
            long revisionId) {
        super(mongoConnection);
        this.path = path;
        this.revisionId = revisionId;
    }

    /**
     * Determines whether all the properties of the node will be fetched.
     *
     * @param fetchAllProperties
     */
    public void setFetchAllPropeties(boolean fetchAllProperties) {
        this.fetchAllProperties = fetchAllProperties;
    }

    @Override
    public NodeMongo execute() {
        DBCursor dbCursor = performQuery();
        List<Long> validRevisions = new FetchValidRevisionsQuery(mongoConnection, revisionId).execute();
        List<NodeMongo> nodes = QueryUtils.getMostRecentValidNodes(dbCursor, validRevisions);
        // Return the first node with the path, there should be only one.
        for (NodeMongo node : nodes) {
            if (node.getPath().equals(path)) {
                return node;
            }
        }
        return null;
    }

    private DBCursor performQuery() {
        QueryBuilder queryBuilder = QueryBuilder.start(NodeMongo.KEY_PATH).is(path);
        if (revisionId > 0) {
            queryBuilder = queryBuilder.and(NodeMongo.KEY_REVISION_ID).lessThanEquals(revisionId);
        }
        DBObject query = queryBuilder.get();

        LOG.debug(String.format("Executing query: %s", query));

        DBObject filter = null;
        if (!fetchAllProperties) {
            QueryBuilder filterBuilder = QueryBuilder.start(NodeMongo.KEY_REVISION_ID).is(1);
            filterBuilder.and(NodeMongo.KEY_CHILDREN).is(1);
            filter = filterBuilder.get();
        }

        DBCollection nodeCollection = mongoConnection.getNodeCollection();
        return nodeCollection.find(query, filter);
    }
}