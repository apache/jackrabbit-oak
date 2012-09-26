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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An query for fetching nodes for a specific revision.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class FetchNodesForRevisionQuery extends AbstractQuery<List<NodeMongo>> {

    private static final Logger LOG = LoggerFactory.getLogger(FetchNodesForRevisionQuery.class);

    private final Set<String> paths;
    private final String revisionId;

    /**
     * Constructs a new {@code FetchNodesForRevisionQuery}.
     *
     * @param mongoConnection
     *            The {@link MongoConnection}.
     * @param paths
     *            The paths to fetch.
     * @param revisionId
     *            The revision id.
     */
    public FetchNodesForRevisionQuery(MongoConnection mongoConnection, Set<String> paths, String revisionId) {
        super(mongoConnection);
        this.paths = paths;
        this.revisionId = revisionId;
    }

    /**
     * Constructs a new {@code FetchNodesForRevisionQuery}.
     *
     * @param mongoConnection
     *            The {@link MongoConnection}.
     * @param paths
     *            The paths to fetch.
     * @param revisionId
     *            The revision id.
     */
    public FetchNodesForRevisionQuery(MongoConnection mongoConnection, String[] paths, String revisionId) {
        this(mongoConnection, new HashSet<String>(Arrays.asList(paths)), revisionId);
    }

    @Override
    public List<NodeMongo> execute() {
        List<Long> validRevisions = fetchValidRevisions(mongoConnection, revisionId);

        DBCursor dbCursor = retrieveAllNodes();
        List<NodeMongo> nodes = QueryUtils.convertToNodes(dbCursor, validRevisions);

        return nodes;
    }

    private List<Long> fetchValidRevisions(MongoConnection mongoConnection, String revisionId) {
        return new FetchValidRevisionsQuery(mongoConnection, revisionId).execute();
    }

    private DBCursor retrieveAllNodes() {
        DBCollection nodeCollection = mongoConnection.getNodeCollection();
        DBObject query = QueryBuilder.start(NodeMongo.KEY_PATH).in(paths).and(NodeMongo.KEY_REVISION_ID)
                .lessThanEquals(MongoUtil.toMongoRepresentation(revisionId)).get();

        LOG.debug(String.format("Executing query: %s", query));

        DBCursor dbCursor = nodeCollection.find(query);

        return dbCursor;
    }
}
