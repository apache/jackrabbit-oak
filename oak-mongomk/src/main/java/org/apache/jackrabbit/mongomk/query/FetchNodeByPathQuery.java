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

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.model.NodeMongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * An query for fetching a node by a given path.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class FetchNodeByPathQuery extends AbstractQuery<NodeMongo> {

    private final String path;
    private long revisionId;
    private boolean fetchAll;

    /**
     * Constructs a new {@code FetchNodeByPathQuery}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param path The path.
     */
    public FetchNodeByPathQuery(MongoConnection mongoConnection, String path) {
        this(mongoConnection, path, 0);
    }

    /**
     * Constructs a new {@code FetchNodeByPathQuery}.
     *
     * @param mongoConnection
     *            The {@link MongoConnection}.
     * @param path
     *            The path.
     * @param revisionId
     *            The revision id.
     */
    public FetchNodeByPathQuery(MongoConnection mongoConnection, String path, long revisionId) {
        super(mongoConnection);

        this.path = path;
        this.revisionId = revisionId;
    }

    public void setFetchAll(boolean fetchAll) {
        this.fetchAll = fetchAll;
    }

    @Override
    public NodeMongo execute() {
        if (!revisionIdExists()) {
            throw new RuntimeException("Revision id '" + revisionId + "' is not valid.");
        }

        DBCollection nodeCollection = mongoConnection.getNodeCollection();
        QueryBuilder queryBuilder = QueryBuilder.start(NodeMongo.KEY_PATH).is(path);
        if (revisionId > 0) {
            queryBuilder = queryBuilder.and(NodeMongo.KEY_REVISION_ID).is(revisionId);
        }
        DBObject query = queryBuilder.get();

        DBObject filter = null;
        if (!fetchAll) {
            QueryBuilder filterBuilder = QueryBuilder.start(NodeMongo.KEY_REVISION_ID).is(1);
            filterBuilder.and(NodeMongo.KEY_CHILDREN).is(1);
            filter = filterBuilder.get();
        }

        NodeMongo nodeMongo = (NodeMongo) nodeCollection.findOne(query, filter);

        return nodeMongo;
    }

    private boolean revisionIdExists() {
        if (revisionId == 0) {
            return true;
        }
        FetchValidRevisionsQuery query = new FetchValidRevisionsQuery(mongoConnection, String.valueOf(Long.MAX_VALUE));
        List<Long> revisionIds = query.execute();
        return revisionIds.contains(revisionId);
    }
}
