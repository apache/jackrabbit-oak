/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.google.common.base.Function;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Commit;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;

import static com.google.common.collect.Iterables.transform;

/**
 * Mongo specific version of VersionGCSupport which uses mongo queries
 * to fetch required NodeDocuments
 *
 * <p>Version collection involves looking into old record and mostly unmodified
 * documents. In such case read from secondaries are preferred</p>
 */
public class MongoVersionGCSupport extends VersionGCSupport {
    private final MongoDocumentStore store;

    public MongoVersionGCSupport(MongoDocumentStore store) {
        super(store);
        this.store = store;
    }

    @Override
    public CloseableIterable<NodeDocument> getPossiblyDeletedDocs(final long lastModifiedTime) {
        //_deletedOnce == true && _modified < lastModifiedTime
        DBObject query = QueryBuilder
                                .start(NodeDocument.DELETED_ONCE).is(Boolean.TRUE)
                                .put(NodeDocument.MODIFIED_IN_SECS).lessThan(Commit.getModifiedInSecs(lastModifiedTime))
                        .get();
        DBCursor cursor = getNodeCollection().find(query).setReadPreference(ReadPreference.secondaryPreferred());
        return CloseableIterable.wrap(transform(cursor, new Function<DBObject, NodeDocument>() {
            @Override
            public NodeDocument apply(DBObject input) {
                return store.convertFromDBObject(Collection.NODES, input);
            }
        }), cursor);
    }

    private DBCollection getNodeCollection(){
        return store.getDBCollection(Collection.NODES);
    }
}
