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

import static com.google.common.collect.Iterables.transform;
import static com.mongodb.QueryBuilder.start;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.RecoverLockState;

import com.google.common.base.Function;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;

/**
 * Mongo specific version of MissingLastRevSeeker which uses mongo queries
 * to fetch candidates which may have missed '_lastRev' updates.
 * 
 * Uses a time range to find documents modified during that interval.
 */
public class MongoMissingLastRevSeeker extends MissingLastRevSeeker {
    private final MongoDocumentStore store;

    public MongoMissingLastRevSeeker(MongoDocumentStore store) {
        super(store);
        this.store = store;
    }

    @Override
    public CloseableIterable<NodeDocument> getCandidates(final long startTime,
            final long endTime) {
        DBObject query =
                start(NodeDocument.MODIFIED_IN_SECS).lessThanEquals(
                                NodeDocument.getModifiedInSecs(endTime))
                        .put(NodeDocument.MODIFIED_IN_SECS).greaterThanEquals(
                                NodeDocument.getModifiedInSecs(startTime))
                        .get();
        DBObject sortFields = new BasicDBObject(NodeDocument.MODIFIED_IN_SECS, -1);

        DBCursor cursor =
                getNodeCollection().find(query)
                        .sort(sortFields)
                        .setReadPreference(
                                ReadPreference.secondaryPreferred());
        return CloseableIterable.wrap(transform(cursor, new Function<DBObject, NodeDocument>() {
            @Override
            public NodeDocument apply(DBObject input) {
                return store.convertFromDBObject(Collection.NODES, input);
            }
        }), cursor);
    }

    @Override
    public boolean acquireRecoveryLock(int clusterId) {
        QueryBuilder query =
                start().and(
                        start(Document.ID).is(Integer.toString(clusterId)).get(),
                        start(ClusterNodeInfo.REV_RECOVERY_LOCK).notEquals(RecoverLockState.ACQUIRED.name()).get()
                );

        DBObject returnFields = new BasicDBObject();
        returnFields.put("_id", 1);

        BasicDBObject setUpdates = new BasicDBObject();
        setUpdates.append(ClusterNodeInfo.REV_RECOVERY_LOCK, RecoverLockState.ACQUIRED.name());

        BasicDBObject update = new BasicDBObject();
        update.append("$set", setUpdates);

        DBObject oldNode = getClusterNodeCollection().findAndModify(query.get(), returnFields,
                null /*sort*/, false /*remove*/, update, false /*returnNew*/,
                false /*upsert*/);

        return oldNode != null;
    }

    @Override
    public boolean isRecoveryNeeded(long currentTime) {
        QueryBuilder query =
                start(ClusterNodeInfo.STATE).is(ClusterNodeInfo.ClusterNodeState.ACTIVE.name())
                .put(ClusterNodeInfo.LEASE_END_KEY).lessThan(currentTime)
                .put(ClusterNodeInfo.REV_RECOVERY_LOCK).notEquals(RecoverLockState.ACQUIRED.name());

        return getClusterNodeCollection().findOne(query.get()) != null;
    }

    private DBCollection getNodeCollection() {
        return store.getDBCollection(Collection.NODES);
    }

    private DBCollection getClusterNodeCollection() {
        return store.getDBCollection(Collection.CLUSTER_NODES);
    }
}

