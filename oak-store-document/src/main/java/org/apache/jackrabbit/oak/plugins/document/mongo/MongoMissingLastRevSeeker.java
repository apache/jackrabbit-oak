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
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.stats.Clock;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;

/**
 * Mongo specific version of MissingLastRevSeeker which uses mongo queries
 * to fetch candidates which may have missed '_lastRev' updates.
 * 
 * Uses a time range to find documents modified during that interval.
 */
public class MongoMissingLastRevSeeker extends MissingLastRevSeeker {
    private final MongoDocumentStore store;

    public MongoMissingLastRevSeeker(MongoDocumentStore store, Clock clock) {
        super(store, clock);
        this.store = store;
    }

    @Override
    @NotNull
    public CloseableIterable<NodeDocument> getCandidates(final long startTime) {
        Bson query = Filters.gte(NodeDocument.MODIFIED_IN_SECS, NodeDocument.getModifiedInSecs(startTime));
        Bson sortFields = new BasicDBObject(NodeDocument.MODIFIED_IN_SECS, 1);

        FindIterable<BasicDBObject> cursor = getNodeCollection()
                .find(query).sort(sortFields);
        return CloseableIterable.wrap(transform(cursor,
                input -> store.convertFromDBObject(NODES, input)));
    }

    @Override
    public boolean isRecoveryNeeded() {
        Bson query = Filters.and(
                Filters.eq(ClusterNodeInfo.STATE, ClusterNodeInfo.ClusterNodeState.ACTIVE.name()),
                Filters.or(
                        Filters.lt(ClusterNodeInfo.LEASE_END_KEY, clock.getTime()),
                        Filters.eq(ClusterNodeInfo.REV_RECOVERY_LOCK, ClusterNodeInfo.RecoverLockState.ACQUIRED.name())
                )
        );

        return getClusterNodeCollection().find(query).iterator().hasNext();
    }

    private MongoCollection<BasicDBObject> getNodeCollection() {
        return store.getDBCollection(NODES, ReadPreference.primary());
    }

    private MongoCollection<BasicDBObject> getClusterNodeCollection() {
        return store.getDBCollection(CLUSTER_NODES, ReadPreference.primary());
    }
}

