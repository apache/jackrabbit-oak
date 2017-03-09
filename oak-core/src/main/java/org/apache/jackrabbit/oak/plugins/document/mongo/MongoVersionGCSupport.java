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

import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.SplitDocumentCleanUp;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.transform;
import static com.mongodb.QueryBuilder.start;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;

/**
 * Mongo specific version of VersionGCSupport which uses mongo queries
 * to fetch required NodeDocuments
 *
 * <p>Version collection involves looking into old record and mostly unmodified
 * documents. In such case read from secondaries are preferred</p>
 */
public class MongoVersionGCSupport extends VersionGCSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MongoVersionGCSupport.class);
    private final MongoDocumentStore store;

    /**
     * The batch size for the query of possibly deleted docs.
     */
    private final int batchSize = Integer.getInteger(
            "oak.mongo.queryDeletedDocsBatchSize", 1000);

    public MongoVersionGCSupport(MongoDocumentStore store) {
        super(store);
        this.store = store;
    }

    @Override
    public CloseableIterable<NodeDocument> getPossiblyDeletedDocs(final long fromModified, final long toModified) {
        //_deletedOnce == true && _modified >= fromModified && _modified < toModified
        DBObject query = start(NodeDocument.DELETED_ONCE).is(Boolean.TRUE).put(NodeDocument.MODIFIED_IN_SECS)
                .greaterThanEquals(NodeDocument.getModifiedInSecs(fromModified))
                .lessThan(NodeDocument.getModifiedInSecs(toModified)).get();
        DBCursor cursor = getNodeCollection().find(query).setReadPreference(ReadPreference.secondaryPreferred());
        cursor.batchSize(batchSize);

        return CloseableIterable.wrap(transform(cursor, new Function<DBObject, NodeDocument>() {
            @Override
            public NodeDocument apply(DBObject input) {
                return store.convertFromDBObject(NODES, input);
            }
        }), cursor);
    }

    @Override
    protected SplitDocumentCleanUp createCleanUp(Set<SplitDocType> gcTypes,
                                                 long oldestRevTimeStamp,
                                                 VersionGCStats stats) {
        return new MongoSplitDocCleanUp(gcTypes, oldestRevTimeStamp, stats);
    }

    @Override
    protected Iterable<NodeDocument> identifyGarbage(final Set<SplitDocType> gcTypes,
                                                     final long oldestRevTimeStamp) {
        return transform(getNodeCollection().find(createQuery(gcTypes, oldestRevTimeStamp)),
                new Function<DBObject, NodeDocument>() {
            @Override
            public NodeDocument apply(DBObject input) {
                return store.convertFromDBObject(NODES, input);
            }
        });
    }

    private DBObject createQuery(Set<SplitDocType> gcTypes,
                                 long oldestRevTimeStamp) {
        //OR condition has to be first as we have a index for that
        //((type == DEFAULT_NO_CHILD || type == PROP_COMMIT_ONLY ..) && _sdMaxRevTime < oldestRevTimeStamp(in secs)
        QueryBuilder orClause = start();
        for(SplitDocType type : gcTypes){
            orClause.or(start(NodeDocument.SD_TYPE).is(type.typeCode()).get());
        }
        return start()
                .and(
                        orClause.get(),
                        start(NodeDocument.SD_MAX_REV_TIME_IN_SECS)
                                .lessThan(NodeDocument.getModifiedInSecs(oldestRevTimeStamp))
                                .get()
                ).get();
    }

    private void logSplitDocIdsTobeDeleted(DBObject query) {
        // Fetch only the id
        final BasicDBObject keys = new BasicDBObject(Document.ID, 1);
        List<String> ids;
        DBCursor cursor = getNodeCollection().find(query, keys)
                .setReadPreference(store.getConfiguredReadPreference(NODES));
        try {
             ids = ImmutableList.copyOf(Iterables.transform(cursor, new Function<DBObject, String>() {
                 @Override
                 public String apply(DBObject input) {
                     return (String) input.get(Document.ID);
                 }
             }));
        } finally {
            cursor.close();
        }
        StringBuilder sb = new StringBuilder("Split documents with following ids were deleted as part of GC \n");
        Joiner.on(StandardSystemProperty.LINE_SEPARATOR.value()).appendTo(sb, ids);
        LOG.debug(sb.toString());
    }

    private DBCollection getNodeCollection(){
        return store.getDBCollection(NODES);
    }

    private class MongoSplitDocCleanUp extends SplitDocumentCleanUp {

        protected final Set<SplitDocType> gcTypes;
        protected final long oldestRevTimeStamp;

        protected MongoSplitDocCleanUp(Set<SplitDocType> gcTypes,
                                       long oldestRevTimeStamp,
                                       VersionGCStats stats) {
            super(MongoVersionGCSupport.this.store, stats,
                    identifyGarbage(gcTypes, oldestRevTimeStamp));
            this.gcTypes = gcTypes;
            this.oldestRevTimeStamp = oldestRevTimeStamp;
        }

        @Override
        protected void collectIdToBeDeleted(String id) {
            // nothing to do here, as we're overwriting deleteSplitDocuments()
        }

        @Override
        protected int deleteSplitDocuments() {
            DBObject query = createQuery(gcTypes, oldestRevTimeStamp);

            if(LOG.isDebugEnabled()){
                //if debug level logging is on then determine the id of documents to be deleted
                //and log them
                logSplitDocIdsTobeDeleted(query);
            }

            return getNodeCollection().remove(query).getN();
        }
    }
}
