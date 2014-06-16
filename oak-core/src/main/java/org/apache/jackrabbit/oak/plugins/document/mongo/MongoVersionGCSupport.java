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

import javax.annotation.Nullable;

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
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.transform;
import static com.mongodb.QueryBuilder.start;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;

/**
 * Mongo specific version of VersionGCSupport which uses mongo queries
 * to fetch required NodeDocuments
 *
 * <p>Version collection involves looking into old record and mostly unmodified
 * documents. In such case read from secondaries are preferred</p>
 */
public class MongoVersionGCSupport extends VersionGCSupport {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MongoDocumentStore store;

    public MongoVersionGCSupport(MongoDocumentStore store) {
        super(store);
        this.store = store;
    }

    @Override
    public CloseableIterable<NodeDocument> getPossiblyDeletedDocs(final long lastModifiedTime) {
        //_deletedOnce == true && _modified < lastModifiedTime
        DBObject query =
                start(NodeDocument.DELETED_ONCE).is(Boolean.TRUE)
                                .put(NodeDocument.MODIFIED_IN_SECS).lessThan(NodeDocument.getModifiedInSecs(lastModifiedTime))
                        .get();
        DBCursor cursor = getNodeCollection().find(query).setReadPreference(ReadPreference.secondaryPreferred());
        return CloseableIterable.wrap(transform(cursor, new Function<DBObject, NodeDocument>() {
            @Override
            public NodeDocument apply(DBObject input) {
                return store.convertFromDBObject(Collection.NODES, input);
            }
        }), cursor);
    }

    @Override
    public int deleteSplitDocuments(Set<SplitDocType> gcTypes, long oldestRevTimeStamp) {
        //OR condition has to be first as we have a index for that
        //((type == DEFAULT_NO_CHILD || type == PROP_COMMIT_ONLY ..) && _sdMaxRevTime < oldestRevTimeStamp(in secs)
        QueryBuilder orClause = start();
        for(SplitDocType type : gcTypes){
            orClause.or(start(NodeDocument.SD_TYPE).is(type.typeCode()).get());
        }
        DBObject query = start()
                .and(
                    orClause.get(),
                    start(NodeDocument.SD_MAX_REV_TIME_IN_SECS)
                        .lessThan(NodeDocument.getModifiedInSecs(oldestRevTimeStamp))
                        .get()
                ).get();

        if(log.isDebugEnabled()){
            //if debug level logging is on then determine the id of documents to be deleted
            //and log them
            logSplitDocIdsTobeDeleted(query);
        }

        WriteResult writeResult = getNodeCollection().remove(query, WriteConcern.SAFE);
        if (writeResult.getError() != null) {
            //TODO This might be temporary error or we fail fast and let next cycle try again
            log.warn("Error occurred while deleting old split documents from Mongo {}", writeResult.getError());
        }
        return writeResult.getN();
    }

    private void logSplitDocIdsTobeDeleted(DBObject query) {
        // Fetch only the id
        final BasicDBObject keys = new BasicDBObject(Document.ID, 1);
        List<String> ids;
        DBCursor cursor = getNodeCollection().find(query, keys)
                .setReadPreference(ReadPreference.secondaryPreferred());
        try {
             ids = ImmutableList.copyOf(Iterables.transform(cursor, new Function<DBObject, String>() {
                 @Override
                 public String apply(@Nullable DBObject input) {
                     return (String) input.get(Document.ID);
                 }
             }));
        } finally {
            cursor.close();
        }
        StringBuilder sb = new StringBuilder("Split documents with following ids were deleted as part of GC \n");
        Joiner.on(StandardSystemProperty.LINE_SEPARATOR.value()).appendTo(sb, ids);
        log.debug(sb.toString());
    }

    private DBCollection getNodeCollection(){
        return store.getDBCollection(Collection.NODES);
    }
}
