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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.SplitDocumentCleanUp;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.mongodb.QueryBuilder.start;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Document.ID;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PATH;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_MAX_REV_TIME_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_TYPE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_NO_BRANCH;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;

/**
 * Mongo specific version of VersionGCSupport which uses mongo queries
 * to fetch required NodeDocuments
 *
 * <p>Version collection involves looking into old record and mostly unmodified
 * documents. In such case read from secondaries are preferred</p>
 */
public class MongoVersionGCSupport extends VersionGCSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MongoVersionGCSupport.class);
    private static final DBObject SD_TYPE_HINT = start(SD_TYPE).is(1).get();

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
    public long getDeletedOnceCount() {
        DBObject query = start(NodeDocument.DELETED_ONCE).is(Boolean.TRUE).get();
        return getNodeCollection().count(query, ReadPreference.secondaryPreferred());
    }

    @Override
    protected SplitDocumentCleanUp createCleanUp(Set<SplitDocType> gcTypes,
                                                 RevisionVector sweepRevs,
                                                 long oldestRevTimeStamp,
                                                 VersionGCStats stats) {
        return new MongoSplitDocCleanUp(gcTypes, sweepRevs, oldestRevTimeStamp, stats);
    }

    @Override
    protected Iterable<NodeDocument> identifyGarbage(final Set<SplitDocType> gcTypes,
                                                     final RevisionVector sweepRevs,
                                                     final long oldestRevTimeStamp) {
        return filter(transform(getNodeCollection().find(
                createQuery(gcTypes, sweepRevs, oldestRevTimeStamp)),
                new Function<DBObject, NodeDocument>() {
            @Override
            public NodeDocument apply(DBObject input) {
                return store.convertFromDBObject(NODES, input);
            }
        }), new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                return !isDefaultNoBranchSplitNewerThan(input, sweepRevs);
            }
        });
    }

    @Override
    public long getOldestDeletedOnceTimestamp(Clock clock, long precisionMs) {
        LOG.debug("getOldestDeletedOnceTimestamp() <- start");
        DBObject query = start(NodeDocument.DELETED_ONCE).is(Boolean.TRUE).get();
        DBCursor cursor = getNodeCollection().find(query).sort(start(NodeDocument.MODIFIED_IN_SECS).is(1).get()).limit(1);
        CloseableIterable<NodeDocument> results = CloseableIterable.wrap(transform(cursor, new Function<DBObject, NodeDocument>() {
            @Override
            public NodeDocument apply(DBObject input) {
                return store.convertFromDBObject(NODES, input);
            }
        }), cursor);
        try {
            Iterator<NodeDocument> i = results.iterator();
            if (i.hasNext()) {
                NodeDocument doc = i.next();
                long modifiedMs = doc.getModified() * TimeUnit.SECONDS.toMillis(1);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getOldestDeletedOnceTimestamp() -> {}", Utils.timestampToString(modifiedMs));
                }
                return modifiedMs;
            }
        }
        finally {
            Utils.closeIfCloseable(results);
        }
        LOG.debug("getOldestDeletedOnceTimestamp() -> none found, return current time");
        return clock.getTime();
    }

    private DBObject createQuery(Set<SplitDocType> gcTypes,
                                 RevisionVector sweepRevs,
                                 long oldestRevTimeStamp) {
        List<Integer> gcTypeCodes = Lists.newArrayList();
        QueryBuilder orClause = start();
        for(SplitDocType type : gcTypes) {
            gcTypeCodes.add(type.typeCode());
            for (DBObject query : queriesForType(type, sweepRevs)) {
                orClause.or(query);
            }
        }
        return start()
                .and(
                        start(SD_TYPE).in(gcTypeCodes).get(),
                        orClause.get(),
                        start(NodeDocument.SD_MAX_REV_TIME_IN_SECS)
                                .lessThan(NodeDocument.getModifiedInSecs(oldestRevTimeStamp))
                                .get()
                ).get();
    }

    @Nonnull
    private Iterable<DBObject> queriesForType(SplitDocType type, RevisionVector sweepRevs) {
        if (type != DEFAULT_NO_BRANCH) {
            return singletonList(start(SD_TYPE).is(type.typeCode()).get());
        }
        // default_no_branch split type is special because we can
        // only remove those older than sweep rev
        List<DBObject> queries = Lists.newArrayList();
        for (Revision r : sweepRevs) {
            String idSuffix = Utils.getPreviousIdFor("/", r, 0);
            idSuffix = idSuffix.substring(idSuffix.lastIndexOf('-'));

            // id/path constraint for previous documents
            QueryBuilder idPathClause = start();
            idPathClause.or(start(ID).regex(Pattern.compile(".*" + idSuffix)).get());
            // previous documents with long paths do not have a '-' in the id
            idPathClause.or(start(ID).regex(Pattern.compile("[^-]*"))
                    .and(PATH).regex(Pattern.compile(".*" + idSuffix)).get());

            queries.add(start(SD_TYPE).is(type.typeCode())
                    .and(idPathClause.get())
                    .and(SD_MAX_REV_TIME_IN_SECS).lessThan(getModifiedInSecs(r.getTimestamp()))
                    .get());
        }
        return queries;
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

        final Set<SplitDocType> gcTypes;
        final RevisionVector sweepRevs;
        final long oldestRevTimeStamp;

        MongoSplitDocCleanUp(Set<SplitDocType> gcTypes,
                                       RevisionVector sweepRevs,
                                       long oldestRevTimeStamp,
                                       VersionGCStats stats) {
            super(MongoVersionGCSupport.this.store, stats,
                    identifyGarbage(gcTypes, sweepRevs, oldestRevTimeStamp));
            this.gcTypes = gcTypes;
            this.sweepRevs = sweepRevs;
            this.oldestRevTimeStamp = oldestRevTimeStamp;
        }

        @Override
        protected void collectIdToBeDeleted(String id) {
            // nothing to do here, as we're overwriting deleteSplitDocuments()
        }

        @Override
        protected int deleteSplitDocuments() {
            DBObject query = createQuery(gcTypes, sweepRevs, oldestRevTimeStamp);

            if(LOG.isDebugEnabled()){
                //if debug level logging is on then determine the id of documents to be deleted
                //and log them
                logSplitDocIdsTobeDeleted(query);
            }

            return getNodeCollection().remove(query).getN();
        }
    }
}
