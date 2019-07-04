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

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Document.ID;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.DELETED_ONCE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PATH;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_MAX_REV_TIME_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_TYPE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_NO_BRANCH;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.SplitDocumentCleanUp;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

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
        Bson query = Filters.and(
                Filters.eq(DELETED_ONCE, true),
                Filters.gte(MODIFIED_IN_SECS, getModifiedInSecs(fromModified)),
                Filters.lt(MODIFIED_IN_SECS, getModifiedInSecs(toModified))
        );
        FindIterable<BasicDBObject> cursor = getNodeCollection()
                .find(query).batchSize(batchSize);

        return CloseableIterable.wrap(transform(cursor,
                input -> store.convertFromDBObject(NODES, input)));
    }

    @Override
    public long getDeletedOnceCount() {
        Bson query = Filters.eq(DELETED_ONCE, Boolean.TRUE);
        return getNodeCollection().countDocuments(query);
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
        // With OAK-8351 this switched from 1 to 2 queries (see createQueries)
        // hence we iterate over the queries returned by createQueries
        List<Bson> queries = createQueries(gcTypes, sweepRevs, oldestRevTimeStamp);
        Iterable<NodeDocument> allResults = emptyList();
        for (Bson query : queries) {
            // this query uses a timeout of 15min. hitting the timeout will
            // result in an exception which should show up in the log file.
            // while this doesn't resolve the situation (the restructuring
            // of the query as part of OAK-8351 does), it nevertheless 
            // makes any future similar problem more visible than long running
            // queries alone (15min is still long).
            Iterable<NodeDocument> iterable = filter(transform(getNodeCollection().find(query)
                    .maxTime(15, TimeUnit.MINUTES),
                    new Function<BasicDBObject, NodeDocument>() {
                @Override
                public NodeDocument apply(BasicDBObject input) {
                    return store.convertFromDBObject(NODES, input);
                }
            }), new Predicate<NodeDocument>() {
                @Override
                public boolean apply(NodeDocument input) {
                    return !isDefaultNoBranchSplitNewerThan(input, sweepRevs);
                }
            });
            allResults = concat(allResults, iterable);
        }
        return allResults;
    }

    @Override
    public long getOldestDeletedOnceTimestamp(Clock clock, long precisionMs) {
        LOG.debug("getOldestDeletedOnceTimestamp() <- start");
        Bson query = Filters.eq(DELETED_ONCE, Boolean.TRUE);
        Bson sort = Filters.eq(MODIFIED_IN_SECS, 1);
        List<Long> result = new ArrayList<>(1);
        getNodeCollection().find(query).sort(sort).limit(1).forEach(
                new Block<BasicDBObject>() {
            @Override
            public void apply(BasicDBObject document) {
                NodeDocument doc = store.convertFromDBObject(NODES, document);
                long modifiedMs = doc.getModified() * TimeUnit.SECONDS.toMillis(1);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getOldestDeletedOnceTimestamp() -> {}", Utils.timestampToString(modifiedMs));
                }
                result.add(modifiedMs);
            }
        });
        if (result.isEmpty()) {
            LOG.debug("getOldestDeletedOnceTimestamp() -> none found, return current time");
            result.add(clock.getTime());
        }
        return result.get(0);
    }

    private List<Bson> createQueries(Set<SplitDocType> gcTypes,
                                 RevisionVector sweepRevs,
                                 long oldestRevTimeStamp) {
        List<Bson> result = Lists.newArrayList();
        List<Bson> orClauses = Lists.newArrayList();
        for(SplitDocType type : gcTypes) {
            if (DEFAULT_NO_BRANCH != type) {
                orClauses.add(Filters.eq(SD_TYPE, type.typeCode()));
            } else {
                result.add(queryForDefaultNoBranch(sweepRevs, getModifiedInSecs(oldestRevTimeStamp)));
            }
        }
        // OAK-8351: this (last) query only contains SD_TYPE and SD_MAX_REV_TIME_IN_SECS
        // so mongodb should really use that _sdType_1__sdMaxRevTime_1 index
        result.add(Filters.and(
                Filters.or(orClauses),
                Filters.lt(SD_MAX_REV_TIME_IN_SECS, getModifiedInSecs(oldestRevTimeStamp))
                ));

        return result;
    }

    @NotNull
    private Bson queryForDefaultNoBranch(RevisionVector sweepRevs, long maxRevTimeInSecs) {
        // default_no_branch split type is special because we can
        // only remove those older than sweep rev
        List<Bson> orClauses = Lists.newArrayList();
        for (Revision r : sweepRevs) {
            String idSuffix = Utils.getPreviousIdFor(Path.ROOT, r, 0);
            idSuffix = idSuffix.substring(idSuffix.lastIndexOf('-'));

            // id/path constraint for previous documents
            Bson idPathClause = Filters.or(
                    Filters.regex(ID, Pattern.compile(".*" + idSuffix)),
                    // previous documents with long paths do not have a '-' in the id
                    Filters.and(
                            Filters.regex(ID, Pattern.compile("[^-]*")),
                            Filters.regex(PATH, Pattern.compile(".*" + idSuffix))
                    )
            );

            orClauses.add(Filters.and(
                    idPathClause,
                    Filters.lt(SD_MAX_REV_TIME_IN_SECS, getModifiedInSecs(r.getTimestamp()))
            ));
        }
        return Filters.and(
                Filters.eq(SD_TYPE, DEFAULT_NO_BRANCH.typeCode()),
                Filters.lt(SD_MAX_REV_TIME_IN_SECS, maxRevTimeInSecs),
                Filters.or(orClauses)
                );
    }

    private void logSplitDocIdsTobeDeleted(Bson query) {
        // Fetch only the id
        final BasicDBObject keys = new BasicDBObject(Document.ID, 1);
        List<String> ids = new ArrayList<>();
        getNodeCollection()
                .withReadPreference(store.getConfiguredReadPreference(NODES))
                .find(query).projection(keys)
                .forEach((Block<BasicDBObject>) doc -> ids.add(getID(doc)));

        StringBuilder sb = new StringBuilder("Split documents with following ids were deleted as part of GC \n");
        Joiner.on(StandardSystemProperty.LINE_SEPARATOR.value()).appendTo(sb, ids);
        LOG.debug(sb.toString());
    }

    private static String getID(BasicDBObject document) {
        return String.valueOf(document.get(Document.ID));
    }

    private MongoCollection<BasicDBObject> getNodeCollection(){
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
            List<Bson> queries = createQueries(gcTypes, sweepRevs, oldestRevTimeStamp);

            if(LOG.isDebugEnabled()){
                //if debug level logging is on then determine the id of documents to be deleted
                //and log them
                for (Bson query : queries) {
                    logSplitDocIdsTobeDeleted(query);
                }
            }

            int cnt = 0;
            for (Bson query : queries) {
                cnt += getNodeCollection().deleteMany(query).getDeletedCount();
            }
            return cnt;
        }
    }
}
