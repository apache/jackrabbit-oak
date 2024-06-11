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

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.include;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.apache.jackrabbit.guava.common.collect.Iterables.concat;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.lt;
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
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.hasIndex;
import static org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable.wrap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.mongodb.client.MongoCursor;
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

import org.apache.jackrabbit.guava.common.base.Function;
import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Predicate;
import org.apache.jackrabbit.guava.common.base.StandardSystemProperty;
import org.apache.jackrabbit.guava.common.collect.Lists;
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

    private final BasicDBObject hint;

    /** keeps track of the sweepRev of the last (successful) deletion */
    private RevisionVector lastDefaultNoBranchDeletionRevs;

    /**
     * The batch size for the query of possibly deleted docs.
     */
    private final int batchSize = Integer.getInteger(
            "oak.mongo.queryDeletedDocsBatchSize", 1000);

    public MongoVersionGCSupport(MongoDocumentStore store) {
        super(store);
        this.store = store;
        if(hasIndex(getNodeCollection(), SD_TYPE, SD_MAX_REV_TIME_IN_SECS)) {
            hint = new BasicDBObject();
            hint.put(SD_TYPE,1);
            hint.put(SD_MAX_REV_TIME_IN_SECS, 1);
        } else {
            hint = null;
        }
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

    /**
     * Returns documents that have a {@link NodeDocument#MODIFIED_IN_SECS} value
     * within the given range and are greater than given @{@link NodeDocument#ID}.
     * <p>
     * The two passed modified timestamps are in milliseconds
     * since the epoch and the implementation will convert them to seconds at
     * the granularity of the {@link NodeDocument#MODIFIED_IN_SECS} field and
     * then perform the comparison.
     * <p/>
     *
     * @param fromModified the lower bound modified timestamp in millis (inclusive)
     * @param toModified   the upper bound modified timestamp in millis (exclusive)
     * @param limit        the limit of documents to return
     * @param fromId       the lower bound {@link NodeDocument#ID}
     * @return matching documents.
     */
    @Override
    public Iterable<NodeDocument> getModifiedDocs(final long fromModified, final long toModified, final int limit,
                                                  @NotNull final String fromId) {
        // (_modified = fromModified && _id > fromId || _modified > fromModified && _modified < toModified)
        final Bson query = or(
                and(eq(MODIFIED_IN_SECS, getModifiedInSecs(fromModified)), gt(ID, fromId)),
                and(gt(MODIFIED_IN_SECS, getModifiedInSecs(fromModified)), lt(MODIFIED_IN_SECS, getModifiedInSecs(toModified))));

        // first sort by _modified and then by _id
        final Bson sort = and(eq(MODIFIED_IN_SECS, 1), eq(ID, 1));

        final FindIterable<BasicDBObject> cursor = getNodeCollection()
                .find(query)
                .sort(sort)
                .limit(limit);
        return wrap(transform(cursor, input -> store.convertFromDBObject(NODES, input)));
    }

    /**
     * Retrieves a document with the given id from the MongoDB collection.
     * If a list of fields is provided, only these fields are included in the returned document.
     *
     * @param id the id of the document to retrieve
     * @param fields the list of fields to include in the returned document. If null or empty, all fields are returned.
     * @return an Optional that contains the requested NodeDocument if it exists, or an empty Optional if it does not.
     */
    @Override
    public Optional<NodeDocument> getDocument(final String id, final List<String> fields) {

        final Bson query = eq(ID, id);

        final FindIterable<BasicDBObject> result = getNodeCollection().find(query);

        if (fields != null && !fields.isEmpty()) {
            result.projection(include(fields));
        }

        try(MongoCursor<BasicDBObject> cur = result.iterator()) {
            return cur.hasNext() ? ofNullable(store.convertFromDBObject(NODES, cur.next())) : empty();
        } catch (Exception ex) {
            LOG.error("getDocument() <- error while fetching data from Mongo", ex);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("No Doc has been found with id [{}], retuning empty", id);
        }
        return empty();

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
                    .maxTime(15, TimeUnit.MINUTES).hint(hint),
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

    /**
     * Retrieve the time of the oldest modified document.
     *
     * @param clock System Clock to measure time in accuracy of millis
     * @return the timestamp of the oldest modified document.
     */
    @Override
    public Optional<NodeDocument> getOldestModifiedDoc(final Clock clock) {
        final Bson sort = and(eq(MODIFIED_IN_SECS, 1), eq(ID, 1));

        // we need to add query condition to ignore `previous` documents which doesn't have this field
        final Bson query = exists(MODIFIED_IN_SECS);

        FindIterable<BasicDBObject> limit = getNodeCollection().find(query).sort(sort).limit(1);

        try(MongoCursor<BasicDBObject> cur = limit.iterator()) {
            return cur.hasNext() ? ofNullable(store.convertFromDBObject(NODES, cur.next())) : empty();
        } catch (Exception ex) {
            LOG.error("getOldestModifiedDoc() <- error while fetching data from Mongo", ex);
        }
        LOG.info("No Modified Doc has been found, retuning empty");
        return empty();
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
                result.addAll(queriesForDefaultNoBranch(sweepRevs, getModifiedInSecs(oldestRevTimeStamp)));
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
    private List<Bson> queriesForDefaultNoBranch(RevisionVector sweepRevs, long maxRevTimeInSecs) {
        // default_no_branch split type is special because we can
        // only remove those older than sweep rev
        List<Bson> result = Lists.newArrayList();
        for (Revision r : sweepRevs) {
            if (lastDefaultNoBranchDeletionRevs != null) {
                Revision dr = lastDefaultNoBranchDeletionRevs.getRevision(r.getClusterId());
                if (dr != null) {
                    if (dr.getTimestamp() == r.getTimestamp()) {
                        // implies for this clusterNodeId the sweepRev is in control wrt RGC
                        // and we've already deleted up to that point.
                        // and meanwhile the sweepRev for that clusterNodeId hasn't changed.
                        // so we can skip it
                        continue;
                    }
                }
            }
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

            long minMaxRevTimeInSecs = Math.min(maxRevTimeInSecs, getModifiedInSecs(r.getTimestamp()));
            result.add(Filters.and(
                    Filters.eq(SD_TYPE, DEFAULT_NO_BRANCH.typeCode()),
                    Filters.lt(SD_MAX_REV_TIME_IN_SECS, minMaxRevTimeInSecs),
                    idPathClause
                    ));
        }
        return result;
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
            // remember the revisions up to which deletion happened
            Set<Revision> deletionRevs = new HashSet<>();
            for (Revision r : sweepRevs) {
                if (r.getTimestamp() <= oldestRevTimeStamp) {
                    // then sweepRev of this clusterNodeId was taking effect: we could only delete up to sweepRev
                    deletionRevs.add(r);
                } else {
                    // then sweepRev is newer than oldestRevTimeStamp, so we could delete up to oldestRevTimeStamp
                    deletionRevs.add(new Revision(oldestRevTimeStamp, 0, r.getClusterId()));
                }
            }
            MongoVersionGCSupport.this.lastDefaultNoBranchDeletionRevs = new RevisionVector(deletionRevs);
            return cnt;
        }
    }
}
