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

package org.apache.jackrabbit.oak.plugins.document;

import static java.util.Comparator.comparing;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.concat;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static java.util.stream.Collectors.toList;
import static org.apache.jackrabbit.oak.plugins.document.Document.ID;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getSelectedDocuments;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.collect.Iterables;

public class VersionGCSupport {

    private static final Logger LOG = LoggerFactory.getLogger(VersionGCSupport.class);

    private final DocumentStore store;

    public VersionGCSupport(DocumentStore store) {
        this.store = store;
    }

    /**
     * Returns documents that have a {@link NodeDocument#MODIFIED_IN_SECS} value
     * within the given range and the {@link NodeDocument#DELETED} set to
     * {@code true}. The two passed modified timestamps are in milliseconds
     * since the epoch and the implementation will convert them to seconds at
     * the granularity of the {@link NodeDocument#MODIFIED_IN_SECS} field and
     * then perform the comparison.
     *
     * @param fromModified the lower bound modified timestamp (inclusive)
     * @param toModified the upper bound modified timestamp (exclusive)
     * @return matching documents.
     */
    public Iterable<NodeDocument> getPossiblyDeletedDocs(final long fromModified, final long toModified) {
        return StreamSupport.stream(getSelectedDocuments(store, NodeDocument.DELETED_ONCE, 1).spliterator(), false)
                .filter(input -> input.wasDeletedOnce() && modifiedGreaterThanEquals(input, fromModified) && modifiedLessThan(input, toModified))
                .collect(toList());
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
     *
     * @param fromModified the lower bound modified timestamp (inclusive)
     * @param toModified   the upper bound modified timestamp (exclusive)
     * @param limit        the limit of documents to return
     * @param fromId       the lower bound {@link NodeDocument#ID}
     * @return matching documents.
     */
    public Iterable<NodeDocument> getModifiedDocs(final long fromModified, final long toModified, final int limit,
                                                  @NotNull final String fromId,
                                                  @NotNull final Set<String> includePaths,
                                                  @NotNull final Set<String> excludePaths) {
        final long fromModifiedQuery;
        if (MIN_ID_VALUE.equals(fromId)) {
            // If fromId is MIN_ID_VALUE, round fromModified to 5 second resolution
            fromModifiedQuery = getModifiedInSecs(fromModified);
        } else {
            // If fromId is not MIN_ID_VALUE, don't round fromModified to 5 second resolution
            fromModifiedQuery = TimeUnit.MILLISECONDS.toSeconds(fromModified);
        }

        // (_modified = fromModified && _id > fromId || _modified > fromModified && _modified < toModified)
        final Stream<NodeDocument> s1 = StreamSupport.stream(getSelectedDocuments(store,
                MODIFIED_IN_SECS, 1, fromId, includePaths, excludePaths).spliterator(), false)
                .filter(input -> modifiedEqualsToExactTime(input, fromModifiedQuery));

        final Stream<NodeDocument> s2 = StreamSupport.stream(getSelectedDocuments(store,
                MODIFIED_IN_SECS, 1, includePaths, excludePaths).spliterator(), false)
                .filter(input -> modifiedGreaterThanExactTime(input, fromModifiedQuery) && modifiedLessThan(input, toModified));

        return concat(s1, s2)
                .sorted((o1, o2) -> comparing(NodeDocument::getModified).thenComparing(Document::getId).compare(o1, o2))
                .limit(limit)
                .collect(toList());
    }

    private boolean modifiedGreaterThanEquals(final NodeDocument doc, final long time) {
        Long modified = doc.getModified();
        return modified != null && modified.compareTo(getModifiedInSecs(time)) >= 0;
    }

    private boolean modifiedGreaterThanExactTime(final NodeDocument doc, final long time) {
        Long modified = doc.getModified();
        return modified != null && modified.compareTo(time) > 0;
    }

    private boolean modifiedEqualsToExactTime(final NodeDocument doc, final long time) {
        Long modified = doc.getModified();
        return modified != null && modified.compareTo(time) == 0;
    }

    private boolean modifiedLessThan(final NodeDocument doc, final long time) {
        Long modified = doc.getModified();
        return modified != null && modified.compareTo(getModifiedInSecs(time)) < 0;
    }

    private boolean idEquals(final NodeDocument doc, final String id) {
        return Objects.equals(doc.getId(), id);
    }

    /**
     * Returns the underlying document store.
     *
     * @return the underlying document store.
     */
    @NotNull
    public DocumentStore getDocumentStore() {
        return store;
    }

    void deleteSplitDocuments(Set<SplitDocType> gcTypes,
                              RevisionVector sweepRevs,
                              long oldestRevTimeStamp,
                              VersionGCStats stats) {
        SplitDocumentCleanUp cu = createCleanUp(gcTypes, sweepRevs, oldestRevTimeStamp, stats);
        try {
            stats.splitDocGCCount += cu.disconnect().deleteSplitDocuments();
        }
        finally {
            Utils.closeIfCloseable(cu);
        }
    }

    protected SplitDocumentCleanUp createCleanUp(Set<SplitDocType> gcTypes,
                                                 RevisionVector sweepRevs,
                                                 long oldestRevTimeStamp,
                                                 VersionGCStats stats) {
        return new SplitDocumentCleanUp(store, stats,
                identifyGarbage(gcTypes, sweepRevs, oldestRevTimeStamp));
    }

    protected Iterable<NodeDocument> identifyGarbage(final Set<SplitDocType> gcTypes,
                                                     final RevisionVector sweepRevs,
                                                     final long oldestRevTimeStamp) {
        return filter(getAllDocuments(store),
                doc -> gcTypes.contains(doc.getSplitDocType())
                        && doc.hasAllRevisionLessThan(oldestRevTimeStamp)
                        && !isDefaultNoBranchSplitNewerThan(doc, sweepRevs));
    }

    /**
     * Retrieve the time of the oldest document marked as 'deletedOnce'.
     *
     * @param precisionMs the exact time may vary by given precision
     * @return the timestamp of the oldest document marked with 'deletecOnce',
     *          module given prevision. If no such document exists, returns the
     *          max time inspected (close to current time).
     */
    public long getOldestDeletedOnceTimestamp(Clock clock, long precisionMs) {
        long ts = 0;
        long now = clock.getTime();
        long duration =  (now - ts) / 2;
        Iterable<NodeDocument> docs;

        while (duration > precisionMs) {
            // check for delete candidates in [ ts, ts + duration]
            LOG.debug("find oldest _deletedOnce, check < {}", Utils.timestampToString(ts + duration));
            docs = getPossiblyDeletedDocs(ts, ts + duration);
            if (docs.iterator().hasNext()) {
                // look if there are still nodes to be found in the lower half
                duration /= 2;
            }
            else {
                // so, there are no delete candidates older than "ts + duration"
                ts = ts + duration;
                duration /= 2;
            }
            Utils.closeIfCloseable(docs);
        }
        LOG.debug("find oldest _deletedOnce to be {}", Utils.timestampToString(ts));
        return ts;
    }

    /**
     * Retrieve the oldest modified document.
     *
     * @return the oldest modified document.
     */
    public Optional<NodeDocument> getOldestModifiedDoc(final Clock clock) {
        long now = clock.getTime();
        Iterable<NodeDocument> docs = null;
        try {
            docs = getModifiedDocs(0, now, 1, MIN_ID_VALUE, Collections.emptySet(), Collections.emptySet());
            if (docs.iterator().hasNext()) {
                final NodeDocument oldestModifiedDoc = docs.iterator().next();
                LOG.info("Oldest modified document is {}", oldestModifiedDoc);
                return ofNullable(oldestModifiedDoc);
            }
        } finally {
            Utils.closeIfCloseable(docs);
        }
        LOG.info("No Modified Doc has been found, retuning empty");
        return empty();
    }

    /**
     * Retrieves a document with the given id from the DocumentStore.
     * If a list of fields is provided, only these fields are included in the returned document.
     *
     * @param id the id of the document to retrieve
     * @param fields the list of fields to include in the returned document. If null or empty, all fields are returned.
     * @return an Optional that contains the requested NodeDocument if it exists, or an empty Optional if it does not.
     */
    public Optional<NodeDocument> getDocument(final String id, final List<String> fields) {

        Iterable<NodeDocument> docs = null;
        try {
            docs = StreamSupport.stream(getSelectedDocuments(store, null, 0, MIN_ID_VALUE).spliterator(), false)
                    .filter(input -> idEquals(input, id)).limit(1).collect(toList());
            if (docs.iterator().hasNext()) {
                final NodeDocument doc = docs.iterator().next();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Found Document with id {}", id);
                }
                if (fields == null || fields.isEmpty()) {
                    return ofNullable(doc);
                }

                final Set<String> projectedSet = new HashSet<>(fields);
                projectedSet.add(ID);

                final NodeDocument newDoc = Collection.NODES.newDocument(store);
                doc.deepCopy(newDoc);
                newDoc.keySet().retainAll(projectedSet);
                return of(newDoc);
            }

        } finally {
            Utils.closeIfCloseable(docs);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("No Doc has been found with id [{}]", id);
        }
        return empty();
    }

    public long getDeletedOnceCount() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("getDeletedOnceCount()");
    }

    /**
     * Returns {@code true} if the given document is of type
     * {@link SplitDocType#DEFAULT_NO_BRANCH} and the most recent change on the
     * document is newer than the {@code sweepRevs}.
     *
     * @param doc the document to check.
     * @param sweepRevs the current sweep revisions.
     * @return {@code true} if the document is a {@link SplitDocType#DEFAULT_NO_BRANCH}
     *      and it is newer than {@code sweepRevs}; {@code false} otherwise.
     */
    protected static boolean isDefaultNoBranchSplitNewerThan(NodeDocument doc,
                                                             RevisionVector sweepRevs) {
        if (doc.getSplitDocType() != SplitDocType.DEFAULT_NO_BRANCH) {
            return false;
        }
        Revision r = Iterables.getFirst(doc.getAllChanges(), null);
        return r != null && sweepRevs.isRevisionNewer(r);
    }
}
