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
import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static java.util.stream.Collectors.toList;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getSelectedDocuments;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.base.Predicate;
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
        return stream(getSelectedDocuments(store, NodeDocument.DELETED_ONCE, 1).spliterator(), false)
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
     * @param fromModified the lower bound modified timestamp (inclusive)
     * @param toModified   the upper bound modified timestamp (exclusive)
     * @param limit        the limit of documents to return
     * @param fromId       the lower bound {@link NodeDocument#ID}
     * @return matching documents.
     */
    public Iterable<NodeDocument> getModifiedDocs(final long fromModified, final long toModified, final int limit,
                                                  @NotNull final String fromId) {
        // (_modified = fromModified && _id > fromId || _modified > fromModified && _modified < toModified)
        final Stream<NodeDocument> s1 = stream(getSelectedDocuments(store, MODIFIED_IN_SECS, 1, fromId).spliterator(), false)
                .filter(input -> modifiedEqualsTo(input, fromModified));

        final Stream<NodeDocument> s2 = stream(getSelectedDocuments(store, MODIFIED_IN_SECS, 1).spliterator(), false)
                .filter(input -> modifiedGreaterThan(input, fromModified) && modifiedLessThan(input, toModified));

        return concat(s1, s2)
                .sorted((o1, o2) -> comparing(NodeDocument::getModified).thenComparing(Document::getId).compare(o1, o2))
                .limit(limit)
                .collect(toList());
    }

    private boolean modifiedGreaterThanEquals(final NodeDocument doc, final long time) {
        Long modified = doc.getModified();
        return modified != null && modified.compareTo(getModifiedInSecs(time)) >= 0;
    }

    private boolean modifiedGreaterThan(final NodeDocument doc, final long time) {
        Long modified = doc.getModified();
        return modified != null && modified.compareTo(getModifiedInSecs(time)) > 0;
    }

    private boolean modifiedEqualsTo(final NodeDocument doc, final long time) {
        Long modified = doc.getModified();
        return modified != null && modified.compareTo(getModifiedInSecs(time)) == 0;
    }

    private boolean modifiedLessThan(final NodeDocument doc, final long time) {
        Long modified = doc.getModified();
        return modified != null && modified.compareTo(getModifiedInSecs(time)) < 0;
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
        return filter(getAllDocuments(store), new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument doc) {
                return gcTypes.contains(doc.getSplitDocType())
                        && doc.hasAllRevisionLessThan(oldestRevTimeStamp)
                        && !isDefaultNoBranchSplitNewerThan(doc, sweepRevs);
            }
        });
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
            docs = getModifiedDocs(0, now, 1, MIN_ID_VALUE);
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
