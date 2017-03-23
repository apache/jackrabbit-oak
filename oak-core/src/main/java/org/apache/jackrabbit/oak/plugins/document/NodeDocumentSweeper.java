/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.getModifiedInSecs;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isDeletedEntry;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removeCommitRoot;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removeRevision;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setDeletedOnce;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.PROPERTY_OR_DELETED;

/**
 * The {@code NodeDocumentSweeper} is responsible for removing uncommitted
 * changes from {@code NodeDocument}s for a given clusterId. The sweeper scans
 * through documents modified after a given timestamp. This timestamp is derived
 * from the sweep revisions available on the root document
 * ({@link NodeDocument#getSweepRevisions()}). If no sweep revision is present
 * for the current clusterId, the sweeper scans through the entire nodes
 * collection. This is usually a one-time operation when the repository is
 * upgraded from a version older than 1.8.
 * <p>
 * The sweeper can read from an eventually consistent store and does not require
 * that it sees the most up-to-date state of the store. That is, running it off
 * a MongoDB Secondary is fine and even recommended.
 * <p>
 * This class is not thread-safe.
 */
abstract class NodeDocumentSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(NodeDocumentSweeper.class);

    private static final int INVALIDATE_BATCH_SIZE = 100;

    private final RevisionContext context;

    private final int clusterId;

    private Revision headRevision;

    /**
     * Creates a new sweeper for the given context.
     *
     *  @param context the revision context.
     *
     */
    NodeDocumentSweeper(RevisionContext context) {
        this.context = checkNotNull(context);
        this.clusterId = context.getClusterId();
    }

    /**
     * Must be implemented by sub-class and return the current root document
     * in the nodes collection. An implementation is permitted to return a root
     * document that is not up-to-date, as long as it is consistency with
     * {@link #getDocuments(long)}.
     *
     * @return the root document.
     * @throws DocumentStoreException if an exception occurs reading the
     *          document.
     */
    @Nonnull
    protected abstract NodeDocument getRoot() throws DocumentStoreException;

    /**
     * Returns all documents that have a {@link NodeDocument#MODIFIED_IN_SECS}
     * field equal or greater than {@code modifiedInSecs}. An implementation is
     * permitted to return documents that are not up-to-date, as long as they
     * are consistent with {@link #getRoot()}.
     * <p>
     * See also {@link NodeDocument#getModifiedInSecs(long)}.
     *
     * @param modifiedInSecs a timestamp in seconds.
     * @return matching documents.
     * @throws DocumentStoreException if an exception occurs reading documents.
     */
    @Nonnull
    protected abstract Iterable<NodeDocument> getDocuments(long modifiedInSecs)
            throws DocumentStoreException;

    /**
     * Performs a sweep and reports the required updates to the given sweep
     * listener. The returned revision is the new sweep revision for the
     * clusterId associated with the revision context used to create this
     * sweeper. The caller is responsible for storing the returned sweep
     * revision on the root document. This method returns {@code null} if no
     * update was needed or possible.
     *
     * @param listener the listener to receive required sweep update operations.
     * @return the new sweep revision or {@code null} if no updates were done.
     * @throws DocumentStoreException if reading from the store or writing to
     *          the store failed.
     */
    @CheckForNull
    Revision sweep(NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        return performSweep(listener);
    }

    //----------------------------< internal >----------------------------------

    @CheckForNull
    private Revision performSweep(NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        NodeDocument rootDoc = getRoot();
        RevisionVector head = getHeadRevision(rootDoc);
        headRevision = head.getRevision(clusterId);
        if (headRevision == null) {
            LOG.warn("Head revision {} does not have an entry for " +
                            "clusterId {}. Sweeping of documents is skipped.",
                    head, clusterId);
            return null;
        }
        RevisionVector sweepRevs = rootDoc.getSweepRevisions();
        Revision lastSweepHead = sweepRevs.getRevision(clusterId);
        if (lastSweepHead == null) {
            // sweep all
            lastSweepHead = new Revision(0, 0, clusterId);
        }
        // only sweep documents when the _modified time changed
        long lastSweepTick = getModifiedInSecs(lastSweepHead.getTimestamp());
        long currentTick = getModifiedInSecs(context.getClock().getTime());
        if (lastSweepTick == currentTick) {
            return null;
        }

        LOG.debug("Starting document sweep. Head: {}, starting at {}",
                headRevision, lastSweepHead);

        CloseableIterable<Map.Entry<String, UpdateOp>> ops = sweepOperations(lastSweepTick);
        try {
            for (List<Map.Entry<String, UpdateOp>> batch : partition(ops, INVALIDATE_BATCH_SIZE)) {
                Map<String, UpdateOp> updates = newHashMap();
                for (Map.Entry<String, UpdateOp> entry : batch) {
                    updates.put(entry.getKey(), entry.getValue());
                }
                listener.sweepUpdate(updates);
            }
        } finally {
            try {
                ops.close();
            } catch (IOException e) {
                LOG.warn("Failed to close sweep operations", e);
            }
        }
        LOG.debug("Document sweep finished");
        return headRevision;
    }

    private CloseableIterable<Map.Entry<String, UpdateOp>> sweepOperations(long modifiedInSecs) {
        final Iterable<NodeDocument> docs = getDocuments(modifiedInSecs);
        return CloseableIterable.wrap(filter(transform(docs,
                new Function<NodeDocument, Map.Entry<String, UpdateOp>>() {
            @Override
            public Map.Entry<String, UpdateOp> apply(NodeDocument doc) {
                return immutableEntry(doc.getPath(), sweepOne(doc));
            }
        }), new Predicate<Map.Entry<String, UpdateOp>>() {
            @Override
            public boolean apply(Map.Entry<String, UpdateOp> input) {
                return input.getValue() != null;
            }
        }), new Closeable() {
            @Override
            public void close() throws IOException {
                Utils.closeIfCloseable(docs);
            }
        });
    }

    @Nonnull
    private RevisionVector getHeadRevision(NodeDocument rootDoc) {
        return new RevisionVector(rootDoc.getLastRev().values());
    }

    private UpdateOp sweepOne(NodeDocument doc) throws DocumentStoreException {
        UpdateOp op = createUpdateOp(doc);
        for (String property : filter(doc.keySet(), PROPERTY_OR_DELETED)) {
            Map<Revision, String> valueMap = doc.getLocalMap(property);
            for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
                Revision rev = entry.getKey();
                // only consider change for this cluster node
                if (rev.getClusterId() != clusterId) {
                    continue;
                }
                Revision cRev = getCommitRevision(doc, rev);
                if (cRev == null) {
                    uncommitted(doc, property, rev, op);
                } else if (cRev.equals(rev)) {
                    committed(property, rev, op);
                } else {
                    committedBranch(property, rev, cRev, op);
                }
            }
        }
        return op.hasChanges() ? op : null;
    }

    private void uncommitted(NodeDocument doc,
                             String property,
                             Revision rev,
                             UpdateOp op) {
        if (headRevision.compareRevisionTime(rev) < 0) {
            // ignore changes that happen after the
            // head we are currently looking at
            return;
        }
        if (isV18BranchCommit(rev, doc)) {
            // this is a not yet merged branch commit
            // -> do nothing
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unmerged branch commit on {}, {} @ {}",
                        op.getId(), property, rev);
            }
        } else {
            // this may be a not yet merged branch commit, but since it
            // wasn't created by this Oak version, it must be a left over
            // from an old branch which cannot be merged anyway.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Uncommitted change on {}, {} @ {}",
                        op.getId(), property, rev);
            }
            op.removeMapEntry(property, rev);
            if (doc.getLocalCommitRoot().containsKey(rev)) {
                removeCommitRoot(op, rev);
            } else {
                removeRevision(op, rev);
            }
            // set _deletedOnce if uncommitted change is a failed create
            // node operation and doc does not have _deletedOnce yet
            if (isDeletedEntry(property)
                    && !doc.wasDeletedOnce()
                    && "false".equals(doc.getLocalDeleted().get(rev))) {
                setDeletedOnce(op);
            }
        }
    }

    /**
     * Returns {@code true} if the given revision is marked as a branch commit
     * on the document. This method only checks local branch commit information
     * available on the document ({@link NodeDocument#getLocalBranchCommits()}).
     * If the given revision is related to a branch commit that was created
     * prior to Oak 1.8, the method will return {@code false}.
     *
     * @param rev a revision.
     * @param doc the document to check.
     * @return {@code true} if the revision is marked as a branch commit;
     *          {@code false} otherwise.
     */
    private boolean isV18BranchCommit(Revision rev, NodeDocument doc) {
        return doc.getLocalBranchCommits().contains(rev);
    }

    private void committed(String property,
                           Revision rev,
                           UpdateOp op) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committed change on {}, {} @ {}",
                    op.getId(), property, rev);
        }
    }

    private void committedBranch(String property,
                                 Revision rev,
                                 Revision cRev,
                                 UpdateOp op) {
        boolean newerThanHead = cRev.compareRevisionTime(headRevision) > 0;
        if (LOG.isDebugEnabled()) {
            String msg = newerThanHead ? " (newer than head)" : "";
            LOG.debug("Committed branch change on {}, {} @ {}/{}{}",
                    op.getId(), property, rev, cRev, msg);
        }
    }

    private static UpdateOp createUpdateOp(NodeDocument doc) {
        return new UpdateOp(doc.getId(), false);
    }

    @CheckForNull
    private Revision getCommitRevision(final NodeDocument doc,
                                       final Revision rev)
            throws DocumentStoreException {
        String cv = context.getCommitValue(rev, doc);
        if (cv == null) {
            return null;
        }
        return Utils.resolveCommitRevision(rev, cv);
    }
}
