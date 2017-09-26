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

import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isDeletedEntry;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removeCommitRoot;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removeRevision;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setDeletedOnce;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.PROPERTY_OR_DELETED;

/**
 * The {@code NodeDocumentSweeper} is responsible for removing uncommitted
 * changes from {@code NodeDocument}s for a given clusterId.
 * <p>
 * This class is not thread-safe.
 */
final class NodeDocumentSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(NodeDocumentSweeper.class);

    private static final int INVALIDATE_BATCH_SIZE = 100;

    private final RevisionContext context;

    private final int clusterId;

    private final RevisionVector headRevision;

    private final boolean sweepNewerThanHead;

    private Revision head;

    private long documentCount;

    /**
     * Creates a new sweeper for the given context. The sweeper is initialized
     * in the constructor with the head revision provided by the revision
     * context. This is the head revision used later when the documents are
     * check for uncommitted changes in
     * {@link #sweep(Iterable, NodeDocumentSweepListener)}.
     * <p>
     * In combination with {@code sweepNewerThanHead == false}, the revision
     * context may return a head revision that is not up-to-date, as long as it
     * is consistent with documents passed to the {@code sweep()} method. That
     * is, the documents must reflect all changes visible from the provided head
     * revision. The sweeper will then only revert uncommitted changes up to the
     * head revision. With {@code sweepNewerThanHead == true}, the sweeper will
     * also revert uncommitted changes that are newer than the head revision.
     * This is usually only useful during recovery of a cluster node, when it is
     * guaranteed that there are no in-progress commits newer than the current
     * head revision.
     *
     * @param context the revision context.
     * @param sweepNewerThanHead whether uncommitted changes newer than the head
     *                 revision should be reverted.
     */
    NodeDocumentSweeper(RevisionContext context,
                        boolean sweepNewerThanHead) {
        this.context = checkNotNull(context);
        this.clusterId = context.getClusterId();
        this.headRevision= context.getHeadRevision();
        this.sweepNewerThanHead = sweepNewerThanHead;
    }

    /**
     * Performs a sweep and reports the required updates to the given sweep
     * listener. The returned revision is the new sweep revision for the
     * clusterId associated with the revision context used to create this
     * sweeper. The caller is responsible for storing the returned sweep
     * revision on the root document. This method returns {@code null} if no
     * update was possible.
     *
     * @param documents the documents to sweep
     * @param listener the listener to receive required sweep update operations.
     * @return the new sweep revision or {@code null} if no updates were done.
     * @throws DocumentStoreException if reading from the store or writing to
     *          the store failed.
     */
    @CheckForNull
    Revision sweep(@Nonnull Iterable<NodeDocument> documents,
                   @Nonnull NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        return performSweep(documents, checkNotNull(listener));
    }

    /**
     * @return the head revision vector in use by this sweeper.
     */
    RevisionVector getHeadRevision() {
        return headRevision;
    }

    //----------------------------< internal >----------------------------------

    @CheckForNull
    private Revision performSweep(Iterable<NodeDocument> documents,
                                  NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        head = headRevision.getRevision(clusterId);
        documentCount = 0;
        if (head == null) {
            LOG.warn("Head revision does not have an entry for " +
                            "clusterId {}. Sweeping of documents is skipped.",
                    clusterId);
            return null;
        }

        Iterable<Map.Entry<String, UpdateOp>> ops = sweepOperations(documents);
        for (List<Map.Entry<String, UpdateOp>> batch : partition(ops, INVALIDATE_BATCH_SIZE)) {
            Map<String, UpdateOp> updates = newHashMap();
            for (Map.Entry<String, UpdateOp> entry : batch) {
                updates.put(entry.getKey(), entry.getValue());
            }
            listener.sweepUpdate(updates);
        }
        LOG.debug("Document sweep finished");
        return head;
    }

    private Iterable<Map.Entry<String, UpdateOp>> sweepOperations(
            final Iterable<NodeDocument> docs) {
        return filter(transform(docs,
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
        });
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
                    committedBranch(doc, property, rev, cRev, op);
                }
            }
        }
        if (++documentCount % 100000 == 0) {
            LOG.info("Checked {} documents so far", documentCount);
        }
        return op.hasChanges() ? op : null;
    }

    private void uncommitted(NodeDocument doc,
                             String property,
                             Revision rev,
                             UpdateOp op) {
        if (head.compareRevisionTime(rev) < 0 && !sweepNewerThanHead) {
            // ignore changes that happen after the
            // head we are currently looking at
            if (LOG.isDebugEnabled()) {
                LOG.debug("Uncommitted change on {}, {} @ {} newer than head {} ",
                        op.getId(), property, rev, head);
            }
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

    private void committedBranch(NodeDocument doc,
                                 String property,
                                 Revision rev,
                                 Revision cRev,
                                 UpdateOp op) {
        boolean newerThanHead = cRev.compareRevisionTime(head) > 0;
        if (LOG.isDebugEnabled()) {
            String msg = newerThanHead ? " (newer than head)" : "";
            LOG.debug("Committed branch change on {}, {} @ {}/{}{}",
                    op.getId(), property, rev, cRev, msg);
        }
        if (!isV18BranchCommit(rev, doc)) {
            NodeDocument.setBranchCommit(op, rev);
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
