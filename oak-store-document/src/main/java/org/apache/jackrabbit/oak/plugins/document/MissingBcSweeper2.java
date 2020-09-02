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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.COMMITROOT_OR_REVISIONS;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.commons.TimeDurationFormatter;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

/**
 * The {@code MissingBcSweeper2} is used for the so-called sweep2, which is
 * a repository traversal updating documents that have missing branch commit ("_bc") 
 * properties (details see OAK-9176).
 * This class is similar to NodeDocumentSweeper as it is based on the same principles,
 * with a few notable exceptions (like it only looks at _commitRoot and _revisions etc).
 * And due to these exceptions the class is forked rather than modified/subclasses
 * (also to enable later refactoring of the NodeDocumentSweeper itself).
 * <p>
 * This class is not thread-safe.
 */
final class MissingBcSweeper2 {

    private static final Logger LOG = LoggerFactory.getLogger(MissingBcSweeper2.class);

    private static final int YIELD_SIZE = 500;

    private static final int INVALIDATE_BATCH_SIZE = 100;

    private static final long LOGINTERVALMS = TimeUnit.MINUTES.toMillis(1);

    private final RevisionContext context;

    private final CommitValueResolver commitValueResolver;

    private final int clusterId;

    private final RevisionVector headRevision;

    private long totalCount;
    private long lastCount;
    private long startOfScan;
    private long lastLog;


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
    MissingBcSweeper2(RevisionContext context,
                        CommitValueResolver commitValueResolver) {
        this.context = checkNotNull(context);
        this.commitValueResolver = checkNotNull(commitValueResolver);
        this.clusterId = context.getClusterId();
        this.headRevision= context.getHeadRevision();
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
     * @throws DocumentStoreException if reading from the store or writing to
     *          the store failed.
     */
    void sweep(@NotNull Iterable<NodeDocument> documents,
                   @NotNull NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        performSweep(documents, checkNotNull(listener));
    }

    //----------------------------< internal >----------------------------------

    @Nullable
    private void performSweep(Iterable<NodeDocument> documents,
                                  NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        totalCount = 0;
        lastCount = 0;
        startOfScan = context.getClock().getTime();
        lastLog = startOfScan;

        Iterable<Map.Entry<Path, UpdateOp>> ops = sweepOperations(documents);
        for (List<Map.Entry<Path, UpdateOp>> batch : partition(ops, INVALIDATE_BATCH_SIZE)) {
            Map<Path, UpdateOp> updates = newHashMap();
            for (Map.Entry<Path, UpdateOp> entry : batch) {
                updates.put(entry.getKey(), entry.getValue());
            }
            listener.sweepUpdate(updates);
        }
        LOG.debug("Document sweep finished");
    }

    private Iterable<Map.Entry<Path, UpdateOp>> sweepOperations(
            final Iterable<NodeDocument> docs) {
        return filter(transform(docs,
                new Function<NodeDocument, Map.Entry<Path, UpdateOp>>() {

            int yieldCnt = 0;
            long lastYield = System.currentTimeMillis();

            @Override
            public Map.Entry<Path, UpdateOp> apply(NodeDocument doc) {
                if (++yieldCnt >= YIELD_SIZE) {
                    try {
                        Thread.sleep(Math.max(1, System.currentTimeMillis() - lastYield));
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    lastYield = System.currentTimeMillis();
                    yieldCnt = 0;
                }
                return immutableEntry(doc.getPath(), sweepOne(doc));
            }
        }), new Predicate<Map.Entry<Path, UpdateOp>>() {
            @Override
            public boolean apply(Map.Entry<Path, UpdateOp> input) {
                return input.getValue() != null;
            }
        });
    }

    private UpdateOp sweepOne(NodeDocument doc) throws DocumentStoreException {
        UpdateOp op = null;
        // go through PROPERTY_OR_DELETED_OR_COMMITROOT, whereas :
        // - PROPERTY : for content changes
        // - DELETED : for new node (this)
        // - COMMITROOT : for new child (parent)
        // - REVISIONS : for commit roots (root for branch commits)
        for (String property : filter(doc.keySet(), COMMITROOT_OR_REVISIONS)) {
            Map<Revision, String> valueMap = doc.getLocalMap(property);
            for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
                Revision rev = entry.getKey();

                // consider any clusterId for sweep2

                Revision cRev = getCommitRevision(doc, rev);
                if (cRev == null) {
                    // originally the uncommitted case
                    // however, sweep2 doesn't touch anything uncommitted
                } else if (cRev.equals(rev)) {
                    // originally the committed case
                    // however, that was only logging and sweep2 is not about that,
                    // hence nothing to be done here
                } else {
                    // this is what sweep2 is about : adding _bc for _commitRoot and _revisions
                    if (op == null) {
                        op = createUpdateOp(doc);
                    }
                    committedBranch(doc, property, rev, cRev, op);
                }
            }
        }

        totalCount++;
        lastCount++;
        long now = context.getClock().getTime();
        long lastElapsed = now - lastLog;

        if (lastElapsed >= LOGINTERVALMS) {
            TimeDurationFormatter df = TimeDurationFormatter.forLogging();

            long totalElapsed = now - startOfScan;
            long totalRateMin = (totalCount * TimeUnit.MINUTES.toMillis(1)) / totalElapsed;
            long lastRateMin = (lastCount * TimeUnit.MINUTES.toMillis(1)) / lastElapsed;

            String message = String.format(
                    "Sweep on cluster node [%d]: %d nodes scanned in %s (~%d/m) - last interval %d nodes in %s (~%d/m)",
                    clusterId, totalCount, df.format(totalElapsed, TimeUnit.MILLISECONDS), totalRateMin, lastCount,
                    df.format(lastElapsed, TimeUnit.MILLISECONDS), lastRateMin);

            LOG.info(message);
            lastLog = now;
            lastCount = 0;
        }

        return op == null ? null : op.hasChanges() ? op : null;
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

    private void committedBranch(NodeDocument doc,
                                 String property,
                                 Revision rev,
                                 Revision cRev,
                                 UpdateOp op) {
        boolean newerThanHead = headRevision.isRevisionNewer(cRev);
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

    private String getCommitValue(@NotNull Revision changeRevision,
            @NotNull NodeDocument doc) {
        return commitValueResolver.resolve(changeRevision, doc);
    }

    @Nullable
    private Revision getCommitRevision(final NodeDocument doc,
                                       final Revision rev)
            throws DocumentStoreException {
        String cv = /*context.*/getCommitValue(rev, doc);
        if (cv == null) {
            return null;
        }
        return Utils.resolveCommitRevision(rev, cv);
    }
}
