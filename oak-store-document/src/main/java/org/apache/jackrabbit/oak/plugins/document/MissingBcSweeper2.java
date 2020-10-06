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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final int executingClusterId;

    private final List<Integer> includedClusterIds;

    private final RevisionVector headRevision;

    private final AtomicBoolean isDisposed;

    private long totalCount;
    private long lastCount;
    private long startOfScan;
    private long lastLog;

    /**
     * Creates a new sweeper v2 for the given context..
     *
     * @param context the revision context.
     */
    MissingBcSweeper2(RevisionContext context,
                    CommitValueResolver commitValueResolver,
                    List<Integer> includedClusterIds,
                    AtomicBoolean isDisposed) {
        this.context = checkNotNull(context);
        this.commitValueResolver = checkNotNull(commitValueResolver);
        this.executingClusterId = context.getClusterId();
        this.includedClusterIds = includedClusterIds == null ? new LinkedList<>() : Collections.unmodifiableList(includedClusterIds);
        this.headRevision= context.getHeadRevision();
        this.isDisposed = isDisposed;
    }

    /**
     * Performs a sweep2 and reports the required updates to the given sweep
     * listener.
     *
     * @param documents the documents to sweep
     * @param listener the listener to receive required sweep update operations.
     * @throws DocumentStoreException if reading from the store or writing to
     *          the store failed.
     */
    void sweep2(@NotNull Iterable<NodeDocument> documents,
                   @NotNull NodeDocumentSweepListener listener)
            throws DocumentStoreException {
        performSweep2(documents, checkNotNull(listener));
    }

    //----------------------------< internal >----------------------------------

    private void performSweep2(Iterable<NodeDocument> documents,
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
            if (isDisposed.get()) {
                throw new DocumentStoreException("sweep2 interrupted by shutdown");
            }
        }
        LOG.debug("Document sweep2 finished");
    }

    private Iterable<Map.Entry<Path, UpdateOp>> sweepOperations(
            final Iterable<NodeDocument> docs) {
        return filter(transform(docs,
                new Function<NodeDocument, Map.Entry<Path, UpdateOp>>() {

            int yieldCnt = 0;
            long lastYield = context.getClock().getTime();

            @Override
            public Map.Entry<Path, UpdateOp> apply(NodeDocument doc) {
                if (++yieldCnt >= YIELD_SIZE) {
                    try {
                        final long now = context.getClock().getTime();
                        final long timeSinceLastYield = now - lastYield;
                        // wait the same amount of time that passed since last yield
                        // that corresponds to roughly 50% throttle (ignoring the min 1ms sleep)
                        final long waitUntil = now + Math.max(1, timeSinceLastYield);
                        context.getClock().waitUntil(waitUntil);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    lastYield = context.getClock().getTime();
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
        // the blueprint NodeDocumentSweeper.sweepOne goes through
        // PROPERTY_OR_DELETED_OR_COMMITROOT here, as that's the new full sweep1.
        // the sweep2 though, only goes through COMMITROOT_OR_REVISIONS,
        // as that's what was left out by the original sweep1:
        // - COMMITROOT : for new child (parent)
        // - REVISIONS : for commit roots (root for branch commits)
        for (String property : filter(doc.keySet(), COMMITROOT_OR_REVISIONS)) {
            Map<Revision, String> valueMap = doc.getLocalMap(property);
            for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
                Revision rev = entry.getKey();

                if (!includedClusterIds.isEmpty() && !includedClusterIds.contains(rev.getClusterId())) {
                    // sweep2 is only done for those clusterIds that went through sweep1 (before 1.8).
                    // includedClusterIds contains the latter list of clusterIds.
                    // For testing, the code also supports going through all clusterIds.
                    // (in this case the includedClusterIds list is empty).
                    // Note that sweep2 is never triggered if "_sweepRev" is empty,
                    // so the empty case here never applies for production.
                    continue;
                }

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

            String restrictionMsg;
            if (includedClusterIds.isEmpty()) {
                restrictionMsg = "unrestricted, ie for all clusterIds";
            } else {
                restrictionMsg = "restricted to clusterIds " + includedClusterIds;
            }
            String message = String.format(
                    "Sweep2 executed by cluster node [%d] (%s): %d nodes scanned in %s (~%d/m) - last interval %d nodes in %s (~%d/m)",
                    executingClusterId, restrictionMsg, totalCount, df.format(totalElapsed, TimeUnit.MILLISECONDS), totalRateMin, lastCount,
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
