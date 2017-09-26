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

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.filterKeys;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.PROPERTY_OR_DELETED;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isCommitted;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.resolveCommitRevision;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for recovering potential missing _lastRev updates of nodes due
 * to crash of a node. The recovery agent is also responsible for document
 * sweeping (reverting uncommitted changes).
 * <p>
 * The recovery agent will only sweep documents for a given clusterId if the
 * root document contains a sweep revision for the clusterId. A missing sweep
 * revision for a clusterId indicates an upgrade from an earlier Oak version and
 * a crash before the initial sweep finished. This is not the responsibility of
 * the recovery agent. An initial sweep for an upgrade must either happen with
 * the oak-run 'revisions' sweep command or on startup of an upgraded Oak
 * instance.
 */
public class LastRevRecoveryAgent {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DocumentNodeStore nodeStore;

    private final MissingLastRevSeeker missingLastRevUtil;

    public LastRevRecoveryAgent(DocumentNodeStore nodeStore,
                                MissingLastRevSeeker seeker) {
        this.nodeStore = nodeStore;
        this.missingLastRevUtil = seeker;
    }

    public LastRevRecoveryAgent(DocumentNodeStore nodeStore) {
        this(nodeStore, new MissingLastRevSeeker(
                nodeStore.getDocumentStore(), nodeStore.getClock()));
    }

    /**
     * Recover the correct _lastRev updates for potentially missing candidate
     * nodes. If another cluster node is already performing the recovery for the
     * given {@code clusterId}, this method will {@code waitUntil} the given
     * time in milliseconds for the recovery to finish.
     *
     * This method will return:
     * <ul>
     *     <li>{@code -1} when another cluster node is busy performing recovery
     *     for the given {@code clusterId} and the {@code waitUntil} time is
     *     reached.</li>
     *     <li>{@code 0} when no recovery was needed or this thread waited
     *     for another cluster node to finish the recovery within the given
     *     {@code waitUntil} time.</li>
     *     <li>A positive value for the number of recovered documents when
     *     recovery was performed by this thread / cluster node.</li>
     * </ul>
     *
     * @param clusterId the cluster id for which the _lastRev are to be recovered
     * @param waitUntil wait until this time is milliseconds for recovery of the
     *                  given {@code clusterId} if another cluster node is
     *                  already performing the recovery.
     * @return the number of restored nodes or {@code -1} if a timeout occurred
     *          while waiting for an ongoing recovery by another cluster node.
     */
    public int recover(int clusterId, long waitUntil)
            throws DocumentStoreException {
        ClusterNodeInfoDocument nodeInfo = missingLastRevUtil.getClusterNodeInfo(clusterId);

        //TODO Currently leaseTime remains same per cluster node. If this
        //is made configurable then it should be read from DB entry
        final long leaseTime = ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS;
        final long asyncDelay = nodeStore.getAsyncDelay();

        if (nodeInfo != null) {
            // Check if _lastRev recovery needed for this cluster node
            // state is Active && current time past leaseEnd
            if (missingLastRevUtil.isRecoveryNeeded(nodeInfo)) {
                long leaseEnd = nodeInfo.getLeaseEndTime();

                // retrieve the root document's _lastRev
                NodeDocument root = missingLastRevUtil.getRoot();
                Revision lastRev = root.getLastRev().get(clusterId);
                Revision sweepRev = root.getSweepRevisions().getRevision(clusterId);

                // start time is the _lastRev timestamp of the cluster node
                long startTime;
                String reason;
                //lastRev can be null if other cluster node did not got
                //chance to perform lastRev rollup even once
                if (lastRev != null) {
                    startTime = lastRev.getTimestamp();
                    reason = "lastRev: " + lastRev.toString();
                } else {
                    startTime = leaseEnd - leaseTime - asyncDelay;
                    reason = String.format(
                            "no lastRev for root, using timestamp based on leaseEnd %d - leaseTime %d - asyncDelay %d", leaseEnd,
                            leaseTime, asyncDelay);
                }
                if (sweepRev != null && sweepRev.getTimestamp() < startTime) {
                    startTime = sweepRev.getTimestamp();
                    reason = "sweepRev: " + sweepRev.toString();
                }

                return recoverCandidates(nodeInfo, startTime, waitUntil, reason);
            }
        }

        log.debug("No recovery needed for clusterId {}", clusterId);
        return 0;
    }

    /**
     * Same as {@link #recover(int, long)}, but does not wait for ongoing
     * recovery.
     *
     * @param clusterId the cluster id for which the _lastRev are to be recovered
     * @return the number of restored nodes or {@code -1} if there is an ongoing
     *          recovery by another cluster node.
     */
    public int recover(int clusterId) {
        return recover(clusterId, 0);
    }

    /**
     * Recover the correct _lastRev updates for the given candidate nodes.
     *
     * @param suspects the potential suspects
     * @param clusterId the cluster id for which _lastRev recovery needed
     * @return the number of documents that required recovery.
     */
    public int recover(Iterable<NodeDocument> suspects, int clusterId) {
        return recover(suspects, clusterId, false);
    }

    /**
     * Recover the correct _lastRev updates for the given candidate nodes.
     *
     * @param suspects the potential suspects
     * @param clusterId the cluster id for which _lastRev recovery needed
     * @param dryRun if {@code true}, this method will only perform a check
     *               but not apply the changes to the _lastRev fields.
     * @return the number of documents that required recovery. This method
     *          returns the number of the affected documents even if
     *          {@code dryRun} is set true and no document was changed.
     */
    public int recover(final Iterable<NodeDocument> suspects,
                       final int clusterId, final boolean dryRun)
            throws DocumentStoreException {
        final DocumentStore docStore = nodeStore.getDocumentStore();
        NodeDocument rootDoc = Utils.getRootDocument(docStore);

        // first run a sweep
        final AtomicReference<Revision> sweepRev = new AtomicReference<>();
        if (rootDoc.getSweepRevisions().getRevision(clusterId) != null) {
            // only run a sweep for a cluster node that already has a
            // sweep revision. Initial sweep is not the responsibility
            // of the recovery agent.
            final RevisionContext context = new InactiveRevisionContext(
                    rootDoc, nodeStore, clusterId);
            final NodeDocumentSweeper sweeper = new NodeDocumentSweeper(context, true);
            sweeper.sweep(suspects, new NodeDocumentSweepListener() {
                @Override
                public void sweepUpdate(Map<String, UpdateOp> updates)
                        throws DocumentStoreException {
                    if (dryRun) {
                        log.info("Dry run of sweeper identified [{}] documents for " +
                                        "cluster node [{}]: {}", updates.size(), clusterId,
                                updates.values());
                        return;
                    }
                    // create an invalidate entry
                    JournalEntry inv = JOURNAL.newDocument(docStore);
                    inv.modified(updates.keySet());
                    Revision r = context.newRevision().asBranchRevision();
                    UpdateOp invOp = inv.asUpdateOp(r);
                    // and reference it from a regular entry
                    JournalEntry entry = JOURNAL.newDocument(docStore);
                    entry.invalidate(Collections.singleton(r));
                    Revision jRev = context.newRevision();
                    UpdateOp jOp = entry.asUpdateOp(jRev);
                    if (!docStore.create(JOURNAL, newArrayList(invOp, jOp))) {
                        String msg = "Unable to create journal entries for " +
                                "document invalidation.";
                        throw new DocumentStoreException(msg);
                    }
                    sweepRev.set(Utils.max(sweepRev.get(), jRev));
                    // now that journal entry is in place, perform the actual
                    // updates on the documents
                    docStore.createOrUpdate(NODES, newArrayList(updates.values()));
                    log.info("Sweeper updated {}", updates.keySet());
                }
            });
        }

        // now deal with missing _lastRev updates
        UnsavedModifications unsaved = new UnsavedModifications();
        UnsavedModifications unsavedParents = new UnsavedModifications();

        //Map of known last rev of checked paths
        Map<String, Revision> knownLastRevOrModification = MapFactory.getInstance().create();
        final JournalEntry changes = JOURNAL.newDocument(docStore);

        long count = 0;
        for (NodeDocument doc : suspects) {
            count++;
            if (count % 100000 == 0) {
                log.info("Scanned {} suspects so far...", count);
            }

            Revision currentLastRev = doc.getLastRev().get(clusterId);

            // 1. determine last committed modification on document
            Revision lastModifiedRev = determineLastModification(doc, clusterId);

            Revision lastRevForParents = Utils.max(lastModifiedRev, currentLastRev);
            // remember the higher of the two revisions. this is the
            // most recent revision currently obtained from either a
            // _lastRev entry or an explicit modification on the document
            if (lastRevForParents != null) {
                knownLastRevOrModification.put(doc.getPath(), lastRevForParents);
            }

            //If both currentLastRev and lostLastRev are null it means
            //that no change is done by suspect cluster on this document
            //so nothing needs to be updated. Probably it was only changed by
            //other cluster nodes. If this node is parent of any child node which
            //has been modified by cluster then that node roll up would
            //add this node path to unsaved

            //2. Update lastRev for parent paths aka rollup
            if (lastRevForParents != null) {
                String path = doc.getPath();
                changes.modified(path); // track all changes
                while (true) {
                    if (PathUtils.denotesRoot(path)) {
                        break;
                    }
                    path = PathUtils.getParentPath(path);
                    unsavedParents.put(path, lastRevForParents);
                }
            }
        }

        for (String parentPath : unsavedParents.getPaths()) {
            Revision calcLastRev = unsavedParents.get(parentPath);
            Revision knownLastRev = knownLastRevOrModification.get(parentPath);
            if (knownLastRev == null) {
                // we don't know when the document was last modified with
                // the given clusterId. need to read from store
                String id = Utils.getIdFromPath(parentPath);
                NodeDocument doc = docStore.find(NODES, id);
                if (doc != null) {
                    Revision lastRev = doc.getLastRev().get(clusterId);
                    Revision lastMod = determineLastModification(doc, clusterId);
                    knownLastRev = Utils.max(lastRev, lastMod);
                } else {
                    log.warn("Unable to find document: {}", id);
                    continue;
                }
            }

            //Copy the calcLastRev of parent only if they have changed
            //In many case it might happen that parent have consistent lastRev
            //This check ensures that unnecessary updates are not made
            if (knownLastRev == null
                    || calcLastRev.compareRevisionTime(knownLastRev) > 0) {
                unsaved.put(parentPath, calcLastRev);
            }
        }

        if (sweepRev.get() != null) {
            unsaved.put(ROOT_PATH, sweepRev.get());
        }

        // take the root's lastRev
        final Revision lastRootRev = unsaved.get(ROOT_PATH);

        //Note the size before persist as persist operation
        //would empty the internal state
        int size = unsaved.getPaths().size();
        String updates = unsaved.toString();

        if (dryRun) {
            log.info("Dry run of lastRev recovery identified [{}] documents for " +
                    "cluster node [{}]: {}", size, clusterId, updates);
        } else {
            //UnsavedModifications is designed to be used in concurrent
            //access mode. For recovery case there is no concurrent access
            //involve so just pass a new lock instance

            // the lock uses to do the persisting is a plain reentrant lock
            // thus it doesn't matter, where exactly the check is done
            // as to whether the recovered lastRev has already been
            // written to the journal.
            unsaved.persist(docStore, new Supplier<Revision>() {
                @Override
                public Revision get() {
                    return sweepRev.get();
                }
            }, new UnsavedModifications.Snapshot() {

                @Override
                public void acquiring(Revision mostRecent) {
                    if (lastRootRev == null) {
                        // this should never happen - when unsaved has no changes
                        // that is reflected in the 'map' to be empty - in that
                        // case 'persist()' quits early and never calls
                        // acquiring() here.
                        //
                        // but even if it would occur - if we have no lastRootRev
                        // then we cannot and probably don't have to persist anything
                        return;
                    }

                    final String id = JournalEntry.asId(lastRootRev); // lastRootRev never null at this point
                    final JournalEntry existingEntry = docStore.find(Collection.JOURNAL, id);
                    if (existingEntry != null) {
                        // then the journal entry was already written - as can happen if
                        // someone else (or the original instance itself) wrote the
                        // journal entry, then died.
                        // in this case, don't write it again.
                        // hence: nothing to be done here. return.
                        return;
                    }

                    // otherwise store a new journal entry now
                    docStore.create(JOURNAL, singletonList(changes.asUpdateOp(lastRootRev)));
                }
            }, new ReentrantLock());

            log.info("Updated lastRev of [{}] documents while performing lastRev recovery for " +
                    "cluster node [{}]: {}", size, clusterId, updates);
        }

        return size;
    }

    /**
     * Retrieves possible candidates which have been modified after the given
     * {@code startTime} and recovers the missing updates.
     *
     * @param nodeInfo the info of the cluster node to recover.
     * @param startTime the start time
     * @param waitUntil wait at most until this time for an ongoing recovery
     *                  done by another cluster node.
     * @param info a string with additional information how recovery is run.
     * @return the number of restored nodes or {@code -1} if recovery is still
     *      ongoing by another process even when {@code waitUntil} time was
     *      reached.
     */
    private int recoverCandidates(final ClusterNodeInfoDocument nodeInfo,
                                  final long startTime,
                                  final long waitUntil,
                                  final String info) {
        ClusterNodeInfoDocument infoDoc = nodeInfo;
        int clusterId = infoDoc.getClusterId();
        for (;;) {
            if (missingLastRevUtil.acquireRecoveryLock(
                    clusterId, nodeStore.getClusterId())) {
                break;
            }

            Clock clock = nodeStore.getClock();
            long remaining = waitUntil - clock.getTime();
            if (remaining < 0) {
                // no need to wait for lock release, waitUntil already reached
                return -1;
            }

            log.info("Last revision recovery already being performed by " +
                    "cluster node {}. Waiting at most until {} for recovery " +
                    "to finish ({} seconds remaining).",
                    infoDoc.getRecoveryBy(), Utils.timestampToString(waitUntil),
                    remaining / 1000);
            // check once every five seconds
            long time = Math.min(waitUntil, clock.getTime() + 5000);
            try {
                clock.waitUntil(time);
            } catch (InterruptedException e) {
                Thread.interrupted();
                String msg = "Interrupted while waiting for _lastRev recovery to finish.";
                throw new DocumentStoreException(msg, e);
            }
            infoDoc = missingLastRevUtil.getClusterNodeInfo(clusterId);
            if (!missingLastRevUtil.isRecoveryNeeded(infoDoc)) {
                // meanwhile another process finished recovery
                return 0;
            }
        }

        // if we get here, the recovery lock was acquired successfully
        boolean success = false;
        try {
            log.info("Recovering candidates modified after: [{}] for clusterId [{}] [{}]",
                    Utils.timestampToString(startTime), clusterId, info);

            Iterable<NodeDocument> suspects = missingLastRevUtil.getCandidates(startTime);
            try {
                log.info("Performing Last Revision Recovery for clusterNodeId {}", clusterId);
                int num = recover(suspects, clusterId);
                success = true;
                return num;
            } finally {
                Utils.closeIfCloseable(suspects);
            }
        } finally {
            missingLastRevUtil.releaseRecoveryLock(clusterId, success);

            nodeStore.signalClusterStateChange();
        }
    }

    /**
     * Determines the last committed modification to the given document by
     * a {@code clusterId}.
     *
     * @param doc a document.
     * @param clusterId clusterId for which the last committed modification is
     *                  looked up.
     * @return the commit revision of the last modification by {@code clusterId}
     *          to the given document.
     */
    @CheckForNull
    private Revision determineLastModification(NodeDocument doc, int clusterId) {
        ClusterPredicate cp = new ClusterPredicate(clusterId);

        Revision lastModified = null;
        for (String property : Sets.filter(doc.keySet(), PROPERTY_OR_DELETED)) {
            Map<Revision, String> valueMap = doc.getLocalMap(property);
            // collect committed changes of this cluster node
            for (Map.Entry<Revision, String> entry : filterKeys(valueMap, cp).entrySet()) {
                Revision rev = entry.getKey();
                String cv = nodeStore.getCommitValue(rev, doc);
                if (isCommitted(cv)) {
                    lastModified = Utils.max(lastModified, resolveCommitRevision(rev, cv));
                    break;
                }
            }
        }
        return lastModified;
    }

    /**
     * Determines if any of the cluster node failed to renew its lease and
     * did not properly shutdown. If any such cluster node is found then are
     * potential candidates for last rev recovery. This method also returns
     * true when there is a cluster node with an ongoing recovery.
     *
     * @return true if last rev recovery needs to be performed for any of the
     *          cluster nodes
     */
    public boolean isRecoveryNeeded(){
        return missingLastRevUtil.isRecoveryNeeded();
    }

    public void performRecoveryIfNeeded() {
        if (isRecoveryNeeded()) {
            Iterable<Integer> clusterIds = getRecoveryCandidateNodes();
            log.info("ClusterNodeId [{}] starting Last Revision Recovery for clusterNodeId(s) {}",
                    nodeStore.getClusterId(), clusterIds);
            for (int clusterId : clusterIds) {
                if (recover(clusterId) == -1) {
                    log.info("Last Revision Recovery for cluster node {} " +
                            "ongoing by other cluster node.", clusterId);
                }
            }
        }
    }

    /**
     * Gets the _lastRev recovery candidate cluster nodes. This also includes
     * cluster nodes that are currently being recovered. The method would not
     * return self as a candidate for recovery even if it has failed to update
     * lease in time
     *
     * @return the recovery candidate nodes.
     */
    public Iterable<Integer> getRecoveryCandidateNodes() {
        return Iterables.transform(filter(missingLastRevUtil.getAllClusters(),
                new Predicate<ClusterNodeInfoDocument>() {
            @Override
            public boolean apply(ClusterNodeInfoDocument input) {
                return nodeStore.getClusterId() != input.getClusterId() && missingLastRevUtil.isRecoveryNeeded(input);
            }
        }), new Function<ClusterNodeInfoDocument, Integer>() {
            @Override
            public Integer apply(ClusterNodeInfoDocument input) {
                return input.getClusterId();
            }
        });
    }

    private static class ClusterPredicate implements Predicate<Revision> {
        private final int clusterId;

        private ClusterPredicate(int clusterId) {
            this.clusterId = clusterId;
        }

        @Override
        public boolean apply(Revision input) {
            return clusterId == input.getClusterId();
        }
    }

    /**
     * A revision context that represents an inactive cluster node for which
     * recovery is performed.
     */
    private static class InactiveRevisionContext implements RevisionContext {

        private final NodeDocument root;
        private final RevisionContext context;
        private final int clusterId;

        InactiveRevisionContext(NodeDocument root,
                                RevisionContext context,
                                int clusterId) {
            this.root = root;
            this.context = context;
            this.clusterId = clusterId;
        }

        @Override
        public UnmergedBranches getBranches() {
            // an inactive cluster node does not have active unmerged branches
            return new UnmergedBranches();
        }

        @Override
        public UnsavedModifications getPendingModifications() {
            // an inactive cluster node does not have
            // pending in-memory _lastRev updates
            return new UnsavedModifications();
        }

        @Override
        public int getClusterId() {
            return clusterId;
        }

        @Nonnull
        @Override
        public RevisionVector getHeadRevision() {
            return new RevisionVector(root.getLastRev().values());
        }

        @Nonnull
        @Override
        public Revision newRevision() {
            return Revision.newRevision(clusterId);
        }

        @Nonnull
        @Override
        public Clock getClock() {
            return context.getClock();
        }

        @Override
        public String getCommitValue(@Nonnull Revision changeRevision,
                                     @Nonnull NodeDocument doc) {
            return context.getCommitValue(changeRevision, doc);
        }
    }
}

