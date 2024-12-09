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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Document.MOD_COUNT;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SPLIT_CANDIDATE_THRESHOLD;

/**
 * A higher level object representing a commit.
 */
public class Commit {

    private static final Logger LOG = LoggerFactory.getLogger(Commit.class);

    private static final String PROPERTY_NAME_CHILDORDER = ":childOrder";

    protected final DocumentNodeStore nodeStore;
    private final RevisionVector baseRevision;
    private final RevisionVector startRevisions;
    private final Revision revision;
    private final HashMap<Path, UpdateOp> operations = new LinkedHashMap<>();
    private final Set<Revision> collisions = new LinkedHashSet<Revision>();
    private Branch b;
    private Rollback rollback = Rollback.NONE;

    /**
     * List of all node paths which have been modified in this commit. In addition to the nodes
     * which are actually changed it also contains there parent node paths
     */
    private final HashSet<Path> modifiedNodes = new HashSet<>();

    private final HashSet<Path> addedNodes = new HashSet<>();
    private final HashSet<Path> removedNodes = new HashSet<>();

    /** Set of all nodes which have binary properties. **/
    private final HashSet<Path> nodesWithBinaries = new HashSet<>();
    private final HashMap<Path, Path> bundledNodes = new HashMap<>();

    /**
     * Create a new Commit.
     *  
     * @param nodeStore the node store.
     * @param revision the revision for this commit.
     * @param baseRevision the base revision for this commit or {@code null} if
     *                     there is none.
     * @param startRevisions the revisions for each cluster node corresponding
     *          to the start time of the cluster nodes.
     */
    Commit(@NotNull DocumentNodeStore nodeStore,
           @NotNull Revision revision,
           @Nullable RevisionVector baseRevision,
           @NotNull RevisionVector startRevisions) {
        this.nodeStore = requireNonNull(nodeStore);
        this.revision = requireNonNull(revision);
        this.baseRevision = baseRevision;
        this.startRevisions = startRevisions;
    }

    Commit(@NotNull DocumentNodeStore nodeStore,
           @NotNull Revision revision,
           @Nullable RevisionVector baseRevision,
           @NotNull RevisionVector startRevisions,
           @NotNull Map<Path, UpdateOp> operations,
           @NotNull Set<Path> addedNodes,
           @NotNull Set<Path> removedNodes,
           @NotNull Set<Path> nodesWithBinaries,
           @NotNull Map<Path, Path> bundledNodes) {
        this(nodeStore, revision, baseRevision, startRevisions);
        this.operations.putAll(operations);
        this.addedNodes.addAll(addedNodes);
        this.removedNodes.addAll(removedNodes);
        this.nodesWithBinaries.addAll(nodesWithBinaries);
        this.bundledNodes.putAll(bundledNodes);
    }

    UpdateOp getUpdateOperationForNode(Path path) {
        UpdateOp op = operations.get(path);
        if (op == null) {
            op = createUpdateOp(path, revision, isBranchCommit());
            operations.put(path, op);
        }
        return op;
    }

    static UpdateOp createUpdateOp(Path path,
                                   Revision revision,
                                   boolean isBranch) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, false);
        NodeDocument.setModified(op, revision);
        if (isBranch) {
            NodeDocument.setBranchCommit(op, revision);
        }
        return op;
    }

    /**
     * The revision for this new commit. That is, the changes within this commit
     * will be visible with this revision.
     *
     * @return the revision for this new commit.
     */
    @NotNull
    Revision getRevision() {
        return revision;
    }

    /**
     * Returns the base revision for this commit. That is, the revision passed
     * to {@link DocumentNodeStore#newCommit}. The base revision may be
     * <code>null</code>, e.g. for the initial commit of the root node, when
     * there is no base revision.
     *
     * @return the base revision of this commit or <code>null</code>.
     */
    @Nullable
    RevisionVector getBaseRevision() {
        return baseRevision;
    }

    /**
     * @return all modified paths, including ancestors without explicit
     *          modifications.
     */
    @NotNull
    Iterable<Path> getModifiedPaths() {
        return modifiedNodes;
    }

    boolean isEmpty() {
        return operations.isEmpty();
    }

    /**
     * Performs a rollback of this commit if necessary.
     *
     * @return {@code false} if a rollback was necessary and the rollback did
     *         not complete successfully, {@code true} otherwise.
     */
    boolean rollback() {
        boolean success = false;
        try {
            rollback.perform(this.nodeStore.getDocumentStore());
            success = true;
        } catch (Throwable t) {
            // catch any exception caused by the rollback and log it
            LOG.warn("Rollback failed", t);
        }
        return success;
    }

    /**
     * Applies this commit to the store.
     *
     * @throws ConflictException if the commit failed because of a conflict.
     * @throws DocumentStoreException if the commit cannot be applied.
     */
    void apply() throws ConflictException, DocumentStoreException {
        boolean success = false;
        RevisionVector baseRev = getBaseRevision();
        boolean isBranch = baseRev != null && baseRev.isBranch();
        Revision rev = getRevision();
        if (isBranch && !nodeStore.isDisableBranches()) {
            try {
                // prepare commit
                prepare(baseRev);
                success = true;
            } finally {
                if (!success) {
                    rollback();
                    Branch branch = getBranch();
                    if (branch != null) {
                        branch.removeCommit(rev.asBranchRevision());
                        if (!branch.hasCommits()) {
                            nodeStore.getBranches().remove(branch);
                        }
                    }
                }
            }
        } else {
            applyInternal();
        }
    }

    /**
     * Apply the changes to the document store and the cache.
     */
    private void applyInternal()
            throws ConflictException, DocumentStoreException {
        if (!operations.isEmpty()) {
            updateParentChildStatus();
            updateBinaryStatus();
            applyToDocumentStore();
        }
    }

    private void prepare(RevisionVector baseRevision)
            throws ConflictException, DocumentStoreException {
        if (!operations.isEmpty()) {
            updateParentChildStatus();
            updateBinaryStatus();
            applyToDocumentStoreWithTiming(baseRevision);
        }
    }

    /**
     * Update the binary status in the update op.
     */
    private void updateBinaryStatus() {
        for (Path path : this.nodesWithBinaries) {
            NodeDocument.setHasBinary(getUpdateOperationForNode(path));
        }
    }

    /**
     * Apply the changes to the document store.
     */
    void applyToDocumentStore() throws ConflictException, DocumentStoreException {
        applyToDocumentStoreWithTiming(null);
    }

    /**
     * Apply the changes to the document store.
     *
     * @param baseBranchRevision the base revision of this commit. Currently only
     *                     used for branch commits.
     * @throws ConflictException if a conflict is detected with another commit.
     * @throws DocumentStoreException if an error occurs while writing to the
     *          underlying store.
     */
    private void applyToDocumentStoreWithTiming(RevisionVector baseBranchRevision)
            throws ConflictException, DocumentStoreException {
        long start = System.nanoTime();
        try {
            applyToDocumentStore(baseBranchRevision);
        } finally {
            nodeStore.getStatsCollector().doneChangesApplied(
                    TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start));
        }
    }

    /**
     * Apply the changes to the document store.
     *
     * @param baseBranchRevision the base revision of this commit. Currently only
     *                     used for branch commits.
     * @throws ConflictException if a conflict is detected with another commit.
     * @throws DocumentStoreException if an error occurs while writing to the
     *          underlying store.
     */
    private void applyToDocumentStore(RevisionVector baseBranchRevision)
            throws ConflictException, DocumentStoreException {
        // initially set the rollback to always fail until we have changes
        // in an oplog list and a commit root.
        rollback = Rollback.FAILED;

        // the value in _revisions.<revision> property of the commit root node
        // regular commits use "c", which makes the commit visible to
        // other readers. branch commits use the base revision to indicate
        // the visibility of the commit
        String commitValue = baseBranchRevision != null ? baseBranchRevision.getBranchRevision().toString() : "c";
        DocumentStore store = nodeStore.getDocumentStore();
        Path commitRootPath = null;
        if (baseBranchRevision != null) {
            // branch commits always use root node as commit root
            commitRootPath = Path.ROOT;
        }
        ArrayList<UpdateOp> changedNodes = new ArrayList<UpdateOp>();
        // operations are added to this list before they are executed,
        // so that all operations can be rolled back if there is a conflict
        ArrayList<UpdateOp> opLog = new ArrayList<UpdateOp>();

        // Compute the commit root
        for (Path p : operations.keySet()) {
            markChanged(p);
            if (commitRootPath == null) {
                commitRootPath = p;
            } else {
                while (!commitRootPath.isAncestorOf(p)) {
                    Path parent = commitRootPath.getParent();
                    if (parent == null) {
                        break;
                    }
                    commitRootPath = parent;
                }
            }
        }

        // adjust commit root when it falls on a bundled node
        commitRootPath = bundledNodes.getOrDefault(commitRootPath, commitRootPath);

        rollback = new Rollback(revision, opLog,
                Utils.getIdFromPath(commitRootPath),
                nodeStore.getCreateOrUpdateBatchSize());

        for (Path p : bundledNodes.keySet()){
            markChanged(p);
        }

        // push branch changes to journal
        if (baseBranchRevision != null) {
            // store as external change
            JournalEntry doc = JOURNAL.newDocument(store);
            doc.modified(modifiedNodes);
            Revision r = revision.asBranchRevision();
            boolean success = store.create(JOURNAL, singletonList(doc.asUpdateOp(r)));
            if (!success) {
                LOG.error("Failed to update journal for revision {}", r);
                LOG.debug("Failed to update journal for revision {} with doc {}", r, doc.format());
            }
        }

        int commitRootDepth = commitRootPath.getDepth();
        // check if there are real changes on the commit root
        boolean commitRootHasChanges = operations.containsKey(commitRootPath);
        for (UpdateOp op : operations.values()) {
            NodeDocument.setCommitRoot(op, revision, commitRootDepth);

            // special case for :childOrder updates
            if (nodeStore.isChildOrderCleanupEnabled()) {
                final Branch localBranch = getBranch();
                if (localBranch != null) {
                    final NavigableSet<Revision> commits = new TreeSet<>(localBranch.getCommits());
                    boolean removePreviousSetOperations = false;
                    for (Map.Entry<Key, Operation> change : op.getChanges().entrySet()) {
                        if (PROPERTY_NAME_CHILDORDER.equals(change.getKey().getName()) && Operation.Type.SET_MAP_ENTRY == change.getValue().type) {
                            // we are setting child order, so we should remove previous set operations from the same branch
                            removePreviousSetOperations = true;
                            // branch.getCommits contains all revisions of the branch
                            // including the new one we're about to make
                            // so don't do a removeMapEntry for that
                            commits.remove(change.getKey().getRevision().asBranchRevision());
                        }
                    }
                    if (removePreviousSetOperations) {
                        if (!commits.isEmpty()) {
                            int countRemoves = 0;
                            for (Revision rev : commits.descendingSet()) {
                                op.removeMapEntry(PROPERTY_NAME_CHILDORDER, rev.asTrunkRevision());
                                if (++countRemoves >= 256) {
                                    LOG.debug("applyToDocumentStore : only cleaning up last {} branch commits.",
                                            countRemoves);
                                    break;
                                }
                            }
                            LOG.debug("applyToDocumentStore : childOrder-edited op is: {}", op);
                        }
                    }
                }
            }
            changedNodes.add(op);
        }
        // create a "root of the commit" if there is none
        UpdateOp commitRoot = getUpdateOperationForNode(commitRootPath);

        boolean success = false;
        try {
            opLog.addAll(changedNodes);

            if (conditionalCommit(changedNodes, commitValue)) {
                success = true;
            } else {
                int batchSize = nodeStore.getCreateOrUpdateBatchSize();
                for (List<UpdateOp> updates : CollectionUtils.partitionList(changedNodes, batchSize)) {
                    List<NodeDocument> oldDocs = store.createOrUpdate(NODES, updates);
                    checkConflicts(oldDocs, updates);
                    checkSplitCandidate(oldDocs);
                }

                // finally write the commit root (the commit root might be written
                // twice, first to check if there was a conflict, and only then to
                // commit the revision, with the revision property set)
                NodeDocument.setRevision(commitRoot, revision, commitValue);
                if (commitRootHasChanges) {
                    // remove previously added commit root
                    NodeDocument.removeCommitRoot(commitRoot, revision);
                }
                opLog.add(commitRoot);
                if (baseBranchRevision == null) {
                    // create a clone of the commitRoot in order
                    // to set isNew to false. If we get here the
                    // commitRoot document already exists and
                    // only needs an update
                    UpdateOp commit = commitRoot.copy();
                    commit.setNew(false);
                    // only set revision on commit root when there is
                    // no collision for this commit revision
                    commit.containsMapEntry(COLLISIONS, revision, false);
                    NodeDocument before = nodeStore.updateCommitRoot(commit, revision);
                    if (before == null) {
                        String msg = "Conflicting concurrent change. " +
                                "Update operation failed: " + commit;
                        NodeDocument commitRootDoc = store.find(NODES, commit.getId());
                        if (commitRootDoc == null) {
                            throw new DocumentStoreException(msg);
                        } else {
                            throw new ConflictException(msg,
                                    commitRootDoc.getConflictsFor(
                                            Collections.singleton(revision)));
                        }
                    } else {
                        success = true;
                        // if we get here the commit was successful and
                        // the commit revision is set on the commitRoot
                        // document for this commit.
                        // now check for conflicts/collisions by other commits.
                        // use original commitRoot operation with
                        // correct isNew flag.
                        checkConflicts(commitRoot, before);
                        checkSplitCandidate(before);
                    }
                } else {
                    // this is a branch commit, do not fail on collisions now
                    // trying to merge the branch will fail later
                    createOrUpdateNode(store, commitRoot);
                }
            }
        } catch (Exception e) {
            // OAK-3084 do not roll back if already committed
            if (success) {
                LOG.error("Exception occurred after commit. Rollback will be suppressed.", e);
            } else {
                if (e instanceof ConflictException) {
                    throw e;
                } else {
                    throw DocumentStoreException.convert(e);
                }
            }
        } finally {
            if (success) {
                rollback = Rollback.NONE;
            }
        }
    }

    private boolean conditionalCommit(List<UpdateOp> changedNodes,
                                      String commitValue)
            throws DocumentStoreException {
        // conditional commit is only possible when not on a branch
        // and commit root is on the same document as the changes
        if (!Utils.isCommitted(commitValue) || changedNodes.size() != 1) {
            return false;
        }
        UpdateOp op = changedNodes.get(0);
        DocumentStore store = nodeStore.getDocumentStore();
        NodeDocument doc = store.getIfCached(NODES, op.getId());
        if (doc == null || doc.getModCount() == null) {
            // document not in cache or store does not maintain modCount
            return false;
        }
        try {
            checkConflicts(op, doc);
        } catch (ConflictException e) {
            // remove collision marker again
            removeCollisionMarker(op.getId());
            return false;
        }
        // if we get here, update based on current doc does not conflict
        // create a new commit update operation, setting the revisions
        // commit entry together with the other changes
        UpdateOp commit = op.copy();
        NodeDocument.unsetCommitRoot(commit, revision);
        NodeDocument.setRevision(commit, revision, commitValue);
        // make the update conditional on the modCount
        commit.equals(MOD_COUNT, doc.getModCount());
        NodeDocument before = nodeStore.updateCommitRoot(commit, revision);
        if (before != null) {
            checkSplitCandidate(before);
        }
        return before != null;
    }

    private void removeCollisionMarker(String id) {
        UpdateOp removeCollision = new UpdateOp(id, false);
        NodeDocument.removeCollision(removeCollision, revision);
        nodeStore.getDocumentStore().findAndUpdate(NODES, removeCollision);
    }

    private void updateParentChildStatus() {
        final Set<Path> processedParents = new HashSet<>();
        for (Path path : addedNodes) {
            Path parentPath = path.getParent();
            if (parentPath == null) {
                continue;
            }

            if (processedParents.contains(parentPath)) {
                continue;
            }

            //Ignore setting children path for bundled nodes
            if (isBundled(parentPath)){
                continue;
            }

            processedParents.add(parentPath);
            UpdateOp op = getUpdateOperationForNode(parentPath);
            NodeDocument.setChildrenFlag(op, true);
        }
    }

    /**
     * Try to create or update the node. If there was a conflict, this method
     * throws a {@link ConflictException}, even though the change is still applied.
     *
     * @param store the store
     * @param op the operation
     * @throws ConflictException if there was a conflict introduced by the
     *          given update operation.
     */
    private void createOrUpdateNode(DocumentStore store, UpdateOp op)
            throws ConflictException, DocumentStoreException {
        NodeDocument doc = store.createOrUpdate(NODES, op);
        checkConflicts(op, doc);
        checkSplitCandidate(doc);
    }

    private void checkSplitCandidate(Iterable<NodeDocument> docs) {
        for (NodeDocument doc : docs) {
            checkSplitCandidate(doc);
        }
    }

    private void checkSplitCandidate(@Nullable NodeDocument doc) {
        if (doc == null) {
            return;
        }
        if (doc.getMemory() > SPLIT_CANDIDATE_THRESHOLD || doc.hasBinary()) {
            nodeStore.addSplitCandidate(doc.getId());
        }
    }

    /**
     * Checks if the update operation introduced any conflicts on the given
     * document. The document shows the state right before the operation was
     * applied.
     *
     * @param op the update operation.
     * @param before how the document looked before the update was applied or
     *               {@code null} if it didn't exist before.
     * @throws ConflictException if there was a conflict introduced by the
     *          given update operation.
     */
    private void checkConflicts(@NotNull UpdateOp op,
                                @Nullable NodeDocument before)
            throws ConflictException {
        DocumentStore store = nodeStore.getDocumentStore();
        collisions.clear();
        if (baseRevision != null) {
            Revision newestRev = null;
            Branch branch = null;
            if (before != null) {
                RevisionVector base = baseRevision;
                if (nodeStore.isDisableBranches()) {
                    base = base.asTrunkRevision();
                }
                branch = getBranch();
                newestRev = before.getNewestRevision(
                        nodeStore, base, revision, branch, collisions);
            }
            String conflictMessage = null;
            Set<Revision> conflictRevisions = new HashSet<>();
            if (newestRev == null) {
                if ((op.isDelete() || !op.isNew())
                        && !allowConcurrentAddRemove(before, op)) {
                    conflictMessage = "The node " +
                            op.getId() + " does not exist or is already deleted " +
                            "at base revision " + baseRevision + ", branch: " + branch;
                    if (before != null && !before.getLocalDeleted().isEmpty()) {
                        conflictRevisions.add(before.getLocalDeleted().firstKey());
                    }
                }
            } else {
                conflictRevisions.add(newestRev);
                if (op.isNew() && !allowConcurrentAddRemove(before, op)) {
                    conflictMessage = "The node " +
                            op.getId() + " already existed in revision\n" +
                            formatConflictRevision(newestRev);
                } else if (baseRevision.isRevisionNewer(newestRev)
                        && (op.isDelete() || isConflicting(before, op))) {
                    conflictMessage = "The node " +
                            op.getId() + " was changed in revision\n" +
                            formatConflictRevision(newestRev) +
                            ", which was applied after the base revision\n" +
                            baseRevision;
                }
            }
            if (conflictMessage == null && before != null) {
                // the modification was successful
                // -> check for collisions and conflict (concurrent updates
                // on a node are possible if property updates do not overlap)
                // TODO: unify above conflict detection and isConflicting()
                boolean allowConflictingDeleteChange = allowConcurrentAddRemove(before, op);
                for (Revision r : collisions) {
                    Collision c = new Collision(before, r, op, revision, nodeStore, startRevisions);
                    if (c.isConflicting() && !allowConflictingDeleteChange) {
                        // mark collisions on commit root
                        if (c.mark(store).equals(revision)) {
                            // our revision was marked
                            if (baseRevision.isBranch()) {
                                // this is a branch commit. do not fail immediately
                                // merging this branch will fail later.
                            } else {
                                // fail immediately
                                conflictMessage = "The node " +
                                        op.getId() + " was changed in revision\n" +
                                        formatConflictRevision(r) +
                                        ", which was applied after the base revision\n" +
                                        baseRevision;
                                conflictRevisions.add(r);
                            }
                        }
                    }
                }
            }
            if (conflictMessage != null) {
                conflictMessage += ", commit revision: " + revision;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(conflictMessage  + "; document:\n" +
                            (before == null ? "" : before.format()));
                }
                throw new ConflictException(conflictMessage, conflictRevisions);
            }
        }
    }

    private void checkConflicts(List<NodeDocument> oldDocs,
                                List<UpdateOp> updates)
            throws ConflictException {
        int i = 0;
        List<ConflictException> exceptions = new ArrayList<ConflictException>();
        Set<Revision> revisions = new HashSet<Revision>();
        for (NodeDocument doc : oldDocs) {
            UpdateOp op = updates.get(i++);
            try {
                checkConflicts(op, doc);
            } catch (ConflictException e) {
                exceptions.add(e);
                Iterables.addAll(revisions, e.getConflictRevisions());
            }
        }
        if (!exceptions.isEmpty()) {
            throw new ConflictException("Following exceptions occurred during the bulk update operations: " + exceptions, revisions);
        }
    }

    private String formatConflictRevision(Revision r) {
        if (nodeStore.getHeadRevision().isRevisionNewer(r)) {
            return r + " (not yet visible)";
        } else if (baseRevision != null
                && !baseRevision.isRevisionNewer(r)
                && !Objects.equals(baseRevision.getRevision(r.getClusterId()), r)) {
            return r + " (older than base " + baseRevision + ")";
        } else {
            return r.toString();
        }
    }

    /**
     * Checks whether the given <code>UpdateOp</code> conflicts with the
     * existing content in <code>doc</code>. The check is done based on the
     * {@link #baseRevision} of this commit. An <code>UpdateOp</code> conflicts
     * when there were changes after {@link #baseRevision} on properties also
     * contained in <code>UpdateOp</code>.
     *
     * @param doc the contents of the nodes before the update.
     * @param op the update to perform.
     * @return <code>true</code> if the update conflicts; <code>false</code>
     *         otherwise.
     */
    private boolean isConflicting(@Nullable NodeDocument doc,
                                  @NotNull UpdateOp op) {
        if (baseRevision == null || doc == null) {
            // no conflict is possible when there is no baseRevision
            // or document did not exist before
            return false;
        }
        return doc.isConflicting(op, baseRevision, revision,
                nodeStore.getEnableConcurrentAddRemove());
    }

    /**
     * Checks whether a concurrent add/remove operation is allowed with the
     * given before document and update operation. This method will first check
     * if the concurrent add/remove feature is enable and return {@code false}
     * immediately if it is disabled. Only when enabled will this method check
     * if there is a conflict based on the given document and update operation.
     * See also {@link #isConflicting(NodeDocument, UpdateOp)}.
     *
     * @param before the contents of the document before the update.
     * @param op the update to perform.
     * @return {@code true} is a concurrent add/remove update is allowed;
     *      {@code false} otherwise.
     */
    private boolean allowConcurrentAddRemove(@Nullable NodeDocument before,
                                             @NotNull UpdateOp op) {
        return nodeStore.getEnableConcurrentAddRemove()
                && !isConflicting(before, op);
    }

    /**
     * @return the branch if this is a branch commit, otherwise {@code null}.
     */
    @Nullable
    private Branch getBranch() {
        if (baseRevision == null || !baseRevision.isBranch()) {
            return null;
        }
        if (b == null) {
            b = nodeStore.getBranches().getBranch(
                    new RevisionVector(revision.asBranchRevision()));
        }
        return b;
    }

    /**
     * @return {@code true} if this is a branch commit.
     */
    private boolean isBranchCommit() {
        return baseRevision != null && baseRevision.isBranch();
    }

    /**
     * Applies the lastRev updates to the {@link LastRevTracker} of the
     * DocumentNodeStore.
     *
     * @param isBranchCommit whether this is a branch commit.
     */
    void applyLastRevUpdates(boolean isBranchCommit) {
        LastRevTracker tracker = nodeStore.createTracker(revision, isBranchCommit);
        for (Path path : modifiedNodes) {
            UpdateOp op = operations.get(path);
            // track _lastRev only when path is not for a bundled node state
            if ((op == null || !hasContentChanges(op) || path.isRoot())
                    && !isBundled(path)) {
                // track intermediate node and root
                tracker.track(path);
            }
        }
    }

    /**
     * Apply the changes to the DocumentNodeStore (to update the cache).
     *
     * @param before the revision right before this commit.
     * @param isBranchCommit whether this is a commit to a branch
     */
    public void applyToCache(RevisionVector before, boolean isBranchCommit) {
        HashMap<Path, ArrayList<Path>> nodesWithChangedChildren = new HashMap<>();
        for (Path p : modifiedNodes) {
            if (p.isRoot()) {
                continue;
            }
            Path parent = p.getParent();
            ArrayList<Path> list = nodesWithChangedChildren
                    .computeIfAbsent(parent, k -> new ArrayList<>());
            list.add(p);
        }
        // the commit revision with branch flag if this is a branch commit
        Revision rev = isBranchCommit ? revision.asBranchRevision() : revision;
        RevisionVector after = before.update(rev);
        DiffCache.Entry cacheEntry = nodeStore.getDiffCache().newEntry(before, after, true);
        List<Path> added = new ArrayList<>();
        List<Path> removed = new ArrayList<>();
        List<Path> changed = new ArrayList<>();
        for (Path path : modifiedNodes) {
            added.clear();
            removed.clear();
            changed.clear();
            ArrayList<Path> changes = nodesWithChangedChildren.get(path);
            if (changes != null) {
                for (Path s : changes) {
                    if (addedNodes.contains(s)) {
                        added.add(s);
                    } else if (removedNodes.contains(s)) {
                        removed.add(s);
                    } else {
                        changed.add(s);
                    }
                }
            }
            UpdateOp op = operations.get(path);

            // apply to cache only when path is not for a bundled node state
            if (!isBundled(path)) {
                boolean isNew = op != null && op.isNew();
                nodeStore.applyChanges(before, after, rev, path, isNew,
                        added, removed, changed);
            }
            addChangesToDiffCacheEntry(path, added, removed, changed, cacheEntry);
        }
        cacheEntry.done();
    }

    void markChanged(Path path) {
        while (true) {
            if (!modifiedNodes.add(path)) {
                break;
            }
            path = path.getParent();
            if (path == null) {
                break;
            }
        }
    }


    @NotNull
    RevisionVector getStartRevisions() {
        return startRevisions;
    }

    /**
     * Apply the changes of a node to the cache.
     *
     * @param path the path
     * @param added the list of added child nodes
     * @param removed the list of removed child nodes
     * @param changed the list of changed child nodes
     * @param cacheEntry the cache entry changes are added to
     */
    private void addChangesToDiffCacheEntry(Path path,
                                            List<Path> added,
                                            List<Path> removed,
                                            List<Path> changed,
                                            DiffCache.Entry cacheEntry) {
        // update diff cache
        JsopWriter w = new JsopStream();
        for (Path p : added) {
            w.tag('+').key(p.getName()).object().endObject();
        }
        for (Path p : removed) {
            w.tag('-').value(p.getName());
        }
        for (Path p : changed) {
            w.tag('^').key(p.getName()).object().endObject();
        }
        cacheEntry.append(path, w.toString());
    }

    private boolean isBundled(Path path) {
        return bundledNodes.containsKey(path);
    }

    private static boolean hasContentChanges(UpdateOp op) {
        return filter(transform(op.getChanges().keySet(),
                input -> input.getName()), Utils.PROPERTY_OR_DELETED::test).iterator().hasNext();
    }
}
