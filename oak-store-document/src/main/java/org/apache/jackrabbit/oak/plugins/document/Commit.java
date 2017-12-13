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
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
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

    protected final DocumentNodeStore nodeStore;
    private final RevisionVector baseRevision;
    private final Revision revision;
    private final HashMap<String, UpdateOp> operations = new LinkedHashMap<String, UpdateOp>();
    private final Set<Revision> collisions = new LinkedHashSet<Revision>();
    private Branch b;
    private boolean rollbackFailed;

    /**
     * List of all node paths which have been modified in this commit. In addition to the nodes
     * which are actually changed it also contains there parent node paths
     */
    private HashSet<String> modifiedNodes = new HashSet<String>();

    private HashSet<String> addedNodes = new HashSet<String>();
    private HashSet<String> removedNodes = new HashSet<String>();

    /** Set of all nodes which have binary properties. **/
    private HashSet<String> nodesWithBinaries = Sets.newHashSet();
    private HashMap<String, String> bundledNodes = Maps.newHashMap();

    /**
     * Create a new Commit.
     *  
     * @param nodeStore the node store.
     * @param revision the revision for this commit.
     * @param baseRevision the base revision for this commit or {@code null} if
     *                     there is none.
     */
    Commit(@Nonnull DocumentNodeStore nodeStore,
           @Nonnull Revision revision,
           @Nullable RevisionVector baseRevision) {
        this.nodeStore = checkNotNull(nodeStore);
        this.revision = checkNotNull(revision);
        this.baseRevision = baseRevision;
    }

    UpdateOp getUpdateOperationForNode(String path) {
        UpdateOp op = operations.get(path);
        if (op == null) {
            op = createUpdateOp(path, revision, getBranch() != null);
            operations.put(path, op);
        }
        return op;
    }

    static UpdateOp createUpdateOp(String path,
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
    @Nonnull
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
    @CheckForNull
    RevisionVector getBaseRevision() {
        return baseRevision;
    }

    /**
     * @return all modified paths, including ancestors without explicit
     *          modifications.
     */
    @Nonnull
    Iterable<String> getModifiedPaths() {
        return modifiedNodes;
    }

    void updateProperty(String path, String propertyName, String value) {
        UpdateOp op = getUpdateOperationForNode(path);
        String key = Utils.escapePropertyName(propertyName);
        op.setMapEntry(key, revision, value);
    }

    void addBundledNode(String path, String bundlingRootPath) {
        bundledNodes.put(path, bundlingRootPath);
    }

    void markNodeHavingBinary(String path) {
        this.nodesWithBinaries.add(path);
    }

    void addNode(DocumentNodeState n) {
        String path = n.getPath();
        if (operations.containsKey(path)) {
            String msg = "Node already added: " + path;
            LOG.error(msg);
            throw new DocumentStoreException(msg);
        }
        UpdateOp op = n.asOperation(revision);
        if (getBranch() != null) {
            NodeDocument.setBranchCommit(op, revision);
        }
        operations.put(path, op);
        addedNodes.add(path);
    }

    boolean isEmpty() {
        return operations.isEmpty();
    }

    /**
     * @return {@code true} if this commit did not succeed and the rollback
     *      was unable to revert all changes; otherwise {@code false}.
     */
    boolean rollbackFailed() {
        return rollbackFailed;
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
    private void applyInternal() {
        if (!operations.isEmpty()) {
            updateParentChildStatus();
            updateBinaryStatus();
            applyToDocumentStore();
        }
    }

    private void prepare(RevisionVector baseRevision) {
        if (!operations.isEmpty()) {
            updateParentChildStatus();
            updateBinaryStatus();
            applyToDocumentStore(baseRevision);
        }
    }

    /**
     * Update the binary status in the update op.
     */
    private void updateBinaryStatus() {
        DocumentStore store = this.nodeStore.getDocumentStore();

        for (String path : this.nodesWithBinaries) {
            NodeDocument nd = store.getIfCached(Collection.NODES, Utils.getIdFromPath(path));
            if ((nd == null) || !nd.hasBinary()) {
                UpdateOp updateParentOp = getUpdateOperationForNode(path);
                NodeDocument.setHasBinary(updateParentOp);
            }
        }
    }

    /**
     * Apply the changes to the document store.
     */
    void applyToDocumentStore() {
        applyToDocumentStore(null);
    }

    /**
     * Apply the changes to the document store.
     *
     * @param baseBranchRevision the base revision of this commit. Currently only
     *                     used for branch commits.
     * @throws DocumentStoreException if an error occurs while writing to the
     *          underlying store.
     */
    private void applyToDocumentStore(RevisionVector baseBranchRevision)
            throws DocumentStoreException {
        // initially set the rollbackFailed flag to true
        // the flag will be set to false at the end of the method
        // when the commit succeeds
        rollbackFailed = true;

        // the value in _revisions.<revision> property of the commit root node
        // regular commits use "c", which makes the commit visible to
        // other readers. branch commits use the base revision to indicate
        // the visibility of the commit
        String commitValue = baseBranchRevision != null ? baseBranchRevision.getBranchRevision().toString() : "c";
        DocumentStore store = nodeStore.getDocumentStore();
        String commitRootPath = null;
        if (baseBranchRevision != null) {
            // branch commits always use root node as commit root
            commitRootPath = "/";
        }
        ArrayList<UpdateOp> changedNodes = new ArrayList<UpdateOp>();
        // operations are added to this list before they are executed,
        // so that all operations can be rolled back if there is a conflict
        ArrayList<UpdateOp> opLog = new ArrayList<UpdateOp>();

        // Compute the commit root
        for (String p : operations.keySet()) {
            markChanged(p);
            if (commitRootPath == null) {
                commitRootPath = p;
            } else {
                while (!PathUtils.isAncestor(commitRootPath, p)) {
                    commitRootPath = PathUtils.getParentPath(commitRootPath);
                    if (denotesRoot(commitRootPath)) {
                        break;
                    }
                }
            }
        }

        for (String p : bundledNodes.keySet()){
            markChanged(p);
        }

        // push branch changes to journal
        if (baseBranchRevision != null) {
            // store as external change
            JournalEntry doc = JOURNAL.newDocument(store);
            doc.modified(modifiedNodes);
            Revision r = revision.asBranchRevision();
            store.create(JOURNAL, singletonList(doc.asUpdateOp(r)));
        }

        int commitRootDepth = PathUtils.getDepth(commitRootPath);
        // check if there are real changes on the commit root
        boolean commitRootHasChanges = operations.containsKey(commitRootPath);
        for (UpdateOp op : operations.values()) {
            NodeDocument.setCommitRoot(op, revision, commitRootDepth);
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
                List<NodeDocument> oldDocs = store.createOrUpdate(NODES, changedNodes);
                checkConflicts(oldDocs, changedNodes);
                checkSplitCandidate(oldDocs);

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
                                "Update operation failed: " + commitRoot;
                        NodeDocument commitRootDoc = store.find(NODES, commitRoot.getId());
                        DocumentStoreException dse;
                        if (commitRootDoc == null) {
                            dse = new DocumentStoreException(msg);
                        } else {
                            dse = new ConflictException(msg,
                                    commitRootDoc.getConflictsFor(
                                            Collections.singleton(revision)));
                        }
                        throw dse;
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
                try {
                    rollback(opLog, commitRoot);
                    rollbackFailed = false;
                } catch (Throwable ex) {
                    // catch any exception caused by the rollback, log it
                    // and throw the original exception
                    LOG.warn("Rollback failed", ex);
                }
                throw DocumentStoreException.convert(e);
            }
        } finally {
            if (success) {
                rollbackFailed = false;
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
        final Set<String> processedParents = Sets.newHashSet();
        for (String path : addedNodes) {
            if (denotesRoot(path)) {
                continue;
            }

            String parentPath = PathUtils.getParentPath(path);

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

    private void rollback(List<UpdateOp> changed,
                          UpdateOp commitRoot) {
        DocumentStore store = nodeStore.getDocumentStore();
        for (UpdateOp op : changed) {
            UpdateOp reverse = op.getReverseOperation();
            if (op.isNew()) {
                NodeDocument.setDeletedOnce(reverse);
            }
            store.findAndUpdate(NODES, reverse);
        }
        removeCollisionMarker(commitRoot.getId());
    }

    /**
     * Try to create or update the node. If there was a conflict, this method
     * throws an exception, even though the change is still applied.
     *
     * @param store the store
     * @param op the operation
     */
    private void createOrUpdateNode(DocumentStore store, UpdateOp op) {
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
    private void checkConflicts(@Nonnull UpdateOp op,
                                @Nullable NodeDocument before)
            throws ConflictException {
        DocumentStore store = nodeStore.getDocumentStore();
        collisions.clear();
        if (baseRevision != null) {
            Revision newestRev = null;
            if (before != null) {
                RevisionVector base = baseRevision;
                if (nodeStore.isDisableBranches()) {
                    base = base.asTrunkRevision();
                }
                newestRev = before.getNewestRevision(
                        nodeStore, base, revision, getBranch(), collisions);
            }
            String conflictMessage = null;
            Set<Revision> conflictRevisions = Sets.newHashSet();
            if (newestRev == null) {
                if ((op.isDelete() || !op.isNew())
                        && !allowConcurrentAddRemove(before, op)) {
                    conflictMessage = "The node " +
                            op.getId() + " does not exist or is already deleted";
                    if (before != null && !before.getLocalDeleted().isEmpty()) {
                        conflictRevisions.add(before.getLocalDeleted().firstKey());
                    }
                }
            } else {
                conflictRevisions.add(newestRev);
                if (op.isNew() && !allowConcurrentAddRemove(before, op)) {
                    conflictMessage = "The node " +
                            op.getId() + " was already added in revision\n" +
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
                    Collision c = new Collision(before, r, op, revision, nodeStore);
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
                conflictMessage += ", before\n" + revision;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(conflictMessage  + "; document:\n" +
                            (before == null ? "" : before.format()));
                }
                throw new ConflictException(conflictMessage, conflictRevisions);
            }
        }
    }

    private void checkConflicts(List<NodeDocument> oldDocs,
                                List<UpdateOp> updates) {
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
                && !equal(baseRevision.getRevision(r.getClusterId()), r)) {
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
                                  @Nonnull UpdateOp op) {
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
                                             @Nonnull UpdateOp op) {
        return nodeStore.getEnableConcurrentAddRemove()
                && !isConflicting(before, op);
    }

    /**
     * @return the branch if this is a branch commit, otherwise {@code null}.
     */
    @CheckForNull
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
     * Apply the changes to the DocumentNodeStore (to update the cache).
     *
     * @param before the revision right before this commit.
     * @param isBranchCommit whether this is a commit to a branch
     */
    public void applyToCache(RevisionVector before, boolean isBranchCommit) {
        HashMap<String, ArrayList<String>> nodesWithChangedChildren = new HashMap<String, ArrayList<String>>();
        for (String p : modifiedNodes) {
            if (denotesRoot(p)) {
                continue;
            }
            String parent = PathUtils.getParentPath(p);
            ArrayList<String> list = nodesWithChangedChildren.get(parent);
            if (list == null) {
                list = new ArrayList<String>();
                nodesWithChangedChildren.put(parent, list);
            }
            list.add(p);
        }
        // the commit revision with branch flag if this is a branch commit
        Revision rev = isBranchCommit ? revision.asBranchRevision() : revision;
        RevisionVector after = before.update(rev);
        DiffCache.Entry cacheEntry = nodeStore.getDiffCache().newEntry(before, after, true);
        LastRevTracker tracker = nodeStore.createTracker(revision, isBranchCommit);
        List<String> added = new ArrayList<String>();
        List<String> removed = new ArrayList<String>();
        List<String> changed = new ArrayList<String>();
        for (String path : modifiedNodes) {
            added.clear();
            removed.clear();
            changed.clear();
            ArrayList<String> changes = nodesWithChangedChildren.get(path);
            if (changes != null) {
                for (String s : changes) {
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

            // track _lastRev and apply to cache only when
            // path is not for a bundled node state
            if (!isBundled(path)) {
                boolean isNew = op != null && op.isNew();
                if (op == null || !hasContentChanges(op) || denotesRoot(path)) {
                    // track intermediate node and root
                    tracker.track(path);
                }
                nodeStore.applyChanges(before, after, rev, path, isNew,
                        added, removed, changed);
            }
            addChangesToDiffCacheEntry(path, added, removed, changed, cacheEntry);
        }
        cacheEntry.done();
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
    private void addChangesToDiffCacheEntry(String path,
                                            List<String> added,
                                            List<String> removed,
                                            List<String> changed,
                                            DiffCache.Entry cacheEntry) {
        // update diff cache
        JsopWriter w = new JsopStream();
        for (String p : added) {
            w.tag('+').key(PathUtils.getName(p)).object().endObject();
        }
        for (String p : removed) {
            w.tag('-').value(PathUtils.getName(p));
        }
        for (String p : changed) {
            w.tag('^').key(PathUtils.getName(p)).object().endObject();
        }
        cacheEntry.append(path, w.toString());
    }

    private void markChanged(String path) {
        if (!denotesRoot(path) && !PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("path: " + path);
        }
        while (true) {
            if (!modifiedNodes.add(path)) {
                break;
            }
            if (denotesRoot(path)) {
                break;
            }
            path = PathUtils.getParentPath(path);
        }
    }

    public void removeNode(String path, NodeState state) {
        removedNodes.add(path);
        UpdateOp op = getUpdateOperationForNode(path);
        op.setDelete(true);
        NodeDocument.setDeleted(op, revision, true);
        for (PropertyState p : state.getProperties()) {
            updateProperty(path, p.getName(), null);
        }
    }

    private boolean isBundled(String path) {
        return bundledNodes.containsKey(path);
    }

    private static final Function<UpdateOp.Key, String> KEY_TO_NAME =
            new Function<UpdateOp.Key, String>() {
        @Override
        public String apply(UpdateOp.Key input) {
            return input.getName();
        }
    };

    private static boolean hasContentChanges(UpdateOp op) {
        return filter(transform(op.getChanges().keySet(),
                KEY_TO_NAME), Utils.PROPERTY_OR_DELETED).iterator().hasNext();
    }
}
