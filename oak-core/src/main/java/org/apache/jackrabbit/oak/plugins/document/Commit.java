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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SPLIT_CANDIDATE_THRESHOLD;

/**
 * A higher level object representing a commit.
 */
public class Commit {

    private static final Logger LOG = LoggerFactory.getLogger(Commit.class);

    private final DocumentNodeStore nodeStore;
    private final Revision baseRevision;
    private final Revision revision;
    private HashMap<String, UpdateOp> operations = new LinkedHashMap<String, UpdateOp>();
    private JsopWriter diff = new JsopStream();
    private Set<Revision> collisions = new LinkedHashSet<Revision>();

    /**
     * List of all node paths which have been modified in this commit. In addition to the nodes
     * which are actually changed it also contains there parent node paths
     */
    private HashSet<String> modifiedNodes = new HashSet<String>();

    private HashSet<String> addedNodes = new HashSet<String>();
    private HashSet<String> removedNodes = new HashSet<String>();

    /** Set of all nodes which have binary properties. **/
    private HashSet<String> nodesWithBinaries = Sets.newHashSet();

    Commit(DocumentNodeStore nodeStore, Revision baseRevision, Revision revision) {
        this.baseRevision = baseRevision;
        this.revision = revision;
        this.nodeStore = nodeStore;
    }

    UpdateOp getUpdateOperationForNode(String path) {
        UpdateOp op = operations.get(path);
        if (op == null) {
            String id = Utils.getIdFromPath(path);
            op = new UpdateOp(id, false);
            NodeDocument.setModified(op, revision);
            operations.put(path, op);
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
     * to {@link DocumentMK#commit(String, String, String, String)}. The base
     * revision may be <code>null</code>, e.g. for the initial commit of the
     * root node, when there is no base revision.
     *
     * @return the base revision of this commit or <code>null</code>.
     */
    @CheckForNull
    Revision getBaseRevision() {
        return baseRevision;
    }

    void addNodeDiff(DocumentNodeState n) {
        diff.tag('+').key(n.getPath());
        diff.object();
        n.append(diff, false);
        diff.endObject();
        diff.newline();
    }

    void updateProperty(String path, String propertyName, String value) {
        UpdateOp op = getUpdateOperationForNode(path);
        String key = Utils.escapePropertyName(propertyName);
        op.setMapEntry(key, revision, value);
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
        operations.put(path, n.asOperation(true));
        addedNodes.add(path);
    }

    boolean isEmpty() {
        return operations.isEmpty();
    }

    /**
     * Applies this commit to the store.
     *
     * @return the commit revision.
     * @throws DocumentStoreException if the commit cannot be applied.
     */
    @Nonnull
    Revision apply() throws DocumentStoreException {
        boolean success = false;
        Revision baseRev = getBaseRevision();
        boolean isBranch = baseRev != null && baseRev.isBranch();
        Revision rev = getRevision();
        if (isBranch && !nodeStore.isDisableBranches()) {
            rev = rev.asBranchRevision();
            // remember branch commit
            Branch b = nodeStore.getBranches().getBranch(baseRev);
            if (b == null) {
                // baseRev is marker for new branch
                b = nodeStore.getBranches().create(baseRev.asTrunkRevision(), rev);
            } else {
                b.addCommit(rev);
            }
            try {
                // prepare commit
                prepare(baseRev);
                success = true;
            } finally {
                if (!success) {
                    b.removeCommit(rev);
                    if (!b.hasCommits()) {
                        nodeStore.getBranches().remove(b);
                    }
                }
            }
        } else {
            applyInternal();
        }
        if (isBranch) {
            rev = rev.asBranchRevision();
        }
        return rev;
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

    private void prepare(Revision baseRevision) {
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
     */
    private void applyToDocumentStore(Revision baseBranchRevision) {
        // the value in _revisions.<revision> property of the commit root node
        // regular commits use "c", which makes the commit visible to
        // other readers. branch commits use the base revision to indicate
        // the visibility of the commit
        String commitValue = baseBranchRevision != null ? baseBranchRevision.toString() : "c";
        DocumentStore store = nodeStore.getDocumentStore();
        String commitRootPath = null;
        if (baseBranchRevision != null) {
            // branch commits always use root node as commit root
            commitRootPath = "/";
        }
        ArrayList<UpdateOp> newNodes = new ArrayList<UpdateOp>();
        ArrayList<UpdateOp> changedNodes = new ArrayList<UpdateOp>();
        // operations are added to this list before they are executed,
        // so that all operations can be rolled back if there is a conflict
        ArrayList<UpdateOp> opLog = new ArrayList<UpdateOp>();

        //Compute the commit root
        for (String p : operations.keySet()) {
            markChanged(p);
            if (commitRootPath == null) {
                commitRootPath = p;
            } else {
                while (!PathUtils.isAncestor(commitRootPath, p)) {
                    commitRootPath = PathUtils.getParentPath(commitRootPath);
                    if (PathUtils.denotesRoot(commitRootPath)) {
                        break;
                    }
                }
            }
        }
        int commitRootDepth = PathUtils.getDepth(commitRootPath);
        // check if there are real changes on the commit root
        boolean commitRootHasChanges = operations.containsKey(commitRootPath);
        // create a "root of the commit" if there is none
        UpdateOp commitRoot = getUpdateOperationForNode(commitRootPath);
        for (String p : operations.keySet()) {
            UpdateOp op = operations.get(p);
            if (op.isNew()) {
                NodeDocument.setDeleted(op, revision, false);
            }
            if (op == commitRoot) {
                if (!op.isNew() && commitRootHasChanges) {
                    // commit root already exists and this is an update
                    changedNodes.add(op);
                }
            } else {
                NodeDocument.setCommitRoot(op, revision, commitRootDepth);
                if (op.isNew()) {
                    if (baseBranchRevision == null) {
                        // for new non-branch nodes we can safely set _lastRev on
                        // insert. for existing nodes the _lastRev is updated by
                        // the background thread to avoid concurrent updates
                        NodeDocument.setLastRev(op, revision);
                    }
                    newNodes.add(op);
                } else {
                    changedNodes.add(op);
                }
            }
        }
        if (changedNodes.size() == 0 && commitRoot.isNew()) {
            // no updates and root of commit is also new. that is,
            // it is the root of a subtree added in a commit.
            // so we try to add the root like all other nodes
            NodeDocument.setRevision(commitRoot, revision, commitValue);
            NodeDocument.setLastRev(commitRoot, revision);
            newNodes.add(commitRoot);
        }
        try {
            if (newNodes.size() > 0) {
                // set commit root on new nodes
                if (!store.create(NODES, newNodes)) {
                    // some of the documents already exist:
                    // try to apply all changes one by one
                    for (UpdateOp op : newNodes) {
                        if (op == commitRoot) {
                            // don't write the commit root just yet
                            // (because there might be a conflict)
                            NodeDocument.unsetRevision(commitRoot, revision);
                        }
                        // setting _lastRev is only safe on insert. now the
                        // background thread needs to take care of it
                        NodeDocument.unsetLastRev(op, revision.getClusterId());
                        changedNodes.add(op);
                    }
                    newNodes.clear();
                }
            }
            for (UpdateOp op : changedNodes) {
                // set commit root on changed nodes. this may even apply
                // to the commit root. the _commitRoot entry is removed
                // again when the _revisions entry is set at the end
                NodeDocument.setCommitRoot(op, revision, commitRootDepth);
                opLog.add(op);
                createOrUpdateNode(store, op);
            }
            // finally write the commit root, unless it was already written
            // with added nodes (the commit root might be written twice,
            // first to check if there was a conflict, and only then to commit
            // the revision, with the revision property set)
            if (changedNodes.size() > 0 || !commitRoot.isNew()) {
                // set revision to committed
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
                    UpdateOp commit = commitRoot.shallowCopy(commitRoot.getId());
                    commit.setNew(false);
                    // only set revision on commit root when there is
                    // no collision for this commit revision
                    commit.containsMapEntry(COLLISIONS, revision, false);
                    NodeDocument before = nodeStore.updateCommitRoot(commit);
                    if (before == null) {
                        String msg = "Conflicting concurrent change. " +
                                "Update operation failed: " + commitRoot;
                        throw new DocumentStoreException(msg);
                    } else {
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
                operations.put(commitRootPath, commitRoot);
            }
        } catch (DocumentStoreException e) {
            rollback(newNodes, opLog, commitRoot);
            throw e;
        }
    }

    private void updateParentChildStatus() {
        final Set<String> processedParents = Sets.newHashSet();
        for (String path : addedNodes) {
            if (PathUtils.denotesRoot(path)) {
                continue;
            }

            String parentPath = PathUtils.getParentPath(path);

            if (processedParents.contains(parentPath)) {
                continue;
            }

            processedParents.add(parentPath);
            UpdateOp op = getUpdateOperationForNode(parentPath);
            NodeDocument.setChildrenFlag(op, true);
        }
    }

    private void rollback(List<UpdateOp> newDocuments,
                          List<UpdateOp> changed,
                          UpdateOp commitRoot) {
        DocumentStore store = nodeStore.getDocumentStore();
        for (UpdateOp op : changed) {
            UpdateOp reverse = op.getReverseOperation();
            store.findAndUpdate(NODES, reverse);
        }
        for (UpdateOp op : newDocuments) {
            UpdateOp reverse = op.getReverseOperation();
            NodeDocument.unsetLastRev(reverse, revision.getClusterId());
            store.findAndUpdate(NODES, reverse);
        }
        UpdateOp removeCollision = new UpdateOp(commitRoot.getId(), false);
        NodeDocument.removeCollision(removeCollision, revision);
        store.findAndUpdate(NODES, removeCollision);
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

    private void checkSplitCandidate(@Nullable NodeDocument doc) {
        if (doc != null && doc.getMemory() > SPLIT_CANDIDATE_THRESHOLD) {
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
     */
    private void checkConflicts(@Nonnull UpdateOp op,
                                @Nullable NodeDocument before) {
        DocumentStore store = nodeStore.getDocumentStore();
        collisions.clear();
        if (baseRevision != null) {
            Revision newestRev = null;
            if (before != null) {
                newestRev = before.getNewestRevision(nodeStore, revision,
                        new CollisionHandler() {
                            @Override
                            void concurrentModification(Revision other) {
                                collisions.add(other);
                            }
                        });
            }
            String conflictMessage = null;
            if (newestRev == null) {
                if (op.isDelete() || !op.isNew()) {
                    conflictMessage = "The node " +
                            op.getId() + " does not exist or is already deleted";
                }
            } else {
                if (op.isNew()) {
                    conflictMessage = "The node " +
                            op.getId() + " was already added in revision\n" +
                            newestRev;
                } else if (nodeStore.isRevisionNewer(newestRev, baseRevision)
                        && (op.isDelete() || isConflicting(before, op))) {
                    conflictMessage = "The node " +
                            op.getId() + " was changed in revision\n" + newestRev +
                            ", which was applied after the base revision\n" +
                            baseRevision;
                }
            }
            if (conflictMessage == null) {
                // the modification was successful
                // -> check for collisions and conflict (concurrent updates
                // on a node are possible if property updates do not overlap)
                // TODO: unify above conflict detection and isConflicting()
                if (!collisions.isEmpty() && isConflicting(before, op)) {
                    for (Revision r : collisions) {
                        // mark collisions on commit root
                        Collision c = new Collision(before, r, op, revision, nodeStore);
                        if (c.mark(store).equals(revision)) {
                            // our revision was marked
                            if (baseRevision.isBranch()) {
                                // this is a branch commit. do not fail immediately
                                // merging this branch will fail later.
                            } else {
                                // fail immediately
                                conflictMessage = "The node " +
                                        op.getId() + " was changed in revision\n" + r +
                                        ", which was applied after the base revision\n" +
                                        baseRevision;
                            }
                        }
                    }
                }
            }
            if (conflictMessage != null) {
                conflictMessage += ", before\n" + revision +
                        "; document:\n" + (before == null ? "" : before.format()) +
                        ",\nrevision order:\n" + nodeStore.getRevisionComparator();
                throw new DocumentStoreException(conflictMessage);
            }
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
        return doc.isConflicting(op, baseRevision, revision, nodeStore);
    }

    /**
     * Apply the changes to the DocumentNodeStore (to update the cache).
     *
     * @param before the revision right before this commit.
     * @param isBranchCommit whether this is a commit to a branch
     */
    public void applyToCache(Revision before, boolean isBranchCommit) {
        HashMap<String, ArrayList<String>> nodesWithChangedChildren = new HashMap<String, ArrayList<String>>();
        for (String p : modifiedNodes) {
            if (PathUtils.denotesRoot(p)) {
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
        DiffCache.Entry cacheEntry = nodeStore.getDiffCache().newEntry(before, revision);
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
            boolean isNew = op != null && op.isNew();
            boolean pendingLastRev = op == null
                    || !NodeDocument.hasLastRev(op, revision.getClusterId());
            nodeStore.applyChanges(revision, path, isNew, pendingLastRev,
                    isBranchCommit, added, removed, changed, cacheEntry);
        }
        cacheEntry.done();
    }

    public void moveNode(String sourcePath, String targetPath) {
        diff.tag('>').key(sourcePath).value(targetPath);
    }

    public void copyNode(String sourcePath, String targetPath) {
        diff.tag('*').key(sourcePath).value(targetPath);
    }

    private void markChanged(String path) {
        if (!PathUtils.denotesRoot(path) && !PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("path: " + path);
        }
        while (true) {
            if (!modifiedNodes.add(path)) {
                break;
            }
            if (PathUtils.denotesRoot(path)) {
                break;
            }
            path = PathUtils.getParentPath(path);
        }
    }

    public void updatePropertyDiff(String path, String propertyName, String value) {
        diff.tag('^').key(PathUtils.concat(path, propertyName)).value(value);
    }

    public void removeNodeDiff(String path) {
        diff.tag('-').value(path).newline();
    }

    public void removeNode(String path) {
        removedNodes.add(path);
        UpdateOp op = getUpdateOperationForNode(path);
        op.setDelete(true);
        NodeDocument.setDeleted(op, revision, true);
    }

}
