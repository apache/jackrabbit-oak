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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A higher level object representing a commit.
 */
public class Commit {

    /**
     * Whether to purge old revisions if a node gets too large. If false, old
     * revisions are stored in a separate document. If true, old revisions are
     * removed (purged).
     * TODO: enable once document split and garbage collection implementation is complete.
     */
    static final boolean PURGE_OLD_REVISIONS = false;
    
    private static final Logger LOG = LoggerFactory.getLogger(Commit.class);

    /**
     * The maximum size of a document. If it is larger, it is split.
     * TODO: check which value is the best one
     *       Document splitting is currently disabled until the implementation
     *       is complete.
     */
    private static final int MAX_DOCUMENT_SIZE = Integer.MAX_VALUE;
   
    private final MongoMK mk;
    private final Revision baseRevision;
    private final Revision revision;
    private HashMap<String, UpdateOp> operations = new HashMap<String, UpdateOp>();
    private JsopWriter diff = new JsopStream();

    /**
     * List of all node paths which have been modified in this commit. In addition to the nodes
     * which are actually changed it also contains there parent node paths
     */
    private HashSet<String> modifiedNodes = new HashSet<String>();
    
    private HashSet<String> addedNodes = new HashSet<String>();
    private HashSet<String> removedNodes = new HashSet<String>();
    
    Commit(MongoMK mk, Revision baseRevision, Revision revision) {
        this.baseRevision = baseRevision;
        this.revision = revision;
        this.mk = mk;
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

    public static long getModified(long timestamp) {
        // 5 second resolution
        return timestamp / 1000 / 5;
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
     * to {@link MongoMK#commit(String, String, String, String)}. The base
     * revision may be <code>null</code>, e.g. for the initial commit of the
     * root node, when there is no base revision.
     *
     * @return the base revision of this commit or <code>null</code>.
     */
    @CheckForNull
    Revision getBaseRevision() {
        return baseRevision;
    }
    
    void addNodeDiff(Node n) {
        diff.tag('+').key(n.path);
        diff.object();
        n.append(diff, false);
        diff.endObject();
        diff.newline();
    }
    
    public void touchNode(String path) {
        UpdateOp op = getUpdateOperationForNode(path);
        NodeDocument.setLastRev(op, revision);
    }
    
    void updateProperty(String path, String propertyName, String value) {
        UpdateOp op = getUpdateOperationForNode(path);
        String key = Utils.escapePropertyName(propertyName);
        op.setMapEntry(key, revision.toString(), value);
    }

    void addNode(Node n) {
        if (operations.containsKey(n.path)) {
            String msg = "Node already added: " + n.path;
            LOG.error(msg);
            throw new MicroKernelException(msg);
        }
        operations.put(n.path, n.asOperation(true));
        addedNodes.add(n.path);
    }

    /**
     * Apply the changes to the document store and the cache.
     */
    void apply() {
        if (!operations.isEmpty()) {
            applyToDocumentStore();
            applyToCache(false);
        }
    }

    void prepare(Revision baseRevision) {
        if (!operations.isEmpty()) {
            applyToDocumentStore(baseRevision);
            applyToCache(true);
        }
    }

    /**
     * Apply the changes to the document store (to update MongoDB).
     */
    void applyToDocumentStore() {
        applyToDocumentStore(null);
    }

    /**
     * Apply the changes to the document store (to update MongoDB).
     *
     * @param baseBranchRevision the base revision of this commit. Currently only
     *                     used for branch commits.
     */
    void applyToDocumentStore(Revision baseBranchRevision) {
        // the value in _revisions.<revision> property of the commit root node
        // regular commits use "c", which makes the commit visible to
        // other readers. branch commits use the base revision to indicate
        // the visibility of the commit
        String commitValue = baseBranchRevision != null ? baseBranchRevision.toString() : "c";
        DocumentStore store = mk.getDocumentStore();
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
        // create a "root of the commit" if there is none
        UpdateOp commitRoot = getUpdateOperationForNode(commitRootPath);
        for (String p : operations.keySet()) {
            UpdateOp op = operations.get(p);
            if (baseBranchRevision == null) {
                // only apply _lastRev for trunk commits, _lastRev for
                // branch commits only become visible on merge
                NodeDocument.setLastRev(op, revision);
            }
            if (op.isNew) {
                op.setMapEntry(NodeDocument.DELETED, revision.toString(), "false");
            }
            if (op == commitRoot) {
                // apply at the end
            } else {
                NodeDocument.setCommitRoot(op, revision, commitRootDepth);
                if (op.isNew()) {
                    newNodes.add(op);
                } else {
                    changedNodes.add(op);
                }
            }
        }
        if (changedNodes.size() == 0 && commitRoot.isNew) {
            // no updates and root of commit is also new. that is,
            // it is the root of a subtree added in a commit.
            // so we try to add the root like all other nodes
            NodeDocument.setRevision(commitRoot, revision, commitValue);
            newNodes.add(commitRoot);
        }
        try {
            if (newNodes.size() > 0) {
                // set commit root on new nodes
                if (!store.create(Collection.NODES, newNodes)) {
                    // some of the documents already exist:
                    // try to apply all changes one by one
                    for (UpdateOp op : newNodes) {
                        if (op == commitRoot) {
                            // don't write the commit root just yet
                            // (because there might be a conflict)
                            NodeDocument.unsetRevision(commitRoot, revision);
                        }
                        changedNodes.add(op);
                    }
                    newNodes.clear();
                }
            }
            for (UpdateOp op : changedNodes) {
                // set commit root on changed nodes
                NodeDocument.setCommitRoot(op, revision, commitRootDepth);
                opLog.add(op);
                createOrUpdateNode(store, op);
            }
            // finally write the commit root, unless it was already written
            // with added nodes (the commit root might be written twice,
            // first to check if there was a conflict, and only then to commit
            // the revision, with the revision property set)
            if (changedNodes.size() > 0 || !commitRoot.isNew) {
                NodeDocument.setRevision(commitRoot, revision, commitValue);
                opLog.add(commitRoot);
                createOrUpdateNode(store, commitRoot);
                operations.put(commitRootPath, commitRoot);
            }
        } catch (MicroKernelException e) {
            rollback(newNodes, opLog);
            String msg = "Exception committing " + diff.toString();
            LOG.debug(msg, e);
            throw new MicroKernelException(msg, e);
        }
    }
    
    private void rollback(ArrayList<UpdateOp> newDocuments, ArrayList<UpdateOp> changed) {
        DocumentStore store = mk.getDocumentStore();
        for (UpdateOp op : changed) {
            UpdateOp reverse = op.getReverseOperation();
            store.createOrUpdate(Collection.NODES, reverse);
        }
        for (UpdateOp op : newDocuments) {
            store.remove(Collection.NODES, op.key);
        }
    }

    /**
     * Try to create or update the node. If there was a conflict, this method
     * throws an exception, even though the change is still applied.
     * 
     * @param store the store
     * @param op the operation
     */
    public void createOrUpdateNode(DocumentStore store, UpdateOp op) {
        NodeDocument doc = store.createOrUpdate(Collection.NODES, op);
        if (baseRevision != null) {
            final AtomicReference<List<Revision>> collisions = new AtomicReference<List<Revision>>();
            Revision newestRev = null;
            if (doc != null) {
                newestRev = doc.getNewestRevision(mk, store, revision,
                        new CollisionHandler() {
                            @Override
                            void concurrentModification(Revision other) {
                                if (collisions.get() == null) {
                                    collisions.set(new ArrayList<Revision>());
                                }
                                collisions.get().add(other);
                            }
                        });
            }
            String conflictMessage = null;
            if (newestRev == null) {
                if (op.isDelete || !op.isNew) {
                    conflictMessage = "The node " + 
                            op.getKey() + " does not exist or is already deleted";
                }
            } else {
                if (op.isNew) {
                    conflictMessage = "The node " + 
                            op.getKey() + " was already added in revision\n" +
                            newestRev;
                } else if (mk.isRevisionNewer(newestRev, baseRevision)
                        && (op.isDelete || isConflicting(doc, op))) {
                    conflictMessage = "The node " + 
                            op.getKey() + " was changed in revision\n" + newestRev +
                            ", which was applied after the base revision\n" + 
                            baseRevision;
                }
            }
            if (conflictMessage != null) {
                conflictMessage += ", before\n" + revision + 
                        "; document:\n" + doc.format() +
                        ",\nrevision order:\n" + mk.getRevisionComparator();
                throw new MicroKernelException(conflictMessage);
            }
            // if we get here the modification was successful
            // -> check for collisions and conflict (concurrent updates
            // on a node are possible if property updates do not overlap)
            if (collisions.get() != null && isConflicting(doc, op)) {
                for (Revision r : collisions.get()) {
                    // mark collisions on commit root
                    new Collision(doc, r, op, revision).mark(store);
                }
            }
        }

        if (doc != null && doc.getMemory() > MAX_DOCUMENT_SIZE) {
            UpdateOp[] split = doc.splitDocument(mk, revision, mk.getSplitDocumentAgeMillis());
            
            // TODO check if the new main document is actually smaller;
            // otherwise, splitting doesn't make sense
            
            // the old version
            UpdateOp old = split[0];
            if (old != null) {
                store.createOrUpdate(Collection.NODES, old);
            }
            
            // the (shrunken) main document
            UpdateOp main = split[1];
            if (main != null) {
                store.createOrUpdate(Collection.NODES, main);
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
        // did existence of node change after baseRevision?
        @SuppressWarnings("unchecked")
        Map<String, String> deleted = (Map<String, String>) doc.get(NodeDocument.DELETED);
        if (deleted != null) {
            for (Map.Entry<String, String> entry : deleted.entrySet()) {
                if (mk.isRevisionNewer(Revision.fromString(entry.getKey()), baseRevision)) {
                    return true;
                }
            }
        }

        for (Map.Entry<String, UpdateOp.Operation> entry : op.changes.entrySet()) {
            if (entry.getValue().type != UpdateOp.Operation.Type.SET_MAP_ENTRY) {
                continue;
            }
            int idx = entry.getKey().indexOf('.');
            String name = entry.getKey().substring(0, idx);
            if (NodeDocument.DELETED.equals(name)) {
                // existence of node changed, this always conflicts with
                // any other concurrent change
                return true;
            }
            if (!Utils.isPropertyName(name)) {
                continue;
            }
            // was this property touched after baseRevision?
            @SuppressWarnings("unchecked")
            Map<String, Object> changes = (Map<String, Object>) doc.get(name);
            if (changes == null) {
                continue;
            }
            for (String rev : changes.keySet()) {
                if (mk.isRevisionNewer(Revision.fromString(rev), baseRevision)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Apply the changes to the MongoMK (to update the cache).
     * 
     * @param isBranchCommit whether this is a commit to a branch
     */
    public void applyToCache(boolean isBranchCommit) {
        HashMap<String, ArrayList<String>> nodesWithChangedChildren = new HashMap<String, ArrayList<String>>();
        ArrayList<String> addOrRemove = new ArrayList<String>();
        addOrRemove.addAll(addedNodes);
        addOrRemove.addAll(removedNodes);
        for (String p : addOrRemove) {
            String parent = PathUtils.getParentPath(p);
            ArrayList<String> list = nodesWithChangedChildren.get(parent);
            if (list == null) {
                list = new ArrayList<String>();
                nodesWithChangedChildren.put(parent, list);
            }
            list.add(p);
        }
        for (String path : modifiedNodes) {
            ArrayList<String> added = new ArrayList<String>();
            ArrayList<String> removed = new ArrayList<String>();
            ArrayList<String> changed = nodesWithChangedChildren.get(path);
            if (changed != null) {
                for (String s : changed) {
                    if (addedNodes.contains(s)) {
                        added.add(s);
                    } else if (removedNodes.contains(s)) {
                        removed.add(s);
                    }
                }
            }
            UpdateOp op = operations.get(path);
            boolean isNew = op != null && op.isNew;
            boolean isWritten = op != null;
            boolean isDelete = op != null && op.isDelete;
            mk.applyChanges(revision, path, 
                    isNew, isDelete, isWritten, isBranchCommit,
                    added, removed);
        }
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
            modifiedNodes.add(path);
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
        op.setMapEntry(NodeDocument.DELETED, revision.toString(), "true");
    }

}
