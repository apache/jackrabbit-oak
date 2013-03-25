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
package org.apache.jackrabbit.mongomk.prototype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mongomk.prototype.DocumentStore.Collection;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A higher level object representing a commit.
 */
public class Commit {

    private static final Logger LOG = LoggerFactory.getLogger(Commit.class);

    /**
     * The maximum size of a document. If it is larger, it is split.
     */
    // TODO check which value is the best one
    private static final int MAX_DOCUMENT_SIZE = 16 * 1024;
    
    /**
     * Whether to purge old revisions if a node gets too large. If false, old
     * revisions are stored in a separate document. If true, old revisions are
     * removed (purged).
     */
    private static final boolean PURGE_OLD_REVISIONS = true;
    
    private final MongoMK mk;
    private final Revision baseRevision;
    private final Revision revision;
    private HashMap<String, UpdateOp> operations = new HashMap<String, UpdateOp>();
    private JsopWriter diff = new JsopStream();
    private HashSet<String> changedNodes = new HashSet<String>();
    
    private HashSet<String> addedNodes = new HashSet<String>();
    private HashSet<String> removedNodes = new HashSet<String>();
    
    Commit(MongoMK mk, Revision baseRevision, Revision revision) {
        this.baseRevision = baseRevision;
        this.revision = revision;
        this.mk = mk;
    }

    private UpdateOp getUpdateOperationForNode(String path) {
        UpdateOp op = operations.get(path);
        if (op == null) {
            String id = Utils.getIdFromPath(path);
            op = new UpdateOp(path, id, false);
            operations.put(path, op);
        }
        return op;
    }

    public Revision getRevision() {
        return revision;
    }
    
    void addNodeDiff(Node n) {
        diff.tag('+').key(n.path);
        diff.object();
        n.append(diff, false);
        diff.endObject();
        diff.newline();
    }
    
    void updateProperty(String path, String propertyName, String value) {
        UpdateOp op = getUpdateOperationForNode(path);
        String key = Utils.escapePropertyName(propertyName);
        op.addMapEntry(key + "." + revision.toString(), value);
        op.setMapEntry(UpdateOp.LAST_REV + "." + revision.getClusterId(), revision.toString());        
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
            applyToCache();
        }
    }

    /**
     * Apply the changes to the document store (to update MongoDB).
     */
    void applyToDocumentStore() {
        DocumentStore store = mk.getDocumentStore();
        String commitRootPath = null;
        ArrayList<UpdateOp> newNodes = new ArrayList<UpdateOp>();
        ArrayList<UpdateOp> changedNodes = new ArrayList<UpdateOp>();
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
            op.setMapEntry(UpdateOp.LAST_REV + "." + revision.getClusterId(), revision.toString());
            if (op.isNew) {
                op.addMapEntry(UpdateOp.DELETED + "." + revision.toString(), "false");
            }
            if (op == commitRoot) {
                // apply at the end
            } else {
                op.addMapEntry(UpdateOp.COMMIT_ROOT + "." + revision.toString(), commitRootDepth);
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
            commitRoot.addMapEntry(UpdateOp.REVISIONS + "." + revision.toString(), "true");
            newNodes.add(commitRoot);
        }
        try {
            if (newNodes.size() > 0) {
                // set commit root on new nodes
                if (!store.create(Collection.NODES, newNodes)) {
                    // some of the documents already exist:
                    // try to apply all changes one by one
                    for (UpdateOp op : newNodes) {
                        op.unset(UpdateOp.ID);
                        if (op == commitRoot) {
                            // don't write the commit root just yet
                            // (because there might be a conflict)
                            commitRoot.unset(UpdateOp.REVISIONS + "." + revision.toString());
                        }
                        changedNodes.add(op);
                    }
                    newNodes.clear();
                }
            }
            for (UpdateOp op : changedNodes) {
                // set commit root on changed nodes
                op.addMapEntry(UpdateOp.COMMIT_ROOT + "." + revision.toString(), commitRootDepth);
                createOrUpdateNode(store, op);
            }
            // finally write the commit root, unless it was already written
            // with added nodes (the commit root might be written twice,
            // first to check if there was a conflict, and only then to commit
            // the revision, with the revision property set)
            if (changedNodes.size() > 0 || !commitRoot.isNew) {
                commitRoot.addMapEntry(UpdateOp.REVISIONS + "." + revision.toString(), "true");
                createOrUpdateNode(store, commitRoot);
                operations.put(commitRootPath, commitRoot);
            }
        } catch (MicroKernelException e) {
            String msg = "Exception committing " + diff.toString();
            LOG.error(msg, e);
            throw new MicroKernelException(msg, e);
        }
    }
    
    private void createOrUpdateNode(DocumentStore store, UpdateOp op) {
        Map<String, Object> map = store.createOrUpdate(Collection.NODES, op);
        if (baseRevision != null) {
            // TODO detect conflicts here
            Revision newestRev = mk.getNewestRevision(map, revision, true);
            if (mk.isRevisionNewer(newestRev, baseRevision)) {
                // TODO transaction rollback
                throw new MicroKernelException("The node " + 
                        op.path + " was changed in revision " + 
                        newestRev + 
                        ", which was applied after the base revision " + 
                        baseRevision);
            }
        }

        int size = Utils.getMapSize(map);
        if (size > MAX_DOCUMENT_SIZE) {
            UpdateOp[] split = splitDocument(map);
            
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
    
    private UpdateOp[] splitDocument(Map<String, Object> map) {
        String id = (String) map.get(UpdateOp.ID);
        String path = Utils.getPathFromId(id);
        Long previous = (Long) map.get(UpdateOp.PREVIOUS);
        if (previous == null) {
            previous = 0L;
        } else {
            previous++;
        }
        UpdateOp old = new UpdateOp(path, id + "/" + previous, true);
        UpdateOp main = new UpdateOp(path, id, false);
        main.set(UpdateOp.PREVIOUS, previous);
        for (Entry<String, Object> e : map.entrySet()) {
            String key = e.getKey();
            if (key.equals(UpdateOp.ID)) {
                // ok
            } else if (key.equals(UpdateOp.PREVIOUS)) {
                // ok
            } else if (key.equals(UpdateOp.LAST_REV)) {
                // only maintain the lastRev in the main document
                main.setMapEntry(UpdateOp.LAST_REV + "." + revision.getClusterId(), revision.toString());        
            } else {
                // UpdateOp.DELETED,
                // UpdateOp.REVISIONS,
                // and regular properties
                @SuppressWarnings("unchecked")
                Map<String, Object> valueMap = (Map<String, Object>) e.getValue();
                Revision latestRev = null;
                for (String r : valueMap.keySet()) {
                    Revision propRev = Revision.fromString(r);
                    if (latestRev == null || mk.isRevisionNewer(propRev, latestRev)) {
                        latestRev = propRev;
                    }
                }
                for (String r : valueMap.keySet()) {
                    Revision propRev = Revision.fromString(r);
                    Object v = valueMap.get(r);
                    if (propRev.equals(latestRev)) {
                        main.setMapEntry(key + "." + propRev.toString(), v);
                    } else {
                        old.addMapEntry(key + "." + propRev.toString(), v);
                    }
                }
            }
        }
        if (PURGE_OLD_REVISIONS) {
            old = null;
        }
        return new UpdateOp[]{old, main};
    }

    /**
     * Apply the changes to the MongoMK (to update the cache).
     */
    public void applyToCache() {
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
        for (String path : changedNodes) {
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
                    isNew, isDelete, isWritten, 
                    added, removed);
        }
    }

    public void moveNode(String sourcePath, String targetPath) {
        diff.tag('>').key(sourcePath).value(targetPath);
    }
    
    public void copyNode(String sourcePath, String targetPath) {
        diff.tag('*').key(sourcePath).value(targetPath);
    }

    public JsopWriter getDiff() {
        return diff;
    }

    private void markChanged(String path) {
        if (!PathUtils.denotesRoot(path) && !PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("path: " + path);
        }
        while (true) {
            changedNodes.add(path);
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
        op.addMapEntry(UpdateOp.DELETED + "." + revision.toString(), "true");
        op.setMapEntry(UpdateOp.LAST_REV + "." + revision.getClusterId(), revision.toString());
    }

}
