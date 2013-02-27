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

/**
 * A higher level object representing a commit.
 */
public class Commit {
    
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
    private final Revision revision;
    private HashMap<String, UpdateOp> operations = new HashMap<String, UpdateOp>();
    private JsopWriter diff = new JsopStream();
    private HashSet<String> changedNodes = new HashSet<String>();
    
    private HashSet<String> addedNodes = new HashSet<String>();
    private HashSet<String> removedNodes = new HashSet<String>();
    
    private HashMap<String, Long> writeCounts = new HashMap<String, Long>();
    
    Commit(MongoMK mk, Revision revision) {
        this.revision = revision;
        this.mk = mk;
    }

    private UpdateOp getUpdateOperationForNode(String path) {
        UpdateOp op = operations.get(path);
        if (op == null) {
            String id = Node.convertPathToDocumentId(path);
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
        op.addMapEntry(propertyName + "." + revision.toString(), value);
        long increment = mk.getWriteCountIncrement(path);
        op.increment(UpdateOp.WRITE_COUNT, 1 + increment);
    }

    void addNode(Node n) {
        if (operations.containsKey(n.path)) {
            throw new MicroKernelException("Node already added: " + n.path);
        }
        operations.put(n.path, n.asOperation(true));
        addedNodes.add(n.path);
    }

    boolean isEmpty() {
        return operations.isEmpty();
    }

    /**
     * Apply the changes to the document store (to update MongoDB).
     * 
     * @param store the store
     */
    void applyToDocumentStore() {
        DocumentStore store = mk.getDocumentStore();
        String commitRoot = null;
        ArrayList<UpdateOp> newNodes = new ArrayList<UpdateOp>();
        ArrayList<UpdateOp> changedNodes = new ArrayList<UpdateOp>();
        for (String p : operations.keySet()) {
            markChanged(p);
            if (commitRoot == null) {
                commitRoot = p;
            } else {
                while (!PathUtils.isAncestor(commitRoot, p)) {
                    commitRoot = PathUtils.getParentPath(commitRoot);
                    if (PathUtils.denotesRoot(commitRoot)) {
                        break;
                    }
                }
            }
        }
        // create a "root of the commit" if there is none
        UpdateOp root = getUpdateOperationForNode(commitRoot);
        for (String p : operations.keySet()) {
            UpdateOp op = operations.get(p);
            if (op == root) {
                // apply at the end
            } else if (op.isNew()) {
                newNodes.add(op);
            } else {
                changedNodes.add(op);
            }
        }
        if (changedNodes.size() == 0 && root.isNew) {
            // no updates, so we just add the root like the others
            newNodes.add(root);
            root = null;
        }
        try {
            if (newNodes.size() > 0) {
                store.create(Collection.NODES, newNodes);
            }
            for (UpdateOp op : changedNodes) {
                createOrUpdateNode(store, op);
            }
            if (root != null) {
                long increment = mk.getWriteCountIncrement(commitRoot);
                root.increment(UpdateOp.WRITE_COUNT, 1 + increment);
                root.addMapEntry(UpdateOp.REVISIONS + "." + revision.toString(), "true");
                createOrUpdateNode(store, root);
                operations.put(commitRoot, root);
            }
        } catch (MicroKernelException e) {
            throw new MicroKernelException("Exception committing " + diff.toString(), e);
        }
    }
    
    private void createOrUpdateNode(DocumentStore store, UpdateOp op) {
        Map<String, Object> map = store.createOrUpdate(Collection.NODES, op);
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
        // TODO detect conflicts here
        Long count = (Long) map.get(UpdateOp.WRITE_COUNT);
        if (count == null) {
            count = 0L;
        }
        String path = op.getPath();
        writeCounts.put(path, count);
    }
    
    private UpdateOp[] splitDocument(Map<String, Object> map) {
        String path = (String) map.get(UpdateOp.PATH);
        String id = (String) map.get(UpdateOp.ID);
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
            if (key.equals(UpdateOp.PATH)) {
                // ok
            } else if (key.equals(UpdateOp.ID)) {
                // ok
            } else if (key.equals(UpdateOp.PREVIOUS)) {
                // ok
            } else if (key.equals(UpdateOp.WRITE_COUNT)) {
                // only maintain the write count on the main document
                main.set(UpdateOp.WRITE_COUNT, e.getValue());
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
            long writeCountInc = mk.getWriteCountIncrement(path);
            Long writeCount = writeCounts.get(path);
            if (writeCount == null) {
                if (isNew) {
                    writeCount = 0L;
                    writeCountInc = 0;
                } else {
                    writeCountInc++;
                    String id = Node.convertPathToDocumentId(path);
                    Map<String, Object> map = mk.getDocumentStore().find(Collection.NODES, id);
                    Long oldWriteCount = (Long) map.get(UpdateOp.WRITE_COUNT);
                    writeCount = oldWriteCount == null ? 0 : oldWriteCount;
                }
            }
            mk.applyChanges(revision, path, 
                    isNew, isWritten, 
                    writeCount, writeCountInc,
                    added, removed);
        }
    }

    public void moveNode(String sourcePath, String targetPath) {
        diff.tag('>').key(sourcePath).value(targetPath);
    }

    public JsopWriter getDiff() {
        return diff;
    }

    private void markChanged(String path) {
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
        op.addMapEntry(UpdateOp.DELETED + "." + revision.toString(), "true");
        long increment = mk.getWriteCountIncrement(path);
        op.increment(UpdateOp.WRITE_COUNT, 1 + increment);
    }

}
