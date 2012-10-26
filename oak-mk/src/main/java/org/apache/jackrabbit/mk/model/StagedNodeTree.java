/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.model;

import org.apache.jackrabbit.mk.json.JsonObject;
import org.apache.jackrabbit.mk.model.tree.NodeDelta;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.store.RevisionStore;
import org.apache.jackrabbit.oak.commons.PathUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code StagedNodeTree} provides methods to manipulate a specific revision
 * of the tree. The changes are recorded and can be persisted by calling
 * {@link #persist(RevisionStore.PutToken)}.
 */
public class StagedNodeTree {

    private final RevisionStore store;

    private StagedNode root;
    private Id baseRevisionId;

    /**
     * Creates a new {@code StagedNodeTree} instance.
     *
     * @param store          revision store used to read from and persist changes
     * @param baseRevisionId id of revision the changes should be based upon
     */
    public StagedNodeTree(RevisionStore store, Id baseRevisionId) {
        this.store = store;
        this.baseRevisionId = baseRevisionId;
    }

    /**
     * Discards all staged changes and resets the base revision to the
     * specified new revision id.
     *
     * @param newBaseRevisionId id of revision the changes should be based upon
     */
    public void reset(Id newBaseRevisionId) {
        root = null;
        baseRevisionId = newBaseRevisionId;
    }

    /**
     * Returns {@code true} if there are no staged changes, otherwise returns {@code false}.
     *
     * @return {@code true} if there are no staged changes, otherwise returns {@code false}.
     */
    public boolean isEmpty() {
        return root == null;
    }

    /**
     * Persists the staged nodes and returns the {@code Id} of the new root node.
     *
     * @param token
     * @return {@code Id} of new root node or {@code null} if there are no changes to persist.
     * @throws Exception if an error occurs
     */
    public Id /* new id of root node */ persist(RevisionStore.PutToken token) throws Exception {
        return root != null ? root.persist(token) : null;
    }

    /**
     * Performs a three-way merge merging <i>our</i> tree (rooted at {@code ourRoot})
     * and <i>their</i> tree (identified by {@code newBaseRevisionId}),
     * using the common ancestor revision {@code commonAncestorRevisionId} as
     * base reference.
     * <p/>
     * <I>This</I> instance will be initially reset to {@code newBaseRevisionId}, discarding
     * all currently staged changes.
     *
     * @param ourRoot
     * @param newBaseRevisionId
     * @param commonAncestorRevisionId
     * @param token
     * @return {@code Id} of new root node
     * @throws Exception
     */
    public Id merge(StoredNode ourRoot,
                    Id newBaseRevisionId,
                    Id commonAncestorRevisionId,
                    RevisionStore.PutToken token) throws Exception {
        // reset staging area to new base revision
        reset(newBaseRevisionId);

        StoredNode baseRoot = store.getRootNode(commonAncestorRevisionId);
        StoredNode theirRoot = store.getRootNode(newBaseRevisionId);

        // recursively merge 'our' changes with 'their' changes...
        mergeNode(baseRoot, ourRoot, theirRoot, "/");

        // persist staged nodes
        return persist(token);
    }

    //-----------------------------------------< tree manipulation operations >

    /**
     * Creates a new node named {@code nodeName} at {@code parentNodePath}.
     *
     * @param parentNodePath parent node path
     * @param nodeName name of new node
     * @param nodeData {@code JsonObject} representation of the node to be added
     * @throws NotFoundException if there's no node at {@code parentNodePath}
     * @throws Exception if a node named {@code nodeName} already exists at {@code parentNodePath}
     *                   or if another error occurs
     */
    public void add(String parentNodePath, String nodeName, JsonObject nodeData) throws Exception {
        StagedNode parent = getStagedNode(parentNodePath, true);
        if (parent.getChildNodeEntry(nodeName) != null) {
            throw new Exception("there's already a child node with name '" + nodeName + "'");
        }
        parent.add(nodeName, nodeData);
    }

    /**
     * Removes the node at {@code nodePath}.
     *
     * @param nodePath node path
     * @throws Exception
     */
    public void remove(String nodePath) throws Exception {
        String parentPath = PathUtils.getParentPath(nodePath);
        String nodeName = PathUtils.getName(nodePath);

        StagedNode parent = getStagedNode(parentPath, true);
        if (parent.remove(nodeName) == null) {
            throw new NotFoundException(nodePath);
        }

        // discard any staged changes at nodePath
        unstageNode(nodePath);
    }

    /**
     * Creates or updates the property named {@code propName} of the specified node.
     * <p/>
     * if {@code propValue == null} the specified property will be removed.
     *
     * @param nodePath node path
     * @param propName property name
     * @param propValue property value
     * @throws NotFoundException if there's no node at {@code nodePath}
     * @throws Exception if another error occurs
     */
    public void setProperty(String nodePath, String propName, String propValue) throws Exception {
        StagedNode node = getStagedNode(nodePath, true);

        Map<String, String> properties = node.getProperties();
        if (propValue == null) {
            properties.remove(propName);
        } else {
            properties.put(propName, propValue);
        }
    }

    /**
     * Moves the subtree rooted at {@code srcPath} to {@code destPath}.
     *
     * @param srcPath path of node to be moved
     * @param destPath destination path
     * @throws NotFoundException if either the node at {@code srcPath} or the parent
     *                           node of {@code destPath} doesn't exist
     * @throws Exception if a node already exists at {@code destPath},
     *                   if {@code srcPath} denotes an ancestor of {@code destPath}
     *                   or if another error occurs
     */
    public void move(String srcPath, String destPath) throws Exception {
        if (PathUtils.isAncestor(srcPath, destPath)) {
            throw new Exception("target path cannot be descendant of source path: " + destPath);
        }

        String srcParentPath = PathUtils.getParentPath(srcPath);
        String srcNodeName = PathUtils.getName(srcPath);

        String destParentPath = PathUtils.getParentPath(destPath);
        String destNodeName = PathUtils.getName(destPath);

        StagedNode srcParent = getStagedNode(srcParentPath, true);
        if (srcParent.getChildNodeEntry(srcNodeName) == null) {
            throw new NotFoundException(srcPath);
        }
        StagedNode destParent = getStagedNode(destParentPath, true);
        if (destParent.getChildNodeEntry(destNodeName) != null) {
            throw new Exception("node already exists at move destination path: " + destPath);
        }

        if (srcParentPath.equals(destParentPath)) {
            // rename
            srcParent.rename(srcNodeName, destNodeName);
        } else {
            // move
            srcParent.move(srcNodeName, destPath);
        }
    }

    /**
     * Copies the subtree rooted at {@code srcPath} to {@code destPath}.
     *
     * @param srcPath path of node to be copied
     * @param destPath destination path
     * @throws NotFoundException if either the node at {@code srcPath} or the parent
     *                           node of {@code destPath} doesn't exist
     * @throws Exception if a node already exists at {@code destPath}
     *                   or if another error occurs
     */
    public void copy(String srcPath, String destPath) throws Exception {
        String srcParentPath = PathUtils.getParentPath(srcPath);
        String srcNodeName = PathUtils.getName(srcPath);

        String destParentPath = PathUtils.getParentPath(destPath);
        String destNodeName = PathUtils.getName(destPath);

        StagedNode srcParent = getStagedNode(srcParentPath, false);
        if (srcParent == null) {
            // the subtree to be copied has not been modified
            ChildNodeEntry entry = getStoredNode(srcParentPath).getChildNodeEntry(srcNodeName);
            if (entry == null) {
                throw new NotFoundException(srcPath);
            }
            StagedNode destParent = getStagedNode(destParentPath, true);
            if (destParent.getChildNodeEntry(destNodeName) != null) {
                throw new Exception("node already exists at copy destination path: " + destPath);
            }
            destParent.add(new ChildNodeEntry(destNodeName, entry.getId()));
            return;
        }

        ChildNodeEntry srcEntry = srcParent.getChildNodeEntry(srcNodeName);
        if (srcEntry == null) {
            throw new NotFoundException(srcPath);
        }

        StagedNode destParent = getStagedNode(destParentPath, true);
        StagedNode srcNode = getStagedNode(srcPath, false);
        if (srcNode != null) {
            // copy the modified subtree
            destParent.add(destNodeName, srcNode.copy());
        } else {
            destParent.add(new ChildNodeEntry(destNodeName, srcEntry.getId()));
        }
    }

    //-------------------------------------------------------< implementation >

    /**
     * Returns a {@code StagedNode} representation of the specified node.
     * If a {@code StagedNode} representation doesn't exist yet a new
     * {@code StagedNode} instance will be returned if {@code createIfNotStaged == true},
     * otherwise {@code null} will be returned.
     * <p/>
     * A {@code NotFoundException} will be thrown if there's no node at {@code path}.
     *
     * @param path              node path
     * @param createIfNotStaged flag controlling whether a new {@code StagedNode}
     *                          instance should be created on demand
     * @return a {@code StagedNode} instance or {@code null} if there's no {@code StagedNode}
     *         representation of the specified node and {@code createIfNotStaged == false}
     * @throws NotFoundException if there's no child node with the given name
     * @throws Exception         if another error occurs
     */
    private StagedNode getStagedNode(String path, boolean createIfNotStaged) throws Exception {
        assert PathUtils.isAbsolute(path);

        if (root == null) {
            if (!createIfNotStaged) {
                return null;
            }
            root = new StagedNode(store.getRootNode(baseRevisionId), store);
        }

        if (PathUtils.denotesRoot(path)) {
            return root;
        }

        StagedNode parent = root, node = null;
        for (String name : PathUtils.elements(path)) {
            node = parent.getStagedChildNode(name, createIfNotStaged);
            if (node == null) {
                return null;
            }
            parent = node;
        }
        return node;
    }

    /**
     * Discards all staged changes affecting the subtree rooted at {@code path}.
     *
     * @param path node path
     * @return the discarded {@code StagedNode} representation or {@code null} if there wasn't any
     * @throws NotFoundException if there's no node at the specified {@code path}
     * @throws Exception if another error occurs
     */
    private StagedNode unstageNode(String path) throws Exception {
        assert PathUtils.isAbsolute(path);

        if (PathUtils.denotesRoot(path)) {
            StagedNode unstaged = root;
            root = null;
            return unstaged;
        }

        String parentPath = PathUtils.getParentPath(path);
        String name = PathUtils.getName(path);

        StagedNode parent = getStagedNode(parentPath, false);
        if (parent == null) {
            return null;
        }

        return parent.unstageChildNode(name);
    }

    /**
     * Returns the {@code StoredNode} at {@code path}.
     *
     * @param path node path
     * @return the {@code StoredNode} at {@code path}
     * @throws NotFoundException if there's no node at the specified {@code path}
     * @throws Exception if another error occurs
     */
    private StoredNode getStoredNode(String path) throws Exception {
        assert PathUtils.isAbsolute(path);

        if (PathUtils.denotesRoot(path)) {
            return store.getRootNode(baseRevisionId);
        }

        StoredNode parent = store.getRootNode(baseRevisionId), node = null;
        for (String name : PathUtils.elements(path)) {
            ChildNodeEntry entry = parent.getChildNodeEntry(name);
            if (entry == null) {
                throw new NotFoundException(path);
            }
            node = store.getNode(entry.getId());
            if (node == null) {
                throw new NotFoundException(path);
            }
            parent = node;
        }
        return node;
    }

    /**
     * Performs a three-way merge of the trees rooted at {@code ourRoot},
     * {@code theirRoot}, using the tree at {@code baseRoot} as reference.
     */
    private void mergeNode(StoredNode baseNode, StoredNode ourNode, StoredNode theirNode, String path) throws Exception {
        NodeDelta theirChanges = new NodeDelta(
                store, store.getNodeState(baseNode), store.getNodeState(theirNode));
        NodeDelta ourChanges = new NodeDelta(
                store, store.getNodeState(baseNode), store.getNodeState(ourNode));

        StagedNode stagedNode = getStagedNode(path, true);

        // apply our changes
        stagedNode.getProperties().putAll(ourChanges.getAddedProperties());
        stagedNode.getProperties().putAll(ourChanges.getChangedProperties());
        for (String name : ourChanges.getRemovedProperties().keySet()) {
            stagedNode.getProperties().remove(name);
        }

        for (Map.Entry<String, Id> entry : ourChanges.getAddedChildNodes().entrySet()) {
            stagedNode.add(new ChildNodeEntry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, Id> entry : ourChanges.getChangedChildNodes().entrySet()) {
            if (!theirChanges.getChangedChildNodes().containsKey(entry.getKey())) {
                stagedNode.add(new ChildNodeEntry(entry.getKey(), entry.getValue()));
            }
        }
        for (String name : ourChanges.getRemovedChildNodes().keySet()) {
            stagedNode.remove(name);
        }

        List<NodeDelta.Conflict> conflicts = theirChanges.listConflicts(ourChanges);
        // resolve/report merge conflicts
        for (NodeDelta.Conflict conflict : conflicts) {
            String conflictName = conflict.getName();
            String conflictPath = PathUtils.concat(path, conflictName);
            switch (conflict.getType()) {
                case PROPERTY_VALUE_CONFLICT:
                    throw new Exception(
                            "concurrent modification of property " + conflictPath
                                    + " with conflicting values: \""
                                    + ourNode.getProperties().get(conflictName)
                                    + "\", \""
                                    + theirNode.getProperties().get(conflictName));

                case NODE_CONTENT_CONFLICT: {
                    if (ourChanges.getChangedChildNodes().containsKey(conflictName)) {
                        // modified subtrees
                        StoredNode baseChild = store.getNode(baseNode.getChildNodeEntry(conflictName).getId());
                        StoredNode ourChild = store.getNode(ourNode.getChildNodeEntry(conflictName).getId());
                        StoredNode theirChild = store.getNode(theirNode.getChildNodeEntry(conflictName).getId());
                        // merge the dirty subtrees recursively
                        mergeNode(baseChild, ourChild, theirChild, PathUtils.concat(path, conflictName));
                    } else {
                        // todo handle/merge colliding node creation
                        throw new Exception("colliding concurrent node creation: " + conflictPath);
                    }
                    break;
                }

                case REMOVED_DIRTY_PROPERTY_CONFLICT:
                    stagedNode.getProperties().remove(conflictName);
                    break;

                case REMOVED_DIRTY_NODE_CONFLICT:
                    stagedNode.remove(conflictName);
                    break;
            }

        }
    }

    //--------------------------------------------------------< inner classes >

    private class StagedNode extends MutableNode {

        private final Map<String, StagedNode> stagedChildNodes = new HashMap<String, StagedNode>();

        private StagedNode(RevisionStore store) {
            super(store);
        }

        private StagedNode(Node base, RevisionStore store) {
            super(base, store);
        }

        /**
         * Returns a {@code StagedNode} representation of the specified child node.
         * If a {@code StagedNode} representation doesn't exist yet a new
         * {@code StagedNode} instance will be returned if {@code createIfNotStaged == true},
         * otherwise {@code null} will be returned.
         * <p/>
         * A {@code NotFoundException} will be thrown if there's no child node
         * with the given name.
         *
         * @param name              child node name
         * @param createIfNotStaged flag controlling whether a new {@code StagedNode}
         *                          instance should be created on demand
         * @return a {@code StagedNode} instance or {@code null} if there's no {@code StagedNode}
         *         representation of the specified child node and {@code createIfNotStaged == false}
         * @throws NotFoundException if there's no child node with the given name
         * @throws Exception         if another error occurs
         */
        StagedNode getStagedChildNode(String name, boolean createIfNotStaged) throws Exception {
            StagedNode child = stagedChildNodes.get(name);
            if (child == null) {
                ChildNodeEntry entry = getChildNodeEntry(name);
                if (entry != null) {
                    if (createIfNotStaged) {
                        child = new StagedNode(store.getNode(entry.getId()), store);
                        stagedChildNodes.put(name, child);
                    }
                } else {
                    throw new NotFoundException(name);
                }
            }
            return child;
        }

        /**
         * Removes the {@code StagedNode} representation of the specified child node if there is one.
         *
         * @param name child node name
         * @return the removed {@code StagedNode} representation or {@code null} if there wasn't any
         */
        StagedNode unstageChildNode(String name) {
            return stagedChildNodes.remove(name);
        }

        StagedNode add(String name, StagedNode node) {
            stagedChildNodes.put(name, node);
            // child id will be computed on persist
            add(new ChildNodeEntry(name, null));
            return node;
        }

        StagedNode copy() {
            StagedNode copy = new StagedNode(this, store);
            // recursively copy staged child nodes
            for (Map.Entry<String, StagedNode> entry : stagedChildNodes.entrySet()) {
                copy.add(entry.getKey(), entry.getValue().copy());
            }
            return copy;
        }

        StagedNode add(String name, JsonObject obj) {
            StagedNode node = new StagedNode(store);
            node.getProperties().putAll(obj.getProperties());
            for (Map.Entry<String, JsonObject> entry : obj.getChildren().entrySet()) {
                node.add(entry.getKey(), entry.getValue());
            }
            stagedChildNodes.put(name, node);
            // child id will be computed on persist
            add(new ChildNodeEntry(name, null));
            return node;
        }

        void move(String name, String destPath) throws Exception {
            ChildNodeEntry srcEntry = getChildNodeEntry(name);
            assert srcEntry != null;

            String destParentPath = PathUtils.getParentPath(destPath);
            String destName = PathUtils.getName(destPath);

            StagedNode destParent = getStagedNode(destParentPath, true);

            StagedNode target = stagedChildNodes.get(name);

            remove(name);
            destParent.add(new ChildNodeEntry(destName, srcEntry.getId()));

            if (target != null) {
                // move staged child node
                destParent.add(destName, target);
            }
        }

        @Override
        public ChildNodeEntry remove(String name) {
            stagedChildNodes.remove(name);
            return super.remove(name);
        }

        @Override
        public ChildNodeEntry rename(String oldName, String newName) {
            StagedNode child = stagedChildNodes.remove(oldName);
            if (child != null) {
                stagedChildNodes.put(newName, child);
            }
            return super.rename(oldName, newName);
        }

        Id persist(RevisionStore.PutToken token) throws Exception {
            // recursively persist staged nodes
            for (Map.Entry<String, StagedNode> entry : stagedChildNodes.entrySet()) {
                String name = entry.getKey();
                StagedNode childNode = entry.getValue();
                // todo decide whether to inline/store child node separately based on some filter criteria
                Id id = childNode.persist(token);
                // update child node entry
                add(new ChildNodeEntry(name, id));
            }
            // persist this node
            return store.putNode(token, this);
        }
    }
}
