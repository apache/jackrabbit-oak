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
package org.apache.jackrabbit.mongomk.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.mongomk.api.instruction.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.AddPropertyInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.CopyNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.MoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.RemoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.SetPropertyInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.InstructionVisitor;
import org.apache.jackrabbit.mongomk.command.NodeExistsCommandMongo;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.query.FetchNodesQuery;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * This class reads in the instructions generated from JSON, applies basic checks
 * and creates a node map for {@code CommitCommandMongo} to work on later.
 */
public class CommitCommandInstructionVisitor implements InstructionVisitor {

    private final long headRevisionId;
    private final MongoConnection mongoConnection;
    private final Map<String, NodeMongo> pathNodeMap;

    private String branchId;

    /**
     * Creates {@code CommitCommandInstructionVisitor}
     *
     * @param mongoConnection Mongo connection.
     * @param headRevisionId Head revision.
     */
    public CommitCommandInstructionVisitor(MongoConnection mongoConnection,
            long headRevisionId) {
        this.mongoConnection = mongoConnection;
        this.headRevisionId = headRevisionId;
        pathNodeMap = new HashMap<String, NodeMongo>();
    }

    /**
     * Sets the branch id associated with the commit. It can be null.
     *
     * @param branchId Branch id or null.
     */
    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    /**
     * Returns the generated node map after visit methods are called.
     *
     * @return Node map.
     */
    public Map<String, NodeMongo> getPathNodeMap() {
        return pathNodeMap;
    }

    @Override
    public void visit(AddNodeInstruction instruction) {
        String nodePath = instruction.getPath();
        checkAbsolutePath(nodePath);

        String nodeName = PathUtils.getName(nodePath);
        if (nodeName.isEmpty()) { // This happens in initial commit.
            getStagedNode(nodePath);
            return;
        }

        String parentNodePath = PathUtils.getParentPath(nodePath);
        NodeMongo parent = getStoredNode(parentNodePath);
        if (parent.childExists(nodeName)) {
            throw new RuntimeException("There's already a child node with name '" + nodeName + "'");
        }
        getStagedNode(nodePath);
        parent.addChild(nodeName);
    }

    @Override
    public void visit(AddPropertyInstruction instruction) {
        String nodePath = instruction.getPath();
        NodeMongo node = getStoredNode(nodePath);
        node.addProperty(instruction.getKey(), instruction.getValue());
    }

    @Override
    public void visit(SetPropertyInstruction instruction) {
        String key = instruction.getKey();
        Object value = instruction.getValue();
        NodeMongo node = getStoredNode(instruction.getPath());
        if (value == null) {
            node.removeProp(key);
        } else {
            node.addProperty(key, value);
        }
    }

    @Override
    public void visit(RemoveNodeInstruction instruction) {
        String nodePath = instruction.getPath();
        checkAbsolutePath(nodePath);

        String parentPath = PathUtils.getParentPath(nodePath);
        String nodeName = PathUtils.getName(nodePath);
        NodeMongo parent = getStoredNode(parentPath);
        if (!parent.childExists(nodeName)) {
            throw new RuntimeException("Node " + nodeName
                    + " does not exists at parent path: " + parentPath);
        }
        parent.removeChild(PathUtils.getName(nodePath));
    }

    @Override
    public void visit(CopyNodeInstruction instruction) {
        String srcPath = instruction.getSourcePath();
        checkAbsolutePath(srcPath);

        String destPath = instruction.getDestPath();
        if (!PathUtils.isAbsolute(destPath)) {
            destPath = PathUtils.concat(instruction.getPath(), destPath);
            checkAbsolutePath(destPath);
        }

        String srcParentPath = PathUtils.getParentPath(srcPath);
        String srcNodeName = PathUtils.getName(srcPath);

        String destParentPath = PathUtils.getParentPath(destPath);
        String destNodeName = PathUtils.getName(destPath);

        NodeMongo srcParent = getStoredNode(srcParentPath);
        if (!srcParent.childExists(srcNodeName)) {
            throw new NotFoundException(srcPath);
        }
        NodeMongo destParent = getStoredNode(destParentPath);
        if (destParent.childExists(destNodeName)) {
            throw new RuntimeException("Node already exists at copy destination path: " + destPath);
        }

        // First, copy the existing nodes.
        List<NodeMongo> nodesToCopy = new FetchNodesQuery(mongoConnection,
                srcPath, headRevisionId).execute();
        for (NodeMongo nodeMongo : nodesToCopy) {
            String oldPath = nodeMongo.getPath();
            String oldPathRel = PathUtils.relativize(srcPath, oldPath);
            String newPath = PathUtils.concat(destPath, oldPathRel);

            nodeMongo.setPath(newPath);
            nodeMongo.removeField("_id");
            pathNodeMap.put(newPath, nodeMongo);
        }

        // Then, copy any staged changes.
        NodeMongo srcNode = getStoredNode(srcPath);
        NodeMongo destNode = getStagedNode(destPath);

        copyStagedChanges(srcNode, destNode);

        // Finally, add to destParent.
        pathNodeMap.put(destPath, destNode);
        destParent.addChild(destNodeName);
    }

    // FIXME - This is the old functionality, remove it once the new one is working.
//    @Override
//    public void visit(MoveNodeInstruction instruction) {
//        String srcPath = instruction.getSourcePath();
//        String destPath = instruction.getDestPath();
//
//        if (PathUtils.isAncestor(srcPath, destPath)) {
//            throw new RuntimeException("Target path cannot be descendant of source path: "
//                    + destPath);
//        }
//
//        String srcParentPath = PathUtils.getParentPath(srcPath);
//        String srcNodeName = PathUtils.getName(srcPath);
//
//        String destParentPath = PathUtils.getParentPath(destPath);
//        String destNodeName = PathUtils.getName(destPath);
//
//        // Add the old node with the new path.
//        NodeMongo destNode = pathNodeMap.get(destPath);
//        if (destNode == null) {
//            NodeMongo srcNode = getStoredNode(srcPath);
//            destNode = srcNode;
//            destNode.setPath(destPath);
//            pathNodeMap.remove(srcPath);
//            pathNodeMap.put(destPath, destNode);
//        }
//
//        // [Mete] Check that this works in all cases.
//        // Remove all the pending old node child changes.
//        Map<String, NodeMongo> pendingChanges = new HashMap<String, NodeMongo>();
//        for (Iterator<Entry<String, NodeMongo>> iterator = pathNodeMap.entrySet().iterator(); iterator.hasNext();) {
//            Entry<String, NodeMongo> entry = iterator.next();
//            String path = entry.getKey();
//            NodeMongo node = entry.getValue();
//            if (path.startsWith(srcPath)) {
//                iterator.remove();
//                String newPath = destPath + path.substring(srcPath.length());
//                node.setPath(newPath);
//                pendingChanges.put(newPath, node);
//            }
//        }
//        pathNodeMap.putAll(pendingChanges);
//
//
//        // Remove from srcParent - [Mete] What if there is no such child?
//        NodeMongo scrParentNode = getStoredNode(srcParentPath);
//        scrParentNode.removeChild(srcNodeName);
//
//        // Add to destParent
//        NodeMongo destParentNode = getStoredNode(destParentPath);
//        if (destParentNode.childExists(destNodeName)) {
//            throw new RuntimeException("Node already exists at move destination path: " + destPath);
//        }
//        destParentNode.addChild(destNodeName);
//
//        // [Mete] Siblings?
//    }

    @Override
    public void visit(MoveNodeInstruction instruction) {
        String srcPath = instruction.getSourcePath();
        String destPath = instruction.getDestPath();
        if (PathUtils.isAncestor(srcPath, destPath)) {
            throw new RuntimeException("Target path cannot be descendant of source path: "
                    + destPath);
        }

        String srcParentPath = PathUtils.getParentPath(srcPath);
        String srcNodeName = PathUtils.getName(srcPath);

        String destParentPath = PathUtils.getParentPath(destPath);
        String destNodeName = PathUtils.getName(destPath);

        NodeMongo srcParent = getStoredNode(srcParentPath);
        if (!srcParent.childExists(srcNodeName)) {
            throw new NotFoundException(srcPath);
        }

        NodeMongo destParent = getStoredNode(destParentPath);
        if (destParent.childExists(destNodeName)) {
            throw new RuntimeException("Node already exists at move destination path: " + destPath);
        }

        // First, copy the existing nodes.
        List<NodeMongo> nodesToCopy = new FetchNodesQuery(mongoConnection,
                srcPath, headRevisionId).execute();
        for (NodeMongo nodeMongo : nodesToCopy) {
            String oldPath = nodeMongo.getPath();
            String oldPathRel = PathUtils.relativize(srcPath, oldPath);
            String newPath = PathUtils.concat(destPath, oldPathRel);

            nodeMongo.setPath(newPath);
            nodeMongo.removeField("_id");
            pathNodeMap.put(newPath, nodeMongo);
        }

        // Finally, add to destParent and remove from srcParent.
        getStagedNode(destPath);
        destParent.addChild(destNodeName);
        srcParent.removeChild(srcNodeName);

//        if (srcParentPath.equals(destParentPath)) {
//            rename(srcParent, srcNodeName, destNodeName);
//        } else {
//            move(srcParent, srcNodeName, destPath);
//        }
    }

    private void checkAbsolutePath(String srcPath) {
        if (!PathUtils.isAbsolute(srcPath)) {
            throw new RuntimeException("Absolute path expected: " + srcPath);
        }
    }

    private NodeMongo getStagedNode(String path) {
        NodeMongo node = pathNodeMap.get(path);
        if (node == null) {
            node = new NodeMongo();
            node.setPath(path);
            pathNodeMap.put(path, node);
        }
        return node;
    }

    private NodeMongo getStoredNode(String path) {
        NodeMongo node = pathNodeMap.get(path);
        if (node != null) {
            return node;
        }

        // First need to check that the path is indeed valid.
        NodeExistsCommandMongo existCommand = new NodeExistsCommandMongo(mongoConnection,
                path, headRevisionId);
        existCommand.setBranchId(branchId);
        boolean exists = false;
        try {
            exists = existCommand.execute();
        } catch (Exception ignore) {}

        if (!exists) {
            throw new NotFoundException(path);
        }

        // Fetch the node without its descendants.
        FetchNodesQuery query = new FetchNodesQuery(mongoConnection,
                path, headRevisionId);
        query.setBranchId(branchId);
        query.setFetchDescendants(false);
        List<NodeMongo> nodes = query.execute();
        if (!nodes.isEmpty()) {
            node = nodes.get(0);
            node.removeField("_id");
            pathNodeMap.put(path, node);
        }
        return node;
    }

    private void copyStagedChanges(NodeMongo srcNode, NodeMongo destNode) {

        // Copy staged changes at the top level.
        copyAddedNodes(srcNode, destNode);
        copyRemovedNodes(srcNode, destNode);
        copyAddedProperties(srcNode, destNode);
        copyRemovedProperties(srcNode, destNode);

        // Recursively add staged changes of the descendants.
        List<String> srcChildren = srcNode.getChildren();
        if (srcChildren == null || srcChildren.isEmpty()) {
            return;
        }

        for (String childName : srcChildren) {
            String oldChildPath = PathUtils.concat(srcNode.getPath(), childName);
            NodeMongo oldChild = getStoredNode(oldChildPath);

            String newChildPath = PathUtils.concat(destNode.getPath(), childName);
            NodeMongo newChild = getStagedNode(newChildPath);
            copyStagedChanges(oldChild, newChild);
        }
    }

    private void copyRemovedProperties(NodeMongo srcNode, NodeMongo destNode) {
        Map<String, Object> removedProps = srcNode.getRemovedProps();
        if (removedProps == null || removedProps.isEmpty()) {
            return;
        }

        for (String key : removedProps.keySet()) {
            destNode.removeProp(key);
        }
    }

    private void copyAddedNodes(NodeMongo srcNode, NodeMongo destNode) {
        List<String> addedChildren = srcNode.getAddedChildren();
        if (addedChildren == null || addedChildren.isEmpty()) {
            return;
        }

        for (String childName : addedChildren) {
            getStagedNode(PathUtils.concat(destNode.getPath(), childName));
            destNode.addChild(childName);
        }
    }

    private void copyRemovedNodes(NodeMongo srcNode, NodeMongo destNode) {
        List<String> removedChildren = srcNode.getRemovedChildren();
        if (removedChildren == null || removedChildren.isEmpty()) {
            return;
        }

        for (String child : removedChildren) {
            destNode.removeChild(child);
        }
    }

    private void copyAddedProperties(NodeMongo srcNode, NodeMongo destNode) {
        Map<String, Object> addedProps = srcNode.getAddedProps();
        if (addedProps == null || addedProps.isEmpty()) {
            return;
        }

        for (Entry<String, Object> entry : addedProps.entrySet()) {
            destNode.addProperty(entry.getKey(), entry.getValue());
        }
    }

    // FIXME - This is from Oak. Remove them if they are not needed.
    private void rename(NodeMongo srcParent, String oldName, String newName) {
        //StagedNode child = stagedChildNodes.remove(oldName);
        srcParent.removeChild(oldName);
//        if (child != null) {
//            stagedChildNodes.put(newName, child);
//        }
        String newChildPath = PathUtils.concat(srcParent.getPath(), newName);
        getStagedNode(newChildPath);
        srcParent.addChild(newName);
    }

    private void move(NodeMongo srcParent, String name, String destPath) {
        //ChildNodeEntry srcEntry = getChildNodeEntry(name);
        //assert srcEntry != null;
        assert srcParent.childExists(name);

        String destParentPath = PathUtils.getParentPath(destPath);
        String destName = PathUtils.getName(destPath);

        //StagedNode destParent = getStagedNode(destParentPath, true);
        NodeMongo destParent = getStoredNode(destParentPath);

        //StagedNode target = stagedChildNodes.get(name);
        String targetPath = PathUtils.concat(srcParent.getPath(), name);
        NodeMongo target = getStagedNode(targetPath);

        //remove(name);
        srcParent.removeChild(name);
        //destParent.add(new ChildNodeEntry(destName, srcEntry.getId()));
        getStagedNode(destPath);
        destParent.addChild(destName);

//        if (target != null) {
//            // move staged child node
//            destParent.add(destName, target);
//        }
    }
}