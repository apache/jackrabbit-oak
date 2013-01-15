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
package org.apache.jackrabbit.mongomk.impl.instruction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.mongomk.api.instruction.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.CopyNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.MoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.RemoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.Instruction.SetPropertyInstruction;
import org.apache.jackrabbit.mongomk.api.instruction.InstructionVisitor;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.command.NodeExistsCommand;
import org.apache.jackrabbit.mongomk.impl.exception.NotFoundException;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * This class reads in the instructions generated from JSON, applies basic checks
 * and creates a node map for {@code CommitCommandMongo} to work on later.
 */
public class CommitCommandInstructionVisitor implements InstructionVisitor {

    // the revision this commit is based on
    private final long baseRevisionId;
    private final MongoNodeStore nodeStore;
    private final Map<String, MongoNode> pathNodeMap;

    private String branchId;

    /**
     * Creates {@code CommitCommandInstructionVisitor}
     *
     * @param nodeStore Node store.
     * @param baseRevisionId the revision this commit is based on
     */
    public CommitCommandInstructionVisitor(MongoNodeStore nodeStore,
                                           long baseRevisionId,
                                           List<MongoCommit> validCommits) {
        this.nodeStore = nodeStore;
        this.baseRevisionId = baseRevisionId;
        pathNodeMap = new HashMap<String, MongoNode>();
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
    public Map<String, MongoNode> getPathNodeMap() {
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
        MongoNode parent = getStoredNode(parentNodePath);
        if (parent.childExists(nodeName)) {
            throw new RuntimeException("There's already a child node with name '" + nodeName + "'");
        }
        getStagedNode(nodePath);
        parent.addChild(nodeName);
    }

    @Override
    public void visit(SetPropertyInstruction instruction) {
        String key = instruction.getKey();
        Object value = instruction.getValue();
        MongoNode node = getStoredNode(instruction.getPath());
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
        MongoNode parent = getStoredNode(parentPath);
        if (!parent.childExists(nodeName)) {
            throw new RuntimeException("Node " + nodeName
                    + " does not exists at parent path: " + parentPath);
        }
        parent.removeChild(nodeName);
        markAsDeleted(nodePath);
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

        MongoNode srcParent = getStoredNode(srcParentPath);
        if (!srcParent.childExists(srcNodeName)) {
            throw new NotFoundException(srcPath);
        }
        MongoNode destParent = getStoredNode(destParentPath);
        if (destParent.childExists(destNodeName)) {
            throw new RuntimeException("Node already exists at copy destination path: " + destPath);
        }

        copy(getStoredNode(srcPath), destPath);

        // Finally, add to destParent.
        destParent.addChild(destNodeName);
    }

    @Override
    public void visit(MoveNodeInstruction instruction) {
        String srcPath = instruction.getSourcePath();
        String destPath = instruction.getDestPath();

        if (destPath.startsWith(srcPath + "/")) {
            throw new RuntimeException("Cannot move " + srcPath + " to " + destPath);
        }

        // copy source to destination
        visit(new CopyNodeInstructionImpl(instruction.getPath(),
                srcPath, instruction.getDestPath()));
        // delete source tree
        visit(new RemoveNodeInstructionImpl(PathUtils.getParentPath(srcPath),
                PathUtils.getName(srcPath)));
    }

    private void checkAbsolutePath(String srcPath) {
        if (!PathUtils.isAbsolute(srcPath)) {
            throw new RuntimeException("Absolute path expected: " + srcPath);
        }
    }

    private MongoNode getStagedNode(String path) {
        MongoNode node = pathNodeMap.get(path);
        if (node == null) {
            node = new MongoNode();
            node.setPath(path);
            pathNodeMap.put(path, node);
        }
        return node;
    }

    private MongoNode getStoredNode(String path) {
        MongoNode node = pathNodeMap.get(path);
        if (node != null) {
            return node;
        }

        // First need to check that the path is indeed valid.
        NodeExistsCommand existCommand = new NodeExistsCommand(nodeStore,
                path, baseRevisionId);
        existCommand.setBranchId(branchId);
        boolean exists = false;
        try {
            exists = existCommand.execute();
        } catch (Exception ignore) {}

        if (!exists) {
            throw new NotFoundException(path + " @rev" + baseRevisionId);
        }
        node = existCommand.getNode();
        node.removeField("_id");
        pathNodeMap.put(path, node);

        return node;
    }

    /**
     * Recursively copies nodes from <code>srcNode</code> to
     * <code>destPath</code>. This method takes existing nodes as well as
     * staged nodes into account.
     *
     * @param srcNode the source node.
     */
    private void copy(MongoNode srcNode, String destPath) {
        MongoNode destNode = srcNode.copy();
        destNode.setPath(destPath);
        destNode.removeField("_id");
        copyAddedProperties(srcNode, destNode);
        copyRemovedProperties(srcNode, destNode);
        pathNodeMap.put(destPath, destNode);

        List<String> children = new ArrayList<String>();
        if (srcNode.getChildren() != null) {
            children.addAll(srcNode.getChildren());
        }
        if (srcNode.getRemovedChildren() != null) {
            for (String child : srcNode.getRemovedChildren()) {
                destNode.removeChild(child);
                children.remove(child);
            }
        }
        if (srcNode.getAddedChildren() != null) {
            for (String child : srcNode.getAddedChildren()) {
                destNode.addChild(child);
                children.add(child);
            }
        }
        for (String child : children) {
            String srcChildPath = PathUtils.concat(srcNode.getPath(), child);
            String destChildPath = PathUtils.concat(destPath, child);
            copy(getStoredNode(srcChildPath), destChildPath);
        }
    }

    private void copyAddedProperties(MongoNode srcNode, MongoNode destNode) {
        Map<String, Object> addedProps = srcNode.getAddedProps();
        if (addedProps == null || addedProps.isEmpty()) {
            return;
        }

        for (Entry<String, Object> entry : addedProps.entrySet()) {
            destNode.addProperty(entry.getKey(), entry.getValue());
        }
    }

    private void copyRemovedProperties(MongoNode srcNode, MongoNode destNode) {
        Map<String, Object> removedProps = srcNode.getRemovedProps();
        if (removedProps == null || removedProps.isEmpty()) {
            return;
        }

        for (String key : removedProps.keySet()) {
            destNode.removeProp(key);
        }
    }

    private void markAsDeleted(String path) {
        MongoNode node = getStoredNode(path);
        node.setDeleted(true);
        List<String> children = new ArrayList<String>();
        if (node.getChildren() != null) {
            children.addAll(node.getChildren());
        }
        if (node.getAddedChildren() != null) {
            children.addAll(node.getAddedChildren());
        }
        for (String child : children) {
            markAsDeleted(PathUtils.concat(path, child));
        }
        pathNodeMap.put(path, node);
    }
}