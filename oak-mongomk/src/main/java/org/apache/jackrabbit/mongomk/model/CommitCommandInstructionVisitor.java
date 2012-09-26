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

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.AddPropertyInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.CopyNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.MoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.RemoveNodeInstruction;
import org.apache.jackrabbit.mongomk.api.model.Instruction.SetPropertyInstruction;
import org.apache.jackrabbit.mongomk.api.model.InstructionVisitor;
import org.apache.jackrabbit.mongomk.query.FetchNodeByPathQuery;
import org.apache.jackrabbit.oak.commons.PathUtils;

public class CommitCommandInstructionVisitor implements InstructionVisitor {

    private final long headRevisionId;
    private final MongoConnection mongoConnection;
    private final Map<String, NodeMongo> pathNodeMap;

    public CommitCommandInstructionVisitor(MongoConnection mongoConnection,
            long headRevisionId) {
        this.mongoConnection = mongoConnection;
        this.headRevisionId = headRevisionId;
        pathNodeMap = new HashMap<String, NodeMongo>();
    }

    public Map<String, NodeMongo> getPathNodeMap() {
        return pathNodeMap;
    }

    @Override
    public void visit(AddNodeInstruction instruction) {
        String path = instruction.getPath();
        getStagedNode(path);

        String nodeName = PathUtils.getName(path);
        if (nodeName.isEmpty()) {
            return;
        }

        String parentNodePath = PathUtils.getParentPath(path);
        NodeMongo parent = null;
        if (!PathUtils.denotesRoot(parentNodePath)) {
            parent = getStoredNode(parentNodePath);
            if (parent == null) {
                throw new RuntimeException("No such parent: " + PathUtils.getName(parentNodePath));
            }
            // FIXME [Mete] Add once tests are fixed.
            //if (parent.childExists(nodeName)) {
            //    throw new RuntimeException("There's already a child node with name '" + nodeName + "'");
            //}
        } else {
            parent = getStagedNode(parentNodePath);
        }
        parent.addChild(nodeName);
    }

    @Override
    public void visit(AddPropertyInstruction instruction) {
        NodeMongo node = getStagedNode(instruction.getPath());
        node.addProperty(instruction.getKey(), instruction.getValue());
    }

    @Override
    public void visit(CopyNodeInstruction instruction) {
        String srcPath = instruction.getSourcePath();
        String destPath = instruction.getDestPath();

        String srcParentPath = PathUtils.getParentPath(srcPath);
        String srcNodeName = PathUtils.getName(srcPath);

        String destParentPath = PathUtils.getParentPath(destPath);
        String destNodeName = PathUtils.getName(destPath);

        NodeMongo srcParent = pathNodeMap.get(srcParentPath);
        if (srcParent == null) {
            // The subtree to be copied has not been modified
            boolean entryExists = getStoredNode(srcParentPath).childExists(srcNodeName);
            if (!entryExists) {
                throw new RuntimeException("Not found: " + srcPath);
            }
            NodeMongo destParent = getStagedNode(destParentPath);
            if (destParent.childExists(destNodeName)) {
                throw new RuntimeException("Node already exists at copy destination path: " + destPath);
            }

            // Copy src node to destPath.
            NodeMongo srcNode = getStoredNode(srcPath);
            NodeMongo destNode = NodeMongo.fromDBObject(srcNode);
            destNode.setPath(destPath);
            // FIXME - [Mete] This needs to do proper merge instead of just add.
            List<String> addedChildren = srcNode.getAddedChildren();
            if (addedChildren != null && !addedChildren.isEmpty()) {
                for (String child : addedChildren) {
                    getStagedNode(PathUtils.concat(destPath, child));
                    destNode.addChild(child);
                }
            }
            pathNodeMap.put(destPath, destNode);

            // Add to destParent.
            destParent.addChild(destNodeName);

            return;
        }

        boolean srcEntryExists = srcParent.childExists(srcNodeName);
        if (!srcEntryExists) {
            throw new RuntimeException(srcPath);
        }

        // FIXME - [Mete] The rest is not totally correct.
        NodeMongo destParent = getStagedNode(destParentPath);
        NodeMongo srcNode = getStagedNode(srcPath);

        if (srcNode != null) {
            // Copy the modified subtree
            NodeMongo destNode = NodeMongo.fromDBObject(srcNode);
            destNode.setPath(destPath);
            pathNodeMap.put(destPath,  destNode);
            destParent.addChild(destNodeName);
            //destParent.add(destNodeName, srcNode.copy());
        } else {
            NodeMongo destNode = NodeMongo.fromDBObject(srcNode);
            destNode.setPath(destPath);
            pathNodeMap.put(destPath,  destNode);
            destParent.addChild(destNodeName);
            //destParent.add(new ChildNodeEntry(destNodeName, srcEntry.getId()));
        }

        // [Mete] Old code from Philipp.
        // retrieve all nodes beyond and add them as new children to the dest location
//        List<NodeMongo> childNodesToCopy = new FetchNodesByPathAndDepthQuery(mongoConnection, srcPath,
//                revisionId, -1).execute();
//        for (NodeMongo nodeMongo : childNodesToCopy) {
//            String oldPath = nodeMongo.getPath();
//            String oldPathRel = PathUtils.relativize(srcPath, oldPath);
//            String newPath = PathUtils.concat(destPath, oldPathRel);
//
//            nodeMongo.setPath(newPath);
//            nodeMongo.removeField("_id");
//            pathNodeMap.put(newPath, nodeMongo);
//        }

        // tricky part now: In case we already know about any changes to these existing nodes we need to merge
        // those now.
    }

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

        // Add the old node with the new path.
        NodeMongo destNode = pathNodeMap.get(destPath);
        if (destNode == null) {
            NodeMongo srcNode = getStoredNode(srcPath);
            destNode = srcNode;
            destNode.setPath(destPath);
            pathNodeMap.put(destPath, destNode);
        }

        // Remove from srcParent - [Mete] What if there is no such child?
        NodeMongo scrParentNode = getStoredNode(srcParentPath);
        scrParentNode.removeChild(srcNodeName);

        // Add to destParent
        NodeMongo destParentNode = getStoredNode(destParentPath);
        if (destParentNode.childExists(destNodeName)) {
            throw new RuntimeException("Node already exists at move destination path: " + destPath);
        }
        destParentNode.addChild(destNodeName);

        // [Mete] Siblings?
    }

    @Override
    public void visit(RemoveNodeInstruction instruction) {
        String path = instruction.getPath();
        String parentPath = PathUtils.getParentPath(path);
        NodeMongo parentNode = getStoredNode(parentPath);
        String childName = PathUtils.getName(path);
        if (!parentNode.childExists(childName)) {
            throw new RuntimeException(path);
        }
        parentNode.removeChild(PathUtils.getName(path));
    }

    @Override
    public void visit(SetPropertyInstruction instruction) {
        String path = instruction.getPath();
        String key = instruction.getKey();
        Object value = instruction.getValue();
        NodeMongo node = getStagedNode(path);
        if (value == null) {
            node.removeProp(key);
        } else {
            node.addProperty(key, value);
        }
    }

    // TODO - [Mete] I think we need a way to distinguish between Staged
    // and Stored nodes. For example, what if a node is retrieved as Staged
    // but later it needs to be retrieved as Stored?
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
        if (node == null) {
            FetchNodeByPathQuery query = new FetchNodeByPathQuery(mongoConnection,
                    path, headRevisionId);
            query.setFetchAll(true);
            node = query.execute();
            if (node != null) {
                node.removeField("_id");
                pathNodeMap.put(path, node);
            }
        }
        return node;
    }
}