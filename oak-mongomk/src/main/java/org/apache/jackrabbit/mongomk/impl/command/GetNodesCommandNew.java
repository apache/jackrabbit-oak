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
package org.apache.jackrabbit.mongomk.impl.command;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesActionNew;
import org.apache.jackrabbit.mongomk.impl.command.exception.InconsistentNodeHierarchyException;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FIXME - This is same as GetNodesCommand except that it does not fetch all valid
 * commits upfront and it also does not check for stale slave reads. Consolidate the two.
 *
 * {@code Command} for {@code MongoMicroKernel#getNodes(String, String, int, long, int, String)}
 */
public class GetNodesCommandNew extends BaseCommand<Node> {

    private static final Logger LOG = LoggerFactory.getLogger(GetNodesCommandNew.class);

    private final String path;

    private String branchId;
    private int depth = FetchNodesAction.LIMITLESS_DEPTH;
    private Long revisionId;

    private Map<String, MongoNode> pathAndNodeMap;
    private Node rootNode;

    /**
     * Constructs a new {@code GetNodesCommandMongo}.
     *
     * @param nodeStore Node store.
     * @param path The root path of the nodes to get.
     * @param revisionId The revision id or null for head revision.
     */
    public GetNodesCommandNew(MongoNodeStore nodeStore, String path,
            Long revisionId) {
        super(nodeStore);
        this.path = path;
        this.revisionId = revisionId;
    }

    /**
     * Sets the branchId for the command.
     *
     * @param branchId Branch id.
     */
    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    /**
     * Sets the depth for the command.
     *
     * @param depth The depth for the command or -1 for limitless depth.
     */
    public void setDepth(int depth) {
        this.depth = depth;
    }

    @Override
    public Node execute() throws Exception {
        if (revisionId == null) {
            revisionId = new GetHeadRevisionCommand(nodeStore).execute();
        }
        readRootNode();
        return rootNode;
    }

    private void readRootNode() throws InconsistentNodeHierarchyException {
        readNodesByPath();
        boolean verified = verifyNodeHierarchy();
        if (!verified) {
            throw new InconsistentNodeHierarchyException();
        }
        buildNodeStructure();
    }

    @Override
    public int getNumOfRetries() {
        return 3;
    }

    @Override
    public boolean needsRetry(Exception e) {
        return e instanceof InconsistentNodeHierarchyException;
    }

    private void buildNodeStructure() {
        MongoNode nodeMongoRootOfPath = pathAndNodeMap.get(path);
        rootNode = buildNodeStructure(nodeMongoRootOfPath);
    }

    private NodeImpl buildNodeStructure(MongoNode nodeMongo) {
        if (nodeMongo == null) {
            return null;
        }

        NodeImpl node = MongoNode.toNode(nodeMongo);

        for (Iterator<Node> it = node.getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node child = it.next();
            MongoNode nodeMongoChild = pathAndNodeMap.get(child.getPath());
            if (nodeMongoChild != null) {
                NodeImpl nodeChild = buildNodeStructure(nodeMongoChild);
                node.addChildNodeEntry(nodeChild);
            }
        }

        return node;
    }

    private void readNodesByPath() {
        FetchNodesActionNew query = new FetchNodesActionNew(nodeStore, path, revisionId);
        query.setBranchId(branchId);
        query.setDepth(depth);
        pathAndNodeMap = query.execute();
    }

    private boolean verifyNodeHierarchy() {
        boolean verified = verifyNodeHierarchyRec(path, 0);
        if (!verified) {
            LOG.error("Node hierarchy could not be verified because some nodes"
                    + " were inconsistent: {}", path);
        }
        return verified;
    }

    private boolean verifyNodeHierarchyRec(String path, int currentDepth) {
        boolean verified = false;

        if (pathAndNodeMap.isEmpty()) {
            return true;
        }

        MongoNode nodeMongo = pathAndNodeMap.get(path);
        if (nodeMongo != null) {
            verified = true;
            if ((depth == -1) || (currentDepth < depth)) {
                List<String> childNames = nodeMongo.getChildren();
                if (childNames != null) {
                    for (String childName : childNames) {
                        String childPath = PathUtils.concat(path, childName);
                        verified = verifyNodeHierarchyRec(childPath, ++currentDepth);
                        if (!verified) {
                            break;
                        }
                    }
                }
            }
        }

        return verified;
    }
}