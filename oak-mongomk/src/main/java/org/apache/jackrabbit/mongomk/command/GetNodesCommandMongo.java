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
package org.apache.jackrabbit.mongomk.command;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.jackrabbit.mongomk.api.command.AbstractCommand;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.query.FetchBranchBaseRevisionIdQuery;
import org.apache.jackrabbit.mongomk.query.FetchCommitQuery;
import org.apache.jackrabbit.mongomk.query.FetchNodesQuery;
import org.apache.jackrabbit.mongomk.query.FetchCommitsQuery;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code Command} for getting nodes from {@code MongoDB}.
 */
public class GetNodesCommandMongo extends AbstractCommand<Node> {

    private static final Logger LOG = LoggerFactory.getLogger(GetNodesCommandMongo.class);

    private final MongoConnection mongoConnection;
    private final String path;

    private String branchId;
    private int depth = FetchNodesQuery.LIMITLESS_DEPTH;
    private Long revisionId;
    private List<CommitMongo> lastCommits;
    private List<NodeMongo> nodeMongos;

    private Map<String, NodeMongo> pathAndNodeMap;
    private Map<String, Long> problematicNodes;
    private Node rootNode;

    /**
     * Constructs a new {@code GetNodesCommandMongo}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param path The root path of the nodes to get.
     * @param revisionId The revision id or null for head revision.
     */
    public GetNodesCommandMongo(MongoConnection mongoConnection, String path,
            Long revisionId) {
        this.mongoConnection = mongoConnection;
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
        ensureRevisionId();
        readLastCommits();
        deriveProblematicNodes();

        // FIXME - See if we can avoid catching the exception here.
        try {
            readRootNode();
        } catch (InconsistentNodeHierarchyException e) {
            // When branching is used, there might be inconsistent node hierarchy
            // exceptions, so ignore them in the first try.
            if (branchId == null) {
                throw e;
            }
        }

        if (rootNode != null) {
            return rootNode;
        }

        // If branching is not used, node does not exist.
        if (branchId == null) {
            return rootNode;
        }

        // Otherwise, node does not exist in the branch but it might exist in
        // trunk before the branch was created.
        FetchBranchBaseRevisionIdQuery query = new FetchBranchBaseRevisionIdQuery(mongoConnection, branchId);
        Long branchBaseRevId = query.execute();
        revisionId = branchBaseRevId;
        branchId = null;
        readRootNode();

        return rootNode;
    }

    private void readRootNode() throws InconsistentNodeHierarchyException {
        readNodesByPath();
        createPathAndNodeMap();
        boolean verified = verifyProblematicNodes() && verifyNodeHierarchy();
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
        NodeMongo nodeMongoRootOfPath = pathAndNodeMap.get(path);
        rootNode = buildNodeStructure(nodeMongoRootOfPath);
    }

    private NodeImpl buildNodeStructure(NodeMongo nodeMongo) {
        if (nodeMongo == null) {
            return null;
        }

        NodeImpl node = NodeMongo.toNode(nodeMongo);

        for (Iterator<Node> it = node.getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node child = it.next();
            NodeMongo nodeMongoChild = pathAndNodeMap.get(child.getPath());
            if (nodeMongoChild != null) {
                NodeImpl nodeChild = buildNodeStructure(nodeMongoChild);
                node.addChildNodeEntry(nodeChild);
            }
        }

        return node;
    }

    private void createPathAndNodeMap() {
        pathAndNodeMap = new HashMap<String, NodeMongo>();
        for (NodeMongo nodeMongo : nodeMongos) {
            pathAndNodeMap.put(nodeMongo.getPath(), nodeMongo);
        }
    }

    private void deriveProblematicNodes() {
        problematicNodes = new HashMap<String, Long>();

        for (ListIterator<CommitMongo> iterator = lastCommits.listIterator(); iterator.hasPrevious();) {
            CommitMongo commitMongo = iterator.previous();
            long revisionId = commitMongo.getRevisionId();
            List<String> affectedPath = commitMongo.getAffectedPaths();

            for (String path : affectedPath) {
                problematicNodes.put(path, revisionId);
            }
        }
    }

    private void ensureRevisionId() throws Exception {
        if (revisionId == null) {
            revisionId = new GetHeadRevisionCommandMongo(mongoConnection).execute();
        } else {
            // Ensure that commit with revision id exists.
            new FetchCommitQuery(mongoConnection, revisionId).execute();
        }
    }

    private void readLastCommits() throws Exception {
        lastCommits = new FetchCommitsQuery(mongoConnection, revisionId).execute();
    }

    private void readNodesByPath() {
        FetchNodesQuery query = new FetchNodesQuery(mongoConnection,
                path, revisionId);
        query.setBranchId(branchId);
        query.setDepth(depth);
        nodeMongos = query.execute();
    }

    private boolean verifyNodeHierarchy() {
        boolean verified = false;

        verified = verifyNodeHierarchyRec(path, 0);

        if (!verified) {
            LOG.error(String.format("Node hierarchy could not be verified because"
                    + " some nodes were inconsistent: %s", path));
        }

        return verified;
    }

    private boolean verifyNodeHierarchyRec(String path, int currentDepth) {
        boolean verified = false;

        if (pathAndNodeMap.isEmpty()) {
            return true;
        }

        NodeMongo nodeMongo = pathAndNodeMap.get(path);
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

    private boolean verifyProblematicNodes() {
        boolean verified = true;

        for (Map.Entry<String, Long> entry : problematicNodes.entrySet()) {
            String path = entry.getKey();
            Long revisionId = entry.getValue();

            NodeMongo nodeMongo = pathAndNodeMap.get(path);
            if (nodeMongo != null) {
                if (!revisionId.equals(nodeMongo.getRevisionId())) {
                    verified = false;

                    LOG.error(String
                            .format("Node could not be verified because the expected revisionId did not match: %d (expected) vs %d (actual)",
                                    revisionId, nodeMongo.getRevisionId()));

                    break;
                }
            }
        }

        return verified;
    }
}
