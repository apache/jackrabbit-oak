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
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.api.command.AbstractCommand;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.query.FetchNodesByPathAndDepthQuery;
import org.apache.jackrabbit.mongomk.query.FetchValidCommitsQuery;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code Command} for getting nodes from {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class GetNodesCommandMongo extends AbstractCommand<Node> {

    class InconsitentNodeHierarchyException extends Exception {
        private static final long serialVersionUID = 8155418280936077632L;
    }

    private static final Logger LOG = LoggerFactory.getLogger(GetNodesCommandMongo.class);

    private final MongoConnection mongoConnection;
    private final String path;
    private final int depth;

    private String revisionId;
    private List<CommitMongo> lastCommits;
    private List<NodeMongo> nodeMongos;

    private Map<String, NodeMongo> pathAndNodeMap;
    private Map<String, Long> problematicNodes;
    private Node rootOfPath;

    /**
     * Constructs a new {@code GetNodesCommandMongo}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param path The root path of the nodes to get.
     * @param revisionId The {@link RevisionId} or {@code null}.
     * @param depth The depth.
     */
    public GetNodesCommandMongo(MongoConnection mongoConnection, String path,
            String revisionId, int depth) {
        this.mongoConnection = mongoConnection;
        this.path = path;
        this.revisionId = revisionId;
        this.depth = depth;
    }

    @Override
    public Node execute() throws Exception {
        ensureRevisionId();
        readLastCommits();
        deriveProblematicNodes();

        readNodesByPath();
        createPathAndNodeMap();
        boolean verified = verifyProblematicNodes() && verifyNodeHierarchy();

        if (!verified) {
            throw new InconsitentNodeHierarchyException();
        }

        this.buildNodeStructure();

        return rootOfPath;
    }

    @Override
    public int getNumOfRetries() {
        return 3;
    }

    @Override
    public boolean needsRetry(Exception e) {
        return e instanceof InconsitentNodeHierarchyException;
    }

    private void buildNodeStructure() {
        NodeMongo nodeMongoRootOfPath = pathAndNodeMap.get(path);
        rootOfPath = this.buildNodeStructure(nodeMongoRootOfPath);
    }

    private NodeImpl buildNodeStructure(NodeMongo nodeMongo) {
        NodeImpl node = NodeMongo.toNode(nodeMongo);
        Set<Node> children = node.getChildren();
        if (children != null) {
            for (Node child : children) {
                NodeMongo nodeMongoChild = pathAndNodeMap.get(child.getPath());
                if (nodeMongoChild != null) {
                    NodeImpl nodeChild = this.buildNodeStructure(nodeMongoChild);
                    node.addChild(nodeChild);
                }
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
        }
    }

    private void readLastCommits() throws Exception {
        lastCommits = new FetchValidCommitsQuery(mongoConnection, revisionId).execute();

        // TODO Move this into the Query which should throw the exception in case the commit doesn't exist
        if (revisionId != null) {
            boolean revisionExists = false;
            long revId = MongoUtil.toMongoRepresentation(revisionId);
            for (CommitMongo commitMongo : lastCommits) {
                if (commitMongo.getRevisionId() == revId) {
                    revisionExists = true;

                    break;
                }
            }

            if (!revisionExists) {
                throw new Exception(String.format("The revisionId %d could not be found", revId));
            }
        }
    }

    private void readNodesByPath() {
        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection, path, revisionId,
                depth);
        nodeMongos = query.execute();
    }

    private boolean verifyNodeHierarchy() {
        boolean verified = false;

        verified = verifyNodeHierarchyRec(path, 0);

        if (!verified) {
            LOG.error(String.format("Node hierarchy could not be verified because some nodes were inconsistent: %s",
                    path));
        }

        return verified;
    }

    private boolean verifyNodeHierarchyRec(String path, int currentDepth) {
        boolean verified = false;

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
