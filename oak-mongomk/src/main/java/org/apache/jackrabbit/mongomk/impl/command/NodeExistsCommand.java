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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesActionNew;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * {@code Command} for {@code MongoMicroKernel#nodeExists(String, String)}
 */
public class NodeExistsCommand extends BaseCommand<Boolean> {

    private Long revisionId;
    private String branchId;
    private String path;
    private MongoNode node;

    /**
     * Constructs a new {@code NodeExistsCommandMongo}.
     *
     * @param nodeStore Node store.
     * @param path The root path of the nodes to get.
     * @param revisionId The revision id or null.
     */
    public NodeExistsCommand(MongoNodeStore nodeStore, String path, Long revisionId) {
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

    @Override
    public Boolean execute() throws Exception {
        // To check a path really exists, all the paths from root need to be checked.
        Set<String> paths = new HashSet<String>();
        char[] path = this.path.toCharArray();
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < path.length; i++) {
            if (i == 0) {
                paths.add("/");
            } else if (path[i] == '/') {
                paths.add(current.toString());
            }
            current.append(path[i]);
        }
        paths.add(current.toString());

        if (revisionId == null) {
            revisionId = new GetHeadRevisionCommand(nodeStore).execute();
        }

        FetchNodesActionNew action = new FetchNodesActionNew(nodeStore, paths, revisionId);
        action.setBranchId(branchId);
        //action.setValidCommits(validCommits);

        Map<String, MongoNode> pathAndNodeMap = action.execute();
        String currentPath = this.path;
        while (!PathUtils.denotesRoot(currentPath)) {
            String childName = PathUtils.getName(currentPath);
            String parentPath = PathUtils.getParentPath(currentPath);
            MongoNode parentNode = pathAndNodeMap.get(parentPath);
            if (parentNode == null || !parentNode.childExists(childName)) {
                node = null;
                return false;
            }
            currentPath = PathUtils.getParentPath(currentPath);
        }
        node = pathAndNodeMap.get(this.path);
        return true;
    }

    /**
     * After {@link NodeExistsCommand} executed, this method can be used to access
     * the node with the path.
     *
     * @return Node or null if it does not exist.
     */
    public MongoNode getNode() {
        return node;
    }
}