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

import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * {@code Command} for {@code MongoMicroKernel#nodeExists(String, String)}
 */
public class NodeExistsCommand extends BaseCommand<Boolean> {

    private final Long revisionId;

    private String branchId;
    private Node parentNode;
    private String path;

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
        if (PathUtils.denotesRoot(path)) {
            return true;
        }

        // Check that all the paths up to the root actually exist.
        return pathExists();
    }

    private boolean pathExists() throws Exception {
        while (!PathUtils.denotesRoot(path)) {
            readParentNode(revisionId, branchId);
            if (parentNode == null || !childExists()) {
                return false;
            }
            path = PathUtils.getParentPath(path);
        }

        return true;
    }

    private void readParentNode(Long revisionId, String branchId) throws Exception {
        String parentPath = PathUtils.getParentPath(path);
        GetNodesCommand command = new GetNodesCommand(nodeStore, parentPath, revisionId);
        command.setBranchId(branchId);
        parentNode = command.execute();
    }

    private boolean childExists() {
        String childName = PathUtils.getName(path);
        return parentNode.getChildNodeEntry(childName) != null;
    }
}