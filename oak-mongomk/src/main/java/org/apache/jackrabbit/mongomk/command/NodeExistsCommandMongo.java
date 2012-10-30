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

import org.apache.jackrabbit.mongomk.api.command.DefaultCommand;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * A {@code Command} for determine whether a node exists from {@code MongoDB}.
 */
public class NodeExistsCommandMongo extends DefaultCommand<Boolean> {

    private final MongoConnection mongoConnection;
    private final Long revisionId;

    private String branchId;
    private Node parentNode;
    private String path;

    /**
     * Constructs a new {@code NodeExistsCommandMongo}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param path The root path of the nodes to get.
     * @param revisionId The revision id or null.
     */
    public NodeExistsCommandMongo(MongoConnection mongoConnection, String path,
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
        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection,
                parentPath, revisionId);
        command.setBranchId(branchId);
        parentNode = command.execute();
    }

    private boolean childExists() {
        String childName = PathUtils.getName(path);
        return parentNode.getChildNodeEntry(childName) != null;
    }
}