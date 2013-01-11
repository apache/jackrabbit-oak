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

import java.util.Map;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesActionNew;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;

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
        if (revisionId == null) {
            revisionId = new GetHeadRevisionCommand(nodeStore).execute();
        }

        FetchNodesActionNew action = new FetchNodesActionNew(nodeStore, path, 0, revisionId);
        action.setBranchId(branchId);

        Map<String, MongoNode> pathAndNodeMap = action.execute();
        node = pathAndNodeMap.get(this.path);
        return node != null && !node.isDeleted();
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