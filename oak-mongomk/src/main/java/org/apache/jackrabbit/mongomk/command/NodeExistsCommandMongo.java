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

import java.util.Set;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.api.command.AbstractCommand;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * A {@code Command} for determine whether a node exists from {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class NodeExistsCommandMongo extends AbstractCommand<Boolean> {

    private final MongoConnection mongoConnection;
    private final String revisionId;

    private Node parentNode;
    private String path;

    /**
     * Constructs a new {@code NodeExistsCommandMongo}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param path The root path of the nodes to get.
     * @param revisionId The {@link RevisionId} or {@code null}.
     */
    public NodeExistsCommandMongo(MongoConnection mongoConnection, String path, String revisionId) {
        this.mongoConnection = mongoConnection;
        this.path = path;
        this.revisionId = revisionId;
    }

    @Override
    public Boolean execute() throws Exception {
        if (PathUtils.denotesRoot(path)) {
            return true;
        }

        // Check that all the paths up to the parent are valid.
        while (!PathUtils.denotesRoot(path)) {
            readParentNode();
            if (!childExists()) {
                return false;
            }
            path = PathUtils.getParentPath(path);
        }

        return true;
    }

    private void readParentNode() throws Exception {
        String parentPath = PathUtils.getParentPath(path);
        // TODO - This used to be FetchNodeByPathQuery but changed to GetNodesCommandMongo to make
        // sure nodes are in a valid commit etc. Check if GetNodesCommandMongo is really needed.
        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, parentPath, revisionId, -1);
        parentNode = command.execute();
    }

    private boolean childExists() {
        if (parentNode == null) {
            return false;
        }

        Set<Node> children = parentNode.getChildren();
        if (children == null || children.isEmpty()) {
            return false;
        }

        for (Node child : children) {
            if (child.getName().equals(PathUtils.getName(path))) {
                return true;
            }
        }

        return false;
    }
}
