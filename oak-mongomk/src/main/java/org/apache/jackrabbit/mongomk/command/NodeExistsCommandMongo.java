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

import java.util.List;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.api.command.AbstractCommand;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.query.FetchNodeByPathQuery;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * A {@code Command} for determine whether a node exists from {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class NodeExistsCommandMongo extends AbstractCommand<Boolean> {
    private final MongoConnection mongoConnection;
    private NodeMongo parentNode;
    private final String path;
    private String revisionId;

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
        ensureRevisionId();
        readParentNode();
        return childExists();
    }

    private void ensureRevisionId() throws Exception {
        if (revisionId == null) {
            revisionId = new GetHeadRevisionCommandMongo(mongoConnection).execute();
        }
    }

    private void readParentNode() throws Exception {
        String parentPath = PathUtils.getParentPath(path);
        FetchNodeByPathQuery query = new FetchNodeByPathQuery(mongoConnection, parentPath, MongoUtil.toMongoRepresentation(revisionId));
        parentNode = query.execute();
    }

    private boolean childExists() {
        if (parentNode == null) {
            return false;
        }

        List<String> children = parentNode.getChildren();
        if (children == null || children.isEmpty()) {
            return false;
        }

        for (String child : children) {
            if (child.equals(PathUtils.getName(path))) {
                return true;
            }
        }

        return false;
    }
}
