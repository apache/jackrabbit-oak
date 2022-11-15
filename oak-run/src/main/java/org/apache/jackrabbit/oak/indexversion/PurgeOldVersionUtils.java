/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.indexversion;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.RecursiveDelete;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

public class PurgeOldVersionUtils {

    public static final String OAK_INDEX = "oak:index";

    public static long getMillisFromString(String strDate) {
        return ISO8601.parse(strDate).getTimeInMillis();
    }

    /**
     * @param nodeBuilder
     * @param path        Path of node whose nodeBuilder object should be returned.
     * @return nodeBuilder object of node at @param{path}
     */
    public static NodeBuilder getNode(@NotNull NodeBuilder nodeBuilder, @NotNull String path) {
        NodeBuilder resultNodeBuilder = nodeBuilder;
        for (String name : PathUtils.elements(checkNotNull(path))) {
            resultNodeBuilder = resultNodeBuilder.getChildNode(checkNotNull(name));
        }
        return resultNodeBuilder;
    }

    /**
     * @param store
     * @param path
     * @throws CommitFailedException recursively deletes child nodes under path
     */
    public static void recursiveDeleteHiddenChildNodes(NodeStore store, String path) throws CommitFailedException {
        NodeState nodeState = NodeStateUtils.getNode(store.getRoot(), path);
        Iterable<String> childNodeNames = nodeState.getChildNodeNames();
        for (String childNodeName : childNodeNames) {
            if (NodeStateUtils.isHidden(childNodeName)) {
                if (childNodeName.startsWith(IndexDefinition.HIDDEN_OAK_MOUNT_PREFIX)) {
                    continue;
                }
                RecursiveDelete recursiveDelete = new RecursiveDelete(store, EmptyHook.INSTANCE, () -> CommitInfo.EMPTY);
                recursiveDelete.run(path + "/" + childNodeName);
            }
        }
    }

    /**
     * @param commandlineIndexPath
     * @param repositoryIndexPath
     * @return true if baseIndexName at  commandlineIndexPath and repositoryIndexPath are equal
     */
    public static boolean isBaseIndexEqual(String commandlineIndexPath, String repositoryIndexPath) {
        if (PathUtils.getName(PathUtils.getParentPath(commandlineIndexPath)).equals(OAK_INDEX)) {
            String commandlineIndexBaseName = IndexName.parse(commandlineIndexPath).getBaseName();
            String repositoryIndexBaseName = IndexName.parse(repositoryIndexPath).getBaseName();
            if (commandlineIndexBaseName.equals(repositoryIndexBaseName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param commandlineIndexPath
     * @param repositoryIndexPath
     * @return true if repositoryIndexPath is child of commandlineIndexPath
     */
    public static boolean isIndexChildNode(String commandlineIndexPath, String repositoryIndexPath) {
        if (PathUtils.getName(commandlineIndexPath).equals(OAK_INDEX)) {
            if (repositoryIndexPath.startsWith(commandlineIndexPath)) {
                return true;
            }
        }
        return false;
    }
}
