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

    public static String trimSlash(String str) {
        int startIndex = 0;
        int endIndex = str.length() - 1;
        if (str.charAt(startIndex) == '/') {
            startIndex++;
        }
        if (str.charAt(endIndex) == '/') {
            endIndex--;
        }
        return str.substring(startIndex, endIndex + 1);
    }

    public static long getMillisFromString(String strDate) {
        long millis = ISO8601.parse(strDate).getTimeInMillis();
        return millis;
    }

    public static String getBaseIndexName(String versionedIndexName) {
        String indexBaseName = versionedIndexName.split("-")[0];
        return indexBaseName;
    }

    public static NodeBuilder getNode(@NotNull NodeBuilder nodeBuilder, @NotNull String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nodeBuilder = nodeBuilder.getChildNode(checkNotNull(name));
        }
        return nodeBuilder;
    }

    public static void recursiveDeleteHiddenChildNodes(NodeStore store, String trimmedPath) throws CommitFailedException {
        NodeState nodeState = NodeStateUtils.getNode(store.getRoot(), "/" + trimmedPath);
        Iterable<String> childNodeNames = nodeState.getChildNodeNames();
        for (String childNodeName : childNodeNames) {
            if (NodeStateUtils.isHidden(childNodeName)) {
                RecursiveDelete recursiveDelete = new RecursiveDelete(store, EmptyHook.INSTANCE, () -> CommitInfo.EMPTY);
                recursiveDelete.run("/" + trimmedPath + "/" + childNodeName);
            }
        }
    }
}
