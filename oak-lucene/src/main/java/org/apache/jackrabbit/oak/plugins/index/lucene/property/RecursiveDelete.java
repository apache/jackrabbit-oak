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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import java.util.function.Supplier;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecursiveDelete {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeStore nodeStore;
    private final CommitHook commitHook;
    private final Supplier<CommitInfo> commitInfo;
    private int batchSize = 1024;
    private int numRemoved = 0;
    private int mergeCount;
    private NodeBuilder builder;

    public RecursiveDelete(NodeStore nodeStore, CommitHook commitHook,
                           Supplier<CommitInfo> commitInfo) {
        this.nodeStore = nodeStore;
        this.commitHook = commitHook;
        this.commitInfo = commitInfo;
    }

    public void run(Iterable<String> paths) throws CommitFailedException {
        Stopwatch w = Stopwatch.createStarted();
        NodeState root = nodeStore.getRoot();
        builder = root.builder();
        int currentSize = 0;
        for (String path : paths) {
            NodeState node = NodeStateUtils.getNode(root, path);
            currentSize = delete(node, path);
            save(path, currentSize, false);
        }

        String pathDetails = Iterables.toString(paths);
        save(pathDetails, currentSize, true);
        log.debug("Removed subtree under [{}] with {} child nodes " +
                "in {} ({} saves)", pathDetails, numRemoved, w, mergeCount);
    }

    public void run(String path) throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        builder = root.builder();
        NodeState node = NodeStateUtils.getNode(root, path);
        Stopwatch w = Stopwatch.createStarted();

        int currentSize = delete(node, path);
        save(path, currentSize, true);

        log.debug("Removed subtree under [{}] with {} child nodes " +
                "in {} ({} saves)", path, numRemoved, w, mergeCount);
    }

    public int getNumRemoved() {
        return numRemoved;
    }

    public int getMergeCount() {
        return mergeCount;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    private int delete(NodeState node, String path) throws CommitFailedException {
        int currentSize = deleteChildNodes(node, path);
        child(builder, path).remove();
        numRemoved++;
        return currentSize + 1;
    }

    private int deleteChildNodes(NodeState node, String path) throws CommitFailedException {
        int currentSize = 0;
        for (ChildNodeEntry cne : node.getChildNodeEntries()) {
            String name = cne.getName();
            String childPath = PathUtils.concat(path, name);

            currentSize += delete(cne.getNodeState(), childPath);
            if (save(childPath, currentSize, false)) {
                currentSize = 0;
            }
        }
        return currentSize;
    }

    private boolean save(String pathDetails, int currentSize, boolean force) throws CommitFailedException {
        if (currentSize >= batchSize || force) {
            log.debug("Deleting {} nodes under {} ({} removed so far)", currentSize, pathDetails, numRemoved);
            nodeStore.merge(builder, commitHook, commitInfo.get());
            builder = nodeStore.getRoot().builder();
            mergeCount++;
            return true;
        }
        return false;
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(path)) {
            nb = nb.getChildNode(name);
        }
        return nb;
    }
}
