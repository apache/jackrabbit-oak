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
package org.apache.jackrabbit.oak.index.indexer.document.tree.nodeStore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TreeNodeStore implements NodeStore {
    
    private final TreeStore treeStore;
    
    private TreeNodeStore(TreeStore treeStore) {
        this.treeStore = treeStore;
    }
    
    public static NodeStore create(String storeArg, Closer closer) {
        String dir = storeArg.substring("tree:".length());
        File directory = new File(dir);
        MemoryBlobStore blobStore = new MemoryBlobStore();
        NodeStateEntryReader entryReader = new NodeStateEntryReader(blobStore);
        TreeStore treeStore = new TreeStore(directory, entryReader);
        closer.register(treeStore);
        return new TreeNodeStore(treeStore);
    }

    @Override
    public @NotNull NodeState getRoot() {
        return treeStore.getNodeStateEntry("/").getNodeState();
    }

    @Override
    public @NotNull String checkpoint(long lifetime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Iterable<String> checkpoints() {
        return Collections.emptyList();
    }

    @Override
    public @NotNull Blob createBlob(InputStream inputStream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Blob getBlob(@NotNull String reference) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull NodeState merge(
            @NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
            @NotNull CommitInfo info) throws CommitFailedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean release(@NotNull String checkpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable NodeState retrieve(@NotNull String checkpoint) {
        throw new UnsupportedOperationException();
    }

}
