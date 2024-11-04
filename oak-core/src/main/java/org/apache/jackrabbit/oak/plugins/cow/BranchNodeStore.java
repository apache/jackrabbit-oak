/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.cow;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class BranchNodeStore implements NodeStore, Observable {

    private static final long CHECKPOINT_LIFETIME = TimeUnit.HOURS.toMillis(24);

    private final NodeStore nodeStore;

    private final MemoryNodeStore memoryNodeStore;

    private final Collection<String> inheritedCheckpoints;

    private final Map<String, String> checkpointMapping;

    public BranchNodeStore(NodeStore nodeStore) throws CommitFailedException {
        this.nodeStore = nodeStore;
        this.inheritedCheckpoints = CollectionUtils.toList(nodeStore.checkpoints());
        this.checkpointMapping = new ConcurrentHashMap<>();

        String cp = nodeStore.checkpoint(CHECKPOINT_LIFETIME, singletonMap("type", "copy-on-write"));
        memoryNodeStore = new MemoryNodeStore(nodeStore.retrieve(cp));
    }

    public void dispose() {
        for (String cp : nodeStore.checkpoints()) {
            if ("copy-on-write".equals(nodeStore.checkpointInfo(cp).get("type"))) {
                nodeStore.release(cp);
            }
        }
    }

    @NotNull
    @Override
    public NodeState getRoot() {
        return memoryNodeStore.getRoot();
    }

    @NotNull
    @Override
    public synchronized NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
        return memoryNodeStore.merge(builder, commitHook, info);
    }

    @NotNull
    @Override
    public NodeState rebase(@NotNull NodeBuilder builder) {
        return memoryNodeStore.rebase(builder);
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        return memoryNodeStore.reset(builder);
    }

    @NotNull
    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        return memoryNodeStore.createBlob(inputStream);
    }

    @Override
    public Blob getBlob(@NotNull String reference) {
        return memoryNodeStore.getBlob(reference);
    }

    @NotNull
    @Override
    public String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        String checkpoint = memoryNodeStore.checkpoint(lifetime, properties);
        String uuid = UUID.randomUUID().toString();
        checkpointMapping.put(uuid, checkpoint);
        return uuid;
    }

    @NotNull
    @Override
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, emptyMap());
    }


    @NotNull
    @Override
    public Iterable<String> checkpoints() {
        List<String> result = new ArrayList<>(inheritedCheckpoints);
        result.retainAll(CollectionUtils.toList(nodeStore.checkpoints()));

        checkpointMapping.entrySet().stream()
                .filter(e -> memoryNodeStore.listCheckpoints().contains(e.getValue()))
                .map(Map.Entry::getKey)
                .forEach(result::add);

        return result;
    }

    @NotNull
    @Override
    public Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        if (inheritedCheckpoints.contains(checkpoint)) {
            return nodeStore.checkpointInfo(checkpoint);
        } else if (checkpointMapping.containsKey(checkpoint)) {
            return memoryNodeStore.checkpointInfo(checkpointMapping.get(checkpoint));
        } else {
            return emptyMap();
        }
    }

    @Override
    public NodeState retrieve(@NotNull String checkpoint) {
        if (inheritedCheckpoints.contains(checkpoint)) {
            return nodeStore.retrieve(checkpoint);
        } else if (checkpointMapping.containsKey(checkpoint)) {
            return memoryNodeStore.retrieve(checkpointMapping.get(checkpoint));
        } else {
            return null;
        }
    }

    @Override
    public boolean release(@NotNull String checkpoint) {
        if (inheritedCheckpoints.contains(checkpoint)) {
            return nodeStore.release(checkpoint);
        } else if (checkpointMapping.containsKey(checkpoint)) {
            return memoryNodeStore.release(checkpointMapping.remove(checkpoint));
        } else {
            return false;
        }
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return memoryNodeStore.addObserver(observer);
    }
}
