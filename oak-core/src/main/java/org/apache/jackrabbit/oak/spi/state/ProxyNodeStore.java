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
package org.apache.jackrabbit.oak.spi.state;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

public abstract class ProxyNodeStore implements NodeStore {

    protected abstract NodeStore getNodeStore();

    @Override
    public NodeState getRoot() {
        return getNodeStore().getRoot();
    }

    @Override
    public NodeState merge(
            NodeBuilder builder, CommitHook commitHook, CommitInfo info)
            throws CommitFailedException {
        return getNodeStore().merge(builder, commitHook, info);
    }

    @Override
    public NodeState rebase(NodeBuilder builder) {
        return getNodeStore().rebase(builder);
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        return getNodeStore().reset(builder);
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        return getNodeStore().createBlob(inputStream);
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        return getNodeStore().getBlob(reference);
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        return getNodeStore().checkpoint(lifetime, properties);
    }

    @Override
    public String checkpoint(long lifetime) {
        return getNodeStore().checkpoint(lifetime);
    }

    @Nonnull
    @Override
    public Map<String, String> checkpointInfo(@Nonnull String checkpoint) {
        return getNodeStore().checkpointInfo(checkpoint);
    }

    @Nonnull
    @Override
    public Iterable<String> checkpoints() {
        return getNodeStore().checkpoints();
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        return getNodeStore().retrieve(checkpoint);
    }

    @Override
    public boolean release(String checkpoint) {
        return getNodeStore().release(checkpoint);
    }

}
