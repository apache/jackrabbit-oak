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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class TarNodeStore implements NodeStore {

    private final NodeStore ns;

    private final SuperRootProvider superRootProvider;

    public TarNodeStore(NodeStore ns, SuperRootProvider superRootProvider) {
        this.ns = ns;
        this.superRootProvider = superRootProvider;
    }

    public void setSuperRoot(NodeBuilder builder) {
        superRootProvider.setSuperRoot(builder);
    }

    public NodeState getSuperRoot() {
        return superRootProvider.getSuperRoot();
    }

    @Nonnull
    @Override
    public NodeState getRoot() {
        return ns.getRoot();
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook, @Nonnull CommitInfo info) throws CommitFailedException {
        return ns.merge(builder, commitHook, info);
    }

    @Nonnull
    @Override
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        return ns.rebase(builder);
    }

    @Override
    public NodeState reset(@Nonnull NodeBuilder builder) {
        return ns.reset(builder);
    }

    @Nonnull
    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        return ns.createBlob(inputStream);
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        return ns.getBlob(reference);
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        return ns.checkpoint(lifetime, properties);
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime) {
        return ns.checkpoint(lifetime);
    }

    @Nonnull
    @Override
    public Map<String, String> checkpointInfo(@Nonnull String checkpoint) {
        return ns.checkpointInfo(checkpoint);
    }

    @Nonnull
    @Override
    public Iterable<String> checkpoints() {
        return ns.checkpoints();
    }

    @Override
    public NodeState retrieve(@Nonnull String checkpoint) {
        return ns.retrieve(checkpoint);
    }

    @Override
    public boolean release(@Nonnull String checkpoint) {
        return ns.release(checkpoint);
    }

    interface SuperRootProvider {

        void setSuperRoot(NodeBuilder builder);

        NodeState getSuperRoot();

    }
}
