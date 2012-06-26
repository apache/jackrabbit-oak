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
package org.apache.jackrabbit.oak.kernel;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

/**
 * {@code NodeStore} implementations against {@link MicroKernel}.
 */
public class KernelNodeStore extends MemoryNodeStore {

    /**
     * The {@link MicroKernel} instance used to store the content tree.
     */
    private final MicroKernel kernel;

    /**
     * Commit hook.
     */
    private final CommitEditor commitHook;

    /**
     * Value factory backed by the {@link #kernel} instance.
     */
    private final CoreValueFactory valueFactory;

    /**
     * State of the current root node.
     */
    private KernelNodeState root;

    public KernelNodeStore(MicroKernel kernel, CommitEditor commitHook) {
        assert kernel != null;
        assert commitHook != null;

        this.kernel = kernel;
        this.commitHook = commitHook;
        this.valueFactory = new CoreValueFactoryImpl(kernel);
        this.root = new KernelNodeState(
                kernel, valueFactory, "/", kernel.getHeadRevision());
    }

    @Override
    public synchronized NodeState getRoot() {
        String revision = kernel.getHeadRevision();
        if (!revision.equals(root.getRevision())) {
            root = new KernelNodeState(
                    kernel, valueFactory, "/", kernel.getHeadRevision());
        }
        return root;
    }

    @Override
    public NodeStoreBranch branch() {
        return new KernelNodeStoreBranch(this);
    }

    @Override
    public CoreValueFactory getValueFactory() {
        return valueFactory;
    }

    //------------------------------------------------------------< internal >---

    @Nonnull
    MicroKernel getKernel() {
        return kernel;
    }

    @Nonnull
    CommitEditor getCommitHook() {
        return commitHook;
    }
}
