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
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

/**
 * {@code NodeStore} implementations against {@link MicroKernel}.
 */
public class KernelNodeStore implements NodeStore {

    /**
     * The {@link MicroKernel} instance used to store the content tree.
     */
    private final MicroKernel kernel;

    /**
     * Commit hook.
     */
    @Nonnull
    private volatile CommitHook hook = EmptyHook.INSTANCE;

    /**
     * Change observer.
     */
    @Nonnull
    private volatile Observer observer = EmptyObserver.INSTANCE;

    /**
     * State of the current root node.
     */
    private KernelNodeState root;

    public KernelNodeStore(MicroKernel kernel) {
        assert kernel != null;
        this.kernel = kernel;
        this.root = new KernelNodeState(kernel, "/", kernel.getHeadRevision());
    }

    @Nonnull
    public CommitHook getHook() {
        return hook;
    }

    public void setHook(CommitHook hook) {
        assert hook != null;
        this.hook = hook;
    }

    @Nonnull
    public Observer getObserver() {
        return observer;
    }

    public void setObserver(Observer observer) {
        assert observer != null;
        this.observer = observer;
    }

    //----------------------------------------------------------< NodeStore >---

    @Override
    public synchronized NodeState getRoot() {
        String revision = kernel.getHeadRevision();
        if (!revision.equals(root.getRevision())) {
            NodeState before = root;
            root = new KernelNodeState(kernel, "/", kernel.getHeadRevision());
            observer.contentChanged(this, before, root);
        }
        return root;
    }

    @Override
    public NodeStoreBranch branch() {
        return new KernelNodeStoreBranch(this);
    }

    @Override
    public NodeBuilder getBuilder(NodeState base) {
        if (base instanceof KernelNodeState) {
            KernelNodeState kbase = (KernelNodeState) base;
            if ("/".equals(kbase.getPath())) {
                return new KernelRootBuilder(kernel, kbase.getRevision());
            }
        }
        return new MemoryNodeBuilder(base);
    }

    @Override
    public CoreValueFactory getValueFactory() {
        return new CoreValueFactoryImpl(kernel);
    }

    //-----------------------------------------------------------< internal >---

    @Nonnull
    MicroKernel getKernel() {
        return kernel;
    }

}
