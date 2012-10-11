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

import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import static com.google.common.base.Preconditions.checkNotNull;

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

    private final LoadingCache<String, KernelNodeState> cache =
            CacheBuilder.newBuilder().maximumSize(10000).build(
                    new CacheLoader<String, KernelNodeState>() {
                        @Override
                        public KernelNodeState load(String key) {
                            int slash = key.indexOf('/');
                            String revision = key.substring(0, slash);
                            String path = key.substring(slash);
                            return new KernelNodeState(
                                    kernel, path, revision, cache);
                        }
                    });

    /**
     * State of the current root node.
     */
    private KernelNodeState root;

    public KernelNodeStore(MicroKernel kernel) {
        this.kernel = checkNotNull(kernel);
        try {
            this.root = cache.get(kernel.getHeadRevision() + "/");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    public CommitHook getHook() {
        return hook;
    }

    public void setHook(CommitHook hook) {
        this.hook = checkNotNull(hook);
    }

    @Nonnull
    public Observer getObserver() {
        return observer;
    }

    public void setObserver(Observer observer) {
        this.observer = checkNotNull(observer);
    }

    //----------------------------------------------------------< NodeStore >---

    @Override
    public synchronized KernelNodeState getRoot() {
        String revision = kernel.getHeadRevision();
        if (!revision.equals(root.getRevision())) {
            NodeState before = root;
            root = getRootState(revision);
            observer.contentChanged(before, root);
        }
        return root;
    }

    @Override
    public NodeStoreBranch branch() {
        return new KernelNodeStoreBranch(this, getRoot());
    }

    //-----------------------------------------------------------< internal >---

    @Nonnull
    MicroKernel getKernel() {
        return kernel;
    }

    KernelNodeState getRootState(String revision) {
        try {
            return cache.get(revision + "/");
        } catch (ExecutionException e) {
            throw new MicroKernelException(e);
        }
    }

}
