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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * {@link MicroKernel}-based {@link NodeStore} implementation.
 */
public class KernelNodeStore extends AbstractNodeStore {

    /**
     * The {@link MicroKernel} instance used to store the content tree.
     */
    private final MicroKernel kernel;

    /**
     * Value factory backed by the {@link #kernel} instance.
     */
    private final CoreValueFactory valueFactory;

    /**
     * State of the current root node.
     */
    private KernelNodeState root;

    public KernelNodeStore(MicroKernel kernel) {
        this.kernel = kernel;
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
    public NodeStateBuilder getBuilder(NodeState base) {
        if (!(base instanceof KernelNodeState)) {
            throw new IllegalArgumentException("Alien node state");
        }

        KernelNodeState kernelNodeState = (KernelNodeState) base;
        String branchRevision = kernel.branch(kernelNodeState.getRevision());
        String path = kernelNodeState.getPath();
        return KernelNodeStateBuilder.create(kernel, valueFactory, path, branchRevision);
    }

    @Override
    public void apply(NodeStateBuilder builder) throws CommitFailedException {
        if (!(builder instanceof  KernelNodeStateBuilder)) {
            throw new IllegalArgumentException("Alien builder");
        }
        
        KernelNodeStateBuilder kernelNodeStateBuilder = (KernelNodeStateBuilder) builder;
        kernelNodeStateBuilder.apply();
    }

    @Override
    public CoreValueFactory getValueFactory() {
        return valueFactory;
    }

}
