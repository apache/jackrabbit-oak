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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.HashSet;
import java.util.Set;
/**
 * {@link MicroKernel}-based {@link NodeStore} implementation.
 */
public class KernelNodeStore extends AbstractNodeStore {

    final MicroKernel kernel;
    final CoreValueFactory valueFactory;

    public KernelNodeStore(MicroKernel kernel) {
        this.kernel = kernel;
        this.valueFactory = new CoreValueFactoryImpl(kernel);
    }

    @Override
    public NodeState getRoot() {
        return new KernelNodeState(kernel, valueFactory, "/", kernel.getHeadRevision());
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
