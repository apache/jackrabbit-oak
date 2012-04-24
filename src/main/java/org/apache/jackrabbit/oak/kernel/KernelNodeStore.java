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
import org.apache.jackrabbit.oak.api.CoreValueFactory;

/**
 * {@link MicroKernel}-based {@link NodeStore} implementation.
 */
public class KernelNodeStore implements NodeStore {

    final MicroKernel kernel;
    final CoreValueFactory valueFactory;

    public KernelNodeStore(MicroKernel kernel, CoreValueFactory valueFactory) {
        this.kernel = kernel;
        this.valueFactory = valueFactory;
    }

    @Override
    public NodeState getRoot() {
        return new KernelNodeState(kernel, valueFactory, "/", kernel.getHeadRevision());
    }

    @Override
    public void compare(NodeState before, NodeState after, NodeStateDiff diff) {
        throw new UnsupportedOperationException(); // TODO
    }

    // TODO clarify write access to store. Expose through interface
    void save(KernelRoot root, NodeState base) {
        if (!(base instanceof KernelNodeState)) {
            throw new IllegalArgumentException("Base state is not from this store");
        }

        KernelNodeState baseState = (KernelNodeState) base;
        String targetPath = baseState.getPath();
        String targetRevision = baseState.getRevision();
        String jsop = root.getChanges();
        kernel.commit(targetPath, jsop, targetRevision, null);
    }
}
