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
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.NodeStateEditor;
import org.apache.jackrabbit.mk.model.NodeStore;

/**
 * {@link MicroKernel}-based {@link NodeStore} implementation.
 */
public class KernelNodeStore implements NodeStore {

    private final MicroKernel kernel;

    public KernelNodeStore(MicroKernel kernel) {
        this.kernel = kernel;
    }

    @Override
    public NodeState getRoot() {
        return new KernelNodeState(kernel, "/", kernel.getHeadRevision());
    }

    @Override
    public NodeStateEditor branch(NodeState base) {
        return null; // todo implement branch
    }

    @Override
    public NodeState merge(NodeStateEditor branch, NodeState base) {
        return null; // todo implement merge
    }

}
