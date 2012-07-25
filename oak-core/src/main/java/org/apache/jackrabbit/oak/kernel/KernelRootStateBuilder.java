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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class KernelRootStateBuilder extends MemoryNodeStateBuilder {

    /**
     * Number of content updates that need to happen before the updates
     * are automatically committed to a branch in the MicroKernel.
     */
    private static final int UPDATE_LIMIT = 10000;

    private final MicroKernel kernel;

    private String baseRevision;

    private String branchRevision;

    private int updates = 0;

    public KernelRootStateBuilder(MicroKernel kernel, String revision) {
        super(new KernelNodeState(kernel, "/", revision));
        this.kernel = kernel;
        this.baseRevision = revision;
        this.branchRevision = null;
    }

    @Override
    protected MemoryNodeStateBuilder createChildBuilder(
            String name, NodeState child) {
        return new KernelNodeStateBuilder(this, name, child, this);
    }

    @Override
    protected void updated() {
        if (updates++ > UPDATE_LIMIT) {
            if (branchRevision == null) {
                branchRevision = kernel.branch(baseRevision);
            }

            updates = 0;
        }
    }

}
