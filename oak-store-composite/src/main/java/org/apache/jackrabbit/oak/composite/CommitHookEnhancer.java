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
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

import static org.apache.jackrabbit.guava.common.collect.Maps.newHashMap;

class CommitHookEnhancer implements CommitHook {

    private final CompositionContext ctx;

    private final CommitHook hook;

    CommitHookEnhancer(CommitHook hook, CompositionContext ctx) {
        this.ctx = ctx;
        this.hook = hook;
    }

    @NotNull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info)
        throws CommitFailedException {
        Map<MountedNodeStore, NodeState> beforeStates = newHashMap();
        Map<MountedNodeStore, NodeState> afterStates = newHashMap();
        for (MountedNodeStore mns : ctx.getNonDefaultStores()) {
            NodeState root = mns.getNodeStore().getRoot();
            afterStates.put(mns, root);
            beforeStates.put(mns, root);
        }
        afterStates.put(ctx.getGlobalStore(), after);
        beforeStates.put(ctx.getGlobalStore(), before);

        CompositeNodeState compositeBefore = ctx.createRootNodeState(beforeStates);
        CompositeNodeState compositeAfter = ctx.createRootNodeState(afterStates);

        NodeState result = hook.processCommit(compositeBefore, compositeAfter, info);
        if (result instanceof CompositeNodeState) {
            return ((CompositeNodeState) result).getNodeState(ctx.getGlobalStore());
        } else {
            throw new IllegalStateException(
                "The commit hook result should be a composite node state");
        }
    }
}
