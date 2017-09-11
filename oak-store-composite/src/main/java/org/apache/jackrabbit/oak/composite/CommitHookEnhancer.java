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
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Maps.newHashMap;

class CommitHookEnhancer implements CommitHook {

    private final CompositionContext ctx;

    private final CompositeNodeBuilder builder;

    private final CommitHook hook;

    private Optional<CompositeNodeBuilder> updatedBuilder = Optional.empty();

    CommitHookEnhancer(CommitHook hook, CompositionContext ctx, CompositeNodeBuilder builder) {
        this.ctx = ctx;
        this.builder = builder;
        this.hook = hook;
    }

    @Nonnull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
        Map<MountedNodeStore, NodeState> beforeStates = newHashMap();
        Map<MountedNodeStore, NodeState> afterStates = newHashMap();
        for (MountedNodeStore mns : ctx.getNonDefaultStores()) {
            if (mns.getMount().isReadOnly()) {
                NodeState root = mns.getNodeStore().getRoot();
                afterStates.put(mns, root);
                beforeStates.put(mns, root);
            } else {
                afterStates.put(mns, mns.getNodeStore().rebase(builder.getNodeBuilder(mns)));
                beforeStates.put(mns, builder.getNodeBuilder(mns).getBaseState());
            }
        }
        afterStates.put(ctx.getGlobalStore(), after);
        beforeStates.put(ctx.getGlobalStore(), before);

        CompositeNodeState compositeBefore = ctx.createRootNodeState(beforeStates);
        CompositeNodeState compositeAfter = ctx.createRootNodeState(afterStates);

        NodeState result = hook.processCommit(compositeBefore, compositeAfter, info);
        updatedBuilder = Optional.of(toComposite(result, compositeBefore));

        if (result instanceof CompositeNodeState) {
            return ((CompositeNodeState) result).getNodeState(ctx.getGlobalStore());
        } else {
            throw new IllegalStateException("The commit hook result should be a composite node state");
        }
    }

    Optional<CompositeNodeBuilder> getUpdatedBuilder() {
        return updatedBuilder;
    }

    private CompositeNodeBuilder toComposite(NodeState nodeState, CompositeNodeState compositeRoot) {
        CompositeNodeBuilder builder = compositeRoot.builder();
        nodeState.compareAgainstBaseState(compositeRoot, new ApplyDiff(builder));
        return builder;
    }
}
