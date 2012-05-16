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
package org.apache.jackrabbit.oak.plugins.memory;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.kernel.CoreValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;

/**
 * Basic in-memory node state implementation.
 */
public class MemoryNodeStore extends AbstractNodeStore {

    private final AtomicReference<NodeState> root =
            new AtomicReference<NodeState>(MemoryNodeState.EMPTY_NODE);

    private final CommitHook commitHook;

    public MemoryNodeStore(CommitHook commitHook) {
        this.commitHook = commitHook;
    }

    @Override
    public NodeState getRoot() {
        return root.get();
    }

    @Override
    public NodeStateBuilder getBuilder(NodeState base) {
        return new MemoryNodeStateBuilder(base);
    }

    @Override
    public CoreValueFactory getValueFactory() {
        return new CoreValueFactoryImpl(null); // FIXME
    }

    @Override
    public void setRoot(NodeState newRoot) throws CommitFailedException {
        NodeState oldRoot;
        do {
            oldRoot = root.get();
            newRoot = rebase(newRoot, oldRoot);
        } while (!root.compareAndSet(
                oldRoot, commitHook.beforeCommit(this, oldRoot, newRoot)));
    }

    NodeState rebase(NodeState state, NodeState base)
            throws CommitFailedException {
        if (state instanceof ModifiedNodeState) {
            return ((ModifiedNodeState) state).rebase(this, base);
        } else if (state.equals(base)) {
            return state;
        } else {
            throw new CommitFailedException("Failed to rebase changes");
        }
    }

}
