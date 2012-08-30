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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

/**
 * Basic in-memory node store implementation. Useful as a base class for
 * more complex functionality.
 */
public class MemoryNodeStore implements NodeStore {

    private final AtomicReference<NodeState> root =
            new AtomicReference<NodeState>(MemoryNodeState.EMPTY_NODE);

    @Override
    public NodeState getRoot() {
        return root.get();
    }

    @Override
    public NodeBuilder getBuilder(NodeState base) {
        return new MemoryNodeBuilder(base);
    }

    @Override
    public NodeStoreBranch branch() {
        return new MemoryNodeStoreBranch(root.get());
    }

    @Override
    public CoreValueFactory getValueFactory() {
        return MemoryValueFactory.INSTANCE;
    }

    private class MemoryNodeStoreBranch implements NodeStoreBranch {

        private final NodeState base;

        private volatile NodeState root;

        public MemoryNodeStoreBranch(NodeState base) {
            this.base = base;
            this.root = base;
        }

        @Override
        public NodeState getBase() {
            return base;
        }

        @Override
        public NodeState getRoot() {
            return root;
        }

        @Override
        public void setRoot(NodeState newRoot) {
            this.root = newRoot;
        }

        @Override
        public NodeState merge() throws CommitFailedException {
            while (!MemoryNodeStore.this.root.compareAndSet(base, root)) {
                // TODO: rebase();
                throw new UnsupportedOperationException();
            }
            return root;
        }

        @Override
        public boolean copy(String source, String target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean move(String source, String target) {
            throw new UnsupportedOperationException();
        }

    }

}
