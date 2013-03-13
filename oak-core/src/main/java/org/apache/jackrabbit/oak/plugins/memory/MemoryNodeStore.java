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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * Basic in-memory node store implementation. Useful as a base class for
 * more complex functionality.
 */
public class MemoryNodeStore implements NodeStore {

    private final AtomicReference<NodeState> root =
            new AtomicReference<NodeState>(EMPTY_NODE);

    @Override
    public NodeState getRoot() {
        return root.get();
    }

    @Override
    public NodeStoreBranch branch() {
        return new MemoryNodeStoreBranch(this, root.get());
    }

    /**
     * @return An instance of {@link ArrayBasedBlob}.
     */
    @Override
    public ArrayBasedBlob createBlob(InputStream inputStream) throws IOException {
        try {
            return new ArrayBasedBlob(ByteStreams.toByteArray(inputStream));
        }
        finally {
            inputStream.close();
        }
    }

    private static class MemoryNodeStoreBranch implements NodeStoreBranch {

        /** The underlying store to which this branch belongs */
        private final MemoryNodeStore store;

        /** Root state of the base revision of this branch */
        private final NodeState base;

        /** Root state of the head revision of this branch*/
        private volatile NodeState root;

        public MemoryNodeStoreBranch(MemoryNodeStore store, NodeState base) {
            this.store = store;
            this.base = base;
            this.root = base;
        }

        @Override
        public NodeState getBase() {
            return base;
        }

        @Override
        public NodeState getHead() {
            checkNotMerged();
            return root;
        }

        @Override
        public void setRoot(NodeState newRoot) {
            checkNotMerged();
            this.root = newRoot;
        }

        @Override
        public NodeState merge(CommitHook hook) throws CommitFailedException {
            checkNotMerged();
            while (!store.root.compareAndSet(
                    base, checkNotNull(hook).processCommit(base, root))) {
                // TODO: rebase();
                throw new UnsupportedOperationException();
            }
            root = null; // Mark as merged
            return store.getRoot();
        }

        @Override
        public boolean copy(String source, String target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean move(String source, String target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rebase() {
            throw new UnsupportedOperationException();
        }

        // ----------------------------------------------------< private >---

        private void checkNotMerged() {
            checkState(root != null, "Branch has already been merged");
        }
    }

}
