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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.squeeze;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

/**
 * Basic in-memory node store implementation. Useful as a base class for
 * more complex functionality.
 */
public class MemoryNodeStore implements NodeStore, Observable {

    private final AtomicReference<NodeState> root;

    private final Map<String, Checkpoint> checkpoints = newHashMap();

    private final Map<Closeable, Observer> observers = newHashMap();

    private final AtomicInteger checkpointCounter = new AtomicInteger();

    public MemoryNodeStore(NodeState state) {
        this.root = new AtomicReference<NodeState>(MemoryNodeState.wrap(state));
    }

    public MemoryNodeStore() {
        this(EMPTY_NODE);
    }

    /**
     * Returns a string representation the head state of this node store.
     */
    public String toString() {
        return getRoot().toString();
    }

    @Override
    public synchronized Closeable addObserver(Observer observer) {
        observer.contentChanged(getRoot(), CommitInfo.EMPTY_EXTERNAL);

        Closeable closeable = new Closeable() {
            @Override
            public void close() throws IOException {
                synchronized (MemoryNodeStore.this) {
                    observers.remove(this);
                }
            }
        };
        observers.put(closeable, observer);
        return closeable;
    }

    private synchronized void setRoot(NodeState root, CommitInfo info) {
        this.root.getAndSet(root);

        for (Observer observer : observers.values()) {
            observer.contentChanged(root, info);
        }
    }

    @Override
    public NodeState getRoot() {
        return root.get();
    }

    /**
     * This implementation is equal to first rebasing the builder and then applying it to a
     * new branch and immediately merging it back.
     * @param builder  the builder whose changes to apply
     * @param commitHook the commit hook to apply while merging changes
     * @return the node state resulting from the merge.
     * @throws CommitFailedException
     * @throws IllegalArgumentException if the builder is not acquired from a root state of
     *                                  this store
     */
    @Override
    public synchronized NodeState merge(
            @Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            @Nonnull CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof MemoryNodeBuilder);
        MemoryNodeBuilder mnb = (MemoryNodeBuilder) builder;
        checkArgument(mnb.isRoot());
        checkNotNull(commitHook);
        rebase(builder);
        NodeStoreBranch branch = new MemoryNodeStoreBranch(this, getRoot());
        branch.setRoot(builder.getNodeState());
        NodeState merged = branch.merge(commitHook, info);
        mnb.reset(merged);
        return merged;
    }

    /**
     * This implementation is equal to applying the differences between the builders base state
     * and its head state to a fresh builder on the stores root state using
     * {@link org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff} for resolving
     * conflicts.
     * @param builder  the builder to rebase
     * @return the node state resulting from the rebase.
     * @throws IllegalArgumentException if the builder is not acquired from a root state of
     *                                  this store
     */
    @Override
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof MemoryNodeBuilder);
        NodeState head = checkNotNull(builder).getNodeState();
        NodeState base = builder.getBaseState();
        NodeState newBase = getRoot();
        if (base != newBase) {
            ((MemoryNodeBuilder) builder).reset(newBase);
            head.compareAgainstBaseState(
                    base, new ConflictAnnotatingRebaseDiff(builder));
            head = builder.getNodeState();
        }
        return head;
    }

    /**
     * This implementation is equal resetting the builder to the root of the store and returning
     * the resulting node state from the builder.
     * @param builder the builder to reset
     * @return the node state resulting from the reset.
     * @throws IllegalArgumentException if the builder is not acquired from a root state of
     *                                  this store
     */
    @Override
    public NodeState reset(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof MemoryNodeBuilder);
        NodeState head = getRoot();
        ((MemoryNodeBuilder) builder).reset(head);
        return head;
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

    @Override
    public Blob getBlob(@Nonnull String reference) {
        return null;
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        checkArgument(lifetime > 0);
        checkNotNull(properties);
        String checkpoint = "checkpoint" + checkpointCounter.incrementAndGet();
        checkpoints.put(checkpoint, new Checkpoint(getRoot(), properties));
        return checkpoint;
    }

    @Override @Nonnull
    public synchronized String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.<String, String>emptyMap());
    }

    @Nonnull
    @Override
    public Map<String, String> checkpointInfo(@Nonnull String checkpoint) {
        Checkpoint cp = checkpoints.get(checkNotNull(checkpoint));
        if (cp == null) {
            return Collections.emptyMap();
        } else {
            return cp.getProperties();
        }
    }

    @Nonnull
    @Override
    public synchronized Iterable<String> checkpoints() {
        return Lists.newArrayList(checkpoints.keySet());
    }

    @Override @CheckForNull
    public synchronized NodeState retrieve(@Nonnull String checkpoint) {
        Checkpoint cp = checkpoints.get(checkNotNull(checkpoint));
        if (cp == null) {
            return null;
        } else {
            return cp.getRoot();
        }
    }

    @Override
    public synchronized boolean release(String checkpoint) {
        checkpoints.remove(checkpoint);
        return true;
    }

    /** test purpose only! */
    public Set<String> listCheckpoints() {
        return Sets.newHashSet(checkpoints());
    }

    //------------------------------------------------------------< private >---

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
            this.root = ModifiedNodeState.squeeze(newRoot);
        }

        @Override
        public NodeState merge(
                @Nonnull CommitHook hook, @Nonnull CommitInfo info)
                throws CommitFailedException {
            checkNotNull(hook);
            checkNotNull(info);
            // TODO: rebase();
            checkNotMerged();
            NodeState merged = squeeze(hook.processCommit(base, root, info));
            store.setRoot(merged, info);
            root = null; // Mark as merged
            return merged;
        }

        @Override
        public void rebase() {
            throw new UnsupportedOperationException();
        }

        //------------------------------------------------------------< Object >---

        @Override
        public String toString() {
            return root.toString();
        }


        // ----------------------------------------------------< private >---

        private void checkNotMerged() {
            checkState(root != null, "Branch has already been merged");
        }
    }

    private static class Checkpoint {
        private final NodeState root;
        private final Map<String, String> properties;

        private Checkpoint(NodeState root, Map<String, String> properties) {
            this.root = root;
            this.properties = Maps.newHashMap(properties);
        }

        public NodeState getRoot() {
            return root;
        }

        public Map<String, String> getProperties() {
            return properties;
        }
    }
}
