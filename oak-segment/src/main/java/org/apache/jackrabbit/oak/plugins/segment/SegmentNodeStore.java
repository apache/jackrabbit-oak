/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.segment.Record.fastEquals;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.GAIN_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.NO_COMPACTION;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The top level class for the segment store.
 * <p>
 * The root node of the JCR content tree is actually stored in the node "/root",
 * and checkpoints are stored under "/checkpoints".
 */
@Deprecated
public class SegmentNodeStore implements NodeStore, Observable {

    // FIXME OAK-4449: SegmentNodeStore and SegmentStore builders should log their parameters on build()
    @Deprecated
    public static class SegmentNodeStoreBuilder {

        private final SegmentStore store;

        private boolean isCreated;

        private CompactionStrategy compactionStrategy = NO_COMPACTION;

        private volatile SegmentNodeStore segmentNodeStore;

        private SegmentNodeStoreBuilder(@Nonnull SegmentStore store) {
            this.store = store;
        }

        SegmentNodeStoreBuilder withCompactionStrategy(CompactionStrategy compactionStrategy) {
            this.compactionStrategy = compactionStrategy;
            return this;
        }

        SegmentNodeStoreBuilder withCompactionStrategy(
                boolean pauseCompaction,
                boolean cloneBinaries,
                String cleanup,
                long cleanupTs,
                byte memoryThreshold,
                final int lockWaitTime,
                int retryCount,
                boolean forceAfterFail,
                boolean persistCompactionMap,
                byte gainThreshold) {

            compactionStrategy = new CompactionStrategy(
                    pauseCompaction,
                    cloneBinaries,
                    CompactionStrategy.CleanupType.valueOf(cleanup),
                    cleanupTs,
                    memoryThreshold) {

                @Override
                public boolean compacted(Callable<Boolean> setHead) throws Exception {
                    // Need to guard against concurrent commits to avoid
                    // mixed segments. See OAK-2192.
                    return segmentNodeStore.locked(setHead, lockWaitTime, SECONDS);
                }

            };

            compactionStrategy.setRetryCount(retryCount);
            compactionStrategy.setForceAfterFail(forceAfterFail);
            compactionStrategy.setPersistCompactionMap(persistCompactionMap);
            compactionStrategy.setGainThreshold(gainThreshold);

            return this;
        }

        CompactionStrategy getCompactionStrategy() {
            checkState(isCreated);
            return compactionStrategy;
        }

        @Nonnull
        @Deprecated
        public SegmentNodeStore build() {
            checkState(!isCreated);
            isCreated = true;
            segmentNodeStore = new SegmentNodeStore(this);
            return segmentNodeStore;
        }

    }

    @Nonnull
    @Deprecated
    public static SegmentNodeStoreBuilder builder(@Nonnull SegmentStore store) {
        return new SegmentNodeStoreBuilder(checkNotNull(store));
    }

    private static final Logger log = LoggerFactory.getLogger(SegmentNodeStore.class);

    static final String ROOT = "root";

    @Deprecated
    public static final String CHECKPOINTS = "checkpoints";

    private final SegmentStore store;

    private final ChangeDispatcher changeDispatcher;

    /**
     * Local copy of the head of the journal associated with this store.
     */
    private final AtomicReference<SegmentNodeState> head;

    /**
     * Semaphore that controls access to the {@link #head} variable.
     * Only a single local commit is allowed at a time. When such
     * a commit is in progress, no external updates will be seen.
     */
    private final Semaphore commitSemaphore;

    private long maximumBackoff = MILLISECONDS.convert(10, SECONDS);

    /**
     * Sets the number of seconds to wait for the attempt to grab the lock to
     * create a checkpoint
     */
    private int checkpointsLockWaitTime = Integer.getInteger(
            "oak.checkpoints.lockWaitTime", 10);

    /**
     * Flag controlling the commit lock fairness
     */
    private final boolean commitFairLock = Boolean
            .getBoolean("oak.segmentNodeStore.commitFairLock");

    private SegmentNodeStore(SegmentNodeStoreBuilder builder) {
        if (commitFairLock) {
            log.info("initializing SegmentNodeStore with the commitFairLock option enabled.");
        }
        this.commitSemaphore = new Semaphore(1, commitFairLock);
        this.store = builder.store;
        this.head = new AtomicReference<SegmentNodeState>(store.getHead());
        this.changeDispatcher = new ChangeDispatcher(getRoot());
    }

    void setMaximumBackoff(long max) {
        this.maximumBackoff = max;
    }

    /**
     * Execute the passed callable with trying to acquire this store's commit lock.
     * @param c  callable to execute
     * @return  {@code false} if the store's commit lock cannot be acquired, the result
     *          of {@code c.call()} otherwise.
     * @throws Exception
     */
    boolean locked(Callable<Boolean> c) throws Exception {
        if (commitSemaphore.tryAcquire()) {
            try {
                return c.call();
            } finally {
                commitSemaphore.release();
            }
        }
        return false;
    }

    /**
     * Execute the passed callable with trying to acquire this store's commit lock.
     * @param timeout the maximum time to wait for the store's commit lock
     * @param unit the time unit of the {@code timeout} argument
     * @param c  callable to execute
     * @return  {@code false} if the store's commit lock cannot be acquired, the result
     *          of {@code c.call()} otherwise.
     * @throws Exception
     */
    boolean locked(Callable<Boolean> c, long timeout, TimeUnit unit) throws Exception {
        if (commitSemaphore.tryAcquire(timeout, unit)) {
            try {
                return c.call();
            } finally {
                // Explicitly give up reference to the previous root state
                // otherwise they would block cleanup. See OAK-3347
                refreshHead();
                commitSemaphore.release();
            }
        }
        return false;
    }

    /**
     * Refreshes the head state. Should only be called while holding a
     * permit from the {@link #commitSemaphore}.
     */
    private void refreshHead() {
        SegmentNodeState state = store.getHead();
        if (!state.getRecordId().equals(head.get().getRecordId())) {
            head.set(state);
            changeDispatcher.contentChanged(state.getChildNode(ROOT), null);
        }
    }

    @Override
    @Deprecated
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    @Override @Nonnull
    @Deprecated
    public NodeState getRoot() {
        if (commitSemaphore.tryAcquire()) {
            try {
                refreshHead();
            } finally {
                commitSemaphore.release();
            }
        }
        return head.get().getChildNode(ROOT);
    }

    @Nonnull
    @Deprecated
    public NodeState getSuperRoot() {
        if (commitSemaphore.tryAcquire()) {
            try {
                refreshHead();
            } finally {
                commitSemaphore.release();
            }
        }
        return head.get();
    }

    @Override
    @Deprecated
    public NodeState merge(
            @Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            @Nonnull CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof SegmentNodeBuilder);
        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;
        checkArgument(snb.isRootBuilder());
        checkNotNull(commitHook);

        try {
            commitSemaphore.acquire();
            try {
                Commit commit = new Commit(snb, commitHook, info);
                NodeState merged = commit.execute();
                snb.reset(merged);
                return merged;
            } finally {
                commitSemaphore.release();
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new CommitFailedException(
                    "Segment", 2, "Merge interrupted", e);
        } catch (SegmentOverflowException e) {
            throw new CommitFailedException(
                    "Segment", 3, "Merge failed", e);
        }
    }

    @Override @Nonnull
    @Deprecated
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        NodeState before = snb.getBaseState();
        if (!fastEquals(before, root)) {
            SegmentNodeState after = snb.getNodeState();
            snb.reset(root);
            after.compareAgainstBaseState(
                    before, new ConflictAnnotatingRebaseDiff(snb));
        }

        return snb.getNodeState();
    }

    @Override @Nonnull
    @Deprecated
    public NodeState reset(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        snb.reset(root);

        return root;
    }

    @Override
    @Deprecated
    public Blob createBlob(InputStream stream) throws IOException {
        return store.getTracker().getWriter().writeStream(stream);
    }

    @Override
    @Deprecated
    public Blob getBlob(@Nonnull String reference) {
        //Use of 'reference' here is bit overloaded. In terms of NodeStore API
        //a blob reference refers to the secure reference obtained from Blob#getReference()
        //However in SegmentStore terminology a blob is referred via 'external reference'
        //That 'external reference' would map to blobId obtained from BlobStore#getBlobId
        BlobStore blobStore = store.getBlobStore();
        if (blobStore != null) {
            String blobId = blobStore.getBlobId(reference);
            if (blobId != null) {
                return store.readBlob(blobId);
            }
            return null;
        }
        throw new IllegalStateException("Attempt to read external blob with blobId [" + reference + "] " +
                "without specifying BlobStore");
    }

    @Nonnull
    @Override
    @Deprecated
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        checkArgument(lifetime > 0);
        checkNotNull(properties);
        String name = UUID.randomUUID().toString();
        try {
            CPCreator cpc = new CPCreator(name, lifetime, properties);
            if (locked(cpc, checkpointsLockWaitTime, TimeUnit.SECONDS)) {
                return name;
            }
            log.warn("Failed to create checkpoint {} in {} seconds.", name,
                    checkpointsLockWaitTime);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            log.error("Failed to create checkpoint {}.", name, e);
        } catch (Exception e) {
            log.error("Failed to create checkpoint {}.", name, e);
        }
        return name;
    }

    private final class CPCreator implements Callable<Boolean> {

        private final String name;
        private final long lifetime;
        private final Map<String, String> properties;

        CPCreator(String name, long lifetime, Map<String, String> properties) {
            this.name = name;
            this.lifetime = lifetime;
            this.properties = properties;
        }

        @Override
        public Boolean call() {
            long now = System.currentTimeMillis();

            refreshHead();

            SegmentNodeState state = head.get();
            SegmentNodeBuilder builder = state.builder();

            NodeBuilder checkpoints = builder.child("checkpoints");
            for (String n : checkpoints.getChildNodeNames()) {
                NodeBuilder cp = checkpoints.getChildNode(n);
                PropertyState ts = cp.getProperty("timestamp");
                if (ts == null || ts.getType() != LONG
                        || now > ts.getValue(LONG)) {
                    cp.remove();
                }
            }

            NodeBuilder cp = checkpoints.child(name);
            if (Long.MAX_VALUE - now > lifetime) {
                cp.setProperty("timestamp", now + lifetime);
            } else {
                cp.setProperty("timestamp", Long.MAX_VALUE);
            }
            cp.setProperty("created", now);

            NodeBuilder props = cp.setChildNode("properties");
            for (Entry<String, String> p : properties.entrySet()) {
                props.setProperty(p.getKey(), p.getValue());
            }
            cp.setChildNode(ROOT, state.getChildNode(ROOT));

            SegmentNodeState newState = builder.getNodeState();
            if (store.setHead(state, newState)) {
                refreshHead();
                return true;
            } else {
                return false;
            }
        }
    }

    @Override @Nonnull
    @Deprecated
    public synchronized String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.<String, String>emptyMap());
    }

    @Nonnull
    @Override
    @Deprecated
    public Map<String, String> checkpointInfo(@Nonnull String checkpoint) {
        Map<String, String> properties = newHashMap();
        checkNotNull(checkpoint);
        NodeState cp = head.get()
                .getChildNode("checkpoints")
                .getChildNode(checkpoint)
                .getChildNode("properties");

        for (PropertyState prop : cp.getProperties()) {
            properties.put(prop.getName(), prop.getValue(STRING));
        }

        return properties;
    }

    @Nonnull
    @Override
    @Deprecated
    public Iterable<String> checkpoints() {
        return getCheckpoints().getChildNodeNames();
    }

    @Override @CheckForNull
    @Deprecated
    public NodeState retrieve(@Nonnull String checkpoint) {
        checkNotNull(checkpoint);
        NodeState cp = head.get()
                .getChildNode("checkpoints")
                .getChildNode(checkpoint)
                .getChildNode(ROOT);
        if (cp.exists()) {
            return cp;
        }
        return null;
    }

    @Override
    @Deprecated
    public boolean release(@Nonnull String checkpoint) {
        checkNotNull(checkpoint);

        // try 5 times
        for (int i = 0; i < 5; i++) {
            if (commitSemaphore.tryAcquire()) {
                try {
                    refreshHead();

                    SegmentNodeState state = head.get();
                    SegmentNodeBuilder builder = state.builder();

                    NodeBuilder cp = builder.child("checkpoints").child(
                            checkpoint);
                    if (cp.exists()) {
                        cp.remove();
                        SegmentNodeState newState = builder.getNodeState();
                        if (store.setHead(state, newState)) {
                            refreshHead();
                            return true;
                        }
                    }
                } finally {
                    commitSemaphore.release();
                }
            }
        }
        return false;
    }

    NodeState getCheckpoints() {
        return head.get().getChildNode(CHECKPOINTS);
    }

    private class Commit {

        private final Random random = new Random();

        private final NodeState before;

        private final SegmentNodeState after;

        private final CommitHook hook;

        private final CommitInfo info;

        Commit(@Nonnull SegmentNodeBuilder builder,
                @Nonnull CommitHook hook, @Nonnull CommitInfo info) {
            checkNotNull(builder);
            this.before = builder.getBaseState();
            this.after = builder.getNodeState();

            this.hook = checkNotNull(hook);
            this.info = checkNotNull(info);
        }

        private boolean setHead(SegmentNodeState before, SegmentNodeState after) {
            refreshHead();
            if (store.setHead(before, after)) {
                head.set(after);
                changeDispatcher.contentChanged(after.getChildNode(ROOT), info);
                refreshHead();
                return true;
            } else {
                return false;
            }
        }

        private SegmentNodeBuilder prepare(SegmentNodeState state) throws CommitFailedException {
            SegmentNodeBuilder builder = state.builder();
            if (fastEquals(before, state.getChildNode(ROOT))) {
                // use a shortcut when there are no external changes
                builder.setChildNode(
                        ROOT, hook.processCommit(before, after, info));
            } else {
                // there were some external changes, so do the full rebase
                ConflictAnnotatingRebaseDiff diff =
                        new ConflictAnnotatingRebaseDiff(builder.child(ROOT));
                after.compareAgainstBaseState(before, diff);
                // apply commit hooks on the rebased changes
                builder.setChildNode(ROOT, hook.processCommit(
                        builder.getBaseState().getChildNode(ROOT),
                        builder.getNodeState().getChildNode(ROOT),
                        info));
            }
            return builder;
        }

        private long optimisticMerge()
                throws CommitFailedException, InterruptedException {
            long timeout = 1;

            // use exponential backoff in case of concurrent commits
            for (long backoff = 1; backoff < maximumBackoff; backoff *= 2) {
                long start = System.nanoTime();

                refreshHead();
                SegmentNodeState state = head.get();
                if (state.hasProperty("token")
                        && state.getLong("timeout") >= currentTimeMillis()) {
                    // someone else has a pessimistic lock on the journal,
                    // so we should not try to commit anything yet
                } else {
                    SegmentNodeBuilder builder = prepare(state);
                    // use optimistic locking to update the journal
                    if (setHead(state, builder.getNodeState())) {
                        return -1;
                    }
                }

                // someone else was faster, so wait a while and retry later
                Thread.sleep(backoff, random.nextInt(1000000));

                long stop = System.nanoTime();
                if (stop - start > timeout) {
                    timeout = stop - start;
                }
            }

            return MILLISECONDS.convert(timeout, NANOSECONDS);
        }

        private void pessimisticMerge(long timeout)
                throws CommitFailedException, InterruptedException {
            while (true) {
                long now = currentTimeMillis();
                SegmentNodeState state = head.get();
                if (state.hasProperty("token")
                        && state.getLong("timeout") >= now) {
                    // locked by someone else, wait until unlocked or expired
                    Thread.sleep(
                            Math.min(state.getLong("timeout") - now, 1000),
                            random.nextInt(1000000));
                } else {
                    // attempt to acquire the lock
                    SegmentNodeBuilder builder = state.builder();
                    builder.setProperty("token", UUID.randomUUID().toString());
                    builder.setProperty("timeout", now + timeout);

                    if (setHead(state, builder.getNodeState())) {
                         // lock acquired; rebase, apply commit hooks, and unlock
                        builder = prepare(state);
                        builder.removeProperty("token");
                        builder.removeProperty("timeout");

                        // complete the commit
                        if (setHead(state, builder.getNodeState())) {
                            return;
                        }
                    }
                }
            }
        }

        @Nonnull
        NodeState execute()
                throws CommitFailedException, InterruptedException {
            // only do the merge if there are some changes to commit
            if (!fastEquals(before, after)) {
                long timeout = optimisticMerge();
                if (timeout >= 0) {
                    pessimisticMerge(timeout);
                }
            }
            return head.get().getChildNode(ROOT);
        }

    }

    /**
     * Sets the number of seconds to wait for the attempt to grab the lock to
     * create a checkpoint
     */
    void setCheckpointsLockWaitTime(int checkpointsLockWaitTime) {
        this.checkpointsLockWaitTime = checkpointsLockWaitTime;
    }

}
