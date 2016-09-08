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
package org.apache.jackrabbit.oak.segment;

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
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
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
public class SegmentNodeStore implements NodeStore, Observable {

    public static class SegmentNodeStoreBuilder {
        private static final Logger LOG = LoggerFactory.getLogger(SegmentNodeStoreBuilder.class);

        @Nonnull
        private final Revisions revisions;

        @Nonnull
        private final SegmentReader reader;

        @Nonnull
        private final SegmentWriter writer;

        @CheckForNull
        private final BlobStore blobStore;

        private boolean isCreated;

        private SegmentNodeStoreBuilder(
                @Nonnull Revisions revisions,
                @Nonnull SegmentReader reader,
                @Nonnull SegmentWriter writer,
                @Nullable BlobStore blobStore) {
            this.revisions = revisions;
            this.reader = reader;
            this.writer = writer;
            this.blobStore = blobStore;
        }

        @Nonnull
        public SegmentNodeStore build() {
            checkState(!isCreated);
            isCreated = true;
            LOG.info("Creating segment node store {}", this);
            return new SegmentNodeStore(this);
        }

        @Nonnull
        private static String getString(@CheckForNull BlobStore blobStore) {
            return "blobStore=" + (blobStore == null ? "inline" : blobStore);
        }

        @Override
        public String toString() {
            return "SegmentNodeStoreBuilder{" +
                    getString(blobStore) +
                    '}';
        }
    }

    @Nonnull
    public static SegmentNodeStoreBuilder builder(
            @Nonnull Revisions revisions,
            @Nonnull SegmentReader reader,
            @Nonnull SegmentWriter writer,
            @Nullable BlobStore blobStore) {
        return new SegmentNodeStoreBuilder(checkNotNull(revisions),
                checkNotNull(reader), checkNotNull(writer), blobStore);
    }

    private static final Logger log = LoggerFactory.getLogger(SegmentNodeStore.class);

    static final String ROOT = "root";

    public static final String CHECKPOINTS = "checkpoints";

    @Nonnull
    private final SegmentReader reader;

    @Nonnull
    private final SegmentWriter writer;

    @Nonnull
    private final Revisions revisions;

    @CheckForNull
    private final BlobStore blobStore;

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
    private static final boolean COMMIT_FAIR_LOCK = Boolean
            .getBoolean("oak.segmentNodeStore.commitFairLock");

    private SegmentNodeStore(SegmentNodeStoreBuilder builder) {
        if (COMMIT_FAIR_LOCK) {
            log.info("initializing SegmentNodeStore with the commitFairLock option enabled.");
        }
        this.commitSemaphore = new Semaphore(1, COMMIT_FAIR_LOCK);
        this.revisions = builder.revisions;
        this.reader = builder.reader;
        this.writer = builder.writer;
        this.blobStore = builder.blobStore;
        this.head = new AtomicReference<SegmentNodeState>(reader.readHeadState(revisions));
        this.changeDispatcher = new ChangeDispatcher(getRoot());
    }

    void setMaximumBackoff(long max) {
        this.maximumBackoff = max;
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
    // FIXME OAK-4122: Replace the commit semaphore in the segment node store with a scheduler
    // Replace by usage of expeditable lock or commit scheduler
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
        SegmentNodeState state = reader.readHeadState(revisions);
        if (!state.getRecordId().equals(head.get().getRecordId())) {
            head.set(state);
            changeDispatcher.contentChanged(state.getChildNode(ROOT), null);
        }
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    @Override @Nonnull
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

    @Override
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
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        NodeState before = snb.getBaseState();
        if (!SegmentNodeState.fastEquals(before, root)) {
            SegmentNodeState after = snb.getNodeState();
            snb.reset(root);
            after.compareAgainstBaseState(
                    before, new ConflictAnnotatingRebaseDiff(snb));
        }

        return snb.getNodeState();
    }

    @Override @Nonnull
    public NodeState reset(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        snb.reset(root);

        return root;
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return writer.writeStream(stream);
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        //Use of 'reference' here is bit overloaded. In terms of NodeStore API
        //a blob reference refers to the secure reference obtained from Blob#getReference()
        //However in SegmentStore terminology a blob is referred via 'external reference'
        //That 'external reference' would map to blobId obtained from BlobStore#getBlobId
        if (blobStore != null) {
            String blobId = blobStore.getBlobId(reference);
            if (blobId != null) {
                return new BlobStoreBlob(blobStore, blobId);
            }
            return null;
        }
        throw new IllegalStateException("Attempt to read external blob with blobId [" + reference + "] " +
                "without specifying BlobStore");
    }

    @Nonnull
    @Override
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
            if (revisions.setHead(state.getRecordId(), newState.getRecordId())) {
                refreshHead();
                return true;
            } else {
                return false;
            }
        }
    }

    @Override @Nonnull
    public synchronized String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.<String, String>emptyMap());
    }

    @Nonnull
    @Override
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

    @Override @CheckForNull
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
                        if (revisions.setHead(state.getRecordId(), newState.getRecordId())) {
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
            if (revisions.setHead(before.getRecordId(), after.getRecordId())) {
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
            if (SegmentNodeState.fastEquals(before, state.getChildNode(ROOT))) {
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
            if (!SegmentNodeState.fastEquals(before, after)) {
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
