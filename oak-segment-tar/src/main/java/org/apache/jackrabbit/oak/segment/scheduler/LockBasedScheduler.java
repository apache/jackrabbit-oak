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
package org.apache.jackrabbit.oak.segment.scheduler;

import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.oak.api.Type.LONG;

import java.io.Closeable;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreStats;
import org.apache.jackrabbit.oak.segment.SegmentOverflowException;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockBasedScheduler implements Scheduler {

    public static class LockBasedSchedulerBuilder {
        @NotNull
        private final SegmentReader reader;

        @NotNull
        private final Revisions revisions;
        
        @NotNull
        private final SegmentNodeStoreStats stats;

        private boolean dispatchChanges = true;

        private LockBasedSchedulerBuilder(@NotNull Revisions revisions, @NotNull SegmentReader reader,
                @NotNull SegmentNodeStoreStats stats) {
            this.revisions = revisions;
            this.reader = reader;
            this.stats = stats;
        }

        @NotNull
        public LockBasedSchedulerBuilder dispatchChanges(boolean dispatchChanges) {
            this.dispatchChanges = dispatchChanges;
            return this;
        }

        @NotNull
        public LockBasedScheduler build() {
            if (dispatchChanges) {
                return new ObservableLockBasedScheduler(this);
            } else {
                return new LockBasedScheduler(this);
            }
        }

    }

    public static LockBasedSchedulerBuilder builder(@NotNull Revisions revisions, @NotNull SegmentReader reader,
            @NotNull SegmentNodeStoreStats stats) {
        return new LockBasedSchedulerBuilder(requireNonNull(revisions), requireNonNull(reader), requireNonNull(stats));
    }

    private static final Logger log = LoggerFactory.getLogger(LockBasedScheduler.class);

    /**
     * Flag controlling the commit lock fairness
     */
    private static final boolean COMMIT_FAIR_LOCK = Boolean
            .parseBoolean(System.getProperty("oak.segmentNodeStore.commitFairLock", "true"));

    /**
     * Flag controlling the commit time quantile to wait for the lock in order
     * to increase chances of returning an up to date state.
     */
    private static final double SCHEDULER_FETCH_COMMIT_DELAY_QUANTILE = Double
            .parseDouble(System.getProperty("oak.scheduler.fetch.commitDelayQuantile", "0.5"));

    /**
     * Flag controlling the number of milliseconds after which warnings are logged
     * when threads ready to commit have to wait for a commit in progress.
     */
    private static final int COMMIT_WAIT_WARN_MILLIS = Integer
            .getInteger("oak.segmentNodeStore.commitWaitWarnMillis", 60000);
    
    /**
     * Maximum number of milliseconds to wait before re-attempting to update the current
     * head state after a successful commit, provided a concurrent head state update happens.
     */
    private static final long MAXIMUM_BACKOFF = MILLISECONDS.convert(10, SECONDS);
    
    /**
     * Sets the number of seconds to wait for the attempt to grab the lock to
     * create a checkpoint
     */
    private final int checkpointsLockWaitTime = Integer.getInteger("oak.checkpoints.lockWaitTime", 10);
    
    static final String ROOT = "root";

    /**
     * Semaphore that controls access to the {@link #head} variable. Only a
     * single local commit is allowed at a time. When such a commit is in
     * progress, no external updates will be seen.
     */
    private final Semaphore commitSemaphore = new Semaphore(1, COMMIT_FAIR_LOCK);

    @NotNull
    private final SegmentReader reader;

    @NotNull
    private final Revisions revisions;

    protected final AtomicReference<SegmentNodeState> head;

    private final SegmentNodeStoreStats stats;
    
    private final Histogram commitTimeHistogram = new Histogram(new UniformReservoir());
    
    private final Random random = new Random();

    private final CommitSemaphoreLogging commitSemaphoreLogging = new CommitSemaphoreLogging();

    /*
     * Logging of commits that are either blocked for longer than COMMIT_WAIT_WARN_MILLIS
     * or on a commit that crossed the gc boundary. See OAK-8071.
     */
    private class CommitSemaphoreLogging {

        @Nullable
        private volatile Commit commit;

        private volatile long timeStamp;

        public void commitStarted(@NotNull Commit commit) {
            this.commit = commit;
            this.timeStamp = System.currentTimeMillis();
        }

        public void commitEnded() {
            this.commit = null;
        }

        public void warnOnBlockingCommit() {
            Commit currentCommit = commit;
            GCGeneration headGeneration = head.get().getGcGeneration();
            GCGeneration commitGeneration = currentCommit == null
                ? null
                : currentCommit.getGCGeneration();
            long dt = System.currentTimeMillis() - timeStamp;
            boolean isBlocking = currentCommit != null && dt > COMMIT_WAIT_WARN_MILLIS;
            boolean isOldGeneration = commitGeneration != null
                    && headGeneration.getFullGeneration() > commitGeneration.getFullGeneration();

            if (isBlocking) {
                log.warn("This commit is blocked by a commit that is in progress since {} ms", dt);
            }

            if (isOldGeneration) {
                log.warn("The commit in progress is from an old GC generation {}. Head is at {}",
                         commitGeneration, headGeneration);
            }
        }
    }

    public LockBasedScheduler(LockBasedSchedulerBuilder builder) {
        if (COMMIT_FAIR_LOCK) {
            log.info("Initializing SegmentNodeStore with the commitFairLock option enabled.");
        }

        this.reader = builder.reader;
        this.revisions = builder.revisions;
        this.stats = builder.stats;
        this.head = new AtomicReference<SegmentNodeState>(reader.readHeadState(revisions));
    }

    @Override
    public NodeState getHeadNodeState() {
        long delay = (long) commitTimeHistogram.getSnapshot().getValue(SCHEDULER_FETCH_COMMIT_DELAY_QUANTILE);
        try {
            if (commitSemaphore.tryAcquire(delay, NANOSECONDS)) {
                try {
                    refreshHead(true);
                } finally {
                    commitSemaphore.release();
                }
            } 
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }
        return head.get();
    }

    /**
     * Refreshes the head state. Should only be called while holding a permit
     * from the {@link #commitSemaphore}.
     * 
     * @param dispatchChanges
     *            if set to true the changes would also be dispatched
     */
    private void refreshHead(boolean dispatchChanges) {
        SegmentNodeState state = reader.readHeadState(revisions);
        if (!state.getRecordId().equals(head.get().getRecordId())) {
            head.set(state);
            if (dispatchChanges) {
                contentChanged(state.getChildNode(ROOT), CommitInfo.EMPTY_EXTERNAL);
            }
        }
    }

    protected void contentChanged(NodeState root, CommitInfo info) {
        // do nothing without a change dispatcher
    }

    @Override
    public NodeState schedule(@NotNull Commit commit, SchedulerOption... schedulingOptions)
            throws CommitFailedException {
        try {
            commitSemaphoreLogging.warnOnBlockingCommit();

            long queuedTime = System.nanoTime();
            stats.onCommitQueued(Thread.currentThread(), commit::getGCGeneration);

            commitSemaphore.acquire();
            commitSemaphoreLogging.commitStarted(commit);
            try {
                long dequeuedTime = System.nanoTime();
                stats.onCommitDequeued(Thread.currentThread(), dequeuedTime - queuedTime);

                long beforeCommitTime = System.nanoTime();

                SegmentNodeState merged = (SegmentNodeState) execute(commit);
                commit.applied(merged);

                long afterCommitTime = System.nanoTime();
                commitTimeHistogram.update(afterCommitTime - beforeCommitTime);
                stats.onCommit(Thread.currentThread(), afterCommitTime - beforeCommitTime);

                return merged;
            } finally {
                commitSemaphoreLogging.commitEnded();
                commitSemaphore.release();
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new CommitFailedException("Segment", 2, "Merge interrupted", e);
        } catch (SegmentOverflowException e) {
            throw new CommitFailedException("Segment", 3, "Merge failed", e);
        }
    }

    private NodeState execute(Commit commit) throws CommitFailedException, InterruptedException {
        // only do the merge if there are some changes to commit
        if (commit.hasChanges()) {
            long start = System.nanoTime();
            
            int count = 0;
            for (long backoff = 1; backoff < MAXIMUM_BACKOFF; backoff *= 2) {
                refreshHead(true);
                SegmentNodeState before = head.get();
                SegmentNodeState after = commit.apply(before);
                
                if (revisions.setHead(before.getRecordId(), after.getRecordId())) {
                    head.set(after);
                    contentChanged(after.getChildNode(ROOT), commit.info());
                    
                    return head.get().getChildNode(ROOT);
                } 
                
                count++;
                int randNs = random.nextInt(1_000_000);
                log.info("Scheduler detected concurrent commits. Retrying after {} ms and {} ns", backoff, randNs);
                Thread.sleep(backoff, randNs);
            }
            
            long finish = System.nanoTime();
            
            String message = MessageFormat.format(
                    "The commit could not be executed after {} attempts. Total wait time: {} ms",
                    count, NANOSECONDS.toMillis(finish - start));
            throw new CommitFailedException("Segment", 3, message);
        }
        
        return head.get().getChildNode(ROOT);
    }

    @Override
    public String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        checkArgument(lifetime > 0);
        requireNonNull(properties);
        String name = UUID.randomUUID().toString();
        try {
            CPCreator cpc = new CPCreator(name, lifetime, properties);
            if (commitSemaphore.tryAcquire(checkpointsLockWaitTime, TimeUnit.SECONDS)) {
                try {
                    if (cpc.call()) {
                        return name;
                    }
                } finally {
                    // Explicitly give up reference to the previous root state
                    // otherwise they would block cleanup. See OAK-3347
                    refreshHead(true);
                    commitSemaphore.release();
                }
            }
            log.warn("Failed to create checkpoint {} in {} seconds.", name, checkpointsLockWaitTime);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            log.error("Failed to create checkpoint {}.", name, e);
        } catch (Exception e) {
            log.error("Failed to create checkpoint {}.", name, e);
        }
        return name;
    }

    @Override
    public boolean removeCheckpoint(String name) {
        requireNonNull(name);

        // try 5 times
        for (int i = 0; i < 5; i++) {
            if (commitSemaphore.tryAcquire()) {
                try {
                    refreshHead(true);

                    SegmentNodeState state = head.get();
                    SegmentNodeBuilder builder = state.builder();

                    NodeBuilder cp = builder.child("checkpoints").child(name);
                    if (cp.exists()) {
                        cp.remove();
                        SegmentNodeState newState = builder.getNodeState();
                        if (revisions.setHead(state.getRecordId(), newState.getRecordId())) {
                            refreshHead(false);
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

    private static class ObservableLockBasedScheduler extends LockBasedScheduler implements Observable {
        private final ChangeDispatcher changeDispatcher;
        
        public ObservableLockBasedScheduler(LockBasedSchedulerBuilder builder) {
            super(builder);
            this.changeDispatcher = new ChangeDispatcher(head.get().getChildNode(ROOT));
        }

        @Override
        protected void contentChanged(NodeState root, CommitInfo info) {
            changeDispatcher.contentChanged(root, info);
        }
        
        @Override
        public Closeable addObserver(Observer observer) {
            return changeDispatcher.addObserver(observer);
        }
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

            refreshHead(true);

            SegmentNodeState state = head.get();
            SegmentNodeBuilder builder = state.builder();

            NodeBuilder checkpoints = builder.child("checkpoints");
            for (String n : checkpoints.getChildNodeNames()) {
                NodeBuilder cp = checkpoints.getChildNode(n);
                PropertyState ts = cp.getProperty("timestamp");
                if (ts == null || ts.getType() != LONG || now > ts.getValue(LONG)) {
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
                refreshHead(false);
                return true;
            } else {
                return false;
            }
        }
    }
}
