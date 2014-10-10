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
package org.apache.jackrabbit.oak.plugins.index;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean.STATUS_DONE;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

public class AsyncIndexUpdate implements Runnable {

    private static final Logger log = LoggerFactory
            .getLogger(AsyncIndexUpdate.class);

    /**
     * Name of the hidden node under which information about the checkpoints
     * seen and indexed by each async indexer is kept.
     */
    static final String ASYNC = ":async";

    private static final long DEFAULT_LIFETIME = TimeUnit.DAYS.toMillis(1000);

    private static final CommitFailedException CONCURRENT_UPDATE = new CommitFailedException(
            "Async", 1, "Concurrent update detected");

    /**
     * Timeout in milliseconds after which an async job would be considered as
     * timed out. Another node in cluster would wait for timeout before
     * taking over a running job
     */
    private static final long ASYNC_TIMEOUT;

    static {
        int value = 15;
        try {
            value = Integer.parseInt(System.getProperty(
                    "oak.async.lease.timeout", "15"));
        } catch (NumberFormatException e) {
            // use default
        }
        ASYNC_TIMEOUT = TimeUnit.MINUTES.toMillis(value);
    }

    private final String name;

    private final NodeStore store;

    private final IndexEditorProvider provider;

    /**
     * Property name which stores the timestamp upto which the repository is
     * indexed
     */
    private final String lastIndexedTo;

    private final long lifetime = DEFAULT_LIFETIME; // TODO: make configurable

    /** Flag to avoid repeatedly logging failure warnings */
    private boolean failing = false;

    private final AsyncIndexStats indexStats = new AsyncIndexStats();

    /** Flag to switch to synchronous updates once the index caught up to the repo */
    private final boolean switchOnSync;

    /**
     * Set of reindexed definitions updated between runs because a single diff
     * can report less definitions than there really are. Used in coordination
     * with the switchOnSync flag, so we know which def need to be updated after
     * a run with no changes.
     */
    private final Set<String> reindexedDefinitions = new HashSet<String>();

    public AsyncIndexUpdate(@Nonnull String name, @Nonnull NodeStore store,
            @Nonnull IndexEditorProvider provider, boolean switchOnSync) {
        this.name = checkNotNull(name);
        this.lastIndexedTo = name + "-LastIndexedTo";
        this.store = checkNotNull(store);
        this.provider = checkNotNull(provider);
        this.switchOnSync = switchOnSync;
    }

    public AsyncIndexUpdate(@Nonnull String name, @Nonnull NodeStore store,
            @Nonnull IndexEditorProvider provider) {
        this(name, store, provider, false);
    }

    /**
     * Index update callback that tries to raise the async status flag when
     * the first index change is detected.
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-1292">OAK-1292</a>
     */
    private class AsyncUpdateCallback implements IndexUpdateCallback {

        /** The base checkpoint */
        private final String checkpoint;

        /** Expiration time of the last lease we committed */
        private long lease;

        private long updates = 0;

        private final String leaseName;
        private final String tempCpName;

        public AsyncUpdateCallback(String checkpoint, String afterCheckpoint)
                throws CommitFailedException {
            long now = System.currentTimeMillis();
            this.checkpoint = checkpoint;
            this.lease = now + 2 * ASYNC_TIMEOUT;
            this.leaseName = name + "-lease";
            this.tempCpName = name + "-temp";

            NodeState root = store.getRoot();
            long beforeLease = root.getChildNode(ASYNC).getLong(leaseName);
            if (beforeLease > now) {
                throw CONCURRENT_UPDATE;
            }

            NodeBuilder builder = root.builder();
            NodeBuilder async = builder.child(ASYNC);
            async.setProperty(leaseName, lease);
            updateTempCheckpoints(async, checkpoint, afterCheckpoint);
            mergeWithConcurrencyCheck(builder, checkpoint, beforeLease);

            // reset updates counter
            indexStats.setUpdates(this.updates);
        }

        private void updateTempCheckpoints(NodeBuilder async,
                String checkpoint, String afterCheckpoint) {

            indexStats.setReferenceCheckpoint(checkpoint);
            indexStats.setProcessedCheckpoint(afterCheckpoint);

            // try to drop temp cps, add 'currentCp' to the temp cps list
            Set<String> temps = Sets.newHashSet();
            for (String cp : getStrings(async, tempCpName)) {
                if (cp.equals(checkpoint)) {
                    continue;
                }
                boolean released = store.release(cp);
                log.debug("Releasing temporary checkpoint {}: {}", cp, released);
                if (!released) {
                    temps.add(cp);
                }
            }
            temps.add(afterCheckpoint);
            async.setProperty(tempCpName, temps, Type.STRINGS);
            indexStats.setTempCheckpoints(temps);
        }

        private Iterable<String> getStrings(NodeBuilder b, String p) {
            PropertyState ps = b.getProperty(p);
            if (ps != null) {
                return ps.getValue(Type.STRINGS);
            }
            return Sets.newHashSet();
        }

        boolean isDirty() {
            return updates > 0;
        }

        void close() throws CommitFailedException {
            NodeBuilder builder = store.getRoot().builder();
            NodeBuilder async = builder.child(ASYNC);
            async.removeProperty(leaseName);
            mergeWithConcurrencyCheck(builder, async.getString(name), lease);
        }

        @Override
        public void indexUpdate() throws CommitFailedException {
            updates++;
            if (updates % 100 == 0) {
                indexStats.setUpdates(this.updates);
                long now = System.currentTimeMillis();
                if (now + ASYNC_TIMEOUT > lease) {
                    long newLease = now + 2 * ASYNC_TIMEOUT;
                    NodeBuilder builder = store.getRoot().builder();
                    builder.child(ASYNC).setProperty(leaseName, newLease);
                    mergeWithConcurrencyCheck(builder, checkpoint, lease);
                    lease = newLease;
                }
            }
        }

    }

    @Override
    public synchronized void run() {
        if (indexStats.isPaused()) {
            return;
        }
        log.debug("Running background index task {}", name);

        NodeState root = store.getRoot();

        // check for concurrent updates
        NodeState async = root.getChildNode(ASYNC);
        if (async.getLong(name + "-lease") > System.currentTimeMillis()) {
            log.debug("Another copy of the {} index update is already running;"
                    + " skipping this update", name);
            return;
        }

        // find the last indexed state, and check if there are recent changes
        NodeState before;
        String beforeCheckpoint = async.getString(name);
        if (beforeCheckpoint != null) {
            NodeState state = store.retrieve(beforeCheckpoint);
            if (state == null) {
                log.warn("Failed to retrieve previously indexed checkpoint {};"
                        + " re-running the initial {} index update",
                        beforeCheckpoint, name);
                beforeCheckpoint = null;
                before = MISSING_NODE;
            } else if (noVisibleChanges(state, root)) {
                log.debug("No changes since last checkpoint;"
                        + " skipping the {} index update", name);
                return;
            } else {
                before = state;
            }
        } else {
            log.info("Initial {} index update", name);
            before = MISSING_NODE;
        }

        // there are some recent changes, so let's create a new checkpoint
        String afterTime = now();
        String afterCheckpoint = store.checkpoint(lifetime);
        NodeState after = store.retrieve(afterCheckpoint);
        if (after == null) {
            log.debug("Unable to retrieve newly created checkpoint {},"
                    + " skipping the {} index update", afterCheckpoint, name);
            return;
        }

        String checkpointToRelease = afterCheckpoint;
        try {
            updateIndex(before, beforeCheckpoint, after, afterCheckpoint, afterTime);

            // the update succeeded, i.e. it no longer fails
            if (failing) {
                log.info("Index update {} no longer fails", name);
                failing = false;
            }

            // the update succeeded, so we can release the earlier checkpoint
            // otherwise the new checkpoint associated with the failed update
            // will get released in the finally block
            checkpointToRelease = beforeCheckpoint;
            indexStats.setReferenceCheckpoint(afterCheckpoint);
            indexStats.setProcessedCheckpoint("");
            indexStats.releaseTempCheckpoint(afterCheckpoint);

        } catch (CommitFailedException e) {
            if (e == CONCURRENT_UPDATE) {
                log.debug("Concurrent update detected in the {} index update", name);
            } else if (failing) {
                log.debug("The {} index update is still failing", name, e);
            } else {
                log.warn("The {} index update failed", name, e);
                failing = true;
            }

        } finally {
            if (checkpointToRelease != null) { // null during initial indexing
                if (!store.release(checkpointToRelease)) {
                    log.debug("Unable to reelase checkpoint {}",
                            checkpointToRelease);
                }
            }
        }
    }

    private void updateIndex(
            NodeState before, String beforeCheckpoint,
            NodeState after, String afterCheckpoint, String afterTime)
            throws CommitFailedException {
        // start collecting runtime statistics
        preAsyncRunStatsStats(indexStats);

        // create an update callback for tracking index updates
        // and maintaining the update lease
        AsyncUpdateCallback callback =
                new AsyncUpdateCallback(beforeCheckpoint, afterCheckpoint);
        try {
            NodeBuilder builder = store.getRoot().builder();

            IndexUpdate indexUpdate =
                    new IndexUpdate(provider, name, after, builder, callback);
            CommitFailedException exception =
                    EditorDiff.process(VisibleEditor.wrap(indexUpdate), before, after);
            if (exception != null) {
                throw exception;
            }

            builder.child(ASYNC).setProperty(name, afterCheckpoint);
            builder.child(ASYNC).setProperty(PropertyStates.createProperty(lastIndexedTo, afterTime, Type.DATE));
            if (callback.isDirty() || before == MISSING_NODE) {
                if (switchOnSync) {
                    reindexedDefinitions.addAll(
                            indexUpdate.getReindexedDefinitions());
                } else {
                    postAsyncRunStatsStatus(indexStats);
                }
            } else {
                if (switchOnSync) {
                    log.debug(
                            "No changes detected after diff; will try to switch to synchronous updates on {}",
                            reindexedDefinitions);

                    // no changes after diff, switch to sync on the async defs
                    for (String path : reindexedDefinitions) {
                        NodeBuilder c = builder;
                        for (String p : elements(path)) {
                            c = c.getChildNode(p);
                        }
                        if (c.exists() && !c.getBoolean(REINDEX_PROPERTY_NAME)) {
                            c.removeProperty(ASYNC_PROPERTY_NAME);
                        }
                    }
                    reindexedDefinitions.clear();
                }
                postAsyncRunStatsStatus(indexStats);
            }
            mergeWithConcurrencyCheck(builder, beforeCheckpoint, callback.lease);
        } finally {
            callback.close();
        }
    }

    private void mergeWithConcurrencyCheck(
            NodeBuilder builder, final String checkpoint, final long lease)
            throws CommitFailedException {
        CommitHook concurrentUpdateCheck = new CommitHook() {
            @Override @Nonnull
            public NodeState processCommit(
                    NodeState before, NodeState after, CommitInfo info)
                    throws CommitFailedException {
                // check for concurrent updates by this async task
                NodeState async = before.getChildNode(ASYNC);
                if (checkpoint == null || Objects.equal(checkpoint, async.getString(name))
                        && lease == async.getLong(name + "-lease")) {
                    return after;
                } else {
                    throw CONCURRENT_UPDATE;
                }
            }
        };
        CompositeHook hooks = new CompositeHook(
                new ConflictHook(new AnnotatingConflictHandler()),
                new EditorHook(new ConflictValidatorProvider()),
                concurrentUpdateCheck);
        store.merge(builder, hooks, CommitInfo.EMPTY);
    }

    private static void preAsyncRunStatsStats(AsyncIndexStats stats) {
        stats.start(now());
    }

    private static void postAsyncRunStatsStatus(AsyncIndexStats stats) {
        stats.done(now());
    }

    private static String now() {
        return ISO8601.format(Calendar.getInstance());
    }

    public AsyncIndexStats getIndexStats() {
        return indexStats;
    }

    public boolean isFinished() {
        return indexStats.getStatus() == STATUS_DONE;
    }

    final class AsyncIndexStats implements IndexStatsMBean {

        private String start = "";
        private String done = "";
        private String status = STATUS_INIT;
        private String referenceCp = "";
        private String processedCp = "";
        private Set<String> tempCps = new HashSet<String>();

        private volatile boolean isPaused;
        private volatile long updates;

        public void start(String now) {
            status = STATUS_RUNNING;
            start = now;
            done = "";
        }

        public void done(String now) {
            status = STATUS_DONE;
            start = "";
            done = now;
        }

        @Override
        public String getStart() {
            return start;
        }

        @Override
        public String getDone() {
            return done;
        }

        @Override
        public String getStatus() {
            return status;
        }

        @Override
        public String getLastIndexedTime() {
            PropertyState ps = store.getRoot().getChildNode(ASYNC).getProperty(lastIndexedTo);
            return ps != null ? ps.getValue(Type.STRING) : null;
        }

        public void pause() {
            log.debug("Pausing the async indexer");
            this.isPaused = true;
        }

        @Override
        public void resume() {
            log.debug("Resuming the async indexer");
            this.isPaused = false;
        }

        @Override
        public boolean isPaused() {
            return this.isPaused;
        }

        void setUpdates(long updates) {
            this.updates = updates;
        }

        @Override
        public long getUpdates() {
            return updates;
        }

        void setReferenceCheckpoint(String checkpoint) {
            this.referenceCp = checkpoint;
        }

        @Override
        public String getReferenceCheckpoint() {
            return referenceCp;
        }

        void setProcessedCheckpoint(String checkpoint) {
            this.processedCp = checkpoint;
        }

        @Override
        public String getProcessedCheckpoint() {
            return processedCp;
        }

        void setTempCheckpoints(Set<String> tempCheckpoints) {
            this.tempCps = tempCheckpoints;
        }

        void releaseTempCheckpoint(String tempCheckpoint) {
            this.tempCps.remove(tempCheckpoint);
        }

        @Override
        public String getTemporaryCheckpoints() {
            return tempCps.toString();
        }

        @Override
        public String toString() {
            return "AsyncIndexStats [start=" + start + ", done=" + done
                    + ", status=" + status + ", paused=" + isPaused
                    + ", updates=" + updates + ", referenceCheckpoint="
                    + referenceCp + ", processedCheckpoint=" + processedCp
                    + " ,tempCheckpoints=" + tempCps + " ]";
        }
    }

    /**
     * Checks whether there are no visible changes between the given states.
     */
    private static boolean noVisibleChanges(NodeState before, NodeState after) {
        return after.compareAgainstBaseState(before, new NodeStateDiff() {
            @Override
            public boolean propertyAdded(PropertyState after) {
                return isHidden(after.getName());
            }
            @Override
            public boolean propertyChanged(
                    PropertyState before, PropertyState after) {
                return isHidden(after.getName());
            }
            @Override
            public boolean propertyDeleted(PropertyState before) {
                return isHidden(before.getName());
            }
            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                return isHidden(name);
            }
            @Override
            public boolean childNodeChanged(
                    String name, NodeState before, NodeState after) {
                return isHidden(name)
                        || after.compareAgainstBaseState(before, this);
            }
            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                return isHidden(name);
            }
        });
    }

    private static boolean isHidden(String name) {
        return name.charAt(0) == ':';
    }

}
