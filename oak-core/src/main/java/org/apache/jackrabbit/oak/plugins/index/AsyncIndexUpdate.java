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
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean.STATUS_DONE;
import static org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean.STATUS_RUNNING;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncIndexUpdate implements Runnable {

    private static final Logger log = LoggerFactory
            .getLogger(AsyncIndexUpdate.class);

    /**
     * Name of the hidden node under which information about the checkpoints
     * seen and indexed by each async indexer is kept.
     */
    private static final String ASYNC = ":async";

    private static final long DEFAULT_LIFETIME = TimeUnit.HOURS.toMillis(1);

    private static final CommitFailedException CONCURRENT_UPDATE = new CommitFailedException(
            "Async", 1, "Concurrent update detected");

    /**
     * Timeout in minutes after which an async job would be considered as timed out. Another
     * node in cluster would wait for timeout before taking over a running job
     */
    private static final int ASYNC_TIMEOUT = 15;

    private final String name;

    private final NodeStore store;

    private final IndexEditorProvider provider;

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

        private boolean dirty = false;

        @Override
        public void indexUpdate() throws CommitFailedException {
            if (!dirty) {
                dirty = true;
                preAsyncRun(store, name);
            }
        }

    }

    @Override
    public synchronized void run() {
        log.debug("Running background index task {}", name);

        if (isAlreadyRunning(store, name)) {
            log.debug("Async job '{}' found to be already running. Skipping", name);
            return;
        }

        String checkpoint = store.checkpoint(lifetime);
        NodeState after = store.retrieve(checkpoint);
        if (after == null) {
            log.debug("Unable to retrieve checkpoint {}", checkpoint);
            return;
        }

        NodeBuilder builder = after.builder();
        NodeBuilder async = builder.child(ASYNC);

        NodeState before = null;
        final PropertyState state = async.getProperty(name);
        if (state != null && state.getType() == STRING) {
            before = store.retrieve(state.getValue(STRING));
        }
        if (before == null) {
            before = MISSING_NODE;
        }

        AsyncUpdateCallback callback = new AsyncUpdateCallback();
        preAsyncRunStatsStats(indexStats);
        IndexUpdate indexUpdate = new IndexUpdate(provider, name, after,
                builder, callback);

        CommitFailedException exception = EditorDiff.process(indexUpdate,
                before, after);
        if (exception == null) {
            if (callback.dirty) {
                async.setProperty(name, checkpoint);
                try {
                    store.merge(builder, newCommitHook(name, state),
                            CommitInfo.EMPTY);
                } catch (CommitFailedException e) {
                    if (e != CONCURRENT_UPDATE) {
                        exception = e;
                    }
                }
                if (switchOnSync) {
                    reindexedDefinitions.addAll(indexUpdate
                            .getReindexedDefinitions());
                } else {
                    postAsyncRunStatsStatus(indexStats);
                }
            } else if (switchOnSync) {
                log.debug("No changes detected after diff, will try to switch to synchronous updates on "
                        + reindexedDefinitions);
                async.setProperty(name, checkpoint);

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

                try {
                    store.merge(builder, newCommitHook(name, state),
                            CommitInfo.EMPTY);
                    reindexedDefinitions.clear();
                    postAsyncRunStatsStatus(indexStats);
                } catch (CommitFailedException e) {
                    if (e != CONCURRENT_UPDATE) {
                        exception = e;
                    }
                }
            }
        }

        if (exception != null) {
            if (!failing) {
                log.warn("Index update {} failed", name, exception);
            }
            failing = true;
        } else {
            if (failing) {
                log.info("Index update {} no longer fails", name);
            }
            failing = false;
        }
    }

    private static CommitHook newCommitHook(final String name,
            final PropertyState state) throws CommitFailedException {
        return new CommitHook() {
            @Override
            @Nonnull
            public NodeState processCommit(NodeState before, NodeState after,
                    CommitInfo info) throws CommitFailedException {
                // check for concurrent updates by this async task
                PropertyState stateAfterRebase = before.getChildNode(ASYNC)
                        .getProperty(name);
                if (Objects.equal(state, stateAfterRebase)) {
                    return postAsyncRunNodeStatus(after.builder(), name)
                            .getNodeState();
                } else {
                    throw CONCURRENT_UPDATE;
                }
            }
        };
    }

    private static void preAsyncRun(NodeStore store, String name) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        preAsyncRunNodeStatus(builder, name);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static boolean isAlreadyRunning(NodeStore store, String name) {
        NodeState indexState = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);

        //Probably the first run
        if (!indexState.exists()) {
            return false;
        }

        //Check if already running or timed out
        if (STATUS_RUNNING.equals(indexState.getString(name + "-status"))) {
            PropertyState startTime = indexState.getProperty(name + "-start");
            Calendar start = Conversions.convert(startTime.getValue(Type.DATE)).toCalendar();
            Calendar now = Calendar.getInstance();
            long delta = now.getTimeInMillis() - start.getTimeInMillis();

            //Check if the job has timed out and we need to take over
            if (TimeUnit.MILLISECONDS.toMinutes(delta) > ASYNC_TIMEOUT) {
                log.info("Async job found which stated on {} has timed out in {} minutes. " +
                        "This node would take over the job.",
                        startTime.getValue(Type.DATE), ASYNC_TIMEOUT);
                return false;
            }
            return true;
        }

        return false;
    }

    private static void preAsyncRunNodeStatus(NodeBuilder builder, String name) {
        String now = now();
        builder.getChildNode(INDEX_DEFINITIONS_NAME)
                .setProperty(name + "-status", STATUS_RUNNING)
                .setProperty(name + "-start", now, Type.DATE)
                .removeProperty(name + "-done");
    }

    private static void preAsyncRunStatsStats(AsyncIndexStats stats) {
        stats.start(now());
    }

    private static NodeBuilder postAsyncRunNodeStatus(NodeBuilder builder,
            String name) {
        String now = now();
        builder.getChildNode(INDEX_DEFINITIONS_NAME)
                .setProperty(name + "-status", STATUS_DONE)
                .setProperty(name + "-done", now, Type.DATE)
                .removeProperty(name + "-start");
        return builder;
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

    private static final class AsyncIndexStats implements IndexStatsMBean {

        private String start = "";
        private String done = "";
        private String status = STATUS_INIT;

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
        public String toString() {
            return "AsyncIndexStats [start=" + start + ", done=" + done
                    + ", status=" + status + "]";
        }
    }

}
