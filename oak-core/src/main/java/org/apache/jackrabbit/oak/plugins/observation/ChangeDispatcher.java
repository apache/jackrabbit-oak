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
package org.apache.jackrabbit.oak.plugins.observation;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Queues.newLinkedBlockingQueue;

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * A {@code ChangeDispatcher} instance records changes to a {@link NodeStore}
 * and dispatches them to interested parties.
 * <p>
 * Actual changes are reported by calling {@link #beforeCommit(NodeState)},
 * {@link #localCommit(NodeState, CommitInfo)} and {@link #afterCommit(NodeState)} in that order:
 * <pre>
      NodeState root = store.getRoot();
      branch.rebase();
      changeDispatcher.beforeCommit(root);
      try {
          NodeState head = branch.getHead();
          branch.merge();
          changeDispatcher.localCommit(head);
      } finally {
          changeDispatcher.afterCommit(store.getRoot());
      }
 * </pre>
 * <p>
 * The {@link #newListener()} method registers a listener for receiving changes reported
 * into a change dispatcher.
 */
public class ChangeDispatcher implements Observable {
    private final Set<Listener> listeners = Sets.newHashSet();
    private final NodeStore store;

    @Nonnull
    private volatile NodeState root;

    /**
     * Create a new instance for recording changes to {@code store}.
     * @param store  the node store to record changes for
     */
    public ChangeDispatcher(@Nonnull NodeStore store) {
        this.store = store;
        this.root = checkNotNull(store.getRoot());
    }

    /**
     * Create a new {@link Listener} for receiving changes reported into
     * this change dispatcher. Listeners need to be {@link Listener#dispose() disposed}
     * when no longer needed.
     * @return  a new {@code Listener} instance.
     */
    @Override
    @Nonnull
    public Listener newListener() {
        Listener listener = new Listener(root);
        register(listener);
        return listener;
    }

    private final AtomicLong changeCount = new AtomicLong(0);

    private boolean inLocalCommit() {
        return changeCount.get() % 2 == 1;
    }

    /**
     * Call with the latest persisted root node state right before persisting further changes.
     * Calling this method marks this instance to be inside a local commit.
     * <p>
     * The differences from the root node state passed to the last call to
     * {@link #afterCommit(NodeState)} to {@code root} are reported as cluster external
     * changes to any listener.
     *
     * @param root  latest persisted root node state.
     * @throws IllegalStateException  if inside a local commit
     */
    public synchronized void beforeCommit(@Nonnull NodeState root) {
        checkState(!inLocalCommit());
        changeCount.incrementAndGet();
        externalChange(checkNotNull(root));
    }

    /**
     * Call right after changes have been successfully persisted passing
     * the new root node state resulting from the persist operation.
     * <p>
     * The differences from the root node state passed to the last call to
     * {@link #beforeCommit(NodeState)} to {@code root} are reported as
     * cluster local changes to any listener in case non-{@code null}
     * commit information is provided. If no local commit information is
     * given, then no local changes are reported and the committed changes
     * will only show up as an aggregate with any concurrent external changes
     * reported during the {@link #afterCommit(NodeState)} call.
     *
     * @param root root node state just persisted
     * @param info commit information
     * @throws IllegalStateException  if not inside a local commit
     */
    public synchronized void localCommit(
            @Nonnull NodeState root, @Nonnull CommitInfo info) {
        checkState(inLocalCommit());
        checkNotNull(root);
        add(root, info);
        this.root = root;
    }

    /**
     * Call to mark the end of a persist operation passing the latest persisted root node state.
     * Calling this method marks this instance to not be inside a local commit.
     * <p>
     * The difference from the root node state passed to the las call to
     * {@link #localCommit(NodeState, CommitInfo)} to {@code root} are reported as cluster external
     * changes to any listener.

     * @param root  latest persisted root node state.
     * @throws IllegalStateException  if not inside a local commit
     */
    public synchronized void afterCommit(@Nonnull NodeState root) {
        checkState(inLocalCommit());
        externalChange(checkNotNull(root));
        changeCount.incrementAndGet();
    }

    private void externalChange() {
        if (!inLocalCommit()) {
            long c = changeCount.get();
            NodeState root = store.getRoot();  // Need to get root outside sync. See OAK-959
            synchronized (this) {
                if (c == changeCount.get() && !inLocalCommit()) {
                    externalChange(root);
                }
            }
        }
    }

    private synchronized void externalChange(NodeState root) {
        if (!root.equals(this.root)) {
            add(root, null);
            this.root = root;
        }
    }

    private void register(Listener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    private void unregister(Listener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    private void add(NodeState root, CommitInfo info) {
        synchronized (listeners) {
            for (Listener l : getListeners()) {
                l.add(root, info);
            }
        }
    }

    private Listener[] getListeners() {
        synchronized (listeners) {
            return listeners.toArray(new Listener[listeners.size()]);
        }
    }

    //------------------------------------------------------------< Listener >---

    /**
     * Listener for receiving changes reported into a change dispatcher by
     * any of its hooks.
     */
    public class Listener {

        private final LinkedBlockingQueue<ChangeSet> changeSets =
                newLinkedBlockingQueue();

        private NodeState previousRoot;

        private boolean blocked = false;

        Listener(NodeState root) {
            this.previousRoot = checkNotNull(root);
        }

        /**
         * Free up any resources associated by this hook.
         */
        public void dispose() {
            unregister(this);
        }

        /**
         * Poll for changes reported to this listener.
         *
         * @param timeout maximum number of milliseconds to wait for changes
         * @return  {@code ChangeSet} of the changes, or {@code null} if
         *          no changes occurred since the last call to this method
         *          and before the timeout
         * @throws InterruptedException if polling was interrupted
         */
        @CheckForNull
        public ChangeSet getChanges(long timeout) throws InterruptedException {
            if (changeSets.isEmpty()) {
                externalChange();
            }
            return changeSets.poll(timeout, TimeUnit.MILLISECONDS);
        }

        private synchronized void add(NodeState root, CommitInfo info) {
            if (blocked) {
                info = null;
            }
            if (changeSets.offer(new ChangeSet(previousRoot, root, info))) {
                previousRoot = root;
                blocked = false;
            } else {
                blocked = true;
            }
        }
    }

    //------------------------------------------------------------< ChangeSet >---

    /**
     * Instances of this class represent changes to a node store. In addition they
     * record meta data associated with such changes like whether a change occurred
     * on the local cluster node, the user causing the changes and the date the changes
     * where persisted.
     */
    public static class ChangeSet {
        private final NodeState before;
        private final NodeState after;
        private final CommitInfo commitInfo;

        /**
         * Creates a change set
         */
        ChangeSet(@Nonnull NodeState before, @Nonnull NodeState after,
                @Nullable CommitInfo commitInfo) {
            this.before = checkNotNull(before);
            this.after = checkNotNull(after);
            this.commitInfo = commitInfo;
        }

        public boolean isExternal() {
            return commitInfo == null;
        }

        public boolean isLocal(String sessionId) {
            return commitInfo != null
                    && Objects.equal(commitInfo.getSessionId(), sessionId);
        }

        @CheckForNull
        public CommitInfo getCommitInfo() {
            return commitInfo;
        }

        /**
         * State before the change
         * @return  before state
         */
        @Nonnull
        public NodeState getBeforeState() {
            return before;
        }

        /**
         * State after the change
         * @return  after state
         */
        @Nonnull
        public NodeState getAfterState() {
            return after;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("base", before)
                .add("head", after)
                .add("commit info", commitInfo)
                .toString();
        }

    }

}
