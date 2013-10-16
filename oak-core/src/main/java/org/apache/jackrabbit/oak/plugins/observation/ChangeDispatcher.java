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
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * A {@code ChangeDispatcher} instance records changes to a {@link NodeStore}
 * and dispatches them to interested parties.
 * <p>
 * Actual changes are reported by calling {@link #beforeCommit(NodeState)},
 * {@link #localCommit(NodeState)} and {@link #afterCommit(NodeState)} in that order:
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
public class ChangeDispatcher {
    private final Set<Listener> listeners = Sets.newHashSet();
    private final NodeStore store;

    private NodeState previousRoot;

    /**
     * Create a new instance for recording changes to {@code store}.
     * @param store  the node store to record changes for
     */
    public ChangeDispatcher(@Nonnull NodeStore store) {
        this.store = store;
        previousRoot = checkNotNull(store.getRoot());
    }

    /**
     * Create a new {@link Listener} for receiving changes reported into
     * this change dispatcher. Listeners need to be {@link Listener#dispose() disposed}
     * when no longer needed.
     * @return  a new {@code Listener} instance.
     */
    @Nonnull
    public Listener newListener() {
        Listener listener = new Listener();
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
     * Call right after changes have been successfully persisted passing the new root
     * node state resulting from the persist operation.
     * <p>
     * The differences from the root node state passed to the last call to
     * {@link #beforeCommit(NodeState)} to {@code root} are reported as cluster local
     * changes to any listener.

     * @param root  root node state just persisted
     * @throws IllegalStateException  if not inside a local commit
     */
    public synchronized void localCommit(@Nonnull NodeState root) {
        checkState(inLocalCommit());
        internalChange(checkNotNull(root));
    }

    /**
     * Call to mark the end of a persist operation passing the latest persisted root node state.
     * Calling this method marks this instance to not be inside a local commit.
     * <p>
     * The difference from the root node state passed to the las call to
     * {@link #localCommit(NodeState)} to {@code root} are reported as cluster external
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
        if (!root.equals(previousRoot)) {
            add(new ChangeSet(previousRoot, root, true));
            previousRoot = root;
        }
    }

    private synchronized void internalChange(NodeState root) {
        add(new ChangeSet(previousRoot, root, false));
        previousRoot = root;
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

    private void add(ChangeSet changeSet) {
        for (Listener l : getListeners()) {
            l.add(changeSet);
        }
    }

    private Listener[] getListeners() {
        synchronized (listeners) {
            return listeners.toArray(new Listener[listeners.size()]);
        }
    }

    //------------------------------------------------------------< Listener >---

    /**
     * Listener for receiving changes reported into a change dispatcher by any of its hooks.
     */
    public class Listener {
        private final Queue<ChangeSet> changeSets = Queues.newLinkedBlockingQueue();

        /**
         * Free up any resources associated by this hook.
         */
        public void dispose() {
            unregister(this);
        }

        /**
         * Poll for changes reported to this listener.
         * @return  {@code ChangeSet} of the changes or {@code null} if no changes occurred since
         *          the last call to this method.
         */
        @CheckForNull
        public ChangeSet getChanges() {
            if (changeSets.isEmpty()) {
                externalChange();
            }

            return changeSets.isEmpty() ? null : changeSets.remove();
        }

        private void add(ChangeSet changeSet) {
            changeSets.add(changeSet);
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
        private final boolean isExternal;

        ChangeSet(NodeState before, NodeState after, boolean isExternal) {
            this.before = before;
            this.after = after;
            this.isExternal = isExternal;
        }

        public boolean isExternal() {
            return isExternal;
        }

        public boolean isLocal(String sessionId) {
            return !isExternal && Objects.equal(getSessionId(), sessionId);
        }

        @CheckForNull
        public String getSessionId() {
            return getStringOrNull(getCommitInfo(after), CommitInfoEditorProvider.SESSION_ID);
        }

        @CheckForNull
        public String getUserId() {
            return getStringOrNull(getCommitInfo(after), CommitInfoEditorProvider.USER_ID);
        }

        public long getDate() {
            PropertyState property = getCommitInfo(after).getProperty(CommitInfoEditorProvider.TIME_STAMP);
            return property == null ? 0 : property.getValue(LONG);
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
                .add(CommitInfoEditorProvider.USER_ID, getUserId())
                .add(CommitInfoEditorProvider.TIME_STAMP, getDate())
                .add(CommitInfoEditorProvider.SESSION_ID, getSessionId())
                .add("external", isExternal)
                .toString();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other.getClass() != this.getClass()) {
                return false;
            }

            ChangeSet that = (ChangeSet) other;
            return before.equals(that.before) && after.equals(that.after) &&
                    isExternal == that.isExternal;
        }

        @Override
        public int hashCode() {
            return 31 * before.hashCode() + after.hashCode();
        }

        private static String getStringOrNull(NodeState commitInfo, String name) {
            PropertyState property = commitInfo.getProperty(name);
            return property == null ? null : property.getValue(STRING);
        }

        private static NodeState getCommitInfo(NodeState after) {
            return after.getChildNode(CommitInfoEditorProvider.COMMIT_INFO);
        }

    }
}
