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
import static org.apache.jackrabbit.oak.plugins.observation.ObservationConstants.OAK_UNKNOWN;

import java.util.Queue;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.commit.PostCommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.TODO;

/**
 * A {@code ChangeDispatcher} instance records changes to a {@link NodeStore}
 * and dispatches them to interested parties.
 * <p>
 * The {@link #newHook(ContentSession)} method registers a hook for
 * reporting changes. Actual changes are reported by calling
 * {@link Hook#contentChanged(NodeState, NodeState)}. Such changes are considered
 * to have occurred on the local cluster node and are recorded as such. Changes
 * that occurred in-between calls to any hook registered with a change processor
 * are considered to have occurred on a different cluster node and are recorded as such.
 * <p>
 * The {@link #newListener()} registers a listener for receiving changes reported
 * into a change dispatcher by any of its hooks.
 */
public class ChangeDispatcher {
    private final NodeStore store;
    private final Set<Listener> listeners = Sets.newHashSet();

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
     * Create a new {@link Hook} for reporting changes occurring in the
     * passed {@code contentSession}. The content session is used to
     * determine the user associated with the changes recorded through this
     * hook and to determine the originating session of changes.
     * @param contentSession  session which will be associated with any changes reported
     *                        through this hook.
     * @return a new {@code Hook} instance
     */
    @Nonnull
    public Hook newHook(ContentSession contentSession) {
        return new Hook(contentSession);
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

    private synchronized void contentChanged(@Nonnull NodeState before, @Nonnull NodeState after,
            ContentSession contentSession) {
        externalChange(checkNotNull(before));
        internalChange(checkNotNull(after), contentSession);
    }

    private void externalChange(NodeState root) {
        if (!root.equals(previousRoot)) {
            add(ChangeSet.external(previousRoot, root));
            previousRoot = root;
        }
    }

    private void internalChange(NodeState root, ContentSession contentSession) {
        add(ChangeSet.local(previousRoot, root, contentSession));
        previousRoot = root;
    }

    private synchronized void externalChange() {
        externalChange(store.getRoot());
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

    //------------------------------------------------------------< Sink >---

    /**
     * Hook for reporting changes. Actual changes are reported by calling
     * {@link Hook#contentChanged(NodeState, NodeState)}. Such changes are considered
     * to have occurred on the local cluster node and are recorded as such. Changes
     * that occurred in-between calls to any hook registered with a change processor
     * are considered to have occurred on a different cluster node and are recorded as such.
     */
    public class Hook implements PostCommitHook {
        private final ContentSession contentSession;

        private Hook(ContentSession contentSession) {
            this.contentSession = contentSession;
        }

        @Override
        public void contentChanged(@Nonnull NodeState before, @Nonnull NodeState after) {
            ChangeDispatcher.this.contentChanged(before, after, contentSession);
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
    public abstract static class ChangeSet {
        private final NodeState before;
        private final NodeState after;

        static ChangeSet local(NodeState base, NodeState head, ContentSession contentSession) {
            return new InternalChangeSet(base, head, contentSession, System.currentTimeMillis());
        }

        static ChangeSet external(NodeState base, NodeState head) {
            return new ExternalChangeSet(base, head);
        }

        protected ChangeSet(NodeState before, NodeState after) {
            this.before = before;
            this.after = after;
        }

        /**
         * Determine whether these changes originate from the local cluster node
         * or an external cluster node.
         * @return  {@code true} iff the changes originate from a remote cluster node.
         */
        public abstract boolean isExternal();

        /**
         * Determine whether these changes where caused by the passed content
         * session.
         * @param contentSession  content session to test for
         * @return  {@code true} iff these changes where cause by the passed content session.
         *          Always {@code false} if {@link #isExternal()} is {@code true}.
         */
        public abstract boolean isLocal(ContentSession contentSession);

        /**
         * Determine the user associated with these changes.
         * @return  user id or {@link ObservationConstants#OAK_UNKNOWN} if {@link #isExternal()} is {@code true}.
         */
        public abstract String getUserId();

        /**
         * Determine the date when these changes where persisted.
         * @return  date or {@code 0} if {@link #isExternal()} is {@code true}.
         */
        public abstract long getDate();

        /**
         * State before the change
         * @return  before state
         */
        public NodeState getBeforeState() {
            return before;
        }

        /**
         * State after the change
         * @return  after state
         */
        public NodeState getAfterState() {
            return after;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("base", before)
                .add("head", after)
                .add("userId", getUserId())
                .add("date", getDate())
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
            return before.equals(that.before) && after.equals(that.after);
        }

        @Override
        public int hashCode() {
            return 31 * before.hashCode() + after.hashCode();
        }

        private static class InternalChangeSet extends ChangeSet {
            private static final String DUMMY_USER_DATA =
                    TODO.dummyImplementation().returnValueOrNull("oak:not implemented");

            private final ContentSession contentSession;
            private final String userId;
            private final long date;

            InternalChangeSet(NodeState base, NodeState head, ContentSession contentSession, long date) {
                super(base, head);
                this.contentSession = contentSession;
                this.userId = contentSession.getAuthInfo().getUserID();
                this.date = date;
            }

            @Override
            public boolean isExternal() {
                return false;
            }

            @Override
            public boolean isLocal(ContentSession contentSession) {
                return this.contentSession == contentSession;
            }

            @Override
            public String getUserId() {
                return userId;
            }

            @Override
            public long getDate() {
                return date;
            }

            @Override
            public boolean equals(Object other) {
                if (!super.equals(other)) {
                    return false;
                }

                InternalChangeSet that = (InternalChangeSet) other;
                return date == that.date && contentSession == that.contentSession;
            }
        }

        private static class ExternalChangeSet extends ChangeSet {
            ExternalChangeSet(NodeState base, NodeState head) {
                super(base, head);
            }

            @Override
            public boolean isExternal() {
                return true;
            }

            @Override
            public boolean isLocal(ContentSession contentSession) {
                return false;
            }

            @Override
            public String getUserId() {
                return OAK_UNKNOWN;
            }

            @Override
            public long getDate() {
                return 0;
            }
        }

    }
}
