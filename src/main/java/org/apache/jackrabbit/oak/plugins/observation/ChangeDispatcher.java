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
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.TODO;

public class ChangeDispatcher {
    private final NodeStore store;
    private final Set<Listener> listeners = Sets.newHashSet();

    private NodeState previous;

    public ChangeDispatcher(NodeStore store) {
        this.store = store;
        previous = checkNotNull(store.getRoot());
    }

    @Nonnull
    public Hook createHook(ContentSession contentSession) {
        return new Hook(contentSession);
    }

    @Nonnull
    public Listener newListener() {
        Listener listener = new Listener();
        register(listener);
        return listener;
    }

    private void contentChanged(@Nonnull NodeState before, @Nonnull NodeState after, ContentSession contentSession) {
        ChangeSet extChanges;
        ChangeSet intChange;
        synchronized (this) {
            extChanges = externalChange(checkNotNull(before));
            intChange = internalChange(checkNotNull(after), contentSession);
        }
        if (extChanges != null) {
            add(extChanges);
        }
        add(intChange);
    }

    @CheckForNull
    private synchronized ChangeSet externalChange(NodeState root) {
        if (root != previous) {
            ChangeSet changeSet = ChangeSet.external(previous, root);
            previous = root;
            return changeSet;
        }
        return null;
    }

    @Nonnull
    private synchronized ChangeSet internalChange(NodeState root, ContentSession contentSession) {
        ChangeSet changeSet = ChangeSet.local(previous, root, contentSession);
        previous = root;
        return changeSet;
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

    public class Listener {
        private final Queue<ChangeSet> changeSets = Queues.newLinkedBlockingQueue();

        public void dispose() {
            unregister(this);
        }

        @CheckForNull
        public ChangeSet getChanges() {
            if (changeSets.isEmpty()) {
                add(externalChange(store.getRoot()));
            }
            return changeSets.isEmpty() ? null : changeSets.remove();
        }

        private void add(ChangeSet changeSet) {
            changeSets.add(changeSet);
        }
    }

    //------------------------------------------------------------< ChangeSet >---

    public abstract static class ChangeSet {
        private final NodeState before;
        private final NodeState after;

        public static ChangeSet local(NodeState base, NodeState head, ContentSession contentSession) {
            return new InternalChangeSet(base, head, contentSession, System.currentTimeMillis());
        }

        public static ChangeSet external(NodeState base, NodeState head) {
            return new ExternalChangeSet(base, head);
        }

        protected ChangeSet(NodeState before, NodeState after) {
            this.before = before;
            this.after = after;
        }

        public abstract boolean isExternal();
        public abstract boolean isLocal(ContentSession contentSession);
        public abstract String getUserId();
        public abstract String getUserData();
        public abstract long getDate();

        public void diff(NodeStateDiff diff) {
            after.compareAgainstBaseState(before, diff);
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("base", before)
                .add("head", after)
                .add("userId", getUserId())
                .add("userData", getUserData())
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
            public String getUserData() {
                // TODO implement getUserData
                return DUMMY_USER_DATA;
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
            public String getUserData() {
                return OAK_UNKNOWN;
            }

            @Override
            public long getDate() {
                return 0;
            }
        }

    }
}
