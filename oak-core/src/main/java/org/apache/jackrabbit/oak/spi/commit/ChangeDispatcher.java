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
package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Queues.newLinkedBlockingQueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
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
 * The {@link #addObserver(Observer)} method registers an {@link Observer} for receiving
 * notifications about all changes reported to this instance.
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
     * Register a new {@link Observer} for receiving notifications about changes reported to
     * this change dispatcher. Changes are reported asynchronously. Clients need to
     * call {@link java.io.Closeable#close()} close} on the returned {@code Closeable} instance
     * to stop receiving notifications.
     *
     * @return  a {@link Closeable} instance
     */
    @Override
    @Nonnull
    public Closeable addObserver(Observer observer) {
        Listener listener = new Listener(observer, root);
        listener.start();
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
        for (Listener l : getListeners()) {
            l.contentChanged(root, info);
        }
    }

    private Listener[] getListeners() {
        synchronized (listeners) {
            return listeners.toArray(new Listener[listeners.size()]);
        }
    }

    //------------------------------------------------------------< Listener >---

    /**
     * Listener thread receiving changes reported into {@code ChangeDispatcher} and
     * asynchronously distributing these to an associated {@link Observer}.
     */
    private class Listener extends Thread implements Closeable, Observer {
        private final LinkedBlockingQueue<Commit> commits = newLinkedBlockingQueue();
        private final Observer observer;

        private boolean blocked = false;
        private volatile boolean stopping;

        Listener(Observer observer, NodeState root) {
            this.observer = checkNotNull(observer);
            commits.add(new Commit(root, null));
            setDaemon(true);
            setPriority(Thread.MIN_PRIORITY);
        }

        @Override
        public void contentChanged(NodeState root, CommitInfo info) {
            Commit commit = new Commit(root, blocked ? null : info);
            blocked = !commits.offer(commit);
        }

        @Override
        public void run() {
            try {
                while (!stopping) {
                    if (commits.isEmpty()) {
                        externalChange();
                    }
                    Commit commit = commits.poll(100, TimeUnit.MILLISECONDS);
                    if (commit != null) {
                        observer.contentChanged(commit.getRoot(), commit.getCommitInfo());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() throws IOException {
            checkState(!stopping, "Change processor already stopped");

            unregister(this);
            stopping = true;
            if (Thread.currentThread() != this) {
                try {
                    join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException
                            ("Interruption while waiting for the listener thread to terminate", e);
                }
            }
        }
    }

    //------------------------------------------------------------< Commit >---

    private static class Commit {
        private final NodeState root;
        private final CommitInfo commitInfo;

        Commit(@Nonnull NodeState root, @Nullable CommitInfo commitInfo) {
            this.root = checkNotNull(root);
            this.commitInfo = commitInfo;
        }

        @Nonnull
        NodeState getRoot() {
            return root;
        }

        @CheckForNull
        CommitInfo getCommitInfo() {
            return commitInfo;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("root", root)
                .add("commit info", commitInfo)
                .toString();
        }
    }

}
