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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@code ChangeDispatcher} instance records changes to a
 * {@link org.apache.jackrabbit.oak.spi.state.NodeStore}
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
    private final CompositeObserver observers = new CompositeObserver();

    @Nonnull
    private NodeState root;

    /**
     * Create a new instance for recording changes to a {@code NodeStore}
     * @param root  current root node state of the node store
     */
    public ChangeDispatcher(@Nonnull NodeState root) {
        this.root = checkNotNull(root);
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
        // FIXME don't hard code queue size
        final BackgroundObserver backgroundObserver = new BackgroundObserver(observer, 8192);
        backgroundObserver.contentChanged(root, null);
        observers.addObserver(backgroundObserver);
        return new Closeable() {
            @Override
            public void close() {
                backgroundObserver.stop();
                observers.removeObserver(backgroundObserver);
            }
        };
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
        checkNotNull(root);
        changeCount.incrementAndGet();
        observers.contentChanged(root, null);
        this.root = root;
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
        observers.contentChanged(root, info);
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
        checkNotNull(root);
        observers.contentChanged(root, null);
        this.root = root;
        changeCount.incrementAndGet();
    }

}
