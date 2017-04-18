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

import java.io.Closeable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@code ChangeDispatcher} instance dispatches content changes
 * to registered {@link Observer}s.
 * <p>
 * Changes are reported by calling {@link #contentChanged(NodeState, CommitInfo)}.
 * <p>
 * The {@link #addObserver(Observer)} method registers an {@link Observer} for receiving
 * notifications for all changes reported to this instance.
 */
public class ChangeDispatcher implements Observable, Observer {
    private final CompositeObserver observers = new CompositeObserver();

    @Nonnull
    private NodeState root;

    /**
     * Create a new instance for dispatching content changes
     * @param root  current root node state
     */
    public ChangeDispatcher(@Nonnull NodeState root) {
        this.root = checkNotNull(root);
    }

    /**
     * Register a new {@link Observer} for receiving notifications about changes reported to
     * this change dispatcher. Changes are reported synchronously and clients need to ensure
     * to no block any length of time (e.g. by relaying through a {@link BackgroundObserver}).
     * <p>
     * Clients need to call {@link java.io.Closeable#close()} close} on the returned
     * {@code Closeable} instance to stop receiving notifications.
     *
     * @return  a {@link Closeable} instance
     */
    @Override
    @Nonnull
    public synchronized Closeable addObserver(final Observer observer) {
        observer.contentChanged(root, CommitInfo.EMPTY_EXTERNAL);
        observers.addObserver(observer);
        return new Closeable() {
            @Override
            public void close() {
                observers.removeObserver(observer);
            }
        };
    }

    @Override
    public synchronized void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
        checkNotNull(root);
        checkNotNull(info);
        observers.contentChanged(root, info);
        this.root = root;
    }
}
