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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An observer that implements filtering of content changes
 * while at the same time supporting (wrapping) a BackgroundObserver
 * underneath.
 * <p>
 * The FilteringObserver uses an explicit Filter to decide whether
 * or not to forward a content change to the BackgroundObserver.
 * If the Filter decides to include the change things happen as usual.
 * If the Filter decides to exclude the change, this FilteringObserver
 * does not forward the change, but remembers the fact that the last
 * change was filtered. The first included change after excluded ones
 * will cause a NOOP_CHANGE commitInfo to be passed along to the
 * BackgroundObserver. That NOOP_CHANGE is then used by the
 * FilteringDispatcher: if a CommitInfo is a NOOP_CHANGE then the
 * FilteringDispatcher will not forward anything to the FilteringAwareObserver
 * and only adjust the 'before' state accordingly (which it does also
 * for a NOOP_CHANGE, to exactly achieve the skipping effect).
 */
public class FilteringObserver implements Observer, Closeable {

    /** package protected CommitInfo used between FilteringObserver and FilteringDispatcher **/
    final static CommitInfo NOOP_CHANGE = new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN);

    private final BackgroundObserver backgroundObserver;

    private final Filter filter;

    private NodeState lastNoop;

    /**
     * Default constructor which creates a BackgroundObserver automatically, including
     * creating a FilteringDispatcher.
     * @param executor the executor that should be used for the BackgroundObserver
     * @param queueLength the queue length of the BackgroundObserver
     * @param filter the Filter to be used for filtering
     * @param observer the FilteringAwareObserver to which content changes ultimately
     * are delivered after going through a chain of 
     * FilteringObserver-&gt;BackgroundObserver-&gt;FilteringDispatcher.
     */
    public FilteringObserver(@Nonnull Executor executor, int queueLength, @Nonnull Filter filter,
            @Nonnull FilteringAwareObserver observer) {
        this(new BackgroundObserver(new FilteringDispatcher(checkNotNull(observer)), checkNotNull(executor),
                queueLength), filter);
    }

    /**
     * Alternative constructor where the BackgroundObserver is created elsewhere
     * @param backgroundObserver the BackgroundObserver to be used by this FilteringObserver
     * @param filter the Filter to be used for filtering
     */
    public FilteringObserver(@Nonnull BackgroundObserver backgroundObserver, @Nonnull Filter filter) {
        this.backgroundObserver = backgroundObserver;
        this.filter = checkNotNull(filter);
    }

    public BackgroundObserver getBackgroundObserver() {
        return backgroundObserver;
    }

    @Override
    public final void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
        if (filter.excludes(root, info)) {
            lastNoop = root;
            return;
        }
        // current change is not an noop
        if (lastNoop != null) {
            // report up to previous noop
            backgroundObserver.contentChanged(lastNoop, NOOP_CHANGE);
            lastNoop = null;
        }
        backgroundObserver.contentChanged(root, info);
    }

    @Override
    public void close() {
        backgroundObserver.close();
    }

}
