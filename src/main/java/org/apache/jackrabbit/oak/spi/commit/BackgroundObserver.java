/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Queues.newArrayBlockingQueue;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.BlockingQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An observer that uses a change queue and a background thread to forward
 * content changes to another observer. The mechanism is designed so that
 * the {@link #contentChanged(NodeState, CommitInfo)} method will never block,
 * regardless of the behavior of the other observer. If that observer blocks
 * or is too slow to consume all content changes, causing the change queue
 * to fill up, any further update will automatically be merged into just one
 * external content change, causing potential loss of local commit information.
 * To help prevent such cases, any sequential external content changes that
 * the background observer thread has yet to process are automatically merged
 * to just one change.
 */
public class BackgroundObserver implements Observer {
    private static final Logger log = LoggerFactory.getLogger(BackgroundObserver.class);

    private static class ContentChange {
        private final NodeState root;
        private final CommitInfo info;
        ContentChange(NodeState root, CommitInfo info) {
            this.root = root;
            this.info = info;
        }
    }

    /**
     * Signal for the background thread to stop processing changes.
     */
    private static final ContentChange STOP = new ContentChange(null, null);

    private static Logger getLogger(@Nonnull Observer observer) {
        return LoggerFactory.getLogger(checkNotNull(observer).getClass());
    }

    /**
     * The queue of content changes to be processed.
     */
    private final BlockingQueue<ContentChange> queue;

    /**
     * The content change that was last added to the queue.
     * Used to compact external changes.
     */
    private ContentChange last = null;

    /**
     * Flag to indicate that some content changes were dropped because
     * the queue was full.
     */
    private boolean full = false;

    /**
     * The background thread used to process changes.
     */
    private final Thread thread;

    public BackgroundObserver(
            @Nonnull final Observer observer, int queueLength,
            @Nonnull UncaughtExceptionHandler exceptionHandler) {
        checkNotNull(observer);
        checkArgument(queueLength > 0);
        checkNotNull(exceptionHandler);

        this.queue = newArrayBlockingQueue(queueLength);

        this.thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        ContentChange change = queue.take();
                        if (change == STOP) {
                            return;
                        }
                        observer.contentChanged(change.root, change.info);
                    }
                } catch (InterruptedException e) {
                    getLogger(observer).warn(
                            "Event processing interrupted for " + observer, e);
                }
            }
        });
        thread.setName(observer.toString());
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(exceptionHandler);
        thread.start();
    }

    public BackgroundObserver(
            @Nonnull final Observer observer, int queueLength) {
        this(observer, queueLength, new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                getLogger(observer).error(
                        "Uncaught exception in " + observer, e);
            }
        });
    }

    public BackgroundObserver(@Nonnull Observer observer) {
        this(observer, 1000);
    }

    /**
     * Clears the change queue and signals the background thread to stop
     * without making any further {@link #contentChanged(NodeState, CommitInfo)}
     * calls to the background observer. If the thread is currently in the
     * middle of such a call, then that call is allowed to complete; i.e.
     * the thread is not forcibly interrupted. This method returns immediately
     * without blocking to wait for the thread to finish.
     */
    public synchronized void stop() {
        queue.clear();
        queue.add(STOP);
        try {
            if (thread != Thread.currentThread()) {
                thread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Thread interrupted while joining observation thread.", e);
        }
    }

    //----------------------------------------------------------< Observer >--

    @Override
    public synchronized void contentChanged(
            @Nonnull NodeState root, @Nullable CommitInfo info) {
        checkNotNull(root);

        if (info == null && last != null && last.info == null) {
            // This is an external change. If the previous change was
            // also external, we can drop it from the queue (since external
            // changes in any case can cover multiple commits) to help
            // prevent the queue from filling up too fast.
            queue.remove(last);
            full = false;
        }

        ContentChange change;
        if (full) {
            // If the queue is full, some commits have already been skipped
            // so we need to drop the possible local commit information as
            // only external changes can be merged together to larger chunks.
            change = new ContentChange(root, null);
        } else {
            change = new ContentChange(root, info);
        }

        // Try to add this change to the queue without blocking, and
        // mark the queue as full if there wasn't enough space
        full = !queue.offer(change);

        if (!full) {
            // Keep track of the last change added, so we can do the
            // compacting of external changes shown above.
            last = change;
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return thread.getName() + " &";
    }

}
