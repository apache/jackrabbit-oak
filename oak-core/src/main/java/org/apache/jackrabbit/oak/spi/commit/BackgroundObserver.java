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
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Queues.newArrayBlockingQueue;

import java.io.Closeable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.commons.concurrent.NotifyingFutureTask;
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
 * the background observer thread has yet to process are optionally
 * (see {@code alwaysCollapseExternalEvents} and {@code oak.observation.alwaysCollapseExternal})
 * automatically merged to just one change.
 */
public class BackgroundObserver implements Observer, Closeable {

    /**
     * Signal for the background thread to stop processing changes.
     */
    private static final ContentChange STOP = new ContentChange(null, null);

    /**
     * The receiving observer being notified off the background thread.
     */
    private final Observer observer;

    /**
     * Executor used to dispatch events
     */
    private final Executor executor;

    /**
     * Handler for uncaught exception on the background thread
     */
    private final UncaughtExceptionHandler exceptionHandler;

    /**
     * The queue of content changes to be processed.
     */
    private final BlockingQueue<ContentChange> queue;

    /**
     * The max queue length used for this observer's queue
     */
    private final int maxQueueLength;

    /**
     * Whether external events should be collapsed even if queue isn't full yet.
     */
    private final boolean alwaysCollapseExternalEvents =
            Boolean.parseBoolean(System.getProperty("oak.observation.alwaysCollapseExternal", "false"));

    private static class ContentChange {
        private final NodeState root;
        private final CommitInfo info;
        ContentChange(NodeState root, CommitInfo info) {
            this.root = root;
            this.info = info;
        }
    }

    /**
     * The content change that was last added to the queue.
     * Used to compact external changes.
     */
    private ContentChange last;

    /**
     * Flag to indicate that some content changes were dropped because
     * the queue was full.
     */
    private boolean full;

    /**
     * Current background task
     */
    private volatile NotifyingFutureTask currentTask = NotifyingFutureTask.completed();

    /**
     * Completion handler: set the current task to the next task and schedules that one
     * on the background thread.
     */
    private final Runnable completionHandler = new Runnable() {
        Callable<Void> task = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    ContentChange change = queue.poll();
                    if (change != null && change != STOP) {
                        observer.contentChanged(change.root, change.info);
                        currentTask.onComplete(completionHandler);
                    }
                } catch (Throwable t) {
                    exceptionHandler.uncaughtException(Thread.currentThread(), t);
                }
                return null;
            }
        };

        @Override
        public void run() {
            currentTask = new NotifyingFutureTask(task);
            executor.execute(currentTask);
        }
    };

    /**
     * {@code true} after this observer has been stopped
     */
    private volatile boolean stopped;

    public BackgroundObserver(
            @Nonnull Observer observer,
            @Nonnull Executor executor,
            int queueLength,
            @Nonnull UncaughtExceptionHandler exceptionHandler) {
        this.observer = checkNotNull(observer);
        this.executor = checkNotNull(executor);
        this.exceptionHandler = checkNotNull(exceptionHandler);
        this.maxQueueLength = queueLength;
        this.queue = newArrayBlockingQueue(maxQueueLength);
    }

    public BackgroundObserver(
            @Nonnull final Observer observer,
            @Nonnull Executor executor,
            int queueLength) {
        this(observer, executor, queueLength, new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                getLogger(observer).error("Uncaught exception in " + observer, e);
            }
        });
    }

    public BackgroundObserver(
            @Nonnull Observer observer,
            @Nonnull Executor executor) {
        this(observer, executor, 1000);
    }

    /**
     * Called when ever an item has been added to the queue
     * @param queueSize  size of the queue
     */
    protected void added(int queueSize) { }

    /**
     * @return  The max queue length used for this observer's queue
     */
    public int getMaxQueueLength() {
        return maxQueueLength;
    }

    /**
     * Clears the change queue and signals the background thread to stop
     * without making any further {@link #contentChanged(NodeState, CommitInfo)}
     * calls to the background observer. If the thread is currently in the
     * middle of such a call, then that call is allowed to complete; i.e.
     * the thread is not forcibly interrupted. This method returns immediately
     * without blocking to wait for the thread to finish.
     * <p>
     * After a call to this method further calls to {@link #contentChanged(NodeState, CommitInfo)}
     * will throw a {@code IllegalStateException}.
     */
    @Override
    public synchronized void close() {
        queue.clear();
        queue.add(STOP);
        stopped = true;
    }

    @Nonnull
    public BackgroundObserverMBean getMBean(){
        return new BackgroundObserverMBean() {
            @Override
            public String getClassName() {
                return observer.getClass().getName();
            }

            @Override
            public int getQueueSize() {
                return queue.size();
            }

            @Override
            public int getMaxQueueSize() {
                return getMaxQueueLength();
            }

            @Override
            public int getLocalEventCount() {
                return size(filter(queue, new Predicate<ContentChange>() {
                    @Override
                    public boolean apply(ContentChange input) {
                        return input.info != null;
                    }
                }));
            }

            @Override
            public int getExternalEventCount() {
                return size(filter(queue, new Predicate<ContentChange>() {
                    @Override
                    public boolean apply(ContentChange input) {
                        return input.info == null;
                    }
                }));
            }
        };
    }

    //----------------------------------------------------------< Observer >--

    /**
     * @throws IllegalStateException  if {@link #close()} has already been called.
     */
    @Override
    public synchronized void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        checkState(!stopped);
        checkNotNull(root);

        if (alwaysCollapseExternalEvents && info == null && last != null && last.info == null) {
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

        // Set the completion handler on the currently running task. Multiple calls
        // to onComplete are not a problem here since we always pass the same value.
        // Thus there is no question as to which of the handlers will effectively run.
        currentTask.onComplete(completionHandler);
        added(queue.size());
    }

    //------------------------------------------------------------< internal >---

    private static Logger getLogger(@Nonnull Observer observer) {
        return LoggerFactory.getLogger(checkNotNull(observer).getClass());
    }
}
