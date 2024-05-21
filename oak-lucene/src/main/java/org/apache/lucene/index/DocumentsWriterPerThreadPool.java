/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.index;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link DocumentsWriterPerThreadPool} controls {@link ThreadState} instances and their thread
 * assignments during indexing. Each {@link ThreadState} holds a reference to a
 * {@link DocumentsWriterPerThread} that is once a {@link ThreadState} is obtained from the pool
 * exclusively used for indexing a single document by the obtaining thread. Each indexing thread
 * must obtain such a {@link ThreadState} to make progress. Depending on the
 * {@link DocumentsWriterPerThreadPool} implementation {@link ThreadState} assignments might differ
 * from document to document.
 * <p>
 * Once a {@link DocumentsWriterPerThread} is selected for flush the thread pool is reusing the
 * flushing {@link DocumentsWriterPerThread}s ThreadState with a new
 * {@link DocumentsWriterPerThread} instance.
 * </p>
 */
abstract class DocumentsWriterPerThreadPool implements Cloneable {

    /**
     * {@link ThreadState} references and guards a {@link DocumentsWriterPerThread} instance that is
     * used during indexing to build a in-memory index segment. {@link ThreadState} also holds all
     * flush related per-thread data controlled by {@link DocumentsWriterFlushControl}.
     * <p>
     * A {@link ThreadState}, its methods and members should only accessed by one thread a time.
     * Users must acquire the lock via {@link ThreadState#lock()} and release the lock in a finally
     * block via {@link ThreadState#unlock()} before accessing the state.
     */
    @SuppressWarnings("serial")
    final static class ThreadState extends ReentrantLock {

        DocumentsWriterPerThread dwpt;
        // TODO this should really be part of DocumentsWriterFlushControl
        // write access guarded by DocumentsWriterFlushControl
        volatile boolean flushPending = false;
        // TODO this should really be part of DocumentsWriterFlushControl
        // write access guarded by DocumentsWriterFlushControl
        long bytesUsed = 0;
        // guarded by Reentrant lock
        private boolean isActive = true;

        ThreadState(DocumentsWriterPerThread dpwt) {
            this.dwpt = dpwt;
        }

        /**
         * Resets the internal {@link DocumentsWriterPerThread} with the given one. if the given
         * DWPT is <code>null</code> this ThreadState is marked as inactive and should not be used
         * for indexing anymore.
         *
         * @see #isActive()
         */

        private void deactivate() {
            assert this.isHeldByCurrentThread();
            isActive = false;
            reset();
        }

        private void reset() {
            assert this.isHeldByCurrentThread();
            this.dwpt = null;
            this.bytesUsed = 0;
            this.flushPending = false;
        }

        /**
         * Returns <code>true</code> if this ThreadState is still open. This will only return
         * <code>false</code> iff the DW has been closed and this ThreadState is already checked
         * out
         * for flush.
         */
        boolean isActive() {
            assert this.isHeldByCurrentThread();
            return isActive;
        }

        boolean isInitialized() {
            assert this.isHeldByCurrentThread();
            return isActive() && dwpt != null;
        }

        /**
         * Returns the number of currently active bytes in this ThreadState's
         * {@link DocumentsWriterPerThread}
         */
        public long getBytesUsedPerThread() {
            assert this.isHeldByCurrentThread();
            // public for FlushPolicy
            return bytesUsed;
        }

        /**
         * Returns this {@link ThreadState}s {@link DocumentsWriterPerThread}
         */
        public DocumentsWriterPerThread getDocumentsWriterPerThread() {
            assert this.isHeldByCurrentThread();
            // public for FlushPolicy
            return dwpt;
        }

        /**
         * Returns <code>true</code> iff this {@link ThreadState} is marked as flush pending
         * otherwise <code>false</code>
         */
        public boolean isFlushPending() {
            return flushPending;
        }
    }

    private ThreadState[] threadStates;
    private volatile int numThreadStatesActive;

    /**
     * Creates a new {@link DocumentsWriterPerThreadPool} with a given maximum of
     * {@link ThreadState}s.
     */
    DocumentsWriterPerThreadPool(int maxNumThreadStates) {
        if (maxNumThreadStates < 1) {
            throw new IllegalArgumentException(
                "maxNumThreadStates must be >= 1 but was: " + maxNumThreadStates);
        }
        threadStates = new ThreadState[maxNumThreadStates];
        numThreadStatesActive = 0;
        for (int i = 0; i < threadStates.length; i++) {
            threadStates[i] = new ThreadState(null);
        }
    }

    @Override
    public DocumentsWriterPerThreadPool clone() {
        // We should only be cloned before being used:
        if (numThreadStatesActive != 0) {
            throw new IllegalStateException("clone this object before it is used!");
        }

        DocumentsWriterPerThreadPool clone;
        try {
            clone = (DocumentsWriterPerThreadPool) super.clone();
        } catch (CloneNotSupportedException e) {
            // should not happen
            throw new RuntimeException(e);
        }
        clone.threadStates = new ThreadState[threadStates.length];
        for (int i = 0; i < threadStates.length; i++) {
            clone.threadStates[i] = new ThreadState(null);
        }
        return clone;
    }

    /**
     * Returns the max number of {@link ThreadState} instances available in this
     * {@link DocumentsWriterPerThreadPool}
     */
    int getMaxThreadStates() {
        return threadStates.length;
    }

    /**
     * Returns the active number of {@link ThreadState} instances.
     */
    int getActiveThreadState() {
        return numThreadStatesActive;
    }


    /**
     * Returns a new {@link ThreadState} iff any new state is available otherwise
     * <code>null</code>.
     * <p>
     * NOTE: the returned {@link ThreadState} is already locked iff non-
     * <code>null</code>.
     *
     * @return a new {@link ThreadState} iff any new state is available otherwise
     * <code>null</code>
     */
    synchronized ThreadState newThreadState() {
        if (numThreadStatesActive < threadStates.length) {
            final ThreadState threadState = threadStates[numThreadStatesActive];
            threadState.lock(); // lock so nobody else will get this ThreadState
            boolean unlock = true;
            try {
                if (threadState.isActive()) {
                    // unreleased thread states are deactivated during DW#close()
                    numThreadStatesActive++; // increment will publish the ThreadState
                    assert threadState.dwpt == null;
                    unlock = false;
                    return threadState;
                }
                // unlock since the threadstate is not active anymore - we are closed!
                assert assertUnreleasedThreadStatesInactive();
                return null;
            } finally {
                if (unlock) {
                    // in any case make sure we unlock if we fail
                    threadState.unlock();
                }
            }
        }
        return null;
    }

    private synchronized boolean assertUnreleasedThreadStatesInactive() {
        for (int i = numThreadStatesActive; i < threadStates.length; i++) {
            assert threadStates[i].tryLock() : "unreleased threadstate should not be locked";
            try {
                assert !threadStates[i].isInitialized() : "expected unreleased thread state to be inactive";
            } finally {
                threadStates[i].unlock();
            }
        }
        return true;
    }

    /**
     * Deactivate all unreleased threadstates
     */
    synchronized void deactivateUnreleasedStates() {
        for (int i = numThreadStatesActive; i < threadStates.length; i++) {
            final ThreadState threadState = threadStates[i];
            threadState.lock();
            try {
                threadState.deactivate();
            } finally {
                threadState.unlock();
            }
        }
    }

    DocumentsWriterPerThread reset(ThreadState threadState, boolean closed) {
        assert threadState.isHeldByCurrentThread();
        final DocumentsWriterPerThread dwpt = threadState.dwpt;
        if (!closed) {
            threadState.reset();
        } else {
            threadState.deactivate();
        }
        return dwpt;
    }

    void recycle(DocumentsWriterPerThread dwpt) {
        // don't recycle DWPT by default
    }

    // you cannot subclass this without being in o.a.l.index package anyway, so
    // the class is already pkg-private... fix me: see LUCENE-4013
    abstract ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter);


    /**
     * Returns the <i>i</i>th active {@link ThreadState} where <i>i</i> is the given ord.
     *
     * @param ord the ordinal of the {@link ThreadState}
     * @return the <i>i</i>th active {@link ThreadState} where <i>i</i> is the given ord.
     */
    ThreadState getThreadState(int ord) {
        return threadStates[ord];
    }

    /**
     * Returns the ThreadState with the minimum estimated number of threads waiting to acquire its
     * lock or <code>null</code> if no {@link ThreadState} is yet visible to the calling thread.
     */
    ThreadState minContendedThreadState() {
        ThreadState minThreadState = null;
        final int limit = numThreadStatesActive;
        for (int i = 0; i < limit; i++) {
            final ThreadState state = threadStates[i];
            if (minThreadState == null
                || state.getQueueLength() < minThreadState.getQueueLength()) {
                minThreadState = state;
            }
        }
        return minThreadState;
    }

    /**
     * Returns the number of currently deactivated {@link ThreadState} instances. A deactivated
     * {@link ThreadState} should not be used for indexing anymore.
     *
     * @return the number of currently deactivated {@link ThreadState} instances.
     */
    int numDeactivatedThreadStates() {
        int count = 0;
        for (int i = 0; i < threadStates.length; i++) {
            final ThreadState threadState = threadStates[i];
            threadState.lock();
            try {
                if (!threadState.isActive) {
                    count++;
                }
            } finally {
                threadState.unlock();
            }
        }
        return count;
    }

    /**
     * Deactivates an active {@link ThreadState}. Inactive {@link ThreadState} can not be used for
     * indexing anymore once they are deactivated. This method should only be used if the parent
     * {@link DocumentsWriter} is closed or aborted.
     *
     * @param threadState the state to deactivate
     */
    void deactivateThreadState(ThreadState threadState) {
        assert threadState.isActive();
        threadState.deactivate();
    }
}
