/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>CommitQueue</code> ensures a sequence of commits consistent with the
 * commit revision even if commits did not complete in this sequence.
 */
final class CommitQueue {

    static final Logger LOG = LoggerFactory.getLogger(CommitQueue.class);

    /**
     * The default suspend timeout in milliseconds: 60'000.
     */
    static final long DEFAULT_SUSPEND_TIMEOUT = TimeUnit.MINUTES.toMillis(1);

    private final SortedMap<Revision, Entry> commits = new TreeMap<Revision, Entry>(StableRevisionComparator.INSTANCE);

    /**
     * Map of currently suspended commits until a given Revision is visible.
     */
    private final Map<Semaphore, Revision> suspendedCommits = Maps.newIdentityHashMap();

    private final RevisionContext context;

    private long suspendTimeout = Long.getLong("oak.documentMK.suspendTimeoutMillis", DEFAULT_SUSPEND_TIMEOUT);

    CommitQueue(@Nonnull RevisionContext context) {
        this.context = checkNotNull(context);
    }

    @Nonnull
    Revision createRevision() {
        return createRevisions(1).first();
    }

    @Nonnull
    SortedSet<Revision> createRevisions(int num) {
        checkArgument(num > 0);
        SortedSet<Revision> revs = new TreeSet<Revision>(StableRevisionComparator.INSTANCE);
        Revision rev = null;
        synchronized (this) {
            for (int i = 0; i < num; i++) {
                rev = context.newRevision();
                revs.add(rev);
            }
            commits.put(rev, new Entry(rev));
        }
        LOG.debug("created commit {}", rev);
        return revs;
    }

    void done(@Nonnull Revision revision, @Nonnull Callback c) {
        checkNotNull(revision);
        waitUntilHeadOfQueue(revision, c);
    }

    void canceled(@Nonnull Revision rev) {
        removeCommit(rev);
        notifySuspendedCommits(rev);
    }

    boolean contains(@Nonnull Revision revision) {
        synchronized (this) {
            return commits.containsKey(checkNotNull(revision));
        }
    }

    /**
     * Suspends until one of the following happens:
     * <ul>
     *     <li>the given revision is visible from the current headRevision</li>
     *     <li>the given revision is canceled from the commit queue</li>
     *     <li>the suspend timeout is reached. See {@link #setSuspendTimeoutMillis(long)}</li>
     *     <li>the thread is interrupted</li>
     * </ul>
     *
     * @param r the revision to become visible.
     */
    void suspendUntil(@Nonnull Revision r) {
        Comparator<Revision> comparator = context.getRevisionComparator();
        Semaphore s = null;
        synchronized (suspendedCommits) {
            Revision headRevision = context.getHeadRevision();
            if (comparator.compare(r, headRevision) > 0) {
                s = new Semaphore(0);
                suspendedCommits.put(s, r);
            }
        }
        if (s != null) {
            try {
                s.tryAcquire(suspendTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                synchronized (suspendedCommits) {
                    suspendedCommits.remove(s);
                }
            }
        }
    }

    /**
     * Called when the head revision accessible via the {@link RevisionContext}
     * passed to constructor changed.
     */
    void headRevisionChanged() {
        notifySuspendedCommits();
    }

    /**
     * @return the number of suspended threads on this commit queue.
     */
    int numSuspendedThreads() {
        synchronized (suspendedCommits) {
            return suspendedCommits.size();
        }
    }

    /**
     * Sets the suspend timeout in milliseconds.
     * See also {@link #suspendUntil(Revision)}.
     *
     * @param timeout the timeout to set.
     */
    void setSuspendTimeoutMillis(long timeout) {
        this.suspendTimeout = timeout;
    }

    interface Callback {

        void headOfQueue(@Nonnull Revision revision);
    }

    //------------------------< internal >--------------------------------------

    private void notifySuspendedCommits() {
        synchronized (suspendedCommits) {
            if (suspendedCommits.isEmpty()) {
                return;
            }
            Comparator<Revision> comparator = context.getRevisionComparator();
            Revision headRevision = context.getHeadRevision();
            Iterator<Map.Entry<Semaphore, Revision>> it = suspendedCommits.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Semaphore, Revision> entry = it.next();
                if (comparator.compare(entry.getValue(), headRevision) <= 0) {
                    Semaphore s = entry.getKey();
                    it.remove();
                    s.release();
                }
            }
        }
    }

    private void notifySuspendedCommits(@Nonnull Revision revision) {
        checkNotNull(revision);
        synchronized (suspendedCommits) {
            if (suspendedCommits.isEmpty()) {
                return;
            }
            Iterator<Map.Entry<Semaphore, Revision>> it = suspendedCommits.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Semaphore, Revision> entry = it.next();
                if (revision.equals(entry.getValue())) {
                    Semaphore s = entry.getKey();
                    it.remove();
                    s.release();
                }
            }
        }
    }

    private void removeCommit(@Nonnull Revision rev) {
        // simply remove and notify next head if any
        synchronized (this) {
            boolean wasHead = commits.firstKey().equals(rev);
            commits.remove(rev);
            LOG.debug("removed commit {}, wasHead={}", rev, wasHead);
            if (wasHead) {
                notifyHead();
            }
        }
    }

    private void waitUntilHeadOfQueue(@Nonnull Revision rev,
                                      @Nonnull Callback c) {
        assert !commits.isEmpty();

        boolean isHead;
        Entry commitEntry;
        synchronized (this) {
            isHead = commits.firstKey().equals(rev);
            commitEntry = commits.get(rev);
        }
        if (!isHead) {
            LOG.debug("not head: {}, waiting...", rev);
            commitEntry.await();
        }
        synchronized (this) {
            commits.remove(rev);
            try {
                LOG.debug("removed {}, head is now {}", rev, commits.isEmpty() ? null : commits.firstKey());
                c.headOfQueue(rev);
            } finally {
                // notify next if there is any
                notifyHead();
            }
        }
    }

    private void notifyHead() {
        if (!commits.isEmpty()) {
            LOG.debug("release {}", commits.firstKey());
            commits.get(commits.firstKey()).release();
        }
    }

    /**
     * An entry in the commit queue.
     */
    private static final class Entry {

        /**
         * The revision of the commit (used for debugging).
         */
        private final Revision revision;

        /**
         * The latch. Initially set to 1, so that release() needs to be called
         * once for await() to continue.
         */
        private final CountDownLatch latch = new CountDownLatch(1);

        Entry(Revision revision) {
            this.revision = revision;
        }

        /**
         * Release all threads that are waiting.
         */
        void release() {
            latch.countDown();
        }

        /**
         * Wait for the latch to be released.
         */
        void await() {
            for (;;) {
                try {
                    LOG.debug("awaiting {}", revision);
                    latch.await();
                    break;
                } catch (InterruptedException e) {
                    // retry
                }
            }
        }
    }
}
