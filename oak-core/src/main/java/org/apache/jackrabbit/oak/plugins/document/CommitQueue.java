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

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>CommitQueue</code> ensures a sequence of commits consistent with the
 * commit revision even if commits did not complete in this sequence.
 */
class CommitQueue {

    static final Logger LOG = LoggerFactory.getLogger(CommitQueue.class);

    private final DocumentNodeStore store;

    private final SortedMap<Revision, Entry> commits = new TreeMap<Revision, Entry>(StableRevisionComparator.INSTANCE);

    private final ChangeDispatcher dispatcher;

    CommitQueue(DocumentNodeStore store, ChangeDispatcher dispatcher) {
        this.store = store;
        this.dispatcher = dispatcher;
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
                rev = store.newRevision();
                revs.add(rev);
            }
            commits.put(rev, new Entry(rev));
        }
        LOG.debug("created commit {}", rev);
        return revs;
    }

    void done(@Nonnull Revision rev, boolean isBranch, @Nullable CommitInfo info) {
        checkNotNull(rev);
        if (isBranch) {
            removeCommit(rev);
        } else {
            afterTrunkCommit(rev, info);
        }
    }

    void canceled(@Nonnull Revision rev) {
        removeCommit(rev);
    }

    //------------------------< internal >--------------------------------------

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

    private void afterTrunkCommit(Revision rev, CommitInfo info) {
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
                // remember before revision
                Revision before = store.getHeadRevision();
                // update head revision
                store.setHeadRevision(rev);
                NodeState root = store.getRoot();
                // TODO: correct?
                dispatcher.contentChanged(store.getRoot(before), null);
                try {
                    dispatcher.contentChanged(root, info);
                } finally {
                    dispatcher.contentChanged(root, null);
                }
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
