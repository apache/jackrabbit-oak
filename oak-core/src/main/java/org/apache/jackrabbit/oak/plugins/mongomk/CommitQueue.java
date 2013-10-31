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
package org.apache.jackrabbit.oak.plugins.mongomk;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher;
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

    private final MongoNodeStore store;

    private final SortedMap<Revision, Entry> commits = new TreeMap<Revision, Entry>(new StableRevisionComparator());

    private final ChangeDispatcher dispatcher;

    CommitQueue(MongoNodeStore store, ChangeDispatcher dispatcher) {
        this.store = store;
        this.dispatcher = dispatcher;
    }

    Commit createCommit(Revision base) {
        synchronized (this) {
            Commit c = new Commit(store, base, store.newRevision());
            commits.put(c.getRevision(), new Entry(c));
            LOG.debug("created commit {}", c.getRevision());
            return c;
        }
    }

    void done(@Nonnull Commit c, boolean isBranch, @Nullable CommitInfo info) {
        checkNotNull(c);
        if (isBranch) {
            removeCommit(c);
        } else {
            afterTrunkCommit(c, info);
        }
    }

    void canceled(@Nonnull Commit c) {
        removeCommit(c);
    }

    //------------------------< internal >--------------------------------------

    private void removeCommit(@Nonnull Commit c) {
        // simply remove and notify next head if any
        synchronized (this) {
            boolean wasHead = commits.firstKey().equals(c.getRevision());
            commits.remove(c.getRevision());
            LOG.debug("removed commit {}, wasHead={}", c.getRevision(), wasHead);
            if (wasHead) {
                notifyHead();
            }
        }
    }

    private void afterTrunkCommit(Commit c, CommitInfo info) {
        assert !commits.isEmpty();

        boolean isHead;
        Entry commitEntry;
        synchronized (this) {
            isHead = commits.firstKey().equals(c.getRevision());
            commitEntry = commits.get(c.getRevision());
        }
        if (!isHead) {
            LOG.debug("not head: {}, waiting...", c.getRevision());
            commitEntry.await();
        }
        synchronized (this) {
            commits.remove(c.getRevision());
            try {
                LOG.debug("removed {}, head is now {}", c.getRevision(), commits.isEmpty() ? null : commits.firstKey());
                // remember before revision
                Revision before = store.getHeadRevision();
                // update head revision
                store.setHeadRevision(c.getRevision());
                NodeState root = store.getRoot();
                // TODO: correct?
                dispatcher.beforeCommit(store.getRoot(before));
                try {
                    dispatcher.localCommit(root, info);
                } finally {
                    dispatcher.afterCommit(root);
                }
            } finally {
                // notify next if there is any
                notifyHead();
            }
        }
    }

    private void notifyHead() {
        if (!commits.isEmpty()) {
            LOG.debug("count down for {}", commits.firstKey());
            commits.get(commits.firstKey()).latch.countDown();
        }
    }

    private static final class Entry {

        final Commit commit;
        final CountDownLatch latch;

        Entry(Commit commit) {
            this.commit = commit;
            this.latch = new CountDownLatch(1);
        }

        void await() {
            for (;;) {
                try {
                    LOG.debug("awaiting {}", commit.getRevision());
                    latch.await();
                    break;
                } catch (InterruptedException e) {
                    // retry
                }
            }
        }
    }
}
