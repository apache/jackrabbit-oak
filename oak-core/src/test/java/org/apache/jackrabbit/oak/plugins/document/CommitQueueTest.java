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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Sets.union;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests for {@link CommitQueue}.
 */
public class CommitQueueTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private static final Logger LOG = LoggerFactory.getLogger(CommitQueueTest.class);

    private static final int NUM_WRITERS = 10;

    private static final int COMMITS_PER_WRITER = 100;

    private List<Exception> exceptions = synchronizedList(new ArrayList<Exception>());

    @Test
    public void concurrentCommits() throws Exception {
        final DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        AtomicBoolean running = new AtomicBoolean(true);

        Closeable observer = store.addObserver(new Observer() {
            private RevisionVector before = new RevisionVector(
                    new Revision(0, 0, store.getClusterId()));

            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                DocumentNodeState after = (DocumentNodeState) root;
                RevisionVector r = after.getRootRevision();
                LOG.debug("seen: {}", r);
                if (r.compareTo(before) < 0) {
                    exceptions.add(new Exception(
                            "Inconsistent revision sequence. Before: " +
                                    before + ", after: " + r));
                }
                before = r;
            }
        });

        // perform commits with multiple threads
        List<Thread> writers = new ArrayList<Thread>();
        for (int i = 0; i < NUM_WRITERS; i++) {
            final Random random = new Random(i);
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < COMMITS_PER_WRITER; i++) {
                            Commit commit = store.newCommit(null, null);
                            try {
                                Thread.sleep(0, random.nextInt(1000));
                            } catch (InterruptedException e) {
                                // ignore
                            }
                            if (random.nextInt(5) == 0) {
                                // cancel 20% of the commits
                                store.canceled(commit);
                            } else {
                                boolean isBranch = random.nextInt(5) == 0;
                                store.done(commit, isBranch, CommitInfo.EMPTY);
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }));
        }
        for (Thread t : writers) {
            t.start();
        }
        for (Thread t : writers) {
            t.join();
        }
        running.set(false);
        observer.close();
        store.dispose();
        assertNoExceptions();
    }

    @Test
    public void concurrentCommits2() throws Exception {
        final CommitQueue queue = new CommitQueue(DummyRevisionContext.INSTANCE);

        final CommitQueue.Callback c = new CommitQueue.Callback() {
            private Revision before = Revision.newRevision(1);

            @Override
            public void headOfQueue(@Nonnull Revision r) {
                LOG.debug("seen: {}", r);
                if (r.compareRevisionTime(before) < 0) {
                    exceptions.add(new Exception(
                            "Inconsistent revision sequence. Before: " +
                                    before + ", after: " + r));
                }
                before = r;
            }
        };

        // perform commits with multiple threads
        List<Thread> writers = new ArrayList<Thread>();
        for (int i = 0; i < NUM_WRITERS; i++) {
            final Random random = new Random(i);
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < COMMITS_PER_WRITER; i++) {
                            Revision r = queue.createRevision();
                            try {
                                Thread.sleep(0, random.nextInt(1000));
                            } catch (InterruptedException e) {
                                // ignore
                            }
                            if (random.nextInt(5) == 0) {
                                // cancel 20% of the commits
                                queue.canceled(r);
                            } else {
                                queue.done(r, c);
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }));
        }
        for (Thread t : writers) {
            t.start();
        }
        for (Thread t : writers) {
            t.join();
        }
        assertNoExceptions();
    }

    // OAK-2868
    @Test
    public void branchCommitMustNotBlockTrunkCommit() throws Exception {
        final DocumentNodeStore ds = builderProvider.newBuilder().getNodeStore();

        // simulate start of a branch commit
        Commit c = ds.newCommit(ds.getHeadRevision().asBranchRevision(ds.getClusterId()), null);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    NodeBuilder builder = ds.getRoot().builder();
                    builder.child("foo");
                    ds.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                } catch (CommitFailedException e) {
                    exceptions.add(e);
                }
            }
        });
        t.start();

        t.join(3000);
        assertFalse("Commit did not succeed within 3 seconds", t.isAlive());

        ds.canceled(c);
        assertNoExceptions();
    }

    @Test
    public void suspendUntil() throws Exception {
        final AtomicReference<RevisionVector> headRevision = new AtomicReference<RevisionVector>();
        RevisionContext context = new DummyRevisionContext() {
            @Nonnull
            @Override
            public RevisionVector getHeadRevision() {
                return headRevision.get();
            }
        };
        headRevision.set(new RevisionVector(context.newRevision()));
        final CommitQueue queue = new CommitQueue(context);

        final Revision newHeadRev = context.newRevision();
        final Set<Revision> revisions = queue.createRevisions(10);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                queue.suspendUntilAll(union(of(newHeadRev), revisions));
            }
        });
        t.start();

        // wait until t is suspended
        for (int i = 0; i < 100; i++) {
            if (queue.numSuspendedThreads() > 0) {
                break;
            }
            Thread.sleep(10);
        }
        assertEquals(1, queue.numSuspendedThreads());

        queue.headRevisionChanged();
        // must still be suspended
        assertEquals(1, queue.numSuspendedThreads());

        headRevision.set(new RevisionVector(newHeadRev));
        queue.headRevisionChanged();
        // must still be suspended
        assertEquals(1, queue.numSuspendedThreads());

        for (Revision rev : revisions) {
            queue.canceled(rev);
        }
        // must not be suspended anymore
        assertEquals(0, queue.numSuspendedThreads());
    }

    @Test
    public void suspendUntilTimeout() throws Exception {
        final AtomicReference<RevisionVector> headRevision = new AtomicReference<RevisionVector>();
        RevisionContext context = new DummyRevisionContext() {
            @Nonnull
            @Override
            public RevisionVector getHeadRevision() {
                return headRevision.get();
            }
        };
        headRevision.set(new RevisionVector(context.newRevision()));
        final CommitQueue queue = new CommitQueue(context);
        queue.setSuspendTimeoutMillis(0);

        final Revision r = context.newRevision();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                queue.suspendUntilAll(of(r));
            }
        });
        t.start();

        t.join(1000);
        assertFalse(t.isAlive());
    }

    @Test
    public void concurrentSuspendUntil() throws Exception {
        final AtomicReference<RevisionVector> headRevision = new AtomicReference<RevisionVector>();
        RevisionContext context = new DummyRevisionContext() {
            @Nonnull
            @Override
            public RevisionVector getHeadRevision() {
                return headRevision.get();
            }
        };
        headRevision.set(new RevisionVector(context.newRevision()));

        List<Thread> threads = new ArrayList<Thread>();
        List<Revision> allRevisions = new ArrayList<Revision>();

        final CommitQueue queue = new CommitQueue(context);
        for (int i = 0; i < 10; i++) { // threads count
            final Set<Revision> revisions = new HashSet<Revision>();
            for (int j = 0; j < 10; j++) { // revisions per thread
                Revision r = queue.createRevision();
                revisions.add(r);
                allRevisions.add(r);
            }
            Thread t = new Thread(new Runnable() {
                public void run() {
                    queue.suspendUntilAll(revisions);
                }
            });
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < 100; i++) {
            if (queue.numSuspendedThreads() == 10) {
                break;
            }
            Thread.sleep(10);
        }
        assertEquals(10, queue.numSuspendedThreads());

        Collections.shuffle(allRevisions);
        for (Revision r : allRevisions) {
            queue.canceled(r);
            Thread.sleep(10);
        }

        for (int i = 0; i < 100; i++) {
            if (queue.numSuspendedThreads() == 0) {
                break;
            }
            Thread.sleep(10);
        }
        assertEquals(0, queue.numSuspendedThreads());

        for (Thread t : threads) {
            t.join(1000);
            assertFalse(t.isAlive());
        }
    }

    // OAK-4540
    @Test
    public void headOfQueueMustNotBlockNewRevision() throws Exception {
        RevisionContext context = new DummyRevisionContext();
        final CommitQueue queue = new CommitQueue(context);
        final Revision r1 = queue.createRevision();
        final Semaphore s1 = new Semaphore(0);
        final CommitQueue.Callback c = new CommitQueue.Callback() {
            @Override
            public void headOfQueue(@Nonnull Revision revision) {
                s1.acquireUninterruptibly();
            }
        };
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                queue.done(r1, c);
            }
        });
        t1.start();
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                queue.createRevision();
            }
        });
        t2.start();
        t2.join(3000);
        try {
            if (t2.isAlive()) {
                fail("CommitQueue.Callback.headOfQueue() must not " +
                        "block CommitQueue.createRevision()");
            }
        } finally {
            s1.release();
            t1.join();
            t2.join();
        }
    }

    private void assertNoExceptions() throws Exception {
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
    }
}
