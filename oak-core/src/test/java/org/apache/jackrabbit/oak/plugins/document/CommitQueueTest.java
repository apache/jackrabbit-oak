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
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

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

import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertFalse;

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
            private Revision before = new Revision(0, 0, store.getClusterId());

            @Override
            public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
                DocumentNodeState after = (DocumentNodeState) root;
                Revision r = after.getRevision();
                LOG.debug("seen: {}", r);
                if (r.compareRevisionTime(before) < 0) {
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
                                store.done(commit, isBranch, null);
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
        final CommitQueue queue = new CommitQueue() {
            @Override
            protected Revision newRevision() {
                return Revision.newRevision(1);
            }
        };

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
        Commit c = ds.newCommit(ds.getHeadRevision().asBranchRevision(), null);

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

    private void assertNoExceptions() throws Exception {
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
    }
}
