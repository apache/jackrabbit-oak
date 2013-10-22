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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.junit.Test;

/**
 * Tests for {@link CommitQueue}.
 */
public class CommitQueueTest {

    private static final int NUM_WRITERS = 10;

    private static final int COMMITS_PER_WRITER = 100;

    @Test
    public void concurrentCommits() throws Exception {
        final MongoNodeStore store = new MongoMK.Builder().getNodeStore();
        ChangeDispatcher dispatcher = new ChangeDispatcher(store);
        final ChangeDispatcher.Listener listener = dispatcher.newListener();
        final AtomicBoolean running = new AtomicBoolean(true);
        final CommitQueue queue = new CommitQueue(store, dispatcher);
        final List<Exception> exceptions = Collections.synchronizedList(
                new ArrayList<Exception>());
        Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Revision before = new Revision(0, 0, store.getClusterId());
                    while (running.get()) {
                        ChangeDispatcher.ChangeSet changes = listener.getChanges();
                        if (changes != null) {
                            MongoNodeState after = (MongoNodeState) changes.getAfterState();
                            Revision r = after.getRevision();
                            // System.out.println("seen: " + r);
                            if (r.compareRevisionTime(before) < 1) {
                                exceptions.add(new Exception(
                                        "Inconsistent revision sequence. Before: " +
                                                before + ", after: " + r));
                                break;
                            }
                            before = r;
                        } else {
                            // avoid busy wait
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        });
        reader.start();

        // perform commits with multiple threads
        List<Thread> writers = new ArrayList<Thread>();
        for (int i = 0; i < NUM_WRITERS; i++) {
            final Random random = new Random(i);
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < COMMITS_PER_WRITER; i++) {
                            Revision base = store.getHeadRevision();
                            Commit c = queue.createCommit(base);
                            try {
                                Thread.sleep(0, random.nextInt(1000));
                            } catch (InterruptedException e) {
                                // ignore
                            }
                            if (random.nextInt(5) == 0) {
                                // cancel 20% of the commits
                                queue.canceled(c);
                            } else {
                                boolean isBranch = random.nextInt(5) == 0;
                                queue.done(c, isBranch, CommitInfo.create(
                                        null, null, System.currentTimeMillis()));
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
        reader.join();
        for (Exception e : exceptions) {
            throw e;
        }
    }

}
