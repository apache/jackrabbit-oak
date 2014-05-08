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

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class BackgroundObserverTest {
    private static final CommitInfo COMMIT_INFO = new CommitInfo("no-session", null);
    public static final int CHANGE_COUNT = 1024;

    private final List<Runnable> assertions = Lists.newArrayList();
    private CountDownLatch doneCounter;

    /**
     * Assert that each observer of many running concurrently sees the same
     * linearly sequence of commits (i.e. sees the commits in the correct order).
     */
    @Test
    public void concurrentObservers() throws InterruptedException {
        Observer observer = createCompositeObserver(newFixedThreadPool(16), 128);

        for (int k = 0; k < CHANGE_COUNT; k++) {
            contentChanged(observer, k);
        }
        done(observer);

        assertTrue(doneCounter.await(5, TimeUnit.SECONDS));

        for (Runnable assertion : assertions) {
            assertion.run();
        }
    }

    private static void contentChanged(Observer observer, long value) {
        NodeState node = EMPTY_NODE.builder().setProperty("p", value).getNodeState();
        observer.contentChanged(node, COMMIT_INFO);
    }

    private static void done(Observer observer) {
        NodeState node = EMPTY_NODE.builder().setProperty("done", true).getNodeState();
        observer.contentChanged(node, COMMIT_INFO);
    }

    private CompositeObserver createCompositeObserver(ExecutorService executor, int count) {
        CompositeObserver observer = new CompositeObserver();

        for (int k = 0; k < count; k++) {
            observer.addObserver(createBackgroundObserver(executor));
        }
        doneCounter = new CountDownLatch(count);
        return observer;
    }

    private synchronized void done(List<Runnable> assertions) {
        this.assertions.addAll(assertions);
        doneCounter.countDown();
    }

    private Observer createBackgroundObserver(ExecutorService executor) {
        // Ensure the observation revision queue is sufficiently large to hold
        // all revisions. Otherwise waiting for events might block since pending
        // events would only be released on a subsequent commit. See OAK-1491
        int queueLength = CHANGE_COUNT + 1;

        return new BackgroundObserver(new Observer() {
            // Need synchronised list here to maintain correct memory barrier
            // when this is passed on to done(List<Runnable>)
            final List<Runnable> assertions = Collections.synchronizedList(Lists.<Runnable>newArrayList());
            volatile NodeState previous;

            @Override
            public void contentChanged(@Nonnull final NodeState root, @Nullable CommitInfo info) {
                if (root.hasProperty("done")) {
                    done(assertions);
                } else if (previous != null) {
                    // Copy previous to avoid closing over it
                    final NodeState p = previous;
                    assertions.add(new Runnable() {
                        @Override
                        public void run() {
                            assertEquals(getP(p) + 1, (long) getP(root));
                        }
                    });

                }
                previous = root;
            }

            private Long getP(NodeState previous) {
                return previous.getProperty("p").getValue(Type.LONG);
            }
        }, executor, queueLength);
    }

}
