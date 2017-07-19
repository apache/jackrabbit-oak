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
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.observation.Filter;
import org.apache.jackrabbit.oak.plugins.observation.FilteringAwareObserver;
import org.apache.jackrabbit.oak.plugins.observation.FilteringObserver;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.Lists;

import junit.framework.AssertionFailedError;

public class BackgroundObserverTest {
    private static final CommitInfo COMMIT_INFO = new CommitInfo("no-session", null);
    public static final int CHANGE_COUNT = 1024;

    private final List<Runnable> assertions = Lists.newArrayList();
    private CountDownLatch doneCounter;
    private final List<Closeable> closeables = Lists.newArrayList();

    /**
     * Assert that each observer of many running concurrently sees the same
     * linearly sequence of commits (i.e. sees the commits in the correct
     * order).
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
            final List<Runnable> assertions = Collections.synchronizedList(Lists.<Runnable> newArrayList());
            volatile NodeState previous;

            @Override
            public void contentChanged(@Nonnull final NodeState root, @Nonnull CommitInfo info) {
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
    
    class MyFilter implements Filter {

        private boolean excludeNext;

        void excludeNext(boolean excludeNext) {
            this.excludeNext = excludeNext;
        }

        @Override
        public boolean excludes(NodeState root, CommitInfo info) {
            final boolean excludes = excludeNext;
            excludeNext = false;
            return excludes;
        }
        
    }

    class Recorder implements FilteringAwareObserver {

        List<Pair> includedChanges = Collections.synchronizedList(new LinkedList<Pair>());
        private boolean pause;
        private boolean pausing;

        public Recorder() {
        }
        
        @Override
        public void contentChanged(NodeState before, NodeState after, CommitInfo info) {
            includedChanges.add(new Pair(before, after));
            maybePause();
        }

        public void maybePause() {
            synchronized (this) {
                try {
                    while (pause) {
                        pausing = true;
                        this.notifyAll();
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            // should not happen
                        }
                    }
                } finally {
                    pausing = false;
                    this.notifyAll();
                }
            }
        }

        public synchronized void pause() {
            this.pause = true;
        }

        public synchronized void unpause() {
            this.pause = false;
            this.notifyAll();
        }

        public boolean waitForPausing(int timeout, TimeUnit unit) throws InterruptedException {
            final long done = System.currentTimeMillis() + unit.toMillis(timeout);
            synchronized (this) {
                while (!pausing && done > System.currentTimeMillis()) {
                    this.wait(100);
                }
                return pausing;
            }
        }

        public boolean waitForUnpausing(int timeout, TimeUnit unit) throws InterruptedException {
            final long done = System.currentTimeMillis() + unit.toMillis(timeout);
            synchronized (this) {
                while (pausing && done > System.currentTimeMillis()) {
                    this.wait(100);
                }
                return !pausing;
            }
        }

    }

    class Pair {
        private final NodeState before;
        private final NodeState after;

        Pair(NodeState before, NodeState after) {
            this.before = before;
            this.after = after;
        }

        @Override
        public String toString() {
            return "Pair(before=" + before + ", after=" + after + ")";
        }
    }

    class NodeStateGenerator {
        Random r = new Random(1232131); // seed: repeatable tests
        NodeBuilder builder = EMPTY_NODE.builder();

        NodeState next() {
            builder.setProperty("p", r.nextInt());
            NodeState result = builder.getNodeState();
            builder = result.builder();
            return result;
        }
    }

    private void assertMatches(String msg, List<Pair> expected, List<Pair> actual) {
        assertEquals("size mismatch. msg=" + msg, expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertSame("mismatch of before at pos=" + i + ", msg=" + msg, expected.get(i).before, actual.get(i).before);
            assertSame("mismatch of after at pos=" + i + ", msg=" + msg, expected.get(i).after, actual.get(i).after);
        }
    }

    @After
    public void shutDown() throws Exception {
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                throw new AssertionFailedError(e.getMessage());
            }
        }
    }

    @Test
    public void testExcludedAllCommits() throws Exception {
        MyFilter filter = new MyFilter();
        Recorder recorder = new Recorder();
        ExecutorService executor = newSingleThreadExecutor();
        FilteringObserver fo = new FilteringObserver(executor, 5, filter, recorder);
        closeables.add(fo);
        List<Pair> expected = new LinkedList<Pair>();
        NodeStateGenerator generator = new NodeStateGenerator();
        NodeState first = generator.next();
        fo.contentChanged(first, CommitInfo.EMPTY);
        for (int i = 0; i < 100000; i++) {
            filter.excludeNext(true);
            fo.contentChanged(generator.next(), CommitInfo.EMPTY);
        }
        assertTrue("testExcludedAllCommits", fo.getBackgroundObserver().waitUntilStopped(5, TimeUnit.SECONDS));
        assertMatches("testExcludedAllCommits", expected, recorder.includedChanges);
    }

    @Test
    public void testNoExcludedCommits() throws Exception {
        MyFilter filter = new MyFilter();
        Recorder recorder = new Recorder();
        ExecutorService executor = newSingleThreadExecutor();
        FilteringObserver fo = new FilteringObserver(executor, 10002, filter, recorder);
        closeables.add(fo);
        List<Pair> expected = new LinkedList<Pair>();
        NodeStateGenerator generator = new NodeStateGenerator();
        NodeState first = generator.next();
        fo.contentChanged(first, CommitInfo.EMPTY);
        NodeState previous = first;
        for (int i = 0; i < 10000; i++) {
            filter.excludeNext(false);
            NodeState next = generator.next();
            expected.add(new Pair(previous, next));
            previous = next;
            fo.contentChanged(next, CommitInfo.EMPTY);
        }
        assertTrue("testNoExcludedCommits", fo.getBackgroundObserver().waitUntilStopped(5, TimeUnit.SECONDS));
        assertMatches("testNoExcludedCommits", expected, recorder.includedChanges);
    }

    @Test
    public void testExcludeCommitsWithFullQueue() throws Exception {
        MyFilter filter = new MyFilter();
        Recorder recorder = new Recorder();
        ExecutorService executor = newSingleThreadExecutor();
        FilteringObserver fo = new FilteringObserver(executor, 2, filter, recorder);
        closeables.add(fo);
        List<Pair> expected = new LinkedList<Pair>();
        NodeStateGenerator generator = new NodeStateGenerator();
        recorder.pause();

        // the first one will directly go to the recorder
        NodeState initialHeldBack = generator.next();
        fo.contentChanged(initialHeldBack, CommitInfo.EMPTY);
        NodeState firstIncluded = generator.next();
        expected.add(new Pair(initialHeldBack, firstIncluded));
        fo.contentChanged(firstIncluded, CommitInfo.EMPTY);

        assertTrue("observer did not get called (yet?)", recorder.waitForPausing(5, TimeUnit.SECONDS));

        // this one will be queued as #1
        NodeState secondIncluded = generator.next();
        expected.add(new Pair(firstIncluded, secondIncluded));
        fo.contentChanged(secondIncluded, CommitInfo.EMPTY);

        // this one will be queued as #2
        NodeState thirdIncluded = generator.next();
//        expected.add(new Pair(secondIncluded, thirdIncluded));
        fo.contentChanged(thirdIncluded, CommitInfo.EMPTY);

        // this one will cause the queue to 'overflow' (full==true)
        NodeState forthQueueFull = generator.next();
        // not adding to expected, as this one ends up in the overflow element
        fo.contentChanged(forthQueueFull, CommitInfo.EMPTY);

        NodeState next;
        // exclude when queue is full
        filter.excludeNext(true);
        next = generator.next();
        // if excluded==true and full, hence not adding to expected
        fo.contentChanged(next, CommitInfo.EMPTY);
        // include after an exclude when queue was full
        // => this is not supported. when the queue
        filter.excludeNext(false);
        next = generator.next();
        // excluded==false BUT queue full, hence not adding to expected
        fo.contentChanged(next, CommitInfo.EMPTY);

        // with OAK-5740 the overflow entry now looks as follows:
        expected.add(new Pair(secondIncluded, next));

        // let recorder continue
        recorder.unpause();

        recorder.waitForUnpausing(5, TimeUnit.SECONDS);
        Thread.sleep(1000); // wait for 1 element to be dequeued at least
        // exclude when queue is no longer full
        filter.excludeNext(true);
        NodeState seventhAfterQueueFull = generator.next();
        // with the introduction of the FilteringAwareObserver this
        // 'seventhAfterQueueFull' root will not be forwarded
        // to the BackgroundObserver - thus entirely filtered

        fo.contentChanged(seventhAfterQueueFull, CommitInfo.EMPTY);

        // but with the introduction of FilteringAwareObserver the delivery
        // only happens with non-filtered items, so adding yet another one now
        filter.excludeNext(false);
        NodeState last = generator.next();
        // the 'seventhAfterQueueFull' DOES get filtered - and as per behavior
        // pre-OAK-5740 it used to get flushed with the next contentChanged,
        // however, with OAK-5740 this is no longer the case as we now
        // use the last queue entry as the overflow entry
        expected.add(new Pair(seventhAfterQueueFull, last));
        fo.contentChanged(last, CommitInfo.EMPTY);
        
        assertTrue("testExcludeCommitsWithFullQueue", fo.getBackgroundObserver().waitUntilStopped(10, TimeUnit.SECONDS));
        assertMatches("testExcludeCommitsWithFullQueue", expected, recorder.includedChanges);
    }

    @Test
    public void testExcludeSomeCommits() throws Exception {
        ExecutorService executor = newSingleThreadExecutor();
        for (int i = 0; i < 100; i++) {
            doTestExcludeSomeCommits(i, executor);
        }
        for (int i = 100; i < 10000; i += 50) {
            doTestExcludeSomeCommits(i, executor);
        }
        executor.shutdownNow();
    }

    private void doTestExcludeSomeCommits(int cnt, Executor executor) throws Exception {
        MyFilter filter = new MyFilter();
        Recorder recorder = new Recorder();
        FilteringObserver fo = new FilteringObserver(executor, cnt + 2, filter, recorder);
        closeables.add(fo);
        List<Pair> expected = new LinkedList<Pair>();
        Random r = new Random(2343242); // seed: repeatable tests
        NodeStateGenerator generator = new NodeStateGenerator();
        NodeState first = generator.next();
        fo.contentChanged(first, CommitInfo.EMPTY);
        NodeState previous = first;
        for (int i = 0; i < cnt; i++) {
            boolean excludeNext = r.nextInt(100) < 90;
            filter.excludeNext(excludeNext);
            NodeState next = generator.next();
            if (!excludeNext) {
                expected.add(new Pair(previous, next));
            }
            previous = next;
            fo.contentChanged(next, CommitInfo.EMPTY);
        }
        assertTrue("cnt=" + cnt, fo.getBackgroundObserver().waitUntilStopped(5, TimeUnit.SECONDS));
        assertMatches("cnt=" + cnt, expected, recorder.includedChanges);
    }

}
