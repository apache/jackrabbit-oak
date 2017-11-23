/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.commit;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.observation.Filter;
import org.apache.jackrabbit.oak.plugins.observation.FilteringAwareObserver;
import org.apache.jackrabbit.oak.plugins.observation.FilteringObserver;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class PrefilteringBackgroundObserverTest {
    
    private final boolean EXCLUDED = true;
    private final boolean INCLUDED = false;
    
    private List<Runnable> runnableQ;
    private ExecutorService executor;
    private CompositeObserver compositeObserver;
    private List<ContentChanged> received;
    private FilteringObserver filteringObserver;
    private CommitInfo includingCommitInfo = new CommitInfo("includingSession", CommitInfo.OAK_UNKNOWN);
    private CommitInfo excludingCommitInfo = new CommitInfo("excludingSession", CommitInfo.OAK_UNKNOWN);
    private int resetCallCnt;
    
    public void init(int queueLength) throws Exception {
        runnableQ = new LinkedList<Runnable>();
        executor = new EnqueuingExecutorService(runnableQ);
        compositeObserver = new CompositeObserver();
        received = new LinkedList<ContentChanged>();
        filteringObserver = new FilteringObserver(executor, queueLength, new Filter() {
            
            @Override
            public boolean excludes(NodeState root, CommitInfo info) {
                if (info == includingCommitInfo) {
                    return false;
                } else if (info == excludingCommitInfo) {
                    return true;
                } else if (info.isExternal()) {
                    return false;
                }
                throw new IllegalStateException("only supporting include or exclude");
            }
        }, new FilteringAwareObserver() {
            
            NodeState previous;
            
            @Override
            public void contentChanged(NodeState before, NodeState after, CommitInfo info) {
                received.add(new ContentChanged(after, info));
                if (previous !=null && previous != before) {
                    resetCallCnt++;
                }
                previous = after;
            }
        });
        compositeObserver.addObserver(filteringObserver);
    }

    private final class EnqueuingExecutorService extends AbstractExecutorService {
        private final List<Runnable> runnableQ;

        private EnqueuingExecutorService(List<Runnable> runnableQ) {
            this.runnableQ = runnableQ;
        }

        @Override
        public void execute(Runnable command) {
            runnableQ.add(command);
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new IllegalStateException("nyi");
        }

        @Override
        public void shutdown() {
            throw new IllegalStateException("nyi");
        }

        @Override
        public boolean isTerminated() {
            throw new IllegalStateException("nyi");
        }

        @Override
        public boolean isShutdown() {
            throw new IllegalStateException("nyi");
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new IllegalStateException("nyi");
        }
    }

    class ContentChanged {
        NodeState root;
        CommitInfo info;
        ContentChanged(NodeState root, CommitInfo info) {
            this.root = root;
            this.info = info;
        }
    }
    
    private static void executeRunnables(final List<Runnable> runnableQ, int num) {
        for(int i=0; i<num; i++) {
            for (Runnable runnable : new ArrayList<Runnable>(runnableQ)) {
                runnable.run();
            }
        }
    }

    private static NodeState p(int k) {
        return EMPTY_NODE.builder().setProperty("p", k).getNodeState();
    }

    @Test
    public void testFlipping() throws Exception {
        final int queueLength = 2000;
        init(queueLength);

        // initialize observer with an initial contentChanged
        // (see ChangeDispatcher#addObserver)
        {
            compositeObserver.contentChanged(p(-1), CommitInfo.EMPTY_EXTERNAL);
        }
        // Part 1 : first run with filtersEvaluatedMapWithEmptyObservers - empty or null shouldn't matter, it's excluded in both cases
        for (int k = 0; k < 1000; k++) {
            CommitInfo info;
            if (k%2==1) {
                info = includingCommitInfo;
            } else {
                info = excludingCommitInfo;
            }
            final NodeState p = p(k);
            compositeObserver.contentChanged(p, info);
            if (k%10 == 0) {
                executeRunnables(runnableQ, 10);
            }
        }
        executeRunnables(runnableQ, 10);
        
        assertEquals(500, received.size()); // changed from 501 with OAK-5121
        assertEquals(499, resetCallCnt); // changed from 500 with OAK-5121
        
        // Part 2 : run with filtersEvaluatedMapWithNullObservers - empty or null shouldn't matter, it's excluded in both cases
        received.clear();
        resetCallCnt = 0;
        for (int k = 0; k < 1000; k++) {
            CommitInfo info;
            if (k%2==1) {
                info = includingCommitInfo;
            } else {
                info = excludingCommitInfo;
            }
            final NodeState p = p(k);
            compositeObserver.contentChanged(p, info);
            if (k%10 == 0) {
                executeRunnables(runnableQ, 10);
            }
        }
        executeRunnables(runnableQ, 10);
        
        assertEquals(500, received.size());
        assertEquals(500, resetCallCnt);
        
        // Part 3 : unlike the method name suggests, this variant tests with the filter disabled, so should receive all events normally
        received.clear();
        resetCallCnt = 0;
        for (int k = 0; k < 1000; k++) {
            CommitInfo info;
            if (k%2==1) {
                info = includingCommitInfo;
            } else {
                info = includingCommitInfo;
            }
            final NodeState p = p(k);
            compositeObserver.contentChanged(p, info);
            if (k%10 == 0) {
                executeRunnables(runnableQ, 10);
            }
        }
        executeRunnables(runnableQ, 10);
        
        assertEquals(1000, received.size());
        assertEquals(0, resetCallCnt);
    }

    @Test
    public void testFlipping2() throws Exception {
        doTestFullQueue(6, 
                new TestPattern(INCLUDED, 1, true, 1, 0),
                new TestPattern(EXCLUDED, 5, true, 0, 0),
                new TestPattern(INCLUDED, 2, true, 2, 1),
                new TestPattern(EXCLUDED, 1, true, 0, 0),
                new TestPattern(INCLUDED, 2, true, 2, 1));
    }
    
    @Test
    public void testQueueNotFull() throws Exception {
        doTestFullQueue(20, 
                // start: empty queue
                new TestPattern(EXCLUDED, 1000, false, 0, 0),
                // here: still empty, just the previousRoot is set to remember above NOOPs
                new TestPattern(INCLUDED, 5, false, 0, 0),
                // here: 5 changes are in the queue, the queue fits 20, way to go
                new TestPattern(EXCLUDED, 500, false, 0, 0),
                // still 5 in the queue
                new TestPattern(INCLUDED, 5, false, 0, 0),
                // now we added 2, queue still not full
                new TestPattern(EXCLUDED, 0 /* only flush*/, true, 10, 1)
                );
    }
    
    @Test
    public void testIncludeOnQueueFull() throws Exception {
        doTestFullQueue(7, 
                // start: empty queue
                new TestPattern(EXCLUDED, 1000, false, 0, 0, 0, 0),
                // here: still empty, just the previousRoot is set to remember above NOOPs
                new TestPattern(INCLUDED, 5, false, 0, 0, 0, 6),
                // here: 1 init and 5 changes are in the queue, the queue fits 7, so queue is almost full
                new TestPattern(EXCLUDED, 500, false, 0, 0, 6, 6),
                // still 6 in the queue, of 7 
                // due to OAK-5740 the last entry is now an include
                new TestPattern(INCLUDED, 5, false, 0, 0, 6, 7),
                // so with OAK-5740 we now will get 6 includes, not 5
                new TestPattern(EXCLUDED, 0 /* only flush*/, true, 6, 0, 7, 0)
                );
    }
    
    @Test
    public void testExcludeOnQueueFull2() throws Exception {
        doTestFullQueue(1,
                // start: empty queue
                new TestPattern(INCLUDED, 10, false, 0, 0),
                new TestPattern(EXCLUDED, 0 /* only flush*/, true, 1, 0),
                new TestPattern(INCLUDED, 10, false, 0, 0),
                new TestPattern(EXCLUDED, 10, false, 0, 0),
                new TestPattern(INCLUDED, 10, false, 0, 0),
                new TestPattern(EXCLUDED, 0 /* only flush*/, true, 1, 0),
                new TestPattern(INCLUDED, 10, false, 0, 0),
                new TestPattern(EXCLUDED, 10, false, 0, 0),
                new TestPattern(EXCLUDED, 0 /* only flush*/, true, 1, 0),
                new TestPattern(EXCLUDED, 10, false, 0, 0),
                new TestPattern(INCLUDED, 10, false, 0, 0),
                new TestPattern(EXCLUDED, 0 /* only flush*/, true, 1, 0));
    }

    @Test
    public void testExcludeOnQueueFull1() throws Exception {
        doTestFullQueue(4, 
                // start: empty queue
                new TestPattern(EXCLUDED, 1, false, 0, 0, 0, 0),
                // here: still empty, just the previousRoot is set to remember above NOOP
                new TestPattern(INCLUDED, 3, false, 0, 0, 0, 4),
                // here: 3 changes are in the queue, the queue fits 3, so it just got full now
                new TestPattern(EXCLUDED, 1, false, 0, 0, 4, 4),
                // still full but it's ignored, so doesn't have any queue length effect
                new TestPattern(INCLUDED, 3, false, 0, 0, 4, 4),
                // adding 3 will not work, it will result in an overflow entry
                new TestPattern(EXCLUDED, 0 /* only flush*/, true, 3, 0, 4, 0),
                new TestPattern(INCLUDED, 1, false, 0, 0, 0, 1),
                new TestPattern(EXCLUDED, 0 /* only flush*/, true, 1, 0, 1, 0)
                );
    }

    class TestPattern {
        final boolean flush;
        final boolean excluded;
        final int numEvents;
        final int expectedNumEvents;
        final int expectedNumResetCalls;
        private int expectedQueueSizeAtStart = -1;
        private int expectedQueueSizeAtEnd = -1;
        TestPattern(boolean excluded, int numEvents, boolean flush, int expectedNumEvents, int expectedNumResetCalls) {
            this.flush = flush;
            this.excluded = excluded;
            this.numEvents = numEvents;
            this.expectedNumEvents = expectedNumEvents;
            this.expectedNumResetCalls = expectedNumResetCalls;
        }
        TestPattern(boolean excluded, int numEvents, boolean flush, int expectedNumEvents, int expectedNumResetCalls, int expectedQueueSizeAtStart, int expectedQueueSizeAtEnd) {
            this(excluded, numEvents, flush, expectedNumEvents, expectedNumResetCalls);
            this.expectedQueueSizeAtStart = expectedQueueSizeAtStart;
            this.expectedQueueSizeAtEnd = expectedQueueSizeAtEnd;
        }
        
        @Override
        public String toString() {
            return "excluded="+excluded+", numEvents="+numEvents+", flush="+flush+", expectedNumEvents="+expectedNumEvents+", expectedNumResetCalls="+expectedNumResetCalls;
        }
    }
    
    private void doTestFullQueue(int queueLength, TestPattern... testPatterns) throws Exception {
        init(queueLength);

        // initialize observer with an initial contentChanged
        // (see ChangeDispatcher#addObserver)
        {
            compositeObserver.contentChanged(p(-1), CommitInfo.EMPTY_EXTERNAL);
        }
        // remove above first event right away
        executeRunnables(runnableQ, 5);
        received.clear();
        resetCallCnt = 0;
        
        int k = 0;
        int loopCnt = 0;
        for (TestPattern testPattern : testPatterns) {
            k++;
            if (testPattern.expectedQueueSizeAtStart >= 0) {
                assertEquals("loopCnt="+loopCnt+", queue size mis-match at start", 
                        testPattern.expectedQueueSizeAtStart, filteringObserver.getBackgroundObserver().getMBean().getQueueSize());
            }
            for(int i=0; i<testPattern.numEvents; i++) {
                CommitInfo info;
                if (!testPattern.excluded) {
                    info = includingCommitInfo;
                } else {
                    info = excludingCommitInfo;
                }
                k++;
                compositeObserver.contentChanged(p(k), info);
            }
            if (testPattern.flush) {
                executeRunnables(runnableQ, testPattern.numEvents + testPattern.expectedNumEvents + testPattern.expectedNumResetCalls + 10);
            }
            assertEquals("loopCnt="+loopCnt, testPattern.expectedNumEvents, received.size());
            assertEquals("loopCnt="+loopCnt, testPattern.expectedNumResetCalls, resetCallCnt);
            received.clear();
            resetCallCnt = 0;
            loopCnt++;
            if (testPattern.expectedQueueSizeAtEnd >= 0) {
                assertEquals("loopCnt="+loopCnt+", queue size mis-match at end", 
                        testPattern.expectedQueueSizeAtEnd, filteringObserver.getBackgroundObserver().getMBean().getQueueSize());
            }
        }
    }

}
