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
package org.apache.jackrabbit.oak.plugins.atomic;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PREFIX_PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PREFIX_PROP_REVISION;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtomicCounterEditorTest {
    /**
     * convenience class to ease construction during tests
     */
    private static class TestableACEProvider extends AtomicCounterEditorProvider {
        public TestableACEProvider(final Clusterable c, final ScheduledExecutorService e,
                                   final NodeStore s, final Whiteboard b) {
            super(new Supplier<Clusterable>() {
                @Override
                public Clusterable get() {
                    return c;
                };
            }, new Supplier<ScheduledExecutorService>() {
                @Override
                public ScheduledExecutorService get() {
                    return e;
                };
            }, new Supplier<NodeStore>() {
                @Override
                public NodeStore get() {
                    return s;
                }
            }, new Supplier<Whiteboard>() {
                @Override
                public Whiteboard get() {
                    return b;
                };
            });
        }
    }
    private static final Clusterable CLUSTER_1 = new Clusterable() {
        @Override
        public String getInstanceId() {
            return "1";
        }

        @Override
        public String getVisibilityToken() {
            return "";
        }

        @Override
        public boolean isVisible(String visibilityToken, long maxWaitMillis) throws InterruptedException {
            return true;
        }
    };
    private static final Clusterable CLUSTER_2 = new Clusterable() {
        @Override
        public String getInstanceId() {
            return "2";
        }

        @Override
        public String getVisibilityToken() {
            return "";
        }

        @Override
        public boolean isVisible(String visibilityToken, long maxWaitMillis) throws InterruptedException {
            return true;
        }
    };
    private static final EditorHook HOOK_NO_CLUSTER = new EditorHook(
        new TestableACEProvider(null, null, null, null));
    private static final EditorHook HOOK_1_SYNC = new EditorHook(
        new TestableACEProvider(CLUSTER_1, null, null, null));
    private static final EditorHook HOOK_2_SYNC = new EditorHook(
        new TestableACEProvider(CLUSTER_2, null, null, null));

    private static final PropertyState INCREMENT_BY_1 = PropertyStates.createProperty(
        PROP_INCREMENT, 1L);
    private static final PropertyState INCREMENT_BY_2 = PropertyStates.createProperty(
        PROP_INCREMENT, 2L);
    
    @Test
    public void increment() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        
        builder = EMPTY_NODE.builder();
        editor = new AtomicCounterEditor(builder, null, null, null, null);
        editor.propertyAdded(INCREMENT_BY_1);
        assertNoCounters(builder.getProperties());
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder, null, null, null, null);
        editor.propertyAdded(INCREMENT_BY_1);
        assertNull("the oak:increment should never be set", builder.getProperty(PROP_INCREMENT));
        assertTotalCountersValue(builder.getProperties(), 1);
    }
    
    @Test
    public void consolidate() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder, null, null, null, null);
        
        editor.propertyAdded(INCREMENT_BY_1);
        assertTotalCountersValue(builder.getProperties(), 1);
        editor.propertyAdded(INCREMENT_BY_1);
        assertTotalCountersValue(builder.getProperties(), 2);
        AtomicCounterEditor.consolidateCount(builder);
        assertCounterNodeState(builder, ImmutableSet.of(PREFIX_PROP_COUNTER, PREFIX_PROP_REVISION), 2);
    }

    /**
     * that a list of properties does not contains any property with name starting with
     * {@link AtomicCounterEditor#PREFIX_PROP_COUNTER}
     * 
     * @param properties
     */
    private static void assertNoCounters(@Nonnull final Iterable<? extends PropertyState> properties) {
        checkNotNull(properties);
        
        for (PropertyState p : properties) {
            assertFalse("there should be no counter property",
                p.getName().startsWith(PREFIX_PROP_COUNTER));
        }
    }
    
    /**
     * assert the total amount of {@link AtomicCounterEditor#PREFIX_PROP_COUNTER}
     * 
     * @param properties
     */
    private static void assertTotalCountersValue(@Nonnull final Iterable<? extends PropertyState> properties,
                                            int expected) {
        int total = 0;
        for (PropertyState p : checkNotNull(properties)) {
            if (p.getName().startsWith(PREFIX_PROP_COUNTER)) {
                total += p.getValue(LONG);
            }
        }
        
        assertEquals("the total amount of :oak-counter properties does not match", expected, total);
    }
    
    private static NodeBuilder setMixin(@Nonnull final NodeBuilder builder) {
        return checkNotNull(builder).setProperty(JCR_MIXINTYPES, of(MIX_ATOMIC_COUNTER), NAMES);
    }
    
    
    private static void assertCounterNodeState(@Nonnull NodeBuilder builder, 
                                               @Nonnull Set<String> hiddenProps, 
                                               long expectedCounter) {
        checkNotNull(builder);
        checkNotNull(hiddenProps);
        long totalHiddenValue = 0;
        PropertyState counter = builder.getProperty(PROP_COUNTER);
        Set<String> hp = Sets.newHashSet(hiddenProps);
        
        assertNotNull("counter property cannot be null", counter);
        assertNull("The increment property should not be there",
            builder.getProperty(PROP_INCREMENT));
        for (PropertyState p : builder.getProperties()) {
            String name = p.getName();
            if (name.startsWith(":")) {
                assertTrue("Unexpected hidden property found: " + name, hp.remove(name));
            }
            if (name.startsWith(PREFIX_PROP_COUNTER)) {
                totalHiddenValue += p.getValue(LONG).longValue();
            }
        }
        assertEquals("The sum of the hidden properties does not match the counter", counter
            .getValue(LONG).longValue(), totalHiddenValue);
        assertEquals("The counter does not match the expected value", expectedCounter, counter
            .getValue(LONG).longValue());
    }

    private static NodeBuilder incrementBy(@Nonnull NodeBuilder builder, @Nonnull PropertyState increment) {
        return checkNotNull(builder).setProperty(checkNotNull(increment));
    }
    
    @Test
    public void notCluster() throws CommitFailedException {
        NodeBuilder builder;
        NodeState before, after;
        
        builder = EMPTY_NODE.builder();
        before = builder.getNodeState();
        builder = setMixin(builder);
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_NO_CLUSTER.processCommit(before, after, EMPTY).builder();
        assertCounterNodeState(builder, ImmutableSet.of(PREFIX_PROP_COUNTER, PREFIX_PROP_REVISION), 1);

        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_2);
        after = builder.getNodeState(); 
        builder = HOOK_NO_CLUSTER.processCommit(before, after, EMPTY).builder();
        assertCounterNodeState(builder, ImmutableSet.of(PREFIX_PROP_COUNTER, PREFIX_PROP_REVISION), 3);
    }
    
    /**
     * simulates the update from multiple oak instances
     * @throws CommitFailedException 
     */
    @Test
    public void multipleNodeUpdates() throws CommitFailedException {
        NodeBuilder builder;
        NodeState before, after;
        
        builder = EMPTY_NODE.builder();
        before = builder.getNodeState(); 
        builder = setMixin(builder);
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_1_SYNC.processCommit(before, after, EMPTY).builder();
        assertCounterNodeState(
            builder, 
            ImmutableSet.of(PREFIX_PROP_COUNTER + "1", PREFIX_PROP_REVISION + "1"),
            1);
        
        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_2_SYNC.processCommit(before, after, EMPTY).builder();
        assertCounterNodeState(
            builder,
            ImmutableSet.of(
                PREFIX_PROP_COUNTER + "1",
                PREFIX_PROP_COUNTER + "2", 
                PREFIX_PROP_REVISION + "1",
                PREFIX_PROP_REVISION + "2"),
            2);
    }
    
    /**
     * covers the revision increments aspect
     * @throws CommitFailedException 
     */
    @Test
    public void revisionIncrements() throws CommitFailedException {
        NodeBuilder builder;
        NodeState before, after;
        PropertyState rev;
        
        builder = EMPTY_NODE.builder();
        before = builder.getNodeState();
        builder = setMixin(builder);
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_1_SYNC.processCommit(before, after, EMPTY).builder();
        rev = builder.getProperty(PREFIX_PROP_REVISION + "1");
        assertNotNull(rev);
        assertEquals(1, rev.getValue(LONG).longValue());

        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_2);
        after = builder.getNodeState();
        builder = HOOK_1_SYNC.processCommit(before, after, EMPTY).builder();
        rev = builder.getProperty(PREFIX_PROP_REVISION + "1");
        assertNotNull(rev);
        assertEquals(2, rev.getValue(LONG).longValue());

        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_2_SYNC.processCommit(before, after, EMPTY).builder();
        rev = builder.getProperty(PREFIX_PROP_REVISION + "1");
        assertNotNull(rev);
        assertEquals(2, rev.getValue(LONG).longValue());
        rev = builder.getProperty(PREFIX_PROP_REVISION + "2");
        assertNotNull(rev);
        assertEquals(1, rev.getValue(LONG).longValue());
    }
    
    @Test
    public void singleNodeAsync() throws CommitFailedException, InterruptedException, ExecutionException {
        NodeStore store = new MemoryNodeStore();
        MyExecutor exec1 = new MyExecutor();
        Whiteboard board = new DefaultWhiteboard();
        EditorHook hook1 = new EditorHook(new TestableACEProvider(CLUSTER_1, exec1, store, board));
        NodeBuilder builder, root;
        PropertyState p;
        
        board.register(CommitHook.class, EmptyHook.INSTANCE, null);
        
        root = store.getRoot().builder();
        builder = root.child("c");
        builder = setMixin(builder);
        builder = incrementBy(builder, INCREMENT_BY_1);
        store.merge(root, hook1, CommitInfo.EMPTY);
        
        // as we're providing all the information we expect the counter not to be consolidated for
        // as long as the scheduled process has run
        builder = store.getRoot().builder().getChildNode("c");
        assertTrue(builder.exists());
        p = builder.getProperty(PREFIX_PROP_REVISION + CLUSTER_1.getInstanceId());
        assertNotNull(p);
        assertEquals(1, p.getValue(LONG).longValue());
        p = builder.getProperty(PREFIX_PROP_COUNTER + CLUSTER_1.getInstanceId());
        assertNotNull(p);
        assertEquals(1, p.getValue(LONG).longValue());
        p = builder.getProperty(PROP_COUNTER);
        assertNull(p);
        
        // executing the consolidation
        exec1.execute();
        
        // fetching the latest store state to see the changes
        builder = store.getRoot().builder().getChildNode("c");
        assertTrue("the counter node should exists", builder.exists());
        assertCounterNodeState(
            builder,
            ImmutableSet.of(PREFIX_PROP_COUNTER + CLUSTER_1.getInstanceId(),
                PREFIX_PROP_REVISION + CLUSTER_1.getInstanceId()), 1);
    }
    
    @Test
    public void noHookInWhiteboard() throws CommitFailedException, InterruptedException, ExecutionException {
        NodeStore store = new MemoryNodeStore();
        MyExecutor exec1 = new MyExecutor();
        Whiteboard board = new DefaultWhiteboard();
        EditorHook hook1 = new EditorHook(new TestableACEProvider(CLUSTER_1, exec1, store, board));
        NodeBuilder builder, root;
        PropertyState p;
        
        
        root = store.getRoot().builder();
        builder = root.child("c");
        builder = setMixin(builder);
        builder = incrementBy(builder, INCREMENT_BY_1);
        store.merge(root, hook1, CommitInfo.EMPTY);
        
        // as we're providing all the information we expect the counter not to be consolidated for
        // as long as the scheduled process has run
        builder = store.getRoot().builder().getChildNode("c");
        assertTrue(builder.exists());
        p = builder.getProperty(PREFIX_PROP_REVISION + CLUSTER_1.getInstanceId());
        assertNotNull(p);
        assertEquals(1, p.getValue(LONG).longValue());
        p = builder.getProperty(PREFIX_PROP_COUNTER + CLUSTER_1.getInstanceId());
        assertNotNull(p);
        assertEquals(1, p.getValue(LONG).longValue());
        p = builder.getProperty(PROP_COUNTER);
        assertEquals(1, p.getValue(LONG).longValue());
        
        assertTrue("without a registered hook it should have fell to sync", exec1.isEmpty());
        
        // fetching the latest store state to see the changes
        builder = store.getRoot().builder().getChildNode("c");
        assertTrue("the counter node should exists", builder.exists());
        assertCounterNodeState(
            builder,
            ImmutableSet.of(PREFIX_PROP_COUNTER + CLUSTER_1.getInstanceId(),
                PREFIX_PROP_REVISION + CLUSTER_1.getInstanceId()), 1);
    }
    
    /**
     * a fake {@link ScheduledExecutorService} which does not schedule and wait for a call on
     * {@link #execute()} to execute the first scheduled task. It works in a FIFO manner.
     */
    private static class MyExecutor extends AbstractExecutorService implements ScheduledExecutorService {
        private static final Logger LOG = LoggerFactory.getLogger(MyExecutor.class);
        
        @SuppressWarnings("rawtypes")
        private Queue<ScheduledFuture> fifo = new LinkedList<ScheduledFuture>();
        
        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Not implemented");
        }

        private synchronized void addToQueue(@SuppressWarnings("rawtypes") @Nonnull ScheduledFuture future) {
            fifo.add(future);
        }
        
        /**
         * return true whether the underlying queue is empty or not
         * 
         * @return
         */
        public boolean isEmpty() {
            return fifo.isEmpty();
        }
        
        @SuppressWarnings("rawtypes")
        private synchronized ScheduledFuture getFromQueue() {
            if (fifo.isEmpty()) {
                return null;
            } else {
                return fifo.remove();
            }
        }
        
        @Override
        public <V> ScheduledFuture<V> schedule(final Callable<V> callable, long delay, TimeUnit unit) {
            LOG.debug("Scheduling with delay: {} and unit: {} the process {}", delay, unit, callable);
            
            checkNotNull(callable);
            checkNotNull(unit);
            if (delay < 0) {
                delay = 0;
            }
                        
            ScheduledFuture<V> future = new ScheduledFuture<V>() {
                final Callable<V> c = callable;
                
                @Override
                public long getDelay(TimeUnit unit) {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public int compareTo(Delayed o) {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public boolean isCancelled() {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public boolean isDone() {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public V get() throws InterruptedException, ExecutionException {
                    try {
                        return c.call();
                    } catch (Exception e) {
                        throw new ExecutionException(e);
                    }
                }

                @Override
                public V get(long timeout, TimeUnit unit) throws InterruptedException,
                                                         ExecutionException, TimeoutException {
                    throw new UnsupportedOperationException("Not implemented");
                }
            };
            
            addToQueue(future);
            return future;
        }

        /**
         * executes the first item scheduled in the queue. If the queue is empty it will silently
         * return {@code null} which can easily be the same returned from the scheduled process.
         * 
         * @return the result of the {@link ScheduledFuture} or {@code null} if the queue is empty.
         * @throws InterruptedException
         * @throws ExecutionException
         */
        @CheckForNull
        public Object execute() throws InterruptedException, ExecutionException {
            ScheduledFuture<?> f = getFromQueue();
            if (f == null) {
                return null;
            } else {
                return f.get();
            }
        }
        
        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
                                                      long period, TimeUnit unit) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                         long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
    
    @Test
    public void checkRevision() {
        NodeBuilder b = EMPTY_NODE.builder();
        PropertyState r = LongPropertyState.createLongProperty("r", 10L);
        
        assertTrue(AtomicCounterEditor.checkRevision(b, null));
        assertFalse(AtomicCounterEditor.checkRevision(b, r));
        
        b.setProperty(LongPropertyState.createLongProperty(r.getName(), 1L));
        assertFalse(AtomicCounterEditor.checkRevision(b, r));
        
        b.setProperty(LongPropertyState.createLongProperty(r.getName(), 10L));
        assertTrue(AtomicCounterEditor.checkRevision(b, r));

        b.setProperty(LongPropertyState.createLongProperty(r.getName(), 20L));
        assertTrue(AtomicCounterEditor.checkRevision(b, r));
    }
    
    @Test
    public void nextDelay() {
        assertEquals(AtomicCounterEditor.ConsolidatorTask.MIN_TIMEOUT,
            AtomicCounterEditor.ConsolidatorTask.nextDelay(-23456789));
        assertEquals(AtomicCounterEditor.ConsolidatorTask.MIN_TIMEOUT,
            AtomicCounterEditor.ConsolidatorTask
                .nextDelay(AtomicCounterEditor.ConsolidatorTask.MIN_TIMEOUT - 1));
        assertEquals(1000, AtomicCounterEditor.ConsolidatorTask.nextDelay(500));
        assertEquals(2000, AtomicCounterEditor.ConsolidatorTask.nextDelay(1000));
        assertEquals(4000, AtomicCounterEditor.ConsolidatorTask.nextDelay(2000));
        assertEquals(8000, AtomicCounterEditor.ConsolidatorTask.nextDelay(4000));
        assertEquals(16000, AtomicCounterEditor.ConsolidatorTask.nextDelay(8000));
        assertEquals(32000, AtomicCounterEditor.ConsolidatorTask.nextDelay(16000));
        assertEquals(Long.MAX_VALUE,
            AtomicCounterEditor.ConsolidatorTask
                .nextDelay(AtomicCounterEditor.ConsolidatorTask.MAX_TIMEOUT));
        assertEquals(Long.MAX_VALUE,
            AtomicCounterEditor.ConsolidatorTask
                .nextDelay(AtomicCounterEditor.ConsolidatorTask.MAX_TIMEOUT + 1));
    }
    
    @Test
    public void isTimeOut() {
        assertFalse(AtomicCounterEditor.ConsolidatorTask.isTimedOut(0));
        assertFalse(AtomicCounterEditor.ConsolidatorTask.isTimedOut(1));
        assertFalse(AtomicCounterEditor.ConsolidatorTask.isTimedOut(2));
        assertFalse(AtomicCounterEditor.ConsolidatorTask.isTimedOut(4));
        assertFalse(AtomicCounterEditor.ConsolidatorTask.isTimedOut(8));
        assertFalse(AtomicCounterEditor.ConsolidatorTask.isTimedOut(16));
        assertFalse(AtomicCounterEditor.ConsolidatorTask.isTimedOut(32));
        assertTrue(AtomicCounterEditor.ConsolidatorTask
            .isTimedOut(AtomicCounterEditor.ConsolidatorTask.MAX_TIMEOUT + 1)); // any number > 32
        assertTrue(AtomicCounterEditor.ConsolidatorTask.isTimedOut(Long.MAX_VALUE));
    }
    
    @Test
    public void isConsolidate() {
        NodeBuilder b = EMPTY_NODE.builder();
        PropertyState counter, hidden1, hidden2;
        String hidden1Name = PREFIX_PROP_COUNTER + "1";
        String hidden2Name = PREFIX_PROP_COUNTER + "2";
        
        assertFalse(AtomicCounterEditor.isConsolidate(b));
        
        counter = LongPropertyState.createLongProperty(PROP_COUNTER, 0);
        hidden1 = LongPropertyState.createLongProperty(hidden1Name, 1);
        b.setProperty(counter);
        b.setProperty(hidden1);
        assertTrue(AtomicCounterEditor.isConsolidate(b));

        counter = LongPropertyState.createLongProperty(PROP_COUNTER, 1);
        hidden1 = LongPropertyState.createLongProperty(hidden1Name, 1);
        hidden2 = LongPropertyState.createLongProperty(hidden2Name, 1);
        b.setProperty(counter);
        b.setProperty(hidden1);
        b.setProperty(hidden2);
        assertTrue(AtomicCounterEditor.isConsolidate(b));

        counter = LongPropertyState.createLongProperty(PROP_COUNTER, 2);
        hidden1 = LongPropertyState.createLongProperty(hidden1Name, 1);
        hidden2 = LongPropertyState.createLongProperty(hidden2Name, 1);
        b.setProperty(counter);
        b.setProperty(hidden1);
        b.setProperty(hidden2);
        assertFalse(AtomicCounterEditor.isConsolidate(b));
    }
}
