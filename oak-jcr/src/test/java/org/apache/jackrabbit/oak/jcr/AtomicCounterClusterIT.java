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
package org.apache.jackrabbit.oak.jcr;

import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.commons.benchmark.PerfLogger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;

public class AtomicCounterClusterIT  extends DocumentClusterIT {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterClusterIT.class);
    private static final PerfLogger LOG_PERF = new PerfLogger(LOG);
    private List<CustomScheduledExecutor> executors = Lists.newArrayList();
    
    @BeforeClass
    public static void assumtions() {
        assumeTrue(FIXTURES.contains(Fixture.DOCUMENT_NS));
        assumeTrue(OakMongoNSRepositoryStub.isMongoDBAvailable());
    }
    
    @Override
    public void before() throws Exception {
        super.before();
        executors = Lists.newArrayList();
    }

    @Override
    public void after() throws Exception {
        super.after();
        for (CustomScheduledExecutor exec : executors) {
            new ExecutorCloser(exec, 10, TimeUnit.SECONDS).close();
        }
    }

    @Test
    public void increments() throws Exception {
        setUpCluster(this.getClass(), mks, repos, NOT_PROVIDED);

        assertEquals("repositories and executors should match", repos.size(), executors.size());
        
        final String counterPath;
        final Random rnd = new Random(14);
        final AtomicLong expected = new AtomicLong(0);
        final Map<String, Exception> exceptions = Collections.synchronizedMap(
            new HashMap<String, Exception>());

        // setting-up the repo state
        Repository repo = repos.get(0);
        Session session = repo.login(ADMIN);
        Node counter;
        
        try {
            counter = session.getRootNode().addNode("counter");
            counter.addMixin(MIX_ATOMIC_COUNTER);
            session.save();
            
            counterPath = counter.getPath();
        } finally {
            session.logout();
        }
        
        // allow the cluster to align
        alignCluster(mks);

        // asserting the initial state
        assertFalse("Path to the counter node should be set", Strings.isNullOrEmpty(counterPath));
        for (Repository r : repos) {
            
            try {
                session = r.login(ADMIN);
                counter = session.getNode(counterPath);
                assertEquals("Nothing should have touched the `expected`", 0, expected.get());
                assertEquals(
                    "Wrong initial counter", 
                    expected.get(), 
                    counter.getProperty(PROP_COUNTER).getLong());
            } finally {
                session.logout();
            }
            
        }
        
        // number of threads per cluster node
        final int numIncrements = Integer.getInteger("oak.test.it.atomiccounter.threads", 100);
        
        LOG.debug(
            "pushing {} increments per each of the {} cluster nodes for a total of {} concurrent updates",
            numIncrements, repos.size(), numIncrements * repos.size());
        
        // for each cluster node, `numIncrements` sessions pushing random increments
        long start = LOG_PERF.start("Firing the threads");
        List<ListenableFutureTask<Void>> tasks = Lists.newArrayList();
        for (Repository rep : repos) {
            final Repository r = rep;
            for (int i = 0; i < numIncrements; i++) {
                ListenableFutureTask<Void> task = ListenableFutureTask.create(new Callable<Void>() {

                        @Override
                        public Void call() throws Exception {
                            Session s = r.login(ADMIN);
                            try {
                                try {
                                    Node n = s.getNode(counterPath);
                                    int increment = rnd.nextInt(10) + 1;
                                    n.setProperty(PROP_INCREMENT, increment);
                                    expected.addAndGet(increment);
                                    s.save();
                                } finally {
                                    s.logout();
                                }                                
                            } catch (Exception e) {
                                exceptions.put(Thread.currentThread().getName(), e);
                            }
                            return null;
                        }
                });
                new Thread(task).start();
                tasks.add(task);
            }
        }
        LOG_PERF.end(start, -1, "Firing threads completed", "");
        Futures.allAsList(tasks).get();
        LOG_PERF.end(start, -1, "Futures completed", "");
        
        waitForTaskCompletion();
        LOG_PERF.end(start, -1, "All tasks completed", "");
        
        // let the time for the async process to kick in and run.
        Thread.sleep(5000);
        
        raiseExceptions(exceptions, LOG);
        
        // assert the final situation
        for (int i = 0; i < repos.size(); i++) {
            Repository r = repos.get(i);
            try {
                session = r.login(ADMIN);
                counter = session.getNode(counterPath);
                LOG.debug("Cluster node: {}, actual counter: {}, expected counter: {}", i + 1,
                    expected.get(), counter.getProperty(PROP_COUNTER).getLong());
                assertEquals(
                    "Wrong counter on node " + (i + 1), 
                    expected.get(), 
                    counter.getProperty(PROP_COUNTER).getLong());
            } finally {
                session.logout();
            }
            
        }
    }
    
    private void waitForTaskCompletion() throws InterruptedException {
        int remainingTasks;
        do {
            remainingTasks = 0;
            for (CustomScheduledExecutor e : executors) {
                remainingTasks += e.getTotal();
            }
            if (remainingTasks > 0) {
                LOG.debug("there are approximately {} tasks left to complete. Sleeping 1 sec",
                    remainingTasks);
                Thread.sleep(1000);
            }
        } while (remainingTasks > 0);
    }
    
    private class CustomScheduledExecutor extends ScheduledThreadPoolExecutor {
        private volatile AtomicInteger total = new AtomicInteger();
        
        private class CustomTask<V> implements RunnableScheduledFuture<V> {
            private final RunnableScheduledFuture<V> task;
            
            public CustomTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
                this.task = task;
            }
            
            @Override
            public void run() {
                task.run();
                total.decrementAndGet();
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return task.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return task.isCancelled();
            }

            @Override
            public boolean isDone() {
                return task.isDone();
            }

            @Override
            public V get() throws InterruptedException, ExecutionException {
                return task.get();
            }

            @Override
            public V get(long timeout, TimeUnit unit) throws InterruptedException,
                                                     ExecutionException, TimeoutException {
                return task.get(timeout, unit);
            }

            @Override
            public long getDelay(TimeUnit unit) {
                return task.getDelay(unit);
            }

            @Override
            public int compareTo(Delayed o) {
                return task.compareTo(o);
            }

            @Override
            public boolean isPeriodic() {
                return task.isPeriodic();
            }
        }
        
        public CustomScheduledExecutor(int corePoolSize) {
            super(corePoolSize);
            total.set(0);
        }

        @Override
        protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable,
                                                              RunnableScheduledFuture<V> task) {
            if (callable instanceof AtomicCounterEditor.ConsolidatorTask) {
                total.incrementAndGet();
                return new CustomTask<V>(callable, task);
            } else {
                return super.decorateTask(callable, task);
            }
        }
        
        /**
         * return the approximate amount of tasks to be completed
         * @return
         */
        public synchronized int getTotal() {
            return total.get();
        }
    }
    
    @Override
    protected Jcr getJcr(NodeStore store) {
        CustomScheduledExecutor e = new CustomScheduledExecutor(10);
        executors.add(e);
        return super.getJcr(store)
            .with(e)
            .withAtomicCounter();
    }
}
