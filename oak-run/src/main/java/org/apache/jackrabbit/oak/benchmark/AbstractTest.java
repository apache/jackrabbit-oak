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
package org.apache.jackrabbit.oak.benchmark;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.jackrabbit.oak.benchmark.util.Profiler;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

/**
 * Abstract base class for individual performance benchmarks.
 */
abstract class AbstractTest<T> extends Benchmark implements CSVResultGenerator {

    /**
     * A random string to guarantee concurrently running tests don't overwrite
     * each others changes (for example in a cluster).
     * <p>
     * The probability of duplicates, for 50 concurrent processes, is less than
     * 1 in 1 million.
     */
    static final String TEST_ID = Integer.toHexString(new Random().nextInt());
    
    static AtomicInteger nodeNameCounter = new AtomicInteger();
    
    /**
     * A node name that is guarantee to be unique within the current JVM.
     */
    static String nextNodeName() {
        return "n" + Integer.toHexString(nodeNameCounter.getAndIncrement());
    }
    
    private static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    private static final long WARMUP = TimeUnit.SECONDS.toMillis(Long.getLong("warmup", 5));

    private static final long RUNTIME = TimeUnit.SECONDS.toMillis(Long.getLong("runtime", 60));
    
    private static final boolean PROFILE = Boolean.getBoolean("profile");
    
    private Repository repository;

    private Credentials credentials;

    private List<Session> sessions;

    private List<Thread> threads;

    private volatile boolean running;

    private Profiler profiler;

    private PrintStream out;

    @Override
    public void setPrintStream(PrintStream out) {
        this.out = out;
    }

    protected static int getScale(int def) {
        int scale = Integer.getInteger("scale", 0);
        if (scale == 0) {
            scale = def;
        }
        return scale;
    }

    /**
     * Prepares this performance benchmark.
     *
     * @param repository the repository to use
     * @param credentials credentials of a user with write access
     * @throws Exception if the benchmark can not be prepared
     */
    public void setUp(Repository repository, Credentials credentials)
            throws Exception {
        this.repository = repository;
        this.credentials = credentials;
        this.sessions = new LinkedList<Session>();
        this.threads = new LinkedList<Thread>();

        this.running = true;

        beforeSuite();
        if (PROFILE) {
            profiler = new Profiler().startCollecting();
        }
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        run(fixtures, null);
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures, List<Integer> concurrencyLevels) {
        System.out.format(
                "# %-26.26s       C     min     10%%     50%%     90%%     max       N%n",
                toString());
        if (out != null) {
            out.format(
                    "# %-26.26s,      C,    min,    10%%,    50%%,    90%%,    max,      N%n",
                    toString());
        }
        for (RepositoryFixture fixture : fixtures) {
            try {
                Repository[] cluster = createRepository(fixture);
                try {
                    runTest(fixture, cluster[0], concurrencyLevels);
                } finally {
                    fixture.tearDownCluster();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private void runTest(RepositoryFixture fixture, Repository repository, List<Integer> concurrencyLevels) throws Exception {

        setUp(repository, CREDENTIALS);
        try {
            
            // Run a few iterations to warm up the system
            long warmupEnd = System.currentTimeMillis() + WARMUP;
            while (System.currentTimeMillis() < warmupEnd) {
                execute();
            }

            if (concurrencyLevels == null || concurrencyLevels.isEmpty()) {
                concurrencyLevels = Arrays.asList(1);
            }

            for (Integer concurrency: concurrencyLevels) {
                // Run the test
                DescriptiveStatistics statistics = runTest(concurrency);
                if (statistics.getN() > 0) {
                    System.out.format(
                            "%-28.28s  %6d  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6d%n",
                            fixture.toString(),
                            concurrency,
                            statistics.getMin(),
                            statistics.getPercentile(10.0),
                            statistics.getPercentile(50.0),
                            statistics.getPercentile(90.0),
                            statistics.getMax(),
                            statistics.getN());
                    if (out != null) {
                        out.format(
                                "%-28.28s, %6d, %6.0f, %6.0f, %6.0f, %6.0f, %6.0f, %6d%n",
                                fixture.toString(),
                                concurrency,
                                statistics.getMin(),
                                statistics.getPercentile(10.0),
                                statistics.getPercentile(50.0),
                                statistics.getPercentile(90.0),
                                statistics.getMax(),
                                statistics.getN());
                    }
                }

            }
        } finally {
            tearDown();
        }
    }

    private class Executor extends Thread {

        private final SynchronizedDescriptiveStatistics statistics;

        private boolean running = true;

        private Executor(String name, SynchronizedDescriptiveStatistics statistics) {
            super(name);
            this.statistics = statistics;
        }

        @Override
        public void run() {
            T context = null;
            try {
                context = prepareThreadExecutionContext();
                while (running) {
                    statistics.addValue(execute(context));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                disposeThreadExecutionContext(context);
            }
        }
    }


    private DescriptiveStatistics runTest(int concurrencyLevel) throws Exception {
        final SynchronizedDescriptiveStatistics statistics = new SynchronizedDescriptiveStatistics();
        if (concurrencyLevel == 1) {
            // Run test iterations, and capture the execution times
            long runtimeEnd = System.currentTimeMillis() + RUNTIME;
            while (System.currentTimeMillis() < runtimeEnd) {
                statistics.addValue(execute());
            }

        } else {
            List<Executor> threads = new LinkedList<Executor>();
            for (int n=0; n<concurrencyLevel; n++) {
                threads.add(new Executor("Background job " + n, statistics));
            }

            // start threads
            for (Thread t: threads) {
                t.start();
            }

            //System.out.printf("Started %d threads%n", threads.size());

            // Run test iterations, and capture the execution times
            long runtimeEnd = System.currentTimeMillis() + RUNTIME;
            while (System.currentTimeMillis() < runtimeEnd) {
                Thread.sleep(runtimeEnd - System.currentTimeMillis());
            }

            // stop threads
            for (Executor e: threads) {
                e.running = false;
            }

            // wait for threads
            for (Executor e: threads) {
                e.join();
            }
        }
        return statistics;
    }


    /**
     * Executes a single iteration of this test.
     *
     * @return number of milliseconds spent in this iteration
     * @throws Exception if an error occurs
     */
    public long execute() throws Exception {
        beforeTest();
        try {
            long start = System.currentTimeMillis();
            // System.out.println("execute " + this);
            runTest();
            return System.currentTimeMillis() - start;
        } finally {
            afterTest();
        }
    }

    private long execute(T executionContext) throws Exception {
        if(executionContext == null){
            return execute();
        }

        beforeTest(executionContext);
        try {
            long start = System.currentTimeMillis();
            // System.out.println("execute " + this);
            runTest(executionContext);
            return System.currentTimeMillis() - start;
        } finally {
            afterTest(executionContext);
        }
    }

    /**
     * Cleans up after this performance benchmark.
     *
     * @throws Exception if the benchmark can not be cleaned up
     */
    public void tearDown() throws Exception {
        this.running = false;
        for (Thread thread : threads) {
            thread.join();
        }

        if (profiler != null) {
            System.out.println(profiler.stopCollecting().getTop(5));
            profiler = null;
        }

        afterSuite();

        for (Session session : sessions) {
            if (session.isLive()) {
                session.logout();
            }
        }

        this.threads = null;
        this.sessions = null;
        this.credentials = null;
        this.repository = null;
    }

    /**
     * Run before any iterations of this test get executed. Subclasses can
     * override this method to set up static test content.
     *
     * @throws Exception if an error occurs
     */
    protected void beforeSuite() throws Exception {
    }

    protected void beforeTest() throws Exception {
    }

    protected abstract void runTest() throws Exception;

    protected void afterTest() throws Exception {
    }

    /**
     * Run after all iterations of this test have been executed. Subclasses can
     * override this method to clean up static test content.
     *
     * @throws Exception if an error occurs
     */
    protected void afterSuite() throws Exception {
    }

    /**
     * Invoked before the thread starts. If the test later requires
     * some thread local context e.g. JCR session per thread then sub
     * classes can return a context instance. That instance would be
     * passed as part of runTest call
     *
     * @return context instance to be used for runTest call for the
     * current thread
     */
    protected T prepareThreadExecutionContext() {
        return null;
    }

    protected void disposeThreadExecutionContext(T context) {

    }

    protected void afterTest(T executionContext) {

    }

    protected void runTest(T executionContext)  throws Exception {
        throw new IllegalStateException("If thread execution context is used then subclass must " +
                "override this method");
    }

    protected void beforeTest(T executionContext) {

    }

    protected void failOnRepositoryVersions(String... versions)
            throws RepositoryException {
        String repositoryVersion =
                repository.getDescriptor(Repository.REP_VERSION_DESC);
        for (String version : versions) {
            if (repositoryVersion.startsWith(version)) {
                throw new RepositoryException(
                        "Unable to run " + getClass().getName()
                        + " on repository version " + version);
            }
        }
    }

    protected Repository getRepository() {
        return repository;
    }

    protected Credentials getCredentials() {
        return credentials;
    }

    /**
     * Returns a new reader session that will be automatically closed once
     * all the iterations of this test have been executed.
     *
     * @return reader session
     */
    protected Session loginAnonymous() {
        return login(new GuestCredentials());
    }

    /**
     * Returns a new admin session that will be automatically closed once
     * all the iterations of this test have been executed.
     *
     * @return admin session
     */
    protected Session loginAdministrative() {
        return login(new SimpleCredentials("admin", "admin".toCharArray()));
    }
    
    /**
    * Returns a new session for the given user
    * that will be automatically closed once
    * all the iterations of this test have been executed.
    * 
    * @param credentials the user credentials
    * @return user session
    */
   protected Session login(Credentials credentials) {
       try {
           Session session = repository.login(credentials);
           synchronized (sessions) {
               sessions.add(session);
           }
           return session;
       } catch (RepositoryException e) {
           throw new RuntimeException(e);
       }
   }

    /**
     * Logs out and removes the session from the internal pool.
     * @param session the session to logout
     */
    protected void logout(Session session) {
        if (session != null) {
            session.logout();
        }
        synchronized (sessions) {
            sessions.remove(session);
        }
    }

    /**
     * Returns a new writer session that will be automatically closed once
     * all the iterations of this test have been executed.
     *
     * @return writer session
     */
    protected Session loginWriter() {
        try {
            Session session = repository.login(credentials);
            synchronized (sessions) {
                sessions.add(session);
            }
            return session;
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a background thread that repeatedly executes the given job
     * until all the iterations of this test have been executed.
     *
     * @param job background job
     */
    protected void addBackgroundJob(final Runnable job) {
        Thread thread = new Thread("Background job " + job) {
            @Override
            public void run() {
                while (running) {
                    job.run();
                }
            }
        };
        thread.start();
        threads.add(thread);
    }

    /**
     * Customize the repository creation process by custom fixture handling
     */
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        return fixture.setUpCluster(1);
    }

}
