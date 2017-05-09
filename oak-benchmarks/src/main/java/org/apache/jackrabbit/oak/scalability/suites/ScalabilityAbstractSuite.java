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
package org.apache.jackrabbit.oak.scalability.suites;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newLinkedHashMap;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.jackrabbit.oak.benchmark.CSVResultGenerator;
import org.apache.jackrabbit.oak.commons.Profiler;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.scalability.ScalabilitySuite;
import org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class which defines a lot of the boiler-plate code needed to run the suite of tests.
 * 
 * Any test suite extending from this class has the following entry points
 * <p>
 * {@link #beforeSuite()} - To configure the whole suite before the tests are started.
 * <p>
 * {@link #afterSuite()} - To shutdown the whole suite after all tests are finished.
 * <p>
 * {@link #beforeIteration(ExecutionContext)} - Any initialization to be performed before each of
 * the test run. Typically, this can be configured to create additional loads for each iteration.
 * This method will be called before each test iteration begins.
 * <p>
 * {@link #afterIteration()} - To configure any post test steps to be executed after each iteration
 * of the test. This method will be called after each test iteration completes.
 * <p>
 * {@link #executeBenchmark(ScalabilityBenchmark, ExecutionContext)} - Actual benchmark/test to be
 * executed. This method will be called in each iteration of the test run.
 *
 * <p>
 * The following system JVM properties can be defined to configure the suite.
 * <ul>
 * <li>
 *     <code>profile</code> - Controls whether to profile the benchmark or not. Defaults to false.
 * </li>
 * <li>
 *     <code>noWarmup</code> - Controls whether to run warmup or not before each benchmark. Defaults to false.
 * </li>
 * <li>
 *     <code>increments</code> - Comma separated list which controls the incremental load for each iteration.
 *     Defaults to 1,2,5.
 * </li>
 * </ul>
 */
public abstract class ScalabilityAbstractSuite implements ScalabilitySuite, CSVResultGenerator {
    public static final String CTX_SEARCH_PATHS_PROP = "searchPaths";

    protected static final Logger LOG = LoggerFactory.getLogger(ScalabilityAbstractSuite.class);

    /**
     * A random string to guarantee concurrently running tests don't overwrite
     * each others changes (for example in a cluster).
     * <p>
     * The probability of duplicates, for 50 concurrent processes, is less than 1 in 1 million.
     */
    protected static final String TEST_ID = Integer.toHexString(new Random().nextInt());

    protected static final boolean PROFILE = Boolean.getBoolean("profile");

    protected static final boolean WARMUP = !Boolean.getBoolean("noWarmup");

    /**
     * Controls the incremental load for each iteration
     */
    protected static final List<String> INCREMENTS = Splitter.on(",").trimResults()
                    .omitEmptyStrings().splitToList(System.getProperty("increments", "1,2,5"));

    protected static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin"
            .toCharArray());

    private PrintStream out;

    protected Map<String, ScalabilityBenchmark> benchmarks;

    /**
     * Variables per suite run
     */
    private Repository repository;

    private Credentials credentials;

    private LinkedList<Session> sessions;

    ExecutionContext context;

    private Result result;

    private volatile boolean running;

    private List<Thread> threads;

    private RepositoryFixture fixture;

    protected ScalabilityAbstractSuite() {
        this.benchmarks = newLinkedHashMap();
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        for (RepositoryFixture fixture : fixtures) {
            try {
                Repository[] cluster = createRepository(fixture);
                try {
                    runSuite(fixture, cluster[0]);
                } finally {
                    fixture.tearDownCluster();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Run the full suite on the given fixture.
     *
     * @param fixture the fixture
     * @param repository the repository
     * @throws Exception the exception
     */
    private void runSuite(RepositoryFixture fixture, Repository repository) throws Exception {

        setUp(repository, fixture, CREDENTIALS);

        try {
            for (String increment : INCREMENTS) {
                context.setIncrement(Integer.parseInt(increment.trim()));

                setupIteration(increment);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Started test");
                }

                // Run one iteration
                runIteration(context);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Finished test");
                }

                tearDownIteration();
            }
        } catch(Exception e) {
           e.printStackTrace(); 
        } finally {
            tearDown();
        }
    }
    
    /**
     * Setup the iteration. Calls {@link #beforeIteration(ExecutionContext)} which can be
     * overridden by subclasses.
     * 
     * @param increment the current iteration's increment
     * @throws Exception
     */
    private void setupIteration(String increment) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start load : " + increment);
        }
        
        initBackgroundJobs();
        
        // create the load for this iteration
        beforeIteration(context);

        if (WARMUP) {
            for (ScalabilityBenchmark benchmark : benchmarks.values()) {
                executeBenchmark(benchmark, context);
            }
        }
    }
    
    /**
     * Post processing for the iteration.
     * 
     * @throws InterruptedException
     * @throws Exception
     */
    private void tearDownIteration() throws Exception {
        shutdownBackgroundJobs();
        
        afterIteration();
    }    

    /**
     * Setup any options before the benchmarks.
     * 
     * @throws Exception
     */
    protected void beforeSuite() throws Exception {
        // Start the profiler. Giving a chance to overriding classes to call it at a different stage
        if (PROFILE) {
            context.startProfiler();
        }
    }

    /**
     * Prepares this performance benchmark.
     * 
     * @param repository the repository to use
     * @param fixture credentials of a user with write access
     * @throws Exception if the benchmark can not be prepared
     */
    public void setUp(Repository repository, RepositoryFixture fixture, Credentials credentials)
            throws Exception {
        this.repository = repository;
        this.credentials = credentials;
        this.sessions = new LinkedList<Session>();
        this.fixture = fixture;
        context = new ExecutionContext();
        result = new Result();

        beforeSuite();

    }

    /**
     * Cleanup after the benchmarks are run.
     * 
     * @throws Exception
     */
    protected void afterSuite() throws Exception {}

    /**
     * Cleans up after this performance benchmark.
     * 
     * @throws Exception if the benchmark can not be cleaned up
     */
    public void tearDown() throws Exception {

        context.stopProfiler();
        result.out();

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
        this.context = null;
        this.result = null;
        this.benchmarks = null;
    }

    /**
     * Removes the benchmark.
     */
    @Override
    public boolean removeBenchmark(String benchmark) {
        return benchmarks.remove(benchmark) != null;
    }
    
    @Override
    public Map<String, ScalabilityBenchmark> getBenchmarks() {
        return benchmarks;
    }
    /**
     * Runs the benchmark.
     * 
     * @param benchmark the benchmark to execute
     * @throws Exception 
     */
    protected abstract void executeBenchmark(ScalabilityBenchmark benchmark,
            ExecutionContext context) throws Exception;

    /**
     * Runs the iteration of the benchmarks added.
     * 
     * @param context the execution context
     * @throws Exception 
     */
    private void runIteration(ExecutionContext context) throws Exception {
        Preconditions.checkArgument(benchmarks != null && !benchmarks.isEmpty(),
                "No Benchmarks configured");

        for (String key : benchmarks.keySet()) {
            ScalabilityBenchmark benchmark = benchmarks.get(key);
            if (result.getBenchmarkStatistics(benchmark) == null) {
                result.addBenchmarkStatistics(benchmark, new SynchronizedDescriptiveStatistics());
            }

            Stopwatch watch = Stopwatch.createStarted();

            executeBenchmark(benchmark, context);

            watch.stop();
            result.getBenchmarkStatistics(benchmark).addValue(watch.elapsed(TimeUnit.MILLISECONDS));

            if (LOG.isDebugEnabled()) {
                LOG.debug("Execution time for " + benchmark + "-"
                                            + watch.elapsed(TimeUnit.MILLISECONDS));
            }
        }
    }

    /**
     * Executes once for each iteration.
     * 
     * @param context the context
     * @throws Exception the repository exception
     */
    public void beforeIteration(ExecutionContext context) throws Exception {}

    /**
     * Run after all iterations of this test have been executed. Subclasses can
     * override this method to clean up static test content.
     * 
     * @throws Exception if an error occurs
     */
    protected void afterIteration() throws Exception {}

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
        thread.setDaemon(true);
        thread.start();
        threads.add(thread);
    }
    
    /**
     * Sets the running flag to true.
     */
    protected void initBackgroundJobs() {
        this.running = true;
        threads = newArrayList();
    }
    
    /**
     * Shutdown the background threads.
     * 
     * @throws InterruptedException
     */
    protected void shutdownBackgroundJobs() throws InterruptedException {
        this.running = false;
        for (Thread thread : threads) {
            thread.join();
        }
    }

    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        return fixture.setUpCluster(1);
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
     * Collects the execution times for each benchmark.
     */
    class Result {
        private final Map<ScalabilityBenchmark, DescriptiveStatistics> stats;

        public Result() {
            this.stats = newLinkedHashMap();
        }

        public void addBenchmarkStatistics(ScalabilityBenchmark benchmark,
                SynchronizedDescriptiveStatistics stat) {
            stats.put(benchmark, stat);
        }

        public DescriptiveStatistics getBenchmarkStatistics(ScalabilityBenchmark benchmark) {
            return stats.get(benchmark);
        }

        public void out() {
            for (Entry<ScalabilityBenchmark, DescriptiveStatistics> entry : stats.entrySet()) {
                DescriptiveStatistics statistics = entry.getValue();
                ScalabilityBenchmark benchmark = entry.getKey();

                System.out
                        .format(
                                "# %-26.26s       min     10%%     50%%     90%%     max       N%n",
                                benchmark.toString());
                if (out != null) {
                    out.format(
                            "# %-26.26s       min     10%%     50%%     90%%     max       N%n",
                            benchmark.toString());
                }

                System.out.format(
                        "%-30.30s  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6d%n",
                        fixture.toString(),
                        statistics.getMin(),
                        statistics.getPercentile(10.0),
                        statistics.getPercentile(50.0),
                        statistics.getPercentile(90.0),
                        statistics.getMax(),
                        statistics.getN());

                if (out != null) {
                    out.format(
                            "%-30.30s  %-6.0f  %-6.0f  %-6.0f  %-6.0f  %-6.0f  %-6d%n",
                            fixture.toString(),
                            statistics.getMin(),
                            statistics.getPercentile(10.0),
                            statistics.getPercentile(50.0),
                            statistics.getPercentile(90.0),
                            statistics.getMax(),
                            statistics.getN());
                }

                StringBuilder header = new StringBuilder();
                header.append("\t# %-26.26s");
                for (String increment : INCREMENTS) {
                    header.append("\t");
                    header.append(increment);
                }
                header.append("%n");
                System.out.format(header.toString(), "Iterations/Load");

                StringBuffer format = new StringBuffer();
                format.append("%-30.30s");
                System.out.format(format.toString(), "\t" + "Time (ms)");
                if (out != null) {
                    out.format(format.toString(), "\t" + "Time (ms)");
                }

                for (int idx = 0; idx < INCREMENTS.size(); idx++) {
                    format = new StringBuffer();
                    format.append("\t");
                    format.append("%-7.0f");
                    System.out.format(format.toString(), statistics.getValues()[idx]);
                    if (out != null) {
                        out.format(format.toString(), statistics.getValues()[idx]);
                    }
                }
                System.out.format("%n");
            }
        }
    }

    /**
     * Execution context to be pass information to and from the suite to the benchmarks.
     */
    public static class ExecutionContext {
        private Profiler profiler;
        private final AtomicLong iteration = new AtomicLong();
        private Map<Object, Object> map = newConcurrentMap();

        protected void setIncrement(int increment) {
            iteration.getAndSet(increment);
        }

        public int getIncrement() {
            return iteration.intValue();
        }

        public void startProfiler() {
                profiler = new Profiler().startCollecting();
        }

        public void stopProfiler() {
            if (profiler != null) {
                LOG.info(profiler.stopCollecting().getTop(5));
                profiler = null;
            }
        }

        public Map<Object, Object> getMap() {
            return map;
        }

        public void setMap(Map<Object, Object> map) {
            this.map = map;
        }
    }

    @Override
    public String toString() {
        String name = getClass().getName();
        return name.substring(name.lastIndexOf('.') + 1);
    }

    @Override
    public void setPrintStream(PrintStream out) {
        this.out = out;
    }

    protected Repository getRepository() {
        return repository;
    }
}

