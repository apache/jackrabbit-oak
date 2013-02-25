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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

/**
 * Abstract base class for individual performance benchmarks.
 */
public abstract class AbstractTest implements Benchmark {

    private Repository repository;

    private Credentials credentials;

    private List<Session> sessions;

    private List<Thread> threads;

    private volatile boolean running;

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
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        System.out.format(
                "# %-34.34s     min     10%%     50%%     90%%     max       N%n",
                toString());
        for (RepositoryFixture fixture : fixtures) {
            try {
                Repository[] cluster = fixture.setUpCluster(1);
                try {
                    // Run the test
                    DescriptiveStatistics statistics = runTest(cluster[0]);
                    if (statistics.getN() > 0) {
                        System.out.format(
                                "%-36.36s  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6d%n",
                                fixture.toString(),
                                statistics.getMin(),
                                statistics.getPercentile(10.0),
                                statistics.getPercentile(50.0),
                                statistics.getPercentile(90.0),
                                statistics.getMax(),
                                statistics.getN());
                    }
                } finally {
                    fixture.tearDownCluster();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static final Credentials CREDENTIALS =
            new SimpleCredentials("admin", "admin".toCharArray());

    private DescriptiveStatistics runTest(Repository repository) throws Exception {
        DescriptiveStatistics statistics = new DescriptiveStatistics();

        setUp(repository, CREDENTIALS);
        try {
            // Run a few iterations to warm up the system
            for (int i = 0; i < 5; i++) {
                execute();
            }

            // Run test iterations, and capture the execution times
            int iterations = 0;
            long runtimeEnd =
                    System.currentTimeMillis()
                    + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
            while (iterations++ < 10
                    || System.currentTimeMillis() < runtimeEnd) {
                statistics.addValue(execute());
            }
        } finally {
            tearDown();
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
    protected Session loginReader() {
        try {
            Session session = repository.login();
            sessions.add(session);
            return session;
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
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
            sessions.add(session);
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
                    try {
                        // rate-limit, to avoid 100% cpu usage
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    job.run();
                }
            }
        };
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.start();
        threads.add(thread);
    }

    public String toString() {
        String name = getClass().getName();
        return name.substring(name.lastIndexOf('.') + 1);
    }

}
