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

package org.apache.jackrabbit.oak.commons.benchmark;

import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


/**
 * Poor man's micro benchmark suite.
 * <p>
 * Implementations of {@link Benchmark} are executed by the {@link #run(Benchmark)} method.
 * Execution consists of a warm up phase followed by the actual benchmark run.
 */
public final class MicroBenchmark {
    private MicroBenchmark() { }

    /**
     * Benchmark base class.
     */
    public abstract static class Benchmark {

        /**
         * The benchmark runner calls this method first and exactly once by for setting up
         * the test fixture.
         */
        public void setup() throws Exception { }

        /**
         * The benchmark runner calls the method  before every call to the {@code run}
         * method for setting the scope of the subsequent call to {@code run}.
         */
        public void beforeRun() throws Exception { }

        /**
         * The benchmark runner calls this method a number of times to measure its
         * runtime performance.
         */
        public abstract void run() throws Exception;

        /**
         * The benchmark runner calls the method  after every call to the {@code run}
         * method for tearing down the scope of the previous call to {@code run}.
         */
        public void afterRun() throws Exception { }

        /**
         * The benchmark runner calls this method exactly once and only if the benchmark
         * did not result in an error. This default implementation tabulates
         * the percentiles of the gathered test statistics.
         */
        public void result(DescriptiveStatistics statistics) {
            System.out.println(this);
            if (statistics.getN() > 0) {
                System.out.format(
                        "%6s  %6s  %6s  %6s  %6s  %6s  %6s  %6s%n",
                        "min", "10%", "50%", "90%", "max", "mean", "stdev", "N");
                System.out.format(
                        "%6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6d%n",
                        statistics.getMin() / 1000000,
                        statistics.getPercentile(10.0) / 1000000,
                        statistics.getPercentile(50.0) / 1000000,
                        statistics.getPercentile(90.0) / 1000000,
                        statistics.getMax() / 1000000,
                        statistics.getMean() / 1000000,
                        statistics.getStandardDeviation() / 1000000,
                        statistics.getN());
            } else {
                System.out.println("No results");
            }
        }

        /**
         * The benchmark runner calls this method last and exactly once for tearing down
         * the test fixture.
         */
        public void tearDown() throws Exception { }
    }

    /**
     * Run a {@code benchmark}
     * @param benchmark
     * @throws Exception
     */
    public static void run(Benchmark benchmark) throws Exception {
        benchmark.setup();
        try {
            // Warm up
            runTest(benchmark);

            // Run the test
            benchmark.result(runTest(benchmark));
        } finally {
            benchmark.tearDown();
        }
    }

    private static DescriptiveStatistics runTest(Benchmark benchmark) throws Exception {
        final DescriptiveStatistics statistics = new DescriptiveStatistics();
        long runtimeEnd = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        while (System.currentTimeMillis() < runtimeEnd) {
            statistics.addValue(execute(benchmark));
        }
        return statistics;
    }

    private static double execute(Benchmark benchmark) throws Exception {
        benchmark.beforeRun();
        try {
            long start = System.nanoTime();
            benchmark.run();
            return System.nanoTime() - start;
        } finally {
            benchmark.afterRun();
        }
    }

}
