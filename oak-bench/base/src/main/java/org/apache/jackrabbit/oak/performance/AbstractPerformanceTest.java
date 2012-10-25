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
package org.apache.jackrabbit.oak.performance;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Pattern;
import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.jcr.Jcr;

/**
 * This class calls all known performance tests.
 */
public abstract class AbstractPerformanceTest {

    /**
     * The warmup time, in ms.
     */
    private final int warmup = Integer.getInteger("oak.performanceTest.warmup", 0);

    /**
     * How long each test is repeated, in ms.
     */
    private final int runtime = Integer.getInteger("oak.performanceTest.runtime", 100);

    private final Credentials credentials = new SimpleCredentials("admin",
            "admin".toCharArray());

    private final Pattern microKernelPattern = Pattern.compile(System
            .getProperty("mk", ".*"));
    private final Pattern testPattern = Pattern.compile(System.getProperty(
            "only", ".*"));

    protected void testPerformance(String name, String microKernel)
            throws Exception {

        runTest(new LoginTest(), name, microKernel);
        runTest(new LoginLogoutTest(), name, microKernel);
        runTest(new ReadPropertyTest(), name, microKernel);
        runTest(new SetPropertyTest(), name, microKernel);
        runTest(new SmallFileReadTest(), name, microKernel);
        runTest(new SmallFileWriteTest(), name, microKernel);
        runTest(new ConcurrentReadTest(), name, microKernel);
        runTest(new ConcurrentReadWriteTest(), name, microKernel);
        runTest(new SimpleSearchTest(), name, microKernel);
        runTest(new SQL2SearchTest(), name, microKernel);
        runTest(new DescendantSearchTest(), name, microKernel);
        runTest(new SQL2DescendantSearchTest(), name, microKernel);
        runTest(new CreateManyChildNodesTest(), name, microKernel);
        runTest(new UpdateManyChildNodesTest(), name, microKernel);
        runTest(new TransientManyChildNodesTest(), name, microKernel);

    }

    private void runTest(AbstractTest test, String name, String microKernel) {
        if (microKernelPattern.matcher(microKernel).matches()
                && testPattern.matcher(test.toString()).matches()) {

            MicroKernel mk = createMicroKernel(microKernel);
            try {
                Repository repository= createRepository(mk);

                // Run the test
                DescriptiveStatistics statistics = runTest(test, repository);
                if (statistics.getN() > 0) {
                    writeReport(test.toString(), name, microKernel, statistics);
                }
            } catch (RepositoryException re) {
                re.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                disposeMicroKernel(mk);
            }
        }
    }

    private DescriptiveStatistics runTest(AbstractTest test,
            Repository repository) throws Exception {
        DescriptiveStatistics statistics = new DescriptiveStatistics();

        test.setUp(repository, credentials);
        try {
            // Run a few iterations to warm up the system
            if (warmup > 0) {
                long warmupEnd = System.currentTimeMillis() + warmup;
                while (System.currentTimeMillis() < warmupEnd) {
                    test.execute();
                }
            }

            // Run test iterations, and capture the execution times
            long runtimeEnd = System.currentTimeMillis() + runtime;
            while (System.currentTimeMillis() < runtimeEnd) {
                statistics.addValue(test.execute());
            }
        } finally {
            test.tearDown();
        }

        return statistics;
    }

    private static void writeReport(String test, String name, String microKernel,
            DescriptiveStatistics statistics) throws IOException {
        File report = new File("target", test + "-" + microKernel + ".txt");

        boolean needsPrefix = !report.exists();
        PrintWriter writer = new PrintWriter(new FileWriterWithEncoding(report,
                "UTF-8", true));
        try {
            if (needsPrefix) {
                writer.format(
                        "# %-34.34s     min     10%%     50%%     90%%     max%n",
                        test);
            }

            writer.format("%-36.36s  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f%n",
                    name, statistics.getMin(), statistics.getPercentile(10.0),
                    statistics.getPercentile(50.0),
                    statistics.getPercentile(90.0), statistics.getMax());
        } finally {
            writer.close();
        }
    }

    protected MicroKernel createMicroKernel(String microKernel) {

        // TODO: depending on the microKernel string a particular repository
        // with that MK must be returned

        return new MicroKernelImpl("target/mk-tck-" + System.currentTimeMillis());

    }

    protected void disposeMicroKernel(MicroKernel kernel) {
        ((MicroKernelImpl) kernel).dispose();
    }

    protected Repository createRepository(MicroKernel mk) {
        return new Jcr(mk).createRepository();
    }

}
