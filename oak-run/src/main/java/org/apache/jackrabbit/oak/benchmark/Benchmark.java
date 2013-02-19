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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.SimpleCredentials;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class Benchmark {

    private static final Map<String, RepositoryFixture> FIXTURES =
            ImmutableMap.<String, RepositoryFixture>builder()
            .put("Jackrabbit", new JackrabbitRepositoryFixture())
            .put("Oak-Memory", new OakRepositoryFixture())
            .build();

    private static final Map<String, AbstractTest> TESTS =
            ImmutableMap.<String, AbstractTest>builder()
            .put("Login",           new LoginTest())
            .put("LoginLogout",     new LoginLogoutTest())
            .put("GetProperty",     new ReadPropertyTest())
            .put("SetProperty",     new SetPropertyTest())
            .put("SmallRead",       new SmallFileReadTest())
            .put("SmallWrite",      new SmallFileWriteTest())
            .put("ConcurrentRead",  new ConcurrentReadTest())
            .put("ConcurrentWrite", new ConcurrentReadWriteTest())
            .put("SimpleSearch",    new SimpleSearchTest())
            .put("SQL2",            new SQL2SearchTest())
            .put("Descendant",      new DescendantSearchTest())
            .put("SQL2Descendant",  new SQL2DescendantSearchTest())
            .put("CreateFlatNode",  new CreateManyChildNodesTest())
            .put("UpdateFlatNode", new UpdateManyChildNodesTest())
            .put("TransientSpace", new TransientManyChildNodesTest())
            .build();

    public static void main(String[] args) throws Exception {
        Map<String, RepositoryFixture> fixtures = Maps.newLinkedHashMap();
        Map<String, AbstractTest> tests = Maps.newLinkedHashMap();
        for (String name : args) {
            if (FIXTURES.containsKey(name)) {
                fixtures.put(name, FIXTURES.get(name));
            } else if (TESTS.containsKey(name)) {
                tests.put(name, TESTS.get(name));
            } else {
                throw new RuntimeException("Unknown argument: " + name);
            }
        }
        if (fixtures.isEmpty()) {
            fixtures.putAll(FIXTURES);
        }
        if (tests.isEmpty()) {
            tests.putAll(TESTS);
        }

        for (Map.Entry<String, AbstractTest> test : tests.entrySet()) {
            System.out.format(
                    "# %-34.34s     min     10%%     50%%     90%%     max%n",
                    test.getKey());
            for (Map.Entry<String, RepositoryFixture> fixture : fixtures.entrySet()) {
                Repository[] cluster = new Repository[1];
                fixture.getValue().setUpCluster(cluster);
                try {
                    // Run the test
                    DescriptiveStatistics statistics =
                            runTest(test.getValue(), cluster[0]);
                    if (statistics.getN() > 0) {
                        System.out.format(
                                "%-36.36s  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f%n",
                                fixture.getKey(),
                                statistics.getMin(),
                                statistics.getPercentile(10.0),
                                statistics.getPercentile(50.0),
                                statistics.getPercentile(90.0), statistics.getMax());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    fixture.getValue().tearDownCluster(cluster);
                }
            }
        }
    }

    private static final Credentials CREDENTIALS =
            new SimpleCredentials("admin", "admin".toCharArray());

    private static DescriptiveStatistics runTest(AbstractTest test,
            Repository repository) throws Exception {
        DescriptiveStatistics statistics = new DescriptiveStatistics();

        test.setUp(repository, CREDENTIALS);
        try {
            // Run a few iterations to warm up the system
            for (int i = 0; i < 5; i++) {
                test.execute();
            }

            // Run test iterations, and capture the execution times
            int iterations = 0;
            long runtimeEnd =
                    System.currentTimeMillis()
                    + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
            while (iterations++ < 10
                    || System.currentTimeMillis() < runtimeEnd) {
                statistics.addValue(test.execute());
            }
        } finally {
            test.tearDown();
        }

        return statistics;
    }

}
