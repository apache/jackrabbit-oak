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

import java.io.File;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class BenchmarkRunner {

    private static final long MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> host = parser.accepts("host", "MongoDB host")
                .withRequiredArg().defaultsTo("localhost");
        OptionSpec<Integer> port = parser.accepts("port", "MongoDB port")
                .withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        OptionSpec<Integer> cache = parser.accepts("cache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
        OptionSpec<File> wikipedia =
                parser.accepts("wikipedia", "Wikipedia dump")
                .withRequiredArg().ofType(File.class);

        OptionSet options = parser.parse(args);
        RepositoryFixture[] allFixtures = new RepositoryFixture[] {
                new JackrabbitRepositoryFixture(),
                OakRepositoryFixture.getMemory(),
                OakRepositoryFixture.getDefault(),
                OakRepositoryFixture.getMongo(host.value(options), port.value(options)),
                OakRepositoryFixture.getSegment(
                        host.value(options), port.value(options),
                        cache.value(options) * MB)
        };
        Benchmark[] allBenchmarks = new Benchmark[] {
            new LoginTest(),
            new LoginLogoutTest(),
            new ReadPropertyTest(),
            new SetPropertyTest(),
            new SmallFileReadTest(),
            new SmallFileWriteTest(),
            new ConcurrentReadTest(),
            new ConcurrentReadWriteTest(),
            new SimpleSearchTest(),
            new SQL2SearchTest(),
            new DescendantSearchTest(),
            new SQL2DescendantSearchTest(),
            new CreateManyChildNodesTest(),
            new UpdateManyChildNodesTest(),
            new TransientManyChildNodesTest(),
            new WikipediaImport(wikipedia.value(options)),
            new ManyNodes(),
            ReadManyTest.linear("LinearReadEmpty", 1, ReadManyTest.EMPTY),
            ReadManyTest.linear("LinearReadFiles", 1, ReadManyTest.FILES),
            ReadManyTest.linear("LinearReadNodes", 1, ReadManyTest.NODES),
            ReadManyTest.uniform("UniformReadEmpty", 1, ReadManyTest.EMPTY),
            ReadManyTest.uniform("UniformReadFiles", 1, ReadManyTest.FILES),
            ReadManyTest.uniform("UniformReadNodes", 1, ReadManyTest.NODES),
        };

        Set<String> argset = Sets.newHashSet(options.nonOptionArguments());
        List<RepositoryFixture> fixtures = Lists.newArrayList();
        for (RepositoryFixture fixture : allFixtures) {
            if (argset.remove(fixture.toString())) {
                fixtures.add(fixture);
            }
        }

        List<Benchmark> benchmarks = Lists.newArrayList();
        for (Benchmark benchmark : allBenchmarks) {
            if (argset.remove(benchmark.toString())) {
                benchmarks.add(benchmark);
            }
        }

        if (argset.isEmpty()) {
            for (Benchmark benchmark : benchmarks) {
                benchmark.run(fixtures);
            }
        } else {
            System.err.println("Unknown arguments: " + argset);
        }
    }

}
