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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

public class BenchmarkRunner {

    private static final long MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> base = parser.accepts("base", "Base directory")
                .withRequiredArg().ofType(File.class)
                .defaultsTo(new File("target"));
        OptionSpec<String> host = parser.accepts("host", "MongoDB host")
                .withRequiredArg().defaultsTo("localhost");
        OptionSpec<Integer> port = parser.accepts("port", "MongoDB port")
                .withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        OptionSpec<Integer> cache = parser.accepts("cache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
        OptionSpec<File> wikipedia =
                parser.accepts("wikipedia", "Wikipedia dump")
                .withRequiredArg().ofType(File.class);
        OptionSpec<Boolean> runAsAdmin = parser.accepts("runAsAdmin", "Run test using admin session")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        OptionSpec<Integer> itemsToRead = parser.accepts("itemsToRead", "Number of items to read")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1000);
        OptionSpec<Integer> bgReaders = parser.accepts("bgReaders", "Number of background readers")
                .withRequiredArg().ofType(Integer.class).defaultsTo(20);


        OptionSet options = parser.parse(args);
        int cacheSize = cache.value(options);
        RepositoryFixture[] allFixtures = new RepositoryFixture[] {
                new JackrabbitRepositoryFixture(
                        base.value(options), cacheSize),
                OakRepositoryFixture.getMemory(cacheSize * MB),
                OakRepositoryFixture.getDefault(
                        base.value(options), cacheSize * MB),
                OakRepositoryFixture.getMongo(
                        host.value(options), port.value(options), cacheSize * MB),
                OakRepositoryFixture.getSegment(
                        host.value(options), port.value(options), cacheSize * MB),
                OakRepositoryFixture.getTar(base.value(options))
        };
        Benchmark[] allBenchmarks = new Benchmark[] {
            new LoginTest(),
            new LoginLogoutTest(),
            new NamespaceTest(),
            new ReadPropertyTest(),
            MessedUpCharacterCaseGetNodeTest.withAdmin(),
            MessedUpCharacterCaseGetNodeTest.withAnonymous(),
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
            new CreateNodesBenchmark(),
            new ManyNodes(),
            new ObservationTest(),
            new XmlImportTest(),
            new FlatTreeWithAceForSamePrincipalTest(),
            new ReadDeepTreeTest(runAsAdmin.value(options), itemsToRead.value(options)),
            new ConcurrentReadAccessControlledTreeTest(runAsAdmin.value(options), itemsToRead.value(options), bgReaders.value(options)),
            new ConcurrentReadDeepTreeTest(runAsAdmin.value(options), itemsToRead.value(options), bgReaders.value(options)),
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
