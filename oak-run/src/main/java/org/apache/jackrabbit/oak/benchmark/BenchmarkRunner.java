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
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

public class BenchmarkRunner {

    private static final int MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> base = parser.accepts("base", "Base directory")
                .withRequiredArg().ofType(File.class)
                .defaultsTo(new File("target"));
        OptionSpec<String> host = parser.accepts("host", "MongoDB host")
                .withRequiredArg().defaultsTo("localhost");
        OptionSpec<Integer> port = parser.accepts("port", "MongoDB port")
                .withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        OptionSpec<String> dbName = parser.accepts("db", "MongoDB database")
                .withRequiredArg();
        OptionSpec<Boolean> dropDBAfterTest = parser.accepts("dropDBAfterTest", "Whether to drop the MongoDB database after the test")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        OptionSpec<Boolean> mmap = parser.accepts("mmap", "TarMK memory mapping")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo("64".equals(System.getProperty("sun.arch.data.model")));
        OptionSpec<Integer> cache = parser.accepts("cache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
        OptionSpec<Integer> fdsCache = parser.accepts("blobCache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(32);
        OptionSpec<File> wikipedia = parser
                .accepts("wikipedia", "Wikipedia dump").withRequiredArg()
                .ofType(File.class);
        OptionSpec<Boolean> withStorage = parser
                .accepts("storage", "Index storage enabled").withOptionalArg()
                .ofType(Boolean.class);
        OptionSpec<Boolean> runAsAdmin = parser.accepts("runAsAdmin", "Run test using admin session")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        OptionSpec<Integer> itemsToRead = parser.accepts("itemsToRead", "Number of items to read")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1000);
        OptionSpec<Integer> concurrency = parser.accepts("concurrency", "Number of test threads.")
                .withRequiredArg().ofType(Integer.class).withValuesSeparatedBy(',');
        OptionSpec<Boolean> report = parser.accepts("report", "Whether to output intermediate results")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<Boolean> randomUser = parser.accepts("randomUser", "Whether to use a random user to read.")
                        .withOptionalArg().ofType(Boolean.class)
                        .defaultsTo(Boolean.FALSE);
        OptionSpec<File> csvFile = parser.accepts("csvFile", "File to write a CSV version of the benchmark data.")
                .withOptionalArg().ofType(File.class);
        OptionSpec<Boolean> flatStructure = parser.accepts("flatStructure", "Whether the test should use a flat structure or not.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        OptionSpec<Integer> numberOfUsers = parser.accepts("numberOfUsers")
                .withOptionalArg().ofType(Integer.class).defaultsTo(10000);

        OptionSet options = parser.parse(args);
        int cacheSize = cache.value(options);
        RepositoryFixture[] allFixtures = new RepositoryFixture[] {
                new JackrabbitRepositoryFixture(base.value(options), cacheSize),
                OakRepositoryFixture.getMemory(cacheSize * MB),
                OakRepositoryFixture.getMemoryNS(cacheSize * MB),
                OakRepositoryFixture.getMemoryMK(cacheSize * MB),
                OakRepositoryFixture.getH2MK(base.value(options), cacheSize * MB),
                OakRepositoryFixture.getMongo(
                        host.value(options), port.value(options),
                        dbName.value(options), dropDBAfterTest.value(options),
                        cacheSize * MB),
                OakRepositoryFixture.getMongoWithFDS(
                        host.value(options), port.value(options),
                        dbName.value(options), dropDBAfterTest.value(options),
                        cacheSize * MB,
                        base.value(options),
                        fdsCache.value(options)),
                OakRepositoryFixture.getMongoNS(
                        host.value(options), port.value(options),
                        dbName.value(options), dropDBAfterTest.value(options),
                        cacheSize * MB),
                OakRepositoryFixture.getMongoMK(
                        host.value(options), port.value(options),
                        dbName.value(options), dropDBAfterTest.value(options),
                        cacheSize * MB),
                OakRepositoryFixture.getTar(
                        base.value(options), 256, cacheSize, mmap.value(options)),
                OakRepositoryFixture.getTarWithBlobStore(
                        base.value(options), 256, cacheSize, mmap.value(options))
        };
        Benchmark[] allBenchmarks = new Benchmark[] {
            new OrderedIndexQueryOrderedIndexTest(),
            new OrderedIndexQueryStandardIndexTest(),
            new OrderedIndexQueryNoIndexTest(),
            new OrderedIndexInsertOrderedPropertyTest(),
            new OrderedIndexInsertStandardPropertyTest(),
            new OrderedIndexInsertNoIndexTest(),
            new LoginTest(),
            new LoginLogoutTest(),
            new NamespaceTest(),
            new NamespaceRegistryTest(),
            new ReadPropertyTest(),
            GetNodeTest.withAdmin(),
            GetNodeTest.withAnonymous(),
            new GetDeepNodeTest(),
            new SetPropertyTest(),
            new SetMultiPropertyTest(),
            new SmallFileReadTest(),
            new SmallFileWriteTest(),
            new ConcurrentReadTest(),
            new ConcurrentReadWriteTest(),
            new ConcurrentWriteReadTest(),
            new ConcurrentWriteTest(),
            new SimpleSearchTest(),
            new SQL2SearchTest(),
            new DescendantSearchTest(),
            new SQL2DescendantSearchTest(),
            new CreateManyChildNodesTest(),
            new CreateManyNodesTest(),
            new UpdateManyChildNodesTest(),
            new TransientManyChildNodesTest(),
            new WikipediaImport(
                    wikipedia.value(options),
                    flatStructure.value(options),
                    report.value(options)),
            new CreateNodesBenchmark(),
            new ManyNodes(),
            new ObservationTest(),
            new XmlImportTest(),
            new FlatTreeWithAceForSamePrincipalTest(),
            new ReadDeepTreeTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadDeepTreeTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadSinglePolicyTreeTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadAccessControlledTreeTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadAccessControlledTreeTest2(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentReadRandomNodeAndItsPropertiesTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentHasPermissionTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ConcurrentHasPermissionTest2(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options)),
            new ManyUserReadTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options),
                    randomUser.value(options)),
            new ConcurrentTraversalTest(
                    runAsAdmin.value(options),
                    itemsToRead.value(options),
                    report.value(options),
                    randomUser.value(options)),
            new ConcurrentWriteACLTest(itemsToRead.value(options)),
            new ConcurrentEveryoneACLTest(runAsAdmin.value(options), itemsToRead.value(options)),
            ReadManyTest.linear("LinearReadEmpty", 1, ReadManyTest.EMPTY),
            ReadManyTest.linear("LinearReadFiles", 1, ReadManyTest.FILES),
            ReadManyTest.linear("LinearReadNodes", 1, ReadManyTest.NODES),
            ReadManyTest.uniform("UniformReadEmpty", 1, ReadManyTest.EMPTY),
            ReadManyTest.uniform("UniformReadFiles", 1, ReadManyTest.FILES),
            ReadManyTest.uniform("UniformReadNodes", 1, ReadManyTest.NODES),
            new ConcurrentCreateNodesTest(),
            new SequentialCreateNodesTest(),
            new CreateManyIndexedNodesTest(),
            new GetPoliciesTest(),
            new ConcurrentFileWriteTest(),
            new GetAuthorizableByIdTest(
                    numberOfUsers.value(options),
                    flatStructure.value(options)),
            new GetAuthorizableByPrincipalTest(
                    numberOfUsers.value(options),
                    flatStructure.value(options)),
            new GetPrincipalTest(
                    numberOfUsers.value(options),
                    flatStructure.value(options)),
            new FullTextSearchTest(
                    wikipedia.value(options),
                    flatStructure.value(options),
                    report.value(options), withStorage.value(options))
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
            PrintStream out = null;
            if (options.has(csvFile)) {
                out = new PrintStream(FileUtils.openOutputStream(csvFile.value(options), true));
            }
            for (Benchmark benchmark : benchmarks) {
                if (benchmark instanceof CSVResultGenerator) {
                    ((CSVResultGenerator) benchmark).setPrintStream(out);
                }
                benchmark.run(fixtures, options.valuesOf(concurrency));
            }
            if (out != null) {
                out.close();
            }
        } else {
            System.err.println("Unknown arguments: " + argset);
        }
    }

}
