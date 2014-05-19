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
package org.apache.jackrabbit.oak.scalability;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.CSVResultGenerator;
import org.apache.jackrabbit.oak.benchmark.util.Date;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Main class for running scalability/longevity tests.
 * 
 */
public class ScalabilityRunner {

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
        OptionSpec<Boolean> dropDBAfterTest =
                parser.accepts("dropDBAfterTest",
                        "Whether to drop the MongoDB database after the test")
                        .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        OptionSpec<Boolean> mmap = parser.accepts("mmap", "TarMK memory mapping")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo("64".equals(System.getProperty("sun.arch.data.model")));
        OptionSpec<Integer> cache = parser.accepts("cache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
        OptionSpec<Integer> fdsCache = parser.accepts("blobCache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(32);
        OptionSpec<Boolean> withStorage = parser
                .accepts("storage", "Index storage enabled").withOptionalArg()
                .ofType(Boolean.class);
        OptionSpec<File> csvFile =
                parser.accepts("csvFile", "File to write a CSV version of the benchmark data.")
                        .withOptionalArg().ofType(File.class);
        OptionSpec<File> dumpFile =
                parser.accepts("dumpFile", "File to write threa dumps.")
                        .withOptionalArg().ofType(File.class);

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
        ScalabilitySuite[] allSuites = new ScalabilitySuite[] {
                new ScalabilityBlobSearchSuite(withStorage.value(options))
                    .addBenchmarks(new FullTextSearcher(), 
                                    new NodeTypeSearcher(),
                                    new FormatSearcher(),
                                    new LastModifiedSearcher(Date.LAST_2_HRS),
                                    new LastModifiedSearcher(Date.LAST_24_HRS),
                                    new LastModifiedSearcher(Date.LAST_7_DAYS),
                                    new LastModifiedSearcher(Date.LAST_MONTH),
                                    new LastModifiedSearcher(Date.LAST_YEAR))
        };

        Set<String> argset = Sets.newHashSet(options.nonOptionArguments());
        List<RepositoryFixture> fixtures = Lists.newArrayList();
        for (RepositoryFixture fixture : allFixtures) {
            if (argset.remove(fixture.toString())) {
                fixtures.add(fixture);
            }
        }
        
        Map<String, List<String>> argmap = Maps.newHashMap();
        // Split the args to get suites and benchmarks (i.e. suite:benchmark1,benchmark2)
        for(String arg : argset) {
            String[] tokens = arg.split(":");
            if (tokens.length > 1) {
                argmap.put(tokens[0], Splitter.on(",").trimResults().splitToList(tokens[1]));
            } else {
                argmap.put(tokens[0], null);
            }
            argset.remove(arg);
        }
        
        List<ScalabilitySuite> suites = Lists.newArrayList();
        for (ScalabilitySuite suite : allSuites) {
            if (argmap.containsKey(suite.toString())) {
                List<String> benchmarks = argmap.get(suite.toString());
                // Only keep requested benchmarks
                if (benchmarks != null) {
                    for (ScalabilityBenchmark availableBenchmark : suite.getBenchmarks().values()) {
                        if (!benchmarks.contains(availableBenchmark.toString())) {
                            suite.removeBenchmark(availableBenchmark.toString());
                        }
                    }
                }
                suites.add(suite);
                argmap.remove(suite.toString());
            }
        }

        if (argmap.isEmpty()) {
            PrintStream out = null;
            if (options.has(csvFile)) {
                out = new PrintStream(FileUtils.openOutputStream(csvFile.value(options), true));
            }
            for (ScalabilitySuite suite : suites) {
                if (suite instanceof CSVResultGenerator) {
                    ((CSVResultGenerator) suite).setPrintStream(out);
                }
                suite.run(fixtures);
            }
            if (out != null) {
                out.close();
            }
        } else {
            System.err.println("Unknown arguments: " + argset);
        }
    }
}

