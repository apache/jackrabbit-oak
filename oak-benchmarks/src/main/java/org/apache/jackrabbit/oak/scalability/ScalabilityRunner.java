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

import static java.util.Arrays.asList;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.CSVResultGenerator;
import org.apache.jackrabbit.oak.benchmark.util.Date;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.AggregateNodeSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.ConcurrentReader;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.ConcurrentWriter;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.FacetSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.FormatSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.FullTextSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.LastModifiedSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.MultiFilterOrderByKeysetPageSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.MultiFilterOrderByOffsetPageSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.MultiFilterOrderBySearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.MultiFilterSplitOrderByKeysetPageSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.MultiFilterSplitOrderByOffsetPageSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.MultiFilterSplitOrderBySearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.NodeTypeSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.OrderByDate;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.OrderByKeysetPageSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.OrderByOffsetPageSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.OrderBySearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.SplitOrderByKeysetPageSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.SplitOrderByOffsetPageSearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.search.SplitOrderBySearcher;
import org.apache.jackrabbit.oak.scalability.benchmarks.segment.standby.StandbyBulkTransferBenchmark;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityBlobSearchSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityNodeRelationshipSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityNodeSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityStandbySuite;

/**
 * Main class for running scalability/longevity tests.
 * 
 */
public class ScalabilityRunner {

    private static final long MB = 1024 * 1024L;

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
        OptionSpec<String> rdbjdbcuri = parser.accepts("rdbjdbcuri", "RDB JDBC URI")
            .withOptionalArg().defaultsTo("jdbc:h2:./target/benchmark");
        OptionSpec<String> rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user")
            .withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password")
            .withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbctableprefix = parser.accepts("rdbjdbctableprefix", "RDB JDBC table prefix")
            .withOptionalArg().defaultsTo("");
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
        OptionSpec<Integer> coldSyncInterval = parser.accepts("coldSyncInterval", "interval between sync cycles in sec (Segment-Tar-Cold only)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(5);
        OptionSpec<Boolean> coldUseDataStore = parser
                .accepts("useDataStore",
                        "Whether to use a datastore in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.TRUE);
        OptionSpec<Boolean> coldShareDataStore = parser
                .accepts("shareDataStore",
                        "Whether to share the datastore for primary and standby in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        OptionSpec<Boolean> coldOneShotRun = parser
                .accepts("oneShotRun",
                        "Whether to do a continuous sync between client and server or sync only once (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.TRUE);
        OptionSpec<Boolean> coldSecure = parser
                .accepts("secure",
                        "Whether to enable secure communication between primary and standby in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        
        OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        OptionSpec<String> nonOption = parser.nonOptions();

        OptionSet options = parser.parse(args);

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        int cacheSize = cache.value(options);
        RepositoryFixture[] allFixtures = new RepositoryFixture[] {
                new JackrabbitRepositoryFixture(base.value(options), cacheSize),
                OakRepositoryFixture.getMemoryNS(cacheSize * MB),
                OakRepositoryFixture.getMongo(
                    host.value(options), port.value(options),
                    dbName.value(options), dropDBAfterTest.value(options),
                    cacheSize * MB),
                OakRepositoryFixture.getMongoWithDS(
                    host.value(options), port.value(options),
                    dbName.value(options), dropDBAfterTest.value(options),
                    cacheSize * MB,
                    base.value(options),
                    fdsCache.value(options)),
                OakRepositoryFixture.getMongoNS(
                    host.value(options), port.value(options),
                    dbName.value(options), dropDBAfterTest.value(options),
                    cacheSize * MB),
                OakRepositoryFixture.getSegmentTar(
                    base.value(options), 256, cacheSize, mmap.value(options)),
                OakRepositoryFixture.getSegmentTarWithDataStore(base.value(options), 256, cacheSize,
                    mmap.value(options), fdsCache.value(options)),
                OakRepositoryFixture.getSegmentTarWithColdStandby(base.value(options), 256, cacheSize,
                        mmap.value(options), coldUseDataStore.value(options), fdsCache.value(options), 
                        coldSyncInterval.value(options), coldShareDataStore.value(options), coldSecure.value(options), 
                        coldOneShotRun.value(options)),
                OakRepositoryFixture.getRDB(rdbjdbcuri.value(options), rdbjdbcuser.value(options),
                    rdbjdbcpasswd.value(options), rdbjdbctableprefix.value(options),
                    dropDBAfterTest.value(options), cacheSize * MB, -1),
                OakRepositoryFixture.getRDBWithDS(rdbjdbcuri.value(options), rdbjdbcuser.value(options),
                    rdbjdbcpasswd.value(options), rdbjdbctableprefix.value(options),
                    dropDBAfterTest.value(options), cacheSize * MB, base.value(options),
                    fdsCache.value(options), -1)
        };
        ScalabilitySuite[] allSuites =
                new ScalabilitySuite[] {
                        new ScalabilityBlobSearchSuite(withStorage.value(options))
                                .addBenchmarks(new FullTextSearcher(),
                                        new NodeTypeSearcher(),
                                        new FormatSearcher(),
                                        new FacetSearcher(),
                                        new LastModifiedSearcher(Date.LAST_2_HRS),
                                        new LastModifiedSearcher(Date.LAST_24_HRS),
                                        new LastModifiedSearcher(Date.LAST_7_DAYS),
                                        new LastModifiedSearcher(Date.LAST_MONTH),
                                        new LastModifiedSearcher(Date.LAST_YEAR),
                                        new OrderByDate()),
                        new ScalabilityNodeSuite(withStorage.value(options))
                                .addBenchmarks(new OrderBySearcher(),
                                        new SplitOrderBySearcher(),
                                        new OrderByOffsetPageSearcher(),
                                        new SplitOrderByOffsetPageSearcher(),
                                        new OrderByKeysetPageSearcher(),
                                        new SplitOrderByKeysetPageSearcher(),
                                        new MultiFilterOrderBySearcher(),
                                        new MultiFilterSplitOrderBySearcher(),
                                        new MultiFilterOrderByOffsetPageSearcher(),
                                        new MultiFilterSplitOrderByOffsetPageSearcher(),
                                        new MultiFilterOrderByKeysetPageSearcher(),
                                        new MultiFilterSplitOrderByKeysetPageSearcher(),
                                        new ConcurrentReader(),
                                        new ConcurrentWriter()),
                        new ScalabilityNodeRelationshipSuite(withStorage.value(options))
                                .addBenchmarks(new AggregateNodeSearcher()),
                        new ScalabilityStandbySuite()
                                .addBenchmarks(new StandbyBulkTransferBenchmark())
                };

        Set<String> argset = Sets.newHashSet(nonOption.values(options));
        List<RepositoryFixture> fixtures = Lists.newArrayList();
        for (RepositoryFixture fixture : allFixtures) {
            if (argset.remove(fixture.toString())) {
                fixtures.add(fixture);
            }
        }
        
        Map<String, List<String>> argmap = Maps.newHashMap();
        // Split the args to get suites and benchmarks (i.e. suite:benchmark1,benchmark2)
        for(String arg : argset) {
            List<String> tokens = Splitter.on(":").limit(2).splitToList(arg);
            if (tokens.size() > 1) {
                argmap.put(tokens.get(0), Splitter.on(",").trimResults().splitToList(tokens.get(1)));
            } else {
                argmap.put(tokens.get(0), null);
            }
            argset.remove(arg);
        }

        if (argmap.isEmpty()) {
            System.err.println("Warning: no scalability suites specified, " +
                "supported  are: " + Arrays.asList(allSuites));
        }

        List<ScalabilitySuite> suites = Lists.newArrayList();
        for (ScalabilitySuite suite : allSuites) {
            if (argmap.containsKey(suite.toString())) {
                List<String> benchmarks = argmap.get(suite.toString());
                // Only keep requested benchmarks
                if (benchmarks != null) {
                    Iterator<String> iter = suite.getBenchmarks().keySet().iterator();
                    for (;iter.hasNext();) {
                        String availBenchmark = iter.next();
                        if (!benchmarks.contains(availBenchmark)) {
                            iter.remove();
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
                out =
                    new PrintStream(FileUtils.openOutputStream(csvFile.value(options), true), false,
                                            Charsets.UTF_8.name());
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
