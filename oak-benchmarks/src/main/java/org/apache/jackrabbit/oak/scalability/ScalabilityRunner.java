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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.guava.common.base.Splitter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.benchmark.CSVResultGenerator;
import org.apache.jackrabbit.oak.fixture.JackrabbitRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.scalability.benchmarks.segment.standby.StandbyBulkTransferBenchmark;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityStandbySuite;
import org.apache.jackrabbit.oak.segment.Segment;

/**
 * Main class for running scalability/longevity tests.
 * 
 */
public class ScalabilityRunner {

    private static final long MB = 1024 * 1024L;

    protected static List<ScalabilitySuite> allSuites = new ArrayList<>();
    private static OptionParser parser = new OptionParser();
    protected static ScalabilityOptions scalabilityOptions = null;
    protected static OptionSet options;
    private static boolean initFlag = false;

    public static void main(String[] args) throws Exception {
        initOptionSet(args);
        OptionSet options = parser.parse(args);

        if (options.has(scalabilityOptions.getHelp())) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        int cacheSize = scalabilityOptions.getCache().value(options);
        RepositoryFixture[] allFixtures = new RepositoryFixture[] {
                new JackrabbitRepositoryFixture(scalabilityOptions.getBase().value(options), cacheSize),
                OakRepositoryFixture.getMemoryNS(cacheSize * MB),
                OakRepositoryFixture.getMongo(
                        scalabilityOptions.getHost().value(options),
                        scalabilityOptions.getPort().value(options),
                        scalabilityOptions.getDbName().value(options),
                        scalabilityOptions.getDropDBAfterTest().value(options),
                        cacheSize * MB,
                        scalabilityOptions.isThrottlingEnabled().value(options)),
                OakRepositoryFixture.getMongoWithDS(
                        scalabilityOptions.getHost().value(options),
                        scalabilityOptions.getPort().value(options),
                        scalabilityOptions.getDbName().value(options),
                        scalabilityOptions.getDropDBAfterTest().value(options),
                        cacheSize * MB,
                        scalabilityOptions.getBase().value(options),
                        scalabilityOptions.getFdsCache().value(options),
                        scalabilityOptions.isThrottlingEnabled().value(options)),
                OakRepositoryFixture.getMongoNS(
                        scalabilityOptions.getHost().value(options),
                        scalabilityOptions.getPort().value(options),
                        scalabilityOptions.getDbName().value(options),
                        scalabilityOptions.getDropDBAfterTest().value(options),
                    cacheSize * MB,
                        scalabilityOptions.isThrottlingEnabled().value(options)),
                OakRepositoryFixture.getSegmentTar(
                        scalabilityOptions.getBase().value(options), 256, cacheSize,
                        scalabilityOptions.getMmap().value(options), Segment.MEDIUM_LIMIT),
                OakRepositoryFixture.getSegmentTarWithDataStore(scalabilityOptions.getBase().value(options),
                        256, cacheSize,
                        scalabilityOptions.getMmap().value(options), Segment.MEDIUM_LIMIT,
                        scalabilityOptions.getFdsCache().value(options)),
                OakRepositoryFixture.getSegmentTarWithColdStandby(scalabilityOptions.getBase().value(options), 256, cacheSize,
                        scalabilityOptions.getMmap().value(options),
                        Segment.MEDIUM_LIMIT,
                        scalabilityOptions.getColdUseDataStore().value(options),
                        scalabilityOptions.getFdsCache().value(options),
                        scalabilityOptions.getColdSyncInterval().value(options),
                        scalabilityOptions.getColdShareDataStore().value(options),
                        scalabilityOptions.getColdSecure().value(options),
                        scalabilityOptions.getColdOneShotRun().value(options)),
                OakRepositoryFixture.getRDB(scalabilityOptions.getRdbjdbcuri().value(options),
                        scalabilityOptions.getRdbjdbcuser().value(options),
                        scalabilityOptions.getRdbjdbcpasswd().value(options),
                        scalabilityOptions.getRdbjdbctableprefix().value(options),
                        scalabilityOptions.getDropDBAfterTest().value(options), cacheSize * MB, -1),
                OakRepositoryFixture.getRDBWithDS(scalabilityOptions.getRdbjdbcuri().value(options),
                        scalabilityOptions.getRdbjdbcuser().value(options),
                        scalabilityOptions.getRdbjdbcpasswd().value(options),
                        scalabilityOptions.getRdbjdbctableprefix().value(options),
                        scalabilityOptions.getDropDBAfterTest().value(options), cacheSize * MB,
                        scalabilityOptions.getBase().value(options),
                        scalabilityOptions.getFdsCache().value(options), -1)
        };

        addToScalabilitySuiteList(
                Arrays.asList(
                        new ScalabilityStandbySuite()
                                .addBenchmarks(new StandbyBulkTransferBenchmark()
                                )
                ));

        Set<String> argset = new HashSet<>(scalabilityOptions.getNonOption().values(options));
        List<RepositoryFixture> fixtures = new ArrayList<>();
        for (RepositoryFixture fixture : allFixtures) {
            if (argset.remove(fixture.toString())) {
                fixtures.add(fixture);
            }
        }
        
        Map<String, List<String>> argmap = new HashMap<>();
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

        List<ScalabilitySuite> suites = new ArrayList<>();
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
            if (options.has(scalabilityOptions.getCsvFile())) {
                out =
                    new PrintStream(FileUtils.openOutputStream(scalabilityOptions.getCsvFile().value(options), true), false,
                                            StandardCharsets.UTF_8);
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

    protected static void addToScalabilitySuiteList(List<ScalabilitySuite> suites) {
        allSuites.addAll(suites);
    }

    protected static void initOptionSet(String[] args) throws IOException {
        if(!initFlag) {
            scalabilityOptions = new ScalabilityOptions(parser);
            options = parser.parse(args);
            initFlag = true;
        }
    }

}
