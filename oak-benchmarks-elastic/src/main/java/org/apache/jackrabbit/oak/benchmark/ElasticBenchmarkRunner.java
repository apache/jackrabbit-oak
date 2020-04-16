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


import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchConnection;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.util.Arrays;

public class ElasticBenchmarkRunner extends BenchmarkRunner {

    private static ElasticsearchConnection coordinate;

    public static void main(String[] args) throws Exception {
        initOptionSet(args);
        statsProvider = options.has(benchmarkOptions.getMetrics()) ? getStatsProvider() : StatisticsProvider.NOOP;
        // Create an Elastic Client Connection here and pass down to all the tests
        // And close the connection in this class itself
        // Can't rely on the tear down methods of the downstream Benchmark classes because they don't handle the case
        // where Exception could be thrown before the test execution is started - in that case if the connection is not closed here
        // we have orphaned HttpClient's I/O disp threads that don't let the process exit.

        try {
            coordinate = new ElasticsearchConnection(benchmarkOptions.getElasticScheme().value(options),
                    benchmarkOptions.getElasticHost().value(options), benchmarkOptions.getElasticPort().value(options), "Benchmark");

            BenchmarkRunner.addToBenchMarkList(
                    Arrays.asList(
                            new ElasticFullTextWithGlobalIndexSearchTest(benchmarkOptions.getWikipedia().value(options),
                                    benchmarkOptions.getFlatStructure().value(options),
                                    benchmarkOptions.getReport().value(options),
                                    benchmarkOptions.getWithStorage().value(options),
                                    coordinate),
                            new ElasticPropertyFTIndexedContentAvailability(benchmarkOptions.getWikipedia().value(options),
                                    benchmarkOptions.getFlatStructure().value(options),
                                    benchmarkOptions.getReport().value(options),
                                    benchmarkOptions.getWithStorage().value(options),
                                    coordinate),
                            new ElasticPropertyFTSeparatedIndexedContentAvailability(benchmarkOptions.getWikipedia().value(options),
                                    benchmarkOptions.getFlatStructure().value(options),
                                    benchmarkOptions.getReport().value(options),
                                    benchmarkOptions.getWithStorage().value(options),
                                    coordinate),
                            new ElasticFullTextWithoutGlobalIndexSearchTest(benchmarkOptions.getWikipedia().value(options),
                                    benchmarkOptions.getFlatStructure().value(options),
                                    benchmarkOptions.getReport().value(options),
                                    benchmarkOptions.getWithStorage().value(options),
                                    coordinate),
                            new ElasticPropertyTextSearchTest(benchmarkOptions.getWikipedia().value(options),
                                    benchmarkOptions.getFlatStructure().value(options),
                                    benchmarkOptions.getReport().value(options),
                                    benchmarkOptions.getWithStorage().value(options),
                                    coordinate)
                    )
            );
            BenchmarkRunner.main(args);
        } finally {
            coordinate.close();
        }

    }
}
