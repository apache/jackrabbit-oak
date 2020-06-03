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


import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.util.Arrays;

public class LuceneBenchmarkRunner extends BenchmarkRunner {

    public static void main(String[] args) throws Exception {
        initOptionSet(args);
        statsProvider = options.has(benchmarkOptions.getMetrics()) ? getStatsProvider() : StatisticsProvider.NOOP;
        BenchmarkRunner.addToBenchMarkList(
                Arrays.asList(
                        new LuceneFullTextWithGlobalIndexSearchTest(
                                benchmarkOptions.getWikipedia().value(options),
                                benchmarkOptions.getFlatStructure().value(options),
                                benchmarkOptions.getReport().value(options),
                                benchmarkOptions.getWithStorage().value(options)),
                        new LucenePropertyFTIndexedContentAvailability(
                                benchmarkOptions.getWikipedia().value(options),
                                benchmarkOptions.getFlatStructure().value(options),
                                benchmarkOptions.getReport().value(options), benchmarkOptions.getWithStorage().value(options)),
                        new LucenePropertyFTSeparatedIndexedContentAvailability(
                                benchmarkOptions.getWikipedia().value(options),
                                benchmarkOptions.getFlatStructure().value(options),
                                benchmarkOptions.getReport().value(options), benchmarkOptions.getWithStorage().value(options)),
                        new HybridIndexTest(benchmarkOptions.getBase().value(options), statsProvider),
                        new LuceneFullTextWithoutGlobalIndexSearchTest(benchmarkOptions.getWikipedia().value(options),
                                benchmarkOptions.getFlatStructure().value(options),
                                benchmarkOptions.getReport().value(options),
                                benchmarkOptions.getWithStorage().value(options)),
                        new LucenePropertySearchTest(benchmarkOptions.getWikipedia().value(options),
                                benchmarkOptions.getFlatStructure().value(options),
                                benchmarkOptions.getReport().value(options),
                                benchmarkOptions.getWithStorage().value(options)),
                        new LuceneFacetSearchTest(benchmarkOptions.getWithStorage().value(options)),
                        new LuceneInsecureFacetSearchTest(benchmarkOptions.getWithStorage().value(options)),
                        new LuceneStatisticalFacetSearchTest(benchmarkOptions.getWithStorage().value(options))
                )
        );

        BenchmarkRunner.main(args);
    }
}
