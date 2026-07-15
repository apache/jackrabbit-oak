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

import org.apache.jackrabbit.oak.benchmark.util.Date;
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
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityBlobSearchSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityNodeRelationshipSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityNodeSuite;

import java.util.Arrays;

public class LuceneScalabilityRunner extends ScalabilityRunner {

    public static void main(String[] args) throws Exception {
        initOptionSet(args);
        ScalabilityRunner.addToScalabilitySuiteList(
                Arrays.asList(
                        new ScalabilityBlobSearchSuite(scalabilityOptions.getWithStorage().value(options))
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
                        new ScalabilityNodeSuite(scalabilityOptions.getWithStorage().value(options))
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
                        new ScalabilityNodeRelationshipSuite(scalabilityOptions.getWithStorage().value(options))
                                .addBenchmarks(new AggregateNodeSearcher())
                )
        );

    }

}
