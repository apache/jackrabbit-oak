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

import java.util.Map;

import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark;

/**
 * Interface for scalability suite for load testing.
 * 
 * {@link ScalabilitySuite} implementations would configure different {@link ScalabilityBenchmark}
 * implementations for executing performance tests and measuring the execution times on those tests.
 * 
 * The entry method for the starting the tests is {@link #run(Iterable)}.
 * 
 */
public interface ScalabilitySuite {
    /**
     * Adds the benchmarks to run.
     *
     * @param benchmarks
     * @return
     */
    ScalabilitySuite addBenchmarks(ScalabilityBenchmark... benchmarks);

    boolean removeBenchmark(String benchmark);
    
    void run(Iterable<RepositoryFixture> fixtures);

    Map<String, ScalabilityBenchmark> getBenchmarks();
}

