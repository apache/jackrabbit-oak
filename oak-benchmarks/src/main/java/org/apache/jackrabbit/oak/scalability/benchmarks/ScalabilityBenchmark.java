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
package org.apache.jackrabbit.oak.scalability.benchmarks;

import javax.jcr.Credentials;
import javax.jcr.Repository;

import org.apache.jackrabbit.oak.scalability.ScalabilitySuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;


/**
 * Base class for all the Scalability/Longevity benchmarks/tests.
 * 
 * The implementations should implement the method
 * {@link ScalabilityBenchmark#execute(Repository, Credentials, ExecutionContext)}.
 * 
 * This method will then be called from the {@link ScalabilitySuite} where configured.
 * 
 */
public abstract class ScalabilityBenchmark {

    /**
     * Runs the benchmark against the given repository.
     *
     * @param repository the repository instance
     * @param credentials the credentials
     * @param context the execution context
     * @throws Exception
     */
    public abstract void execute(Repository repository, Credentials credentials,
            ExecutionContext context) throws Exception;

    /**
     * Run any preparatory steps before the benchmark.
     *
     * @param repository the repository instance
     * @param credentials the credentials
     * @param context the execution context
     */
    public void beforeExecute(Repository repository, Credentials credentials,
        ExecutionContext context) throws Exception {}

    /**
     * Run any cleanup necessary after the benchmark
     *
     * @param repository the repository instance
     * @param credentials the credentials
     * @param context the execution context
     */
    public void afterExecute(Repository repository, Credentials credentials,
        ExecutionContext context) {}

    @Override
    public String toString() {
        String name = getClass().getName();
        return name.substring(name.lastIndexOf('.') + 1);
    }
}

