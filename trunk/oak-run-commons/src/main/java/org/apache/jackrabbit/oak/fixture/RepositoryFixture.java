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
package org.apache.jackrabbit.oak.fixture;

import javax.jcr.Repository;

public interface RepositoryFixture {

    /**
     * Checks whether this fixture is currently available. For example
     * a database-based fixture would only be available when the underlying
     * database service is running.
     *
     * @param n size of the requested cluster
     * @return {@code true} iff the fixture is available
     */
    boolean isAvailable(int n);

    /**
     * Creates a new repository cluster with the given number of nodes.
     * The initial state of the cluster consists of just the default
     * repository content included by the implementation. The caller of
     * this method should have exclusive access to the created cluster.
     * The caller is also responsible for calling {@link #tearDownCluster()}
     * when the test cluster is no longer needed.
     *
     * @param n size of the requested cluster
     * @return nodes of the created cluster
     * @throws Exception if the cluster could not be set up
     */
    Repository[] setUpCluster(int n) throws Exception;

    /**
     * Ensures that all content changes seen by one of the given cluster
     * nodes are seen also by all the other given nodes. Used to help
     * testing features like eventual consistency where the normal APIs
     * don't make strong enough guarantees to enable writing a test case
     * without a potentially unbounded wait for changes to propagate
     * across the cluster.
     *
     * @param nodes cluster nodes to be synchronized
     */
    void syncRepositoryCluster(Repository... nodes);

    /**
     * Releases resources associated with the given repository cluster.
     * The caller of {@link #setUpCluster(int)} shall call this
     * method once the cluster is no longer needed.
     */
    void tearDownCluster();

}
