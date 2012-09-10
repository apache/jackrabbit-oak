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
package org.apache.jackrabbit.oak.spi.query;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Represents an index. The index should use the data in the filter if possible
 * to speed up reading.
 */
public interface QueryIndex {

    /**
     * Estimate the cost to query with the given filter. The returned
     * cost is a value between 1 (very fast; lookup of a unique node) and the
     * estimated number of nodes to traverse.
     *
     * @param filter the filter
     * @return the estimated cost in number of read nodes
     */
    double getCost(Filter filter);

    /**
     * Start a query.
     *
     * @param filter the filter
     * @param revisionId the revision
     * @param root root state of the given revision
     * @return a cursor to iterate over the result
     */
    Cursor query(Filter filter, String revisionId, NodeState root);

    /**
     * Get the query plan for the given filter.
     *
     * @param filter the filter
     * @return the query plan
     */
    String getPlan(Filter filter);

    /**
     * Get the unique index name.
     *
     * @return the index name
     */
    String getIndexName();

}
