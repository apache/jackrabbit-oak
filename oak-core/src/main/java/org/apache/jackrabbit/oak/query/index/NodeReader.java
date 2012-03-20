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
package org.apache.jackrabbit.oak.query.index;

/**
 * A node reader. The reader should use the data in the filter if possible to
 * speed up reading.
 */
public interface NodeReader {

    /**
     * Estimate the cost to use this reader with the given filter. The returned
     * cost is a value between 1 (very fast; lookup of a unique node) and the
     * estimated number of nodes to traverse.
     *
     * @param filter the filter
     * @return the estimated cost in number of read nodes
     */
    double getCost(Filter filter);

    /**
     * Start reading nodes.
     *
     * @param filter the filter
     * @param revisionId the revision
     * @return a cursor to iterate over the result
     */
    Cursor query(Filter filter, String revisionId);

    /**
     * Get the query plan for the given reader.
     *
     * @param filter the filter
     * @return the query plan
     */
    String getPlan(Filter filter);

}
