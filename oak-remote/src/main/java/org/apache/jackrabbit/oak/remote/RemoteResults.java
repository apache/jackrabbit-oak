/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote;

/**
 * A collection of search results.
 */
public interface RemoteResults extends Iterable<RemoteResult> {

    /**
     * If available, it returns the number of results that the query is able to
     * return. The number of results is independent of the offset and limit
     * options used when executing the query.
     *
     * @return The total number of results, or -1 if this information is not
     * available.
     */
    long getTotal();

    /**
     * The name of the columns contained in the search result.
     *
     * @return An instance of {@code Iterable}, where each element represents
     * the name of a column in the search result.
     */
    Iterable<String> getColumns();

    /**
     * The name of the selectors involved in the query.
     *
     * @return An instance of {@code Iterable}, where each element represents
     * the name of a selector involved in the query.
     */
    Iterable<String> getSelectors();

}
